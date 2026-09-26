import { createSignal, createResource, For, Show, onMount, onCleanup } from "solid-js";

const getApiUrl = (path, service = "s4") => {
  const isServer = typeof window === "undefined";
  if (service === "functions") {
    return isServer ? (process.env.FUNCTIONS_GATEWAY_URL || "http://serverless_functions_gateway:8081") : "http://localhost:8081";
  }
  return isServer ? (process.env.GATEWAY_URL || "http://s4_gateway:8080") : "http://localhost:8080";
};

const fetchClusterData = async () => {
  try {
    const res = await fetch(getApiUrl("/api/status"));
    if (res.ok) return await res.json();
  } catch (e) {
    console.warn("Falling back to gateway defaults", e);
  }
  return {
    cluster_id: "s4-local-cluster",
    version: "1.3.0",
    status: "HEALTHY",
    shards: [
      { id: "s4-shard-0", port: 8081, status: "ACTIVE", allocated_ram_mb: 64, used_bytes: 1048576 },
      { id: "s4-shard-1", port: 8082, status: "ACTIVE", allocated_ram_mb: 64, used_bytes: 1048576 }
    ],
    tenants: [
      { id: "tenant-alpha", buckets: ["logs", "backups"], storage_bytes: 2097152 },
      { id: "tenant-beta", buckets: ["media"], storage_bytes: 5242880 }
    ],
    events: [
      { id: 101, type: "S4_EVENT_OBJECT_CREATED", tenant: "tenant-alpha", bucket: "logs", key: "app.log", ts: 1700000000 },
      { id: 102, type: "S4_EVENT_GIT_PUSH", tenant: "tenant-beta", bucket: "media", key: "main.git", ts: 1700000120 }
    ]
  };
};

const fetchPrologRules = async () => {
  try {
    const [kbRes, predRes] = await Promise.all([
      fetch(getApiUrl("/api/kb")),
      fetch(getApiUrl("/api/predicates"))
    ]);
    return {
      kbs: kbRes.ok ? (await kbRes.json()).knowledge_bases : ["tenant.pl", "bucket-default.pl"],
      predicates: predRes.ok ? (await predRes.json()).predicates : ["tenant_exists/1", "bucket_owner/2", "object_replica/3"]
    };
  } catch (e) {
    return {
      kbs: ["tenant.pl", "bucket-default.pl"],
      predicates: ["tenant_exists/1", "bucket_owner/2", "object_replica/3"]
    };
  }
};

export default function Dashboard() {
  const [clusterData] = createResource(fetchClusterData);
  const [prologData] = createResource(fetchPrologRules);
  const [activeTab, setActiveTab] = createSignal("shards");
  const [queryInput, setQueryInput] = createSignal("all_objects_replicated");
  const [queryOutput, setQueryOutput] = createSignal(null);

  // Serverless Functions & Env Vars state
  const [fnTenantId, setFnTenantId] = createSignal("tenant-alpha");
  const [fnAccessKey, setFnAccessKey] = createSignal("key_sample_123");
  const [fnSecretKey, setFnSecretKey] = createSignal("secret_sample_abc");
  const [fnPrompt, setFnPrompt] = createSignal("Summarize recent log streams");
  const [fnIsDaemon, setFnIsDaemon] = createSignal(false);
  const [fnOutput, setFnOutput] = createSignal(null);
  const [fnLoading, setFnLoading] = createSignal(false);
  const [copyStatus, setCopyStatus] = createSignal("");

  // Presigned URL & Object Upload Test State
  const [presignBucket, setPresignBucket] = createSignal("logs");
  const [presignKey, setPresignKey] = createSignal("app.log");
  const [presignExpires, setPresignExpires] = createSignal(3600);
  const [presignedUrlResult, setPresignedUrlResult] = createSignal(null);
  const [uploadContent, setUploadContent] = createSignal("Hello, S4 immutable storage write test!");
  const [uploadStatus, setUploadStatus] = createSignal(null);

  // Platform Admin & Billing State
  const [stripeSecretKey, setStripeSecretKey] = createSignal("");
  const [stripePublishableKey, setStripePublishableKey] = createSignal("");
  const [paypalClientId, setPaypalClientId] = createSignal("");
  const [paypalClientSecret, setPaypalClientSecret] = createSignal("");
  const [paymentMsg, setPaymentMsg] = createSignal("");

  const [apiUser, setApiUser] = createSignal("");
  const [apiKey, setApiKey] = createSignal("");
  const [userName, setUserName] = createSignal("");
  const [clientIp, setClientIp] = createSignal("");
  const [domain, setDomain] = createSignal("");
  const [ociIp, setOciIp] = createSignal("");
  const [sandbox, setSandbox] = createSignal(false);
  const [dnsMsg, setDnsMsg] = createSignal("");

  const [mockMsg, setMockMsg] = createSignal("");
  const [ipTable, setIpTable] = createSignal([]);

  onMount(async () => {
    try {
      const res = await fetch(window.location.origin + '/api/admin/namecheap/config');
      if (res.ok) {
        const cfg = await res.json();
        setApiUser(cfg.apiUser || '');
        setApiKey(cfg.apiKey || '');
        setUserName(cfg.userName || '');
        setClientIp(cfg.clientIp || '');
        setDomain(cfg.domain || '');
        setOciIp(cfg.ociIp || '');
        setSandbox(!!cfg.sandbox);
      }
    } catch (e) {}

    try {
      const payRes = await fetch(window.location.origin + '/api/admin/payment/config');
      if (payRes.ok) {
        const payCfg = await payRes.json();
        setStripeSecretKey(payCfg.stripeSecretKey || '');
        setStripePublishableKey(payCfg.stripePublishableKey || '');
        setPaypalClientId(payCfg.paypalClientId || '');
        setPaypalClientSecret(payCfg.paypalClientSecret || '');
      }
    } catch (e) {}

    fetchIPs();
    const ipInterval = setInterval(fetchIPs, 5000);
    onCleanup(() => clearInterval(ipInterval));
  });

  const savePaymentConfig = async () => {
    const data = {
      stripeSecretKey: stripeSecretKey(),
      stripePublishableKey: stripePublishableKey(),
      paypalClientId: paypalClientId(),
      paypalClientSecret: paypalClientSecret()
    };
    try {
      const res = await fetch(window.location.origin + '/api/admin/payment/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(data)
      });
      const d = await res.json();
      setPaymentMsg(d.message || d.error || "Saved successfully");
    } catch (e) {
      setPaymentMsg("Failed to save payment config.");
    }
  };

  const saveNamecheapConfig = async () => {
    const data = {
      apiUser: apiUser(),
      apiKey: apiKey(),
      userName: userName(),
      clientIp: clientIp(),
      domain: domain(),
      ociIp: ociIp(),
      sandbox: sandbox()
    };
    try {
      const res = await fetch(window.location.origin + '/api/admin/namecheap/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(data)
      });
      const d = await res.json();
      setDnsMsg(d.message || d.error || "Config saved successfully");
    } catch (e) {
      setDnsMsg("Failed to save DNS config.");
    }
  };

  const runNamecheapDNS = async () => {
    setDnsMsg("Configuring DNS records via Namecheap API...");
    try {
      const res = await fetch(window.location.origin + '/api/admin/namecheap/configure-dns', { method: 'POST' });
      const d = await res.json();
      setDnsMsg(d.message || d.error || "DNS update completed");
    } catch (e) {
      setDnsMsg("DNS update network error.");
    }
  };

  const sendMockEmail = async () => {
    setMockMsg("Dispatching test email...");
    try {
      const res = await fetch("/api/admin/send-mock-email", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({})
      });
      const data = await res.json();
      if (res.ok) {
        setMockMsg(`Self-test email sent to admin@rdp.local! Code: ${data.code}`);
      } else {
        setMockMsg("Error: " + (data.error || "Failed to send"));
      }
    } catch (e) {
      setMockMsg("Network error: " + e.message);
    }
  };

  const fetchIPs = async () => {
    try {
      const res = await fetch(window.location.origin + '/api/admin/ips');
      if (res.ok) {
        const data = await res.json();
        setIpTable(data);
      }
    } catch (e) {}
  };

  const blockIP = async (ip) => {
    try {
      await fetch(window.location.origin + '/api/admin/block-ip', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ ip })
      });
      fetchIPs();
    } catch (e) {}
  };

  const runPrologQuery = async () => {
    try {
      const res = await fetch(getApiUrl("/api/query"));
      if (res.ok) {
        setQueryOutput(await res.json());
        return;
      }
    } catch (e) {}
    setQueryOutput({
      query: queryInput(),
      result: true,
      matches: [{ bucket: "logs", count: 42 }]
    });
  };

  const generatePresignedUrl = async () => {
    try {
      const res = await fetch(getApiUrl(`/api/presign?bucket=${presignBucket()}&key=${presignKey()}&expires=${presignExpires()}`));
      if (res.ok) {
        const data = await res.json();
        setPresignedUrlResult(data);
      } else {
        setPresignedUrlResult({ error: `Failed to generate: ${res.statusText}` });
      }
    } catch (e) {
      setPresignedUrlResult({ error: e.toString() });
    }
  };

  const testUploadWithPresignedUrl = async () => {
    const urlObj = presignedUrlResult();
    if (!urlObj || !urlObj.url) {
      setUploadStatus({ success: false, message: "Generate a presigned URL first." });
      return;
    }

    try {
      const res = await fetch(urlObj.url, {
        method: "PUT",
        headers: { "Content-Type": "application/octet-stream" },
        body: uploadContent()
      });
      if (res.ok) {
        setUploadStatus({ success: true, message: "Upload verified successfully via Presigned URL!" });
      } else {
        setUploadStatus({ success: false, message: `Upload failed: ${res.status} ${res.statusText}` });
      }
    } catch (e) {
      setUploadStatus({ success: false, message: e.toString() });
    }
  };

  const invokeServerlessTask = async () => {
    setFnLoading(true);
    try {
      const res = await fetch(getApiUrl("/invoke", "functions"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tenantId: fnTenantId(),
          accessKeyId: fnAccessKey(),
          secretAccessKey: fnSecretKey(),
          prompt: fnPrompt(),
          isDaemon: fnIsDaemon()
        })
      });
      if (res.ok) {
        setFnOutput(await res.json());
      } else {
        setFnOutput({ success: false, error: `Gateway error: ${res.statusText}` });
      }
    } catch (e) {
      setFnOutput({ success: false, error: e.toString() });
    } finally {
      setFnLoading(false);
    }
  };

  const generateEnvConfigSnippet = () => {
    const s4Url = "http://localhost:8080";
    const functionsUrl = "http://localhost:8081";
    return `export S4_GATEWAY_URL="${s4Url}"\nexport S4_FUNCTIONS_GATEWAY_URL="${functionsUrl}"\nexport S4_TENANT_ID="${fnTenantId()}"\nexport S4_ACCESS_KEY_ID="${fnAccessKey()}"\nexport S4_SECRET_ACCESS_KEY="${fnSecretKey()}"`;
  };

  const copyEnvToClipboard = async () => {
    try {
      await navigator.clipboard.writeText(generateEnvConfigSnippet());
      setCopyStatus("Copied env variables to clipboard!");
      setTimeout(() => setCopyStatus(""), 3000);
    } catch (e) {
      setCopyStatus("Failed to copy clipboard.");
    }
  };

  return (
    <div style="display: flex; flex-direction: column; min-height: 100vh; background-color: #0f172a;">
      {/* Header Navigation Bar */}
      <header style="background-color: #1e293b; border-bottom: 1px solid #334155; padding: 16px 24px; display: flex; justify-content: space-between; align-items: center;">
        <div style="display: flex; align-items: center; gap: 16px;">
          <div style="background-color: #2563eb; color: #ffffff; font-weight: bold; padding: 6px 12px; border-radius: 6px; font-size: 14px;">S4 UI</div>
          <div>
            <h1 style="margin: 0; font-size: 18px; font-weight: 600; color: #f8fafc;">Provider Storage Manager</h1>
            <span style="font-size: 12px; color: #94a3b8;">Cluster: {clusterData()?.cluster_id || 's4-cluster'} | Engine v{clusterData()?.version || '1.3.0'}</span>
          </div>
        </div>
        <div style="display: flex; align-items: center; gap: 12px;">
          <span style="display: inline-block; width: 10px; height: 10px; border-radius: 50%; background-color: #10b981;"></span>
          <span style="font-size: 13px; font-weight: 500; color: #34d399;">{clusterData()?.status || 'HEALTHY'}</span>
        </div>
      </header>

      {/* Main Workspace Layout */}
      <div style="display: flex; flex: 1;">
        {/* Sidebar Navigation */}
        <aside style="width: 240px; background-color: #1e293b; border-right: 1px solid #334155; padding: 20px 12px;">
          <nav style="display: flex; flex-direction: column; gap: 6px;">
            <button
              onClick={() => setActiveTab("shards")}
              style={`text-align: left; padding: 10px 14px; border-radius: 6px; font-size: 14px; cursor: pointer; border: none; font-weight: 500; ${activeTab() === 'shards' ? 'background-color: #3b82f6; color: #ffffff;' : 'background-color: transparent; color: #94a3b8;'}`}
            >
              RAM Shards
            </button>
            <button
              onClick={() => setActiveTab("tenants")}
              style={`text-align: left; padding: 10px 14px; border-radius: 6px; font-size: 14px; cursor: pointer; border: none; font-weight: 500; ${activeTab() === 'tenants' ? 'background-color: #3b82f6; color: #ffffff;' : 'background-color: transparent; color: #94a3b8;'}`}
            >
              Tenants & Buckets
            </button>
            <button
              onClick={() => setActiveTab("storage")}
              style={`text-align: left; padding: 10px 14px; border-radius: 6px; font-size: 14px; cursor: pointer; border: none; font-weight: 500; ${activeTab() === 'storage' ? 'background-color: #3b82f6; color: #ffffff;' : 'background-color: transparent; color: #94a3b8;'}`}
            >
              Object Storage & Presign
            </button>
            <button
              onClick={() => setActiveTab("serverless")}
              style={`text-align: left; padding: 10px 14px; border-radius: 6px; font-size: 14px; cursor: pointer; border: none; font-weight: 500; ${activeTab() === 'serverless' ? 'background-color: #3b82f6; color: #ffffff;' : 'background-color: transparent; color: #94a3b8;'}`}
            >
              Serverless Functions
            </button>
            <button
              onClick={() => setActiveTab("prolog")}
              style={`text-align: left; padding: 10px 14px; border-radius: 6px; font-size: 14px; cursor: pointer; border: none; font-weight: 500; ${activeTab() === 'prolog' ? 'background-color: #3b82f6; color: #ffffff;' : 'background-color: transparent; color: #94a3b8;'}`}
            >
              Prolog Logic Engine
            </button>
            <button
              onClick={() => setActiveTab("events")}
              style={`text-align: left; padding: 10px 14px; border-radius: 6px; font-size: 14px; cursor: pointer; border: none; font-weight: 500; ${activeTab() === 'events' ? 'background-color: #3b82f6; color: #ffffff;' : 'background-color: transparent; color: #94a3b8;'}`}
            >
              WAL Event Stream
            </button>
            <button
              onClick={() => setActiveTab("admin")}
              style={`text-align: left; padding: 10px 14px; border-radius: 6px; font-size: 14px; cursor: pointer; border: none; font-weight: 500; ${activeTab() === 'admin' ? 'background-color: #3b82f6; color: #ffffff;' : 'background-color: transparent; color: #94a3b8;'}`}
            >
              Platform Admin & Billing
            </button>
          </nav>
        </aside>

        {/* Content Panel */}
        <main style="flex: 1; padding: 28px; max-width: 1200px; overflow-y: auto;">
          {/* View: Shards */}
          <Show when={activeTab() === "shards"}>
            <section style="margin-bottom: 24px;">
              <h2 style="font-size: 20px; font-weight: 600; margin: 0 0 16px 0; color: #f8fafc;">Isolated Memory Shard Nodes</h2>
              <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 16px;">
                <For each={clusterData()?.shards || []}>
                  {shard => (
                    <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 18px;">
                      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                        <span style="font-weight: 600; font-size: 16px; color: #38bdf8;">{shard.id}</span>
                        <span style="font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 4px; background-color: #064e3b; color: #34d399;">{shard.status}</span>
                      </div>
                      <div style="font-size: 13px; color: #94a3b8; display: flex; flex-direction: column; gap: 6px;">
                        <div>Host Port: <span style="color: #f3f4f6; font-family: monospace;">{shard.port}</span></div>
                        <div>Allocated RAM: <span style="color: #f3f4f6;">{shard.allocated_ram_mb} MB</span></div>
                        <div>Bytes Written: <span style="color: #f3f4f6; font-family: monospace;">{shard.used_bytes} bytes</span></div>
                      </div>
                    </div>
                  )}
                </For>
              </div>
            </section>
          </Show>

          {/* View: Tenants */}
          <Show when={activeTab() === "tenants"}>
            <section>
              <h2 style="font-size: 20px; font-weight: 600; margin: 0 0 16px 0; color: #f8fafc;">Tenant Allocations & Bucket State</h2>
              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; overflow: hidden;">
                <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 14px;">
                  <thead>
                    <tr style="background-color: #0f172a; border-bottom: 1px solid #334155; color: #94a3b8;">
                      <th style="padding: 12px 16px;">Tenant ID</th>
                      <th style="padding: 12px 16px;">Provisioned Buckets</th>
                      <th style="padding: 12px 16px;">Storage Used</th>
                    </tr>
                  </thead>
                  <tbody>
                    <For each={clusterData()?.tenants || []}>
                      {tenant => (
                        <tr style="border-bottom: 1px solid #334155;">
                          <td style="padding: 12px 16px; font-weight: 500; font-family: monospace; color: #f8fafc;">{tenant.id}</td>
                          <td style="padding: 12px 16px;">
                            <div style="display: flex; gap: 6px; flex-wrap: wrap;">
                              <For each={tenant.buckets}>
                                {b => <span style="background-color: #334155; color: #e2e8f0; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-family: monospace;">{b}</span>}
                              </For>
                            </div>
                          </td>
                          <td style="padding: 12px 16px; color: #cbd5e1; font-family: monospace;">{(tenant.storage_bytes / 1024 / 1024).toFixed(2)} MB</td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </div>
            </section>
          </Show>

          {/* View: Object Storage & Presign */}
          <Show when={activeTab() === "storage"}>
            <section style="display: flex; flex-direction: column; gap: 20px;">
              <h2 style="font-size: 20px; font-weight: 600; margin: 0; color: #f8fafc;">Presigned URLs & Object Upload Testing</h2>

              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 20px; display: flex; flex-direction: column; gap: 14px;">
                <h3 style="margin: 0; font-size: 14px; color: #38bdf8;">Generate Presigned URL</h3>
                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px;">
                  <div>
                    <label style="display: block; font-size: 12px; color: #94a3b8; margin-bottom: 4px;">Bucket</label>
                    <input
                      type="text"
                      value={presignBucket()}
                      onInput={e => setPresignBucket(e.currentTarget.value)}
                      style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                    />
                  </div>
                  <div>
                    <label style="display: block; font-size: 12px; color: #94a3b8; margin-bottom: 4px;">Object Key</label>
                    <input
                      type="text"
                      value={presignKey()}
                      onInput={e => setPresignKey(e.currentTarget.value)}
                      style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                    />
                  </div>
                  <div>
                    <label style="display: block; font-size: 12px; color: #94a3b8; margin-bottom: 4px;">Expiration (Seconds)</label>
                    <input
                      type="number"
                      value={presignExpires()}
                      onInput={e => setPresignExpires(Number(e.currentTarget.value))}
                      style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                    />
                  </div>
                </div>
                <button
                  onClick={generatePresignedUrl}
                  style="background-color: #2563eb; color: #ffffff; border: none; border-radius: 6px; padding: 10px 16px; font-size: 14px; font-weight: 500; cursor: pointer; align-self: flex-start;"
                >
                  Generate URL
                </button>
                <Show when={presignedUrlResult()}>
                  <pre style="margin: 0; background-color: #0f172a; padding: 12px; border-radius: 6px; color: #34d399; font-family: monospace; font-size: 12px; overflow-x: auto;">
                    {JSON.stringify(presignedUrlResult(), null, 2)}
                  </pre>
                </Show>
              </div>

              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 20px; display: flex; flex-direction: column; gap: 14px;">
                <h3 style="margin: 0; font-size: 14px; color: #38bdf8;">Test Upload via Presigned URL</h3>
                <div>
                  <label style="display: block; font-size: 12px; color: #94a3b8; margin-bottom: 4px;">Payload Content / Stream Data</label>
                  <textarea
                    value={uploadContent()}
                    onInput={e => setUploadContent(e.currentTarget.value)}
                    rows="2"
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px; resize: vertical;"
                  ></textarea>
                </div>
                <button
                  onClick={testUploadWithPresignedUrl}
                  style="background-color: #059669; color: #ffffff; border: none; border-radius: 6px; padding: 10px 16px; font-size: 14px; font-weight: 500; cursor: pointer; align-self: flex-start;"
                >
                  Upload Object
                </button>
                <Show when={uploadStatus()}>
                  <div style={`padding: 10px; border-radius: 6px; font-size: 13px; font-family: monospace; background-color: ${uploadStatus().success ? '#064e3b' : '#7f1d1d'}; color: ${uploadStatus().success ? '#34d399' : '#fca5a5'};`}>
                    {uploadStatus().message}
                  </div>
                </Show>
              </div>
            </section>
          </Show>

          {/* View: Serverless Functions */}
          <Show when={activeTab() === "serverless"}>
            <section style="display: flex; flex-direction: column; gap: 20px;">
              <h2 style="font-size: 20px; font-weight: 600; margin: 0; color: #f8fafc;">Serverless Functions & Isolate Daemons</h2>
              
              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 16px; display: flex; flex-direction: column; gap: 10px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                  <h3 style="margin: 0; font-size: 14px; color: #38bdf8;">Tenant Application Env Variables</h3>
                  <button
                    onClick={copyEnvToClipboard}
                    style="background-color: #059669; color: #ffffff; border: none; border-radius: 6px; padding: 6px 12px; font-size: 12px; font-weight: 500; cursor: pointer;"
                  >
                    Copy Env to Clipboard
                  </button>
                </div>
                <pre style="margin: 0; background-color: #0f172a; padding: 10px; border-radius: 6px; color: #cbd5e1; font-family: monospace; font-size: 11px; overflow-x: auto;">
                  {generateEnvConfigSnippet()}
                </pre>
                <Show when={copyStatus()}>
                  <span style="font-size: 12px; color: #34d399;">{copyStatus()}</span>
                </Show>
              </div>

              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 20px; display: flex; flex-direction: column; gap: 14px;">
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                  <div>
                    <label style="display: block; font-size: 12px; color: #94a3b8; margin-bottom: 4px;">Tenant ID</label>
                    <input
                      type="text"
                      value={fnTenantId()}
                      onInput={e => setFnTenantId(e.currentTarget.value)}
                      style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                    />
                  </div>
                  <div>
                    <label style="display: block; font-size: 12px; color: #94a3b8; margin-bottom: 4px;">Access Key ID</label>
                    <input
                      type="text"
                      value={fnAccessKey()}
                      onInput={e => setFnAccessKey(e.currentTarget.value)}
                      style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                    />
                  </div>
                </div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                  <div>
                    <label style="display: block; font-size: 12px; color: #94a3b8; margin-bottom: 4px;">Secret Access Key</label>
                    <input
                      type="password"
                      value={fnSecretKey()}
                      onInput={e => setFnSecretKey(e.currentTarget.value)}
                      style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                    />
                  </div>
                  <div>
                    <label style="display: block; font-size: 12px; color: #94a3b8; margin-bottom: 4px;">Execution Mode</label>
                    <label style="display: flex; align-items: center; gap: 8px; margin-top: 10px; color: #f8fafc; font-size: 13px; cursor: pointer;">
                      <input
                        type="checkbox"
                        checked={fnIsDaemon()}
                        onChange={e => setFnIsDaemon(e.currentTarget.checked)}
                      />
                      Start as Serverless Server (Daemon Isolate)
                    </label>
                  </div>
                </div>
                <div>
                  <label style="display: block; font-size: 12px; color: #94a3b8; margin-bottom: 4px;">Plain Text Task Prompt</label>
                  <textarea
                    value={fnPrompt()}
                    onInput={e => setFnPrompt(e.currentTarget.value)}
                    rows="3"
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px; resize: vertical;"
                  ></textarea>
                </div>
                <button
                  onClick={invokeServerlessTask}
                  disabled={fnLoading()}
                  style="background-color: #2563eb; color: #ffffff; border: none; border-radius: 6px; padding: 10px 16px; font-size: 14px; font-weight: 500; cursor: pointer; align-self: flex-start;"
                >
                  {fnLoading() ? "Executing..." : "Invoke Task / Start Server"}
                </button>
                <Show when={fnOutput()}>
                  <pre style="margin: 0; background-color: #0f172a; padding: 12px; border-radius: 6px; color: #34d399; font-family: monospace; font-size: 12px; overflow-x: auto;">
                    {JSON.stringify(fnOutput(), null, 2)}
                  </pre>
                </Show>
              </div>
            </section>
          </Show>

          {/* View: Prolog */}
          <Show when={activeTab() === "prolog"}>
            <section style="display: flex; flex-direction: column; gap: 20px;">
              <h2 style="font-size: 20px; font-weight: 600; margin: 0; color: #f8fafc;">Embedded Prolog KB & Reasoning</h2>
              
              <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
                <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 16px;">
                  <h3 style="margin: 0 0 10px 0; font-size: 14px; color: #38bdf8;">Knowledge Bases</h3>
                  <ul style="margin: 0; padding-left: 20px; font-family: monospace; font-size: 13px; color: #cbd5e1;">
                    <For each={prologData()?.kbs || []}>
                      {kb => <li>{kb}</li>}
                    </For>
                  </ul>
                </div>
                <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 16px;">
                  <h3 style="margin: 0 0 10px 0; font-size: 14px; color: #38bdf8;">Registered Predicates</h3>
                  <ul style="margin: 0; padding-left: 20px; font-family: monospace; font-size: 13px; color: #cbd5e1;">
                    <For each={prologData()?.predicates || []}>
                      {pred => <li>{pred}</li>}
                    </For>
                  </ul>
                </div>
              </div>

              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 16px;">
                <h3 style="margin: 0 0 12px 0; font-size: 14px; color: #38bdf8;">Query Prolog Engine</h3>
                <div style="display: flex; gap: 8px; margin-bottom: 12px;">
                  <input
                    type="text"
                    value={queryInput()}
                    onInput={e => setQueryInput(e.currentTarget.value)}
                    style="flex: 1; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-family: monospace; font-size: 13px;"
                  />
                  <button
                    onClick={runPrologQuery}
                    style="background-color: #2563eb; color: #ffffff; border: none; border-radius: 6px; padding: 8px 16px; font-size: 13px; font-weight: 500; cursor: pointer;"
                  >
                    Run Query
                  </button>
                </div>
                <Show when={queryOutput()}>
                  <pre style="margin: 0; background-color: #0f172a; padding: 12px; border-radius: 6px; color: #34d399; font-family: monospace; font-size: 12px; overflow-x: auto;">
                    {JSON.stringify(queryOutput(), null, 2)}
                  </pre>
                </Show>
              </div>
            </section>
          </Show>

          {/* View: Events */}
          <Show when={activeTab() === "events"}>
            <section>
              <h2 style="font-size: 20px; font-weight: 600; margin: 0 0 16px 0; color: #f8fafc;">WAL Event Stream</h2>
              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; overflow: hidden;">
                <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 14px;">
                  <thead>
                    <tr style="background-color: #0f172a; border-bottom: 1px solid #334155; color: #94a3b8;">
                      <th style="padding: 12px 16px;">Event ID</th>
                      <th style="padding: 12px 16px;">Type</th>
                      <th style="padding: 12px 16px;">Tenant / Bucket</th>
                      <th style="padding: 12px 16px;">Key</th>
                      <th style="padding: 12px 16px;">Timestamp</th>
                    </tr>
                  </thead>
                  <tbody>
                    <For each={clusterData()?.events || []}>
                      {evt => (
                        <tr style="border-bottom: 1px solid #334155;">
                          <td style="padding: 12px 16px; font-family: monospace; color: #f8fafc;">#{evt.id}</td>
                          <td style="padding: 12px 16px; color: #38bdf8; font-family: monospace;">{evt.type}</td>
                          <td style="padding: 12px 16px; color: #cbd5e1;">{evt.tenant} / {evt.bucket}</td>
                          <td style="padding: 12px 16px; font-family: monospace; color: #f8fafc;">{evt.key}</td>
                          <td style="padding: 12px 16px; color: #94a3b8; font-family: monospace;">{evt.ts}</td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </div>
            </section>
          </Show>

          {/* View: Platform Admin & Billing */}
          <Show when={activeTab() === "admin"}>
            <section style="display: flex; flex-direction: column; gap: 20px;">
              <h2 style="font-size: 20px; font-weight: 600; margin: 0; color: #f8fafc;">Platform Admin & IP Traffic Tracking</h2>

              {/* SMTP Self-Test Dispatcher */}
              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 20px;">
                <h3 style="margin: 0 0 10px 0; font-size: 14px; color: #38bdf8; border-bottom: 1px solid #334155; padding-bottom: 8px;">SMTP Self-Test Dispatcher</h3>
                <p style="font-size: 12px; color: #94a3b8; margin-bottom: 12px;">Dispatches a self-to-self test verification code directly to <b>admin@rdp.local</b> in Mailpit.</p>
                <button onClick={sendMockEmail} style="background-color: #2563eb; color: #ffffff; border: none; border-radius: 6px; padding: 10px 16px; font-size: 14px; font-weight: 500; cursor: pointer; width: 100%;">Dispatch Self-Test Email</button>
                <div id="mockMsg" style="font-size: 12px; margin-top: 10px; font-weight: bold; color: #34d399;">{mockMsg()}</div>
              </div>

              {/* Payment Gateway Credentials */}
              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 20px;">
                <h3 style="margin: 0 0 16px 0; font-size: 14px; color: #38bdf8; border-bottom: 1px solid #334155; padding-bottom: 8px;">Payment Gateway Credentials (Stripe & PayPal)</h3>
                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin-bottom: 12px;">
                  <input
                    type="password"
                    placeholder="Stripe Secret Key (sk_...)"
                    value={stripeSecretKey()}
                    onInput={e => setStripeSecretKey(e.currentTarget.value)}
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                  />
                  <input
                    type="text"
                    placeholder="Stripe Publishable Key (pk_...)"
                    value={stripePublishableKey()}
                    onInput={e => setStripePublishableKey(e.currentTarget.value)}
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                  />
                  <input
                    type="text"
                    placeholder="PayPal Client ID"
                    value={paypalClientId()}
                    onInput={e => setPaypalClientId(e.currentTarget.value)}
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                  />
                  <input
                    type="password"
                    placeholder="PayPal Client Secret"
                    value={paypalClientSecret()}
                    onInput={e => setPaypalClientSecret(e.currentTarget.value)}
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                  />
                </div>
                <button onClick={savePaymentConfig} style="background-color: #2563eb; color: #ffffff; border: none; border-radius: 6px; padding: 10px 16px; font-size: 14px; font-weight: 500; cursor: pointer;">Save Payment Config</button>
                <p style="font-size: 12px; margin-top: 8px; font-weight: bold; color: #34d399;">{paymentMsg()}</p>
              </div>

              {/* Namecheap API Configuration */}
              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 20px;">
                <h3 style="margin: 0 0 16px 0; font-size: 14px; color: #38bdf8; border-bottom: 1px solid #334155; padding-bottom: 8px;">Namecheap API Configuration</h3>
                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin-bottom: 12px;">
                  <input
                    type="text"
                    placeholder="API User"
                    value={apiUser()}
                    onInput={e => setApiUser(e.currentTarget.value)}
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                  />
                  <input
                    type="password"
                    placeholder="API Key"
                    value={apiKey()}
                    onInput={e => setApiKey(e.currentTarget.value)}
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                  />
                  <input
                    type="text"
                    placeholder="Username"
                    value={userName()}
                    onInput={e => setUserName(e.currentTarget.value)}
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                  />
                  <input
                    type="text"
                    placeholder="Client IP (Whitelist)"
                    value={clientIp()}
                    onInput={e => setClientIp(e.currentTarget.value)}
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                  />
                  <input
                    type="text"
                    placeholder="Domain (e.g. yourdomain.com)"
                    value={domain()}
                    onInput={e => setDomain(e.currentTarget.value)}
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                  />
                  <input
                    type="text"
                    placeholder="OCI Public IP"
                    value={ociIp()}
                    onInput={e => setOciIp(e.currentTarget.value)}
                    style="width: 100%; background-color: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 8px 12px; color: #f8fafc; font-size: 13px;"
                  />
                </div>
                <label style="font-size: 12px; color: #cbd5e1; display: flex; align-items: center; gap: 6px; cursor: pointer; margin-bottom: 14px;">
                  <input
                    type="checkbox"
                    checked={sandbox()}
                    onChange={e => setSandbox(e.currentTarget.checked)}
                    style="width: auto;"
                  /> Use Sandbox API
                </label>
                <div style="display: flex; gap: 10px;">
                  <button onClick={saveNamecheapConfig} style="background-color: #2563eb; color: #ffffff; border: none; border-radius: 6px; padding: 10px 16px; font-size: 14px; font-weight: 500; cursor: pointer;">Save Config</button>
                  <button onClick={runNamecheapDNS} style="background-color: #334155; color: #ffffff; border: none; border-radius: 6px; padding: 10px 16px; font-size: 14px; font-weight: 500; cursor: pointer;">Update DNS via API</button>
                </div>
                <p style="font-size: 12px; margin-top: 8px; font-weight: bold; color: #34d399;">{dnsMsg()}</p>
              </div>

              {/* Connected Client IP Requests */}
              <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 20px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; border-bottom: 1px solid #334155; padding-bottom: 8px;">
                  <h3 style="margin: 0; font-size: 14px; color: #38bdf8;">Connected Client IP Requests</h3>
                  <button onClick={fetchIPs} style="background-color: #334155; color: #ffffff; border: none; border-radius: 6px; padding: 6px 12px; font-size: 12px; cursor: pointer;">Refresh IP Table</button>
                </div>
                <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 13px; color: #cbd5e1;">
                  <thead>
                    <tr style="border-bottom: 1px solid #334155; color: #94a3b8; font-size: 11px; text-transform: uppercase;">
                      <th style="padding: 10px;">IP Address</th>
                      <th style="padding: 10px;">Requests</th>
                      <th style="padding: 10px;">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    <For each={ipTable()}>
                      {item => (
                        <tr style="border-bottom: 1px solid #334155;">
                          <td style="padding: 10px; font-family: monospace; color: #f8fafc;">{item.ip}</td>
                          <td style="padding: 10px; font-family: monospace;">{item.count}</td>
                          <td style="padding: 10px;">
                            <button onClick={() => blockIP(item.ip)} style="background-color: #7f1d1d; color: #fca5a5; border: none; border-radius: 4px; padding: 4px 8px; font-size: 11px; cursor: pointer;">Block</button>
                          </td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </div>
            </section>
          </Show>
        </main>
      </div>
    </div>
  );
}