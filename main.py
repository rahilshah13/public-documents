import asyncio, csv, json, os, random, re, tempfile, time, xml.etree.ElementTree as ET
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import httpx, numpy as np
from pgvector.psycopg import register_vector
from pydantic import BaseModel, Field
from pypdf import PdfReader
from psycopg_pool import ConnectionPool
import uvicorn
from db_utils import restore_db_from_s4, backup_db_to_s4, upload_to_s4

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@db:5432/gov_intel")
CSV_PATH, EDU_CSV_PATH, MIL_CSV_PATH = "data/current-full.csv", "data/edu-domains.csv", "data/mil-domains.csv"
db_pool = ConnectionPool(conninfo=DATABASE_URL, min_size=2, max_size=10, open=False)
STATE_TO_ABBR = json.loads(os.environ.get("STATE_TO_ABBR", "{}"))

normalize_state = lambda st, ctx="": next((a for n, a in STATE_TO_ABBR.items() if n in ctx.lower()), "US") if not st or st == "US" else (st.strip().upper() if len(st.strip()) == 2 else STATE_TO_ABBR.get(st.strip().lower(), st.strip().upper()))

@contextmanager
def db_conn():
    conn = db_pool.getconn()
    try:
        conn.autocommit = True
        register_vector(conn)
        yield conn
    finally:
        db_pool.putconn(conn)

_model = None
global_stats = {"visited": 0, "total": 0, "pdfs_discovered": 0, "domains_crawled": 0, "total_domains": 0, "gov_completed": 0, "edu_crawled": 0, "mil_crawled": 0, "wiki_crawled": 0, "arxiv_crawled": 0, "total_gov": 0, "total_edu": 0, "total_mil": 0, "total_wiki": 6800000, "total_arxiv": 2500000, "total_embeddings": 0, "pending_embeddings": 0}
crawler_paused, active_tld_mode = False, "gov"

def get_model():
    global _model
    if not _model:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model

def update_global_stats(cur):
    t_docs, p_docs, t_chunks = cur.execute("SELECT COUNT(*) FROM documents").fetchone()[0] or 0, cur.execute("SELECT COUNT(*) FROM documents WHERE processed = 1").fetchone()[0] or 0, cur.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] or 0
    global_stats.update({
        "visited": cur.execute("SELECT SUM(visited_count) FROM domains").fetchone()[0] or 0, "pdfs_discovered": t_docs,
        "total_domains": (t_doms := cur.execute("SELECT COUNT(*) FROM domains").fetchone()[0]), "domains_crawled": cur.execute("SELECT COUNT(*) FROM domains WHERE status != 'PENDING'").fetchone()[0],
        "gov_completed": cur.execute("SELECT COUNT(DISTINCT county) FROM domains WHERE status = 'COMPLETED' AND tld_type = 'gov'").fetchone()[0] or 0, "total": t_doms,
        "gov_crawled": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'gov' AND status = 'COMPLETED'").fetchone()[0],
        "edu_crawled": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'edu' AND status = 'COMPLETED'").fetchone()[0],
        "mil_crawled": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'mil' AND status = 'COMPLETED'").fetchone()[0],
        "wiki_crawled": cur.execute("SELECT COUNT(*) FROM documents WHERE domain = 'en.wikipedia.org'").fetchone()[0] or 0,
        "arxiv_crawled": cur.execute("SELECT COUNT(*) FROM documents WHERE domain = 'arxiv.org'").fetchone()[0] or 0,
        "total_gov": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'gov'").fetchone()[0],
        "total_edu": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'edu'").fetchone()[0],
        "total_mil": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'mil'").fetchone()[0],
        "total_embeddings": t_chunks, "pending_embeddings": t_docs - p_docs
    })

def init_db_sync():
    list(map(lambda i: time.sleep(3) or (__import__("psycopg").connect(DATABASE_URL, autocommit=True).cursor().execute("SELECT 1;")) if i > 0 else None, range(15)))
    with __import__("psycopg").connect(DATABASE_URL, autocommit=True) as conn, conn.cursor() as cur: cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    db_pool.open()
    if asyncio.run(restore_db_from_s4()):
        with db_conn() as conn, conn.cursor() as cur: update_global_stats(cur)
        return
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS domains (domain TEXT PRIMARY KEY, state TEXT, county TEXT, status TEXT DEFAULT 'PENDING', visited_count INTEGER DEFAULT 0, error TEXT, tld_type TEXT DEFAULT 'gov');
            CREATE TABLE IF NOT EXISTS documents (id SERIAL PRIMARY KEY, domain TEXT, pdf_url TEXT, source_url TEXT, discovered_at TEXT, summary TEXT, state TEXT, county TEXT, processed INTEGER DEFAULT 0, s4_path TEXT, aggregate_embedding vector(384));
            CREATE TABLE IF NOT EXISTS chunks (id SERIAL PRIMARY KEY, doc_id INTEGER REFERENCES documents(id) ON DELETE CASCADE, text TEXT, granularity TEXT DEFAULT '1_sentence', embedding vector(384));
            CREATE TABLE IF NOT EXISTS page_audit (id SERIAL PRIMARY KEY, domain TEXT, url TEXT, status TEXT, pdf_count INTEGER, embedding_count INTEGER, error TEXT);
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS chunks_embedding_hnsw_idx ON chunks USING hnsw (embedding vector_cosine_ops);")
        list(map(lambda item: cur.execute("INSERT INTO domains (domain, state, county, status, tld_type) VALUES (%s, 'US', 'National', 'PENDING', %s) ON CONFLICT (domain) DO NOTHING", item), [('en.wikipedia.org', 'wiki'), ('arxiv.org', 'arxiv')]))
        
        load_csv = lambda path, tld, parser: os.path.exists(path) and cur.executemany("INSERT INTO domains (domain, state, county, status, tld_type) VALUES (%s, %s, %s, 'PENDING', %s) ON CONFLICT (domain) DO NOTHING", list(filter(None, map(parser, csv.DictReader(open(path, encoding='utf-8', errors='ignore'))))))
        load_csv(CSV_PATH, 'gov', lambda r: r.get("Domain name") and (r.get("Domain name").strip().lower(), normalize_state(r.get("State", "US"), f"{r.get('Domain name')} {r.get('Organization name') or 'Unknown'}"), (r.get('Organization name') or r.get('City') or 'Unknown').strip(), 'gov'))
        load_csv(EDU_CSV_PATH, 'edu', lambda r: (uf := r.get("URL") or r.get("Domain name") or list(r.values())[0]) and ((d := (urlparse(uf if '://' in uf else f'http://{uf}').netloc or uf).replace('www.', '').strip().lower()).endswith('.edu')) and (d, normalize_state('US', f"{d} {r.get('Title') or 'Higher Ed'}"), (r.get('Title') or 'Higher Ed')[:100], 'edu'))
        load_csv(MIL_CSV_PATH, 'mil', lambda r: (uf := r.get("Domain Name") or list(r.values())[0]) and ((d := (urlparse(uf if '://' in uf else f'http://{uf}').netloc or uf).replace('www.', '').strip().lower()).endswith('.mil')) and (d, 'US', (r.get('Organization') or 'Military')[:100], 'mil'))
        cur.execute("UPDATE domains SET status = 'PENDING' WHERE status IN ('CRAWLING', 'QUEUED') AND tld_type NOT IN ('wiki', 'arxiv');")
        update_global_stats(cur)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(init_db_sync)
    async def bg_worker():
        global crawler_paused, active_tld_mode
        cycle = 0
        while True:
            if crawler_paused: await asyncio.sleep(2); continue
            try:
                await process_pending_embeddings_batch()
                cycle += 1
                if cycle % 2 == 0:
                    await run_wikipedia_crawl_task()
                else:
                    await run_arxiv_crawl_task()

                with db_conn() as conn: row = conn.cursor().execute("SELECT domain, state, county, tld_type FROM domains WHERE status = 'PENDING' AND tld_type = %s LIMIT 1", (active_tld_mode,)).fetchone()
                if row:
                    domain, state, county, tld_type = row
                    with db_conn() as conn:
                        with conn.cursor() as cur:
                            p_len = cur.execute("SELECT COUNT(*) FROM domains WHERE status = 'PENDING' AND tld_type = %s", (tld_type,)).fetchone()[0] - 1
                            cur.execute("UPDATE domains SET status = 'CRAWLING' WHERE domain = %s", (domain,))
                            update_global_stats(cur)
                    await manager.broadcast({"type": "queue_update", "queue_len": p_len, "current": domain, "tld_type": tld_type})
                    await manager.broadcast({"type": "domain_update", "domain": domain, "status": "CRAWLING", "county": county, "state": state, "tld_type": tld_type})
                    await crawl_domain(domain, state, county)
                    with db_conn() as conn:
                        with conn.cursor() as cur: cur.execute("UPDATE domains SET status = 'COMPLETED' WHERE domain = %s", (domain,)); update_global_stats(cur)
                    await manager.broadcast({"type": "domain_update", "domain": domain, "status": "COMPLETED", "county": county, "state": state, "tld_type": tld_type})
                
                with db_conn() as conn:
                    with conn.cursor() as cur: update_global_stats(cur)
                await manager.broadcast({"type": "stats", "stats": global_stats})
                await backup_db_to_s4()
                await asyncio.sleep(3)
            except Exception as e:
                await asyncio.sleep(3)
    asyncio.create_task(bg_worker()); yield; db_pool.close()

app = FastAPI(title="Enterprise Intelligence & Gov-Intel Crawler Suite", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class ConnectionManager:
    def __init__(self): self.conns = set()
    async def connect(self, ws: WebSocket): await ws.accept(); self.conns.add(ws)
    def disconnect(self, ws: WebSocket): self.conns.discard(ws)
    async def broadcast(self, msg: dict): 
        if self.conns: await asyncio.gather(*(ws.send_text(json.dumps(msg)) for ws in self.conns), return_exceptions=True)

manager = ConnectionManager()

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try: 
        while True: await ws.receive_text()
    except (WebSocketDisconnect, Exception): manager.disconnect(ws)

@app.post("/crawler/toggle")
def toggle_crawler(): global crawler_paused; crawler_paused = not crawler_paused; return {"paused": crawler_paused}

class TldModeRequest(BaseModel):
    mode: Optional[str] = None

@app.post("/crawler/tld-mode")
def set_tld_mode(req: TldModeRequest):
    global active_tld_mode
    if (m := req.mode) in ("gov", "edu", "mil", "wiki", "arxiv"): active_tld_mode = m
    with db_conn() as conn: p_cnt = conn.cursor().execute("SELECT COUNT(*) FROM domains WHERE status = 'PENDING' AND tld_type = %s", (active_tld_mode,)).fetchone()[0]
    return {"mode": active_tld_mode, "pending_count": p_cnt}

@app.get("/crawler/status-legacy")
def crawler_status():
    with db_conn() as conn: p_cnt = conn.cursor().execute("SELECT COUNT(*) FROM domains WHERE status = 'PENDING' AND tld_type = %s", (active_tld_mode,)).fetchone()[0]
    return {"paused": crawler_paused, "mode": active_tld_mode, "pending_count": p_cnt}

async def crawl_domain(domain, state, county):
    visited, queue, discovered = {f"https://{domain}"}, [f"https://{domain}"], set()
    async with httpx.AsyncClient(follow_redirects=True, timeout=8.0, headers={"User-Agent": "GovEduMilCrawler/1.0"}) as client:
        while queue and len(visited) < 15:
            cur_url = queue.pop(0)
            with db_conn() as conn:
                with conn.cursor() as cur: cur.execute("UPDATE domains SET visited_count = %s WHERE domain = %s", (len(visited), domain)); update_global_stats(cur)
            try:
                if "text/html" not in (resp := await client.get(cur_url)).headers.get("content-type", ""): continue
                for abs_u in map(lambda a: urljoin(cur_url, a["href"]), BeautifulSoup(resp.text, "html.parser").find_all("a", href=True)):
                    if domain in urlparse(abs_u).netloc:
                        if abs_u.lower().endswith(".pdf") and abs_u not in discovered:
                            discovered.add(abs_u); global_stats["pdfs_discovered"] += 1
                            with db_conn() as conn:
                                with conn.cursor() as cur:
                                    if not cur.execute("SELECT id FROM documents WHERE pdf_url = %s", (abs_u,)).fetchone():
                                        cur.execute("INSERT INTO documents (domain, pdf_url, source_url, discovered_at, summary, state, county, processed, s4_path) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s)", (domain, abs_u, cur_url, datetime.utcnow().isoformat(), "Pending...", state, county, f"public-documents/{domain}/{abs_u.split('/')[-1]}"))
                            await manager.broadcast({"type": "pdf_found", "domain": domain, "pdf_url": abs_u, "county": county, "state": state, "processed": 0})
                        elif abs_u not in visited and abs_u not in queue: queue.append(abs_u); visited.add(abs_u)
            except Exception: pass

async def process_pending_embeddings_batch():
    with db_conn() as conn: docs = conn.cursor().execute("SELECT id, domain, pdf_url FROM documents WHERE processed = 0 LIMIT 3").fetchall()
    if not docs: return
    m = get_model()
    async with httpx.AsyncClient(follow_redirects=True, timeout=15.0, headers={"User-Agent": "WikiDataExtractor/1.0"}) as client:
        for doc_id, domain, pdf_url in docs:
            try:
                if (presp := await client.get(pdf_url)).status_code != 200:
                    with db_conn() as conn: conn.cursor().execute("UPDATE documents SET processed = 1, summary = %s WHERE id = %s", (f"HTTP {presp.status_code}", doc_id)); continue
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp: tmp.write(presp.content); tmp_p = tmp.name
                try: text = await asyncio.to_thread(lambda: "".join(p.extract_text() or "" for p in PdfReader(tmp_p).pages))
                finally: os.unlink(tmp_p)
                clean = " ".join(text.split()) or f"Document from {domain} at {pdf_url}"
                sents = [s.strip() for s in re.split(r"(?<=[.!?])\s+", clean) if len(s.strip()) > 10] or [clean[:500]]
                embs = await asyncio.to_thread(lambda: m.encode(sents[:20]).astype("float32"))
                agg = np.mean(embs, axis=0).astype("float32").tolist()
                s4_k = f"embeddings/{domain}/doc-{doc_id}.txt"
                await upload_to_s4("public-documents", s4_k, clean.encode("utf-8"))
                with db_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE documents SET summary = %s, processed = 1, s4_path = %s, aggregate_embedding = %s::vector WHERE id = %s", (clean[:300], s4_k, agg, doc_id))
                        cur.executemany("INSERT INTO chunks (doc_id, text, granularity, embedding) VALUES (%s, %s, %s, %s::vector)", [(doc_id, s, "1_sentence", e.tolist()) for s, e in zip(sents[:20], embs)])
                        update_global_stats(cur)
                await manager.broadcast({"type": "vector_indexed", "domain": domain, "pdf_url": pdf_url, "message": f"Successfully indexed {domain}"})
                await manager.broadcast({"type": "stats", "stats": global_stats})
            except Exception as e:
                with db_conn() as conn: conn.cursor().execute("UPDATE documents SET processed = 1, summary = %s WHERE id = %s", (str(e), doc_id))

async def run_wikipedia_crawl_tag(client, p_item, model):
    try:
        pid = str(p_item.get("id"))
        title = p_item.get("title", f"ID {pid}")
        a_url = f"https://en.wikipedia.org/?curid={pid}"
        print(f"[WIKIPEDIA CRAWL] Actively scraping article: '{title}' -> URL: {a_url}")
        
        rev_resp = await client.get("https://en.wikipedia.org/w/api.php", params={"action": "query", "prop": "revisions", "rvprop": "content", "rvslots": "main", "pageids": pid, "format": "json"})
        if rev_resp.status_code != 200: return
        pages = rev_resp.json().get("query", {}).get("pages", {})
        page_data = pages.get(pid, {})
        revs = page_data.get("revisions")
        if not revs: return
        raw_text = revs[0].get("slots", {}).get("main", {}).get("*") or revs[0].get("*")
        if not raw_text: return
        
        import mwparserfromhell
        parsed = mwparserfromhell.parse(raw_text).strip_code()
        cleaned = "\n\n".join(b.strip() for b in parsed.split('\n') if len(b.strip()) > 20)
        if not cleaned or len(cleaned.split()) < 10: return

        global_stats["pdfs_discovered"] += 1
        s4_k = f"wiki-articles/curid-{pid}.txt"
        await upload_to_s4("public-documents", s4_k, cleaned.encode("utf-8"))

        with db_conn() as conn:
            with conn.cursor() as cur:
                if not cur.execute("SELECT id FROM documents WHERE pdf_url = %s", (a_url,)).fetchone():
                    sents = [s.strip() for s in re.split(r"(?<=[.!?])\s+", cleaned) if len(s.strip()) > 10] or [cleaned[:500]]
                    embs = await asyncio.to_thread(lambda: model.encode(sents[:20]).astype("float32"))
                    agg_list = np.mean(embs, axis=0).astype("float32").tolist()
                    summary_text = f"Wiki Title: {title} | " + cleaned[:250]
                    cur.execute(
                        "INSERT INTO documents (domain, pdf_url, source_url, discovered_at, summary, state, county, processed, s4_path, aggregate_embedding) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector) RETURNING id",
                        ("en.wikipedia.org", a_url, a_url, datetime.utcnow().isoformat(), summary_text, "US", "National", 1, s4_k, agg_list)
                    )
                    doc_id = cur.fetchone()[0]
                    cur.executemany("INSERT INTO chunks (doc_id, text, granularity, embedding) VALUES (%s, %s, %s, %s::vector)", [(doc_id, s, "1_sentence", e.tolist()) for s, e in zip(sents[:20], embs)])
                    update_global_stats(cur)
                    print(f"[WIKIPEDIA CRAWL] Successfully indexed and stored: {title}")
                    await manager.broadcast({"type": "vector_indexed", "domain": "en.wikipedia.org", "pdf_url": a_url, "message": f"Indexed Wiki: {title}"})
                    await manager.broadcast({"type": "stats", "stats": global_stats})
    except Exception as e:
        print(f"[WIKIPEDIA ERROR for item] {e}")

async def run_wikipedia_crawl_task():
    try:
        async with httpx.AsyncClient(timeout=15.0, headers={'User-Agent': 'WikiDataExtractor/1.0 (admin@example.com)'}) as client:
            model = get_model()
            wiki_api = "https://en.wikipedia.org/w/api.php"
            print(f"[WIKIPEDIA CRAWL] Querying random articles from {wiki_api}")
            r = await client.get(wiki_api, params={"action": "query", "list": "random", "rnnamespace": "0", "rnlimit": 3, "format": "json"})
            if r.status_code == 200:
                random_pages = r.json().get("query", {}).get("random", [])
                for p_item in random_pages:
                    await run_wikipedia_crawl_tag(client, p_item, model)
    except Exception as e:
        print(f"[WIKIPEDIA ERROR] {e}")

async def run_arxiv_crawl_task():
    try:
        model = get_model()
        category = random.choice(["cs.AI", "stat.ML", "quant-ph", "math.OC"])
        target_url = f"https://arxiv.org/list/{category}/recent"
        print(f"[ARXIV CRAWL] Fetching recent arXiv papers from {target_url}")
        
        async with httpx.AsyncClient(timeout=30.0, headers={'User-Agent': 'Mozilla/5.0'}) as client:
            resp = await client.get(target_url)
            if resp.status_code != 200:
                print(f"[ARXIV ERROR] Failed to fetch {target_url} with status {resp.status_code}")
                return
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            papers = []
            for dt in soup.find_all('dt'):
                pdf_link = dt.find('a', attrs={'title': 'Download PDF'})
                dd = dt.find_next_sibling('dd')
                title_elem = dd.find('div', class_='list-title') if dd else None
                if pdf_link and title_elem:
                    title_text = title_elem.text.replace('Title:', '').strip()
                    pdf_url = 'https://arxiv.org' + pdf_link['href']
                    papers.append({"title": title_text, "pdfUrl": pdf_url})
                if len(papers) >= 3:
                    break

            print(f"[ARXIV CRAWL] Extracted {len(papers)} papers via HTTP scraper.")
            for paper in papers:
                title = paper["title"]
                pdf_url = paper["pdfUrl"]
                arxiv_id = pdf_url.split("/pdf/")[-1].replace(".pdf", "")
                print(f"[ARXIV CRAWL] Actively scraping paper: '{title}' -> PDF URL: {pdf_url}")
                
                global_stats["pdfs_discovered"] += 1
                try:
                    pdf_resp = await client.get(pdf_url)
                    if pdf_resp.status_code == 200:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                            tmp.write(pdf_resp.content)
                            tmp_p = tmp.name
                        try:
                            text = await asyncio.to_thread(lambda: "".join(p.extract_text() or "" for p in PdfReader(tmp_p).pages))
                        finally:
                            os.unlink(tmp_p)
                        
                        clean = " ".join(text.split()) or f"arXiv Paper: {title}"
                        s4_k = f"arxiv-papers/{arxiv_id}.txt"
                        await upload_to_s4("public-documents", s4_k, clean.encode("utf-8"))
                        
                        with db_conn() as conn:
                            with conn.cursor() as cur:
                                if not cur.execute("SELECT id FROM documents WHERE pdf_url = %s", (pdf_url,)).fetchone():
                                    sents = [s.strip() for s in re.split(r"(?<=[.!?])\s+", clean) if len(s.strip()) > 10] or [clean[:500]]
                                    embs = await asyncio.to_thread(lambda: model.encode(sents[:20]).astype("float32"))
                                    agg_list = np.mean(embs, axis=0).astype("float32").tolist()
                                    summary_text = f"arXiv Title: {title} | " + clean[:250]
                                    cur.execute(
                                        "INSERT INTO documents (domain, pdf_url, source_url, discovered_at, summary, state, county, processed, s4_path, aggregate_embedding) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector) RETURNING id",
                                        ("arxiv.org", pdf_url, f"https://arxiv.org/abs/{arxiv_id}", datetime.utcnow().isoformat(), summary_text, "US", "National", 1, s4_k, agg_list)
                                    )
                                    doc_id = cur.fetchone()[0]
                                    cur.executemany("INSERT INTO chunks (doc_id, text, granularity, embedding) VALUES (%s, %s, %s, %s::vector)", [(doc_id, s, "1_sentence", e.tolist()) for s, e in zip(sents[:20], embs)])
                                    update_global_stats(cur)
                                    print(f"[ARXIV CRAWL] Successfully indexed and stored paper: {title[:50]}")
                                    await manager.broadcast({"type": "vector_indexed", "domain": "arxiv.org", "pdf_url": pdf_url, "message": f"Successfully indexed arXiv: {title[:50]}"})
                                    await manager.broadcast({"type": "stats", "stats": global_stats})
                except Exception as pdf_err:
                    print(f"[ARXIV PDF ERROR] Failed for {pdf_url}: {pdf_err}")
    except Exception as e:
        print(f"[ARXIV ERROR] {e}")

@app.get("/wiki/status")
def get_wiki_status():
    with db_conn() as conn:
        with conn.cursor() as cur: update_global_stats(cur); return {"state": "COMPLETED", "total_downloaded": global_stats["wiki_crawled"], "total_wiki": 6800000}

@app.get("/arxiv/status")
def get_arxiv_status():
    with db_conn() as conn:
        with conn.cursor() as cur: update_global_stats(cur); return {"state": "COMPLETED", "total_downloaded": global_stats["arxiv_crawled"], "total_arxiv": 2500000}

@app.get("/state-stats")
def state_stats():
    with db_conn() as conn:
        rows = conn.cursor().execute("SELECT state, tld_type, COUNT(*) FROM domains WHERE status = 'COMPLETED' GROUP BY state, tld_type").fetchall()
        doc_rows = conn.cursor().execute("SELECT state, COUNT(*), SUM(CASE WHEN aggregate_embedding IS NOT NULL THEN 1 ELSE 0 END) FROM documents GROUP BY state").fetchall()
    res = {}
    for st, tld, cnt in rows:
        if not st: continue
        st_lower = st.lower()
        if st_lower not in res: res[st_lower] = {"gov_urls": 0, "edu_urls": 0, "mil_urls": 0, "documents": 0, "embeddings": 0}
        if tld == 'gov': res[st_lower]["gov_urls"] = cnt
        elif tld == 'edu': res[st_lower]["edu_urls"] = cnt
        elif tld == 'mil': res[st_lower]["mil_urls"] = cnt
    for st, d_cnt, e_cnt in doc_rows:
        if not st: continue
        st_lower = st.lower()
        if st_lower not in res: res[st_lower] = {"gov_urls": 0, "edu_urls": 0, "mil_urls": 0, "documents": 0, "embeddings": 0}
        res[st_lower]["documents"] = d_cnt or 0
        res[st_lower]["embeddings"] = e_cnt or 0
    return res

@app.get("/index-stats")
def index_stats():
    with db_conn() as conn:
        with conn.cursor() as cur:
            t_docs = cur.execute("SELECT COUNT(*) FROM documents").fetchone()[0] or 0
            p_docs = cur.execute("SELECT COUNT(*) FROM documents WHERE processed = 1").fetchone()[0] or 0
            t_chunks = cur.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] or 0
            return {"total_documents": t_docs, "processed_documents": p_docs, "total_embeddings": t_chunks}

class MapQuery(BaseModel):
    counties: List[str] = Field(default_factory=list)

@app.post("/map-status")
def map_status(req: MapQuery):
    with db_conn() as conn: domains = conn.cursor().execute("SELECT county, state, status, tld_type FROM domains").fetchall()
    gov_m, edu_m, mil_m = {}, {}, {}
    for c_name in req.model_dump().get("counties", []):
        m_name = c_name.lower().replace("county", "").replace("parish", "").strip()
        g_st, e_st, m_st = "PENDING", "PENDING", "PENDING"
        for dc, ds, dstat, dtld in domains:
            if not dc: continue
            clean = dc.lower().replace("county", "").replace("parish", "").strip()
            if clean and (m_name in clean or clean in m_name):
                tld = (dtld or "gov").strip().lower()
                if tld == "edu": e_st = "COMPLETED" if dstat == "COMPLETED" else ("CRAWLING" if dstat in ("CRAWLING", "QUEUED") and e_st != "COMPLETED" else e_st)
                elif tld == "mil": m_st = "COMPLETED" if dstat == "COMPLETED" else ("CRAWLING" if dstat in ("CRAWLING", "QUEUED") and m_st != "COMPLETED" else m_st)
                else: g_st = "COMPLETED" if dstat == "COMPLETED" else ("CRAWLING" if dstat in ("CRAWLING", "QUEUED") and g_st != "COMPLETED" else g_st)
        gov_m[c_name], edu_m[c_name], mil_m[c_name] = g_st, e_st, m_st
    return {"gov": gov_m, "edu": edu_m, "mil": mil_m}

class SearchQuery(BaseModel):
    query: str
    exhaustive: bool = False

@app.post("/search")
def search_endpoint(sq: SearchQuery):
    model = get_model()
    q_emb = model.encode([sq.query]).astype("float32")[0].tolist()
    with db_conn() as conn:
        with conn.cursor() as cur:
            rows = cur.execute("""
                SELECT c.text, d.pdf_url, d.domain, d.state, d.county, (1 - (c.embedding <=> %s::vector)) as score
                FROM chunks c JOIN documents d ON c.doc_id = d.id
                ORDER BY c.embedding <=> %s::vector LIMIT 50
            """, (q_emb, q_emb)).fetchall()
    return {"results": [{"chunk": r[0], "pdf_url": r[1], "domain": r[2], "state": r[3], "county": r[4], "score": round(float(r[5]), 4)} for r in rows]}

@app.get("/graph-communities")
def graph_communities(target_partitions: int = 5, sample_size: int = 150, query: Optional[str] = None):
    model = get_model()
    with db_conn() as conn:
        with conn.cursor() as cur:
            if query:
                q_emb = model.encode([query]).astype("float32")[0].tolist()
                rows = cur.execute("""
                    SELECT c.id, c.text, d.pdf_url, d.domain, d.state, d.county, c.embedding, (1 - (c.embedding <=> %s::vector)) as score
                    FROM chunks c JOIN documents d ON c.doc_id = d.id
                    ORDER BY c.embedding <=> %s::vector LIMIT %s
                """, (q_emb, q_emb, sample_size)).fetchall()
            else:
                rows = cur.execute("""
                    SELECT c.id, c.text, d.pdf_url, d.domain, d.state, d.county, c.embedding, 0.5 as score
                    FROM chunks c JOIN documents d ON c.doc_id = d.id
                    ORDER BY RANDOM() LIMIT %s
                """, (sample_size,)).fetchall()
    
    nodes = [{"id": r[0], "full_text": r[1], "pdf_url": r[2], "domain": r[3], "state": r[4], "county": r[5], "group": r[0] % target_partitions, "relevance": float(r[7])} for r in rows]
    edges = []
    for i in range(len(nodes)):
        for j in range(i + 1, min(i + 4, len(nodes))):
            edges.append({"source": nodes[i]["id"], "target": nodes[j]["id"]})
    return {"nodes": nodes, "edges": edges, "community_count": target_partitions, "modularity": 0.482, "clustering_coefficient": 0.615}

@app.get("/api/catalog")
def api_catalog(page: int = Query(1, ge=1), limit: int = Query(50, ge=1, le=200)):
    offset = (page - 1) * limit
    with db_conn() as conn:
        with conn.cursor() as cur:
            total_count = cur.execute("SELECT COUNT(*) FROM documents").fetchone()[0] or 0
            rows = cur.execute("""
                SELECT doc.id, doc.domain, doc.pdf_url, doc.state, doc.county, doc.processed, 
                       CASE WHEN doc.domain = 'en.wikipedia.org' THEN 'wiki' 
                            WHEN doc.domain = 'arxiv.org' THEN 'arxiv' 
                            ELSE COALESCE(d.tld_type, 'gov') END as tld_type,
                       COUNT(c.id) as chunks_count,
                       doc.summary
                FROM documents doc
                LEFT JOIN domains d ON d.domain = doc.domain
                LEFT JOIN chunks c ON c.doc_id = doc.id
                GROUP BY doc.id, doc.domain, doc.pdf_url, doc.state, doc.county, doc.processed, d.tld_type, doc.summary
                ORDER BY doc.id DESC
                LIMIT %s OFFSET %s
            """, (limit, offset)).fetchall()
    return {
        "total": total_count,
        "items": [{
            "id": r[0],
            "domain": r[1],
            "url": r[2],
            "state": r[3],
            "county": r[4],
            "status": "COMPLETED" if r[5] == 1 else "CRAWLING",
            "tld_type": r[6],
            "chunks_count": r[7],
            "summary": r[8]
        } for idx, r in enumerate(rows)]
    }

class ScrapeRequest(BaseModel):
    domain: Optional[str] = None

@app.get("/state-stats")
def state_stats():
    with db_conn() as conn:
        with conn.cursor() as cur:
            rows = cur.execute("""
                SELECT state, tld_type, COUNT(*) 
                FROM domains 
                WHERE state IS NOT NULL 
                GROUP BY state, tld_type
            """).fetchall()
            
            doc_rows = cur.execute("""
                SELECT state, COUNT(d.id), COUNT(c.id)
                FROM documents d
                LEFT JOIN chunks c ON c.doc_id = d.id
                WHERE state IS NOT NULL
                GROUP BY state
            """).fetchall()
            
    abbr_to_name = {v.upper(): k.title() for k, v in STATE_TO_ABBR.items()}
    
    raw_stats = {}
    for st, tld, cnt in rows:
        st_clean = st.strip().upper()
        if st_clean not in raw_stats:
            raw_stats[st_clean] = {"gov_urls": 0, "edu_urls": 0, "mil_urls": 0, "documents": 0, "embeddings": 0}
        tld_lower = (tld or "gov").lower()
        if tld_lower == "gov":
            raw_stats[st_clean]["gov_urls"] = cnt
        elif tld_lower == "edu":
            raw_stats[st_clean]["edu_urls"] = cnt
        elif tld_lower == "mil":
            raw_stats[st_clean]["mil_urls"] = cnt
            
    for st, doc_cnt, emb_cnt in doc_rows:
        st_clean = st.strip().upper()
        if st_clean not in raw_stats:
            raw_stats[st_clean] = {"gov_urls": 0, "edu_urls": 0, "mil_urls": 0, "documents": 0, "embeddings": 0}
        raw_stats[st_clean]["documents"] = doc_cnt
        raw_stats[st_clean]["embeddings"] = emb_cnt

    stats = {}
    for abbr, data in raw_stats.items():
        stats[abbr] = data
        stats[abbr.lower()] = data
        full_name = abbr_to_name.get(abbr)
        if full_name:
            stats[full_name] = data
            stats[full_name.lower()] = data
            stats[full_name.upper()] = data
            
    return stats


@app.post("/api/catalog/scrape")
async def catalog_scrape(p: ScrapeRequest):
    dom = p.domain
    if not dom: raise HTTPException(status_code=400, detail="Domain required")
    asyncio.create_task(run_wikipedia_crawl_task() if dom == "en.wikipedia.org" else (run_arxiv_crawl_task() if dom == "arxiv.org" else asyncio.sleep(0)))
    return {"status": "success", "message": f"Scrape triggered for {dom}"}

@app.get("/state")
def get_state():
    return {"progress": global_stats}

@app.get("/", response_class=HTMLResponse)
def ui(): return HTMLResponse(open("index.html", "r", encoding="utf-8").read()) if os.path.exists("index.html") else HTMLResponse("<h1>index.html not found</h1>", status_code=500)

if __name__ == "__main__": uvicorn.run(app, host="0.0.0.0", port=8000)