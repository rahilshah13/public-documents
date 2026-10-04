import asyncio
import csv
import json
import os
import random
import re
import tempfile
import time
from contextlib import asynccontextmanager
from datetime import datetime
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
import httpx
import networkx as nx
import numpy as np
from pgvector.psycopg import register_vector
from pydantic import BaseModel
from pypdf import PdfReader
from psycopg_pool import ConnectionPool
import uvicorn

from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors

from db_utils import restore_db_from_s4, backup_db_to_s4, upload_to_s4

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@db:5432/gov_intel"
)
S4_GATEWAY_URL = os.environ.get("S4_GATEWAY_URL", "http://s4_gateway:8080")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://ollama:11434")
CSV_PATH = "data/current-full.csv"
EDU_CSV_PATH = "data/edu-domains.csv"
MIL_CSV_PATH = "data/mil-domains.csv"

db_pool = ConnectionPool(
    conninfo=DATABASE_URL, min_size=2, max_size=10, open=False
)

STATE_TO_ABBR = json.loads(os.environ.get("STATE_TO_ABBR", "{}"))

normalize_state = lambda st, text_context="": (
    next((abbr for name, abbr in STATE_TO_ABBR.items() if name in text_context.lower()), "US")
    if not st or st == "US"
    else (st.strip().upper() if len(st.strip()) == 2 else STATE_TO_ABBR.get(st.strip().lower(), st.strip().upper()))
)

get_db = lambda: (conn := db_pool.getconn(), setattr(conn, "autocommit", True), register_vector(conn), conn)[-1]
put_db = lambda conn: db_pool.putconn(conn)
get_raw_db = lambda: __import__("psycopg").connect(DATABASE_URL, autocommit=True)

_model = None
global_stats = {
    "visited": 0, "total": 0, "pdfs_discovered": 0, "domains_crawled": 0,
    "total_domains": 0, "gov_completed": 0, "edu_crawled": 0, "mil_crawled": 0,
    "wiki_crawled": 0, "total_gov": 0, "total_edu": 0, "total_mil": 0, "total_wiki": 6800000,
    "total_embeddings": 0, "pending_embeddings": 0
}
crawler_paused = False
active_tld_mode = "gov"

def get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model

def init_db_sync():
    list(map(lambda i: time.sleep(3) or (lambda: get_raw_db().__enter__().cursor().__enter__().execute("SELECT 1;"))() if i > 0 else None, range(15)))

    with get_raw_db() as conn, conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    db_pool.open()

    if asyncio.run(restore_db_from_s4()):
        with get_db() as conn, conn.cursor() as cur:
            update_global_stats(cur)
        return

    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS domains (
                domain TEXT PRIMARY KEY, state TEXT, county TEXT, status TEXT DEFAULT 'PENDING',
                visited_count INTEGER DEFAULT 0, error TEXT, tld_type TEXT DEFAULT 'gov'
            );
            CREATE TABLE IF NOT EXISTS documents (
                id SERIAL PRIMARY KEY, domain TEXT, pdf_url TEXT, source_url TEXT, discovered_at TEXT, 
                summary TEXT, state TEXT, county TEXT, processed INTEGER DEFAULT 0, s4_path TEXT,
                aggregate_embedding vector(384)
            );
            CREATE TABLE IF NOT EXISTS chunks (
                id SERIAL PRIMARY KEY, doc_id INTEGER REFERENCES documents(id) ON DELETE CASCADE, 
                text TEXT, granularity TEXT DEFAULT '1_sentence', embedding vector(384)
            );
            CREATE TABLE IF NOT EXISTS page_audit (
                id SERIAL PRIMARY KEY, domain TEXT, url TEXT, status TEXT, pdf_count INTEGER, embedding_count INTEGER, error TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS chunks_embedding_hnsw_idx ON chunks USING hnsw (embedding vector_cosine_ops);")

        if cur.execute("SELECT COUNT(*) FROM domains WHERE domain = 'en.wikipedia.org'").fetchone()[0] == 0:
            cur.execute("INSERT INTO domains (domain, state, county, status, visited_count, error, tld_type) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (domain) DO NOTHING", ("en.wikipedia.org", "US", "National", "PENDING", 0, "", "wiki"))

        if cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'gov'").fetchone()[0] == 0 and os.path.exists(CSV_PATH):
            with open(CSV_PATH, "r", encoding="utf-8", errors="ignore") as f:
                gov_rows = [
                    (row.get("Domain name").strip().lower(), normalize_state(row.get("State", "US"), f"{row.get('Domain name')} {row.get('Organization name') or row.get('City') or 'Unknown'}"), (row.get("Organization name") or row.get("City") or "Unknown").strip(), "PENDING", 0, "", "gov")
                    for row in csv.DictReader(f) if row.get("Domain name")
                ]
                if gov_rows:
                    cur.executemany("INSERT INTO domains (domain, state, county, status, visited_count, error, tld_type) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (domain) DO NOTHING", gov_rows)

        if cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'edu'").fetchone()[0] == 0 and os.path.exists(EDU_CSV_PATH):
            with open(EDU_CSV_PATH, "r", encoding="utf-8", errors="ignore") as f:
                edu_rows = []
                for row in csv.DictReader(f):
                    url_field = row.get("URL") or row.get("Domain name") or row.get("domain") or (list(row.values())[0] if row else None)
                    if url_field:
                        parsed_dom = (urlparse(url_field if "://" in url_field else f"http://{url_field}").netloc or url_field).replace("www.", "").strip().lower()
                        if parsed_dom and parsed_dom.endswith(".edu"):
                            title = row.get("Title") or row.get("Organization name") or row.get("institution") or "Higher Education"
                            raw_state = next((str(v).strip() for v in row.values() if str(v).strip().upper() in STATE_TO_ABBR.values() or str(v).strip().title() in STATE_TO_ABBR), row.get("State") or row.get("state") or row.get("location") or "US")
                            edu_rows.append((parsed_dom, normalize_state(raw_state, f"{parsed_dom} {title}"), title[:100], "PENDING", 0, "", "edu"))
                if edu_rows:
                    cur.executemany("INSERT INTO domains (domain, state, county, status, visited_count, error, tld_type) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (domain) DO NOTHING", edu_rows)

        if cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'mil'").fetchone()[0] == 0 and os.path.exists(MIL_CSV_PATH):
            with open(MIL_CSV_PATH, "r", encoding="utf-8", errors="ignore") as f:
                mil_rows = [
                    ((parsed := (urlparse(uf if "://" in uf else f"http://{uf}").netloc or uf).replace("www.", "").strip().lower()), normalize_state("US", f"{parsed} {row.get('Organization') or row.get('Organization name') or 'Military Entity'}"), (row.get("Organization") or row.get("Organization name") or "Military Entity")[:100], "PENDING", 0, "", "mil")
                    for row in csv.DictReader(f) if (uf := row.get("Domain Name") or row.get("Domain name") or row.get("domain") or (list(row.values())[0] if row else None))
                ]
                mil_rows = [r for r in mil_rows if r[0].endswith(".mil")]
                if mil_rows:
                    cur.executemany("INSERT INTO domains (domain, state, county, status, visited_count, error, tld_type) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (domain) DO NOTHING", mil_rows)

        cur.execute("UPDATE domains SET status = 'PENDING' WHERE status IN ('CRAWLING', 'QUEUED') AND tld_type != 'wiki';")
        update_global_stats(cur)

def update_global_stats(cur):
    total_docs = cur.execute("SELECT COUNT(*) FROM documents").fetchone()[0] or 0
    processed_docs = cur.execute("SELECT COUNT(*) FROM documents WHERE processed = 1").fetchone()[0] or 0
    total_chunks = cur.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] or 0
    global_stats.update({
        "visited": cur.execute("SELECT SUM(visited_count) FROM domains").fetchone()[0] or 0,
        "pdfs_discovered": total_docs,
        "total_domains": (total_doms := cur.execute("SELECT COUNT(*) FROM domains").fetchone()[0]),
        "domains_crawled": cur.execute("SELECT COUNT(*) FROM domains WHERE status != 'PENDING'").fetchone()[0],
        "gov_completed": cur.execute("SELECT COUNT(DISTINCT county) FROM domains WHERE status = 'COMPLETED' AND tld_type = 'gov'").fetchone()[0] or 0,
        "total": total_doms,
        "gov_crawled": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'gov' AND status = 'COMPLETED'").fetchone()[0],
        "edu_crawled": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'edu' AND status = 'COMPLETED'").fetchone()[0],
        "mil_crawled": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'mil' AND status = 'COMPLETED'").fetchone()[0],
        "wiki_crawled": cur.execute("SELECT COUNT(*) FROM documents WHERE domain = 'en.wikipedia.org'").fetchone()[0] or 0,
        "total_gov": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'gov'").fetchone()[0],
        "total_edu": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'edu'").fetchone()[0],
        "total_mil": cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'mil'").fetchone()[0],
        "total_wiki": 6800000,
        "total_embeddings": total_chunks,
        "pending_embeddings": total_docs - processed_docs
    })

@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(init_db_sync)

    async def bg_worker():
        global crawler_paused, active_tld_mode
        while True:
            if crawler_paused:
                await asyncio.sleep(2)
                continue
            try:
                await process_pending_embeddings_batch()

                if active_tld_mode == 'wiki':
                    await run_wikipedia_crawl_task()
                    await backup_db_to_s4()
                    await asyncio.sleep(3)
                    continue

                conn = get_db()
                row = conn.cursor().execute("SELECT domain, state, county, tld_type FROM domains WHERE status = 'PENDING' AND tld_type = %s LIMIT 1", (active_tld_mode,)).fetchone()
                put_db(conn)

                if not row:
                    await asyncio.sleep(5)
                    continue
                domain, state, county, tld_type = row

                conn = get_db()
                with conn.cursor() as cur:
                    pending_count = cur.execute("SELECT COUNT(*) FROM domains WHERE status = 'PENDING' AND tld_type = %s", (tld_type,)).fetchone()[0]
                    cur.execute("UPDATE domains SET status = 'CRAWLING' WHERE domain = %s", (domain,))
                    update_global_stats(cur)
                put_db(conn)

                await manager.broadcast({"type": "queue_update", "queue_len": pending_count - 1, "current": domain, "tld_type": tld_type})
                await manager.broadcast({"type": "domain_update", "domain": domain, "status": "CRAWLING", "county": county, "state": state, "tld_type": tld_type})

                await crawl_domain(domain, state, county)

                conn = get_db()
                with conn.cursor() as cur:
                    cur.execute("UPDATE domains SET status = 'COMPLETED' WHERE domain = %s", (domain,))
                    update_global_stats(cur)
                put_db(conn)

                await manager.broadcast({"type": "domain_update", "domain": domain, "status": "COMPLETED", "county": county, "state": state, "tld_type": tld_type})
                await manager.broadcast({"type": "stats", "stats": global_stats})
                await backup_db_to_s4()
            except Exception as e:
                await asyncio.sleep(3)

    asyncio.create_task(bg_worker())
    yield
    db_pool.close()

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
def toggle_crawler():
    global crawler_paused
    crawler_paused = not crawler_paused
    return {"paused": crawler_paused}

class TldModeReq(BaseModel): mode: str

@app.post("/crawler/tld-mode")
def set_tld_mode(req: TldModeReq):
    global active_tld_mode
    if req.mode in ("gov", "edu", "mil", "wiki"): active_tld_mode = req.mode
    conn = get_db()
    pending_count = conn.cursor().execute("SELECT COUNT(*) FROM domains WHERE status = 'PENDING' AND tld_type = %s", (active_tld_mode,)).fetchone()[0]
    put_db(conn)
    return {"mode": active_tld_mode, "pending_count": pending_count}

@app.get("/crawler/status-legacy")
def crawler_status():
    conn = get_db()
    pending_count = conn.cursor().execute("SELECT COUNT(*) FROM domains WHERE status = 'PENDING' AND tld_type = %s", (active_tld_mode,)).fetchone()[0]
    put_db(conn)
    return {"paused": crawler_paused, "mode": active_tld_mode, "pending_count": pending_count}

async def crawl_domain(domain: str, state: str, county: str):
    base_url, visited, queue, discovered_pdfs = f"https://{domain}", {f"https://{domain}"}, [f"https://{domain}"], set()
    async with httpx.AsyncClient(follow_redirects=True, timeout=8.0, headers={"User-Agent": "GovEduMilCrawler/1.0"}) as client:
        while queue and len(visited) < 15:
            current_url = queue.pop(0)
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute("UPDATE domains SET visited_count = %s WHERE domain = %s", (len(visited), domain))
                update_global_stats(cur)
            put_db(conn)
            try:
                resp = await client.get(current_url)
                if "text/html" not in resp.headers.get("content-type", ""): continue
                soup = BeautifulSoup(resp.text, "html.parser")
                for abs_url in map(lambda a: urljoin(current_url, a["href"]), soup.find_all("a", href=True)):
                    if domain in urlparse(abs_url).netloc:
                        if abs_url.lower().endswith(".pdf") and abs_url not in map(lambda p: p[0], discovered_pdfs):
                            discovered_pdfs.add((abs_url, current_url))
                            global_stats["pdfs_discovered"] += 1
                            s4_key = f"public-documents/{domain}/{abs_url.split('/')[-1]}"
                            conn = get_db()
                            with conn.cursor() as cur:
                                if not cur.execute("SELECT id FROM documents WHERE pdf_url = %s", (abs_url,)).fetchone():
                                    cur.execute("INSERT INTO documents (domain, pdf_url, source_url, discovered_at, summary, state, county, processed, s4_path) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s)", (domain, abs_url, current_url, datetime.utcnow().isoformat(), "Pending embedding...", state, county, s4_key))
                            put_db(conn)
                            await manager.broadcast({"type": "pdf_found", "domain": domain, "pdf_url": abs_url, "county": county, "state": state, "processed": 0})
                        elif abs_url not in visited and abs_url not in queue:
                            queue.append(abs_url)
                            visited.add(abs_url)
            except Exception: pass

async def process_pending_embeddings_batch():
    conn = get_db()
    docs = conn.cursor().execute("SELECT id, domain, pdf_url FROM documents WHERE processed = 0 LIMIT 3").fetchall()
    put_db(conn)

    if not docs:
        return

    m = get_model()
    async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
        for doc_id, domain, pdf_url in docs:
            try:
                await manager.broadcast({"type": "EMBEDDING", "message": f"Auto-embedding document [{doc_id}] for {domain}..."})
                pdf_resp = await client.get(pdf_url)
                if pdf_resp.status_code != 200:
                    conn = get_db()
                    conn.cursor().execute("UPDATE documents SET processed = 1, summary = %s WHERE id = %s", (f"HTTP {pdf_resp.status_code}", doc_id))
                    put_db(conn)
                    continue

                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                    tmp.write(pdf_resp.content); tmp_path = tmp.name
                try:
                    text = await asyncio.to_thread(lambda: "".join(page.extract_text() or "" for page in PdfReader(tmp_path).pages))
                finally:
                    os.unlink(tmp_path)

                clean_text = " ".join(text.split())
                if not clean_text:
                    clean_text = f"Document from {domain} at {pdf_url}"

                sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", clean_text) if len(s.strip()) > 10] or [clean_text[:500]]
                embeddings = await asyncio.to_thread(lambda: m.encode(sentences[:20]).astype("float32"))
                agg_emb = np.mean(embeddings, axis=0).astype("float32").tolist()

                s4_key = f"embeddings/{domain}/doc-{doc_id}.txt"
                await upload_to_s4("public-documents", s4_key, clean_text.encode("utf-8"))

                conn = get_db()
                with conn.cursor() as cur:
                    cur.execute("UPDATE documents SET summary = %s, processed = 1, s4_path = %s, aggregate_embedding = %s::vector WHERE id = %s", (clean_text[:300], s4_key, agg_emb, doc_id))
                    cur.executemany("INSERT INTO chunks (doc_id, text, granularity, embedding) VALUES (%s, %s, %s, %s::vector)", [(doc_id, s, "1_sentence", emb.tolist()) for s, emb in zip(sentences[:20], embeddings)])
                    update_global_stats(cur)
                put_db(conn)

                await manager.broadcast({"type": "vector_indexed", "message": f"Successfully indexed & aggregated embedding for {domain}"})
                await manager.broadcast({"type": "stats", "stats": global_stats})
            except Exception as e:
                conn = get_db()
                conn.cursor().execute("UPDATE documents SET processed = 1, summary = %s WHERE id = %s", (str(e), doc_id))
                put_db(conn)

async def run_wikipedia_crawl_task():
    db_conn = get_db()
    with db_conn.cursor() as cur:
        cur.execute("UPDATE domains SET status = 'CRAWLING' WHERE domain = 'en.wikipedia.org'")
        update_global_stats(cur)
    put_db(db_conn)

    await manager.broadcast({"type": "domain_update", "domain": "en.wikipedia.org", "status": "CRAWLING", "county": "National", "state": "US", "tld_type": "wiki"})
    try:
        import mwparserfromhell
        async with httpx.AsyncClient(timeout=15.0) as client:
            model, r = get_model(), await client.get("https://en.wikipedia.org/w/api.php", params={"action": "query", "list": "random", "rnnamespace": "0", "rnlimit": 3, "format": "json"}, headers={"User-Agent": "WikiDataExtractor/1.0"})
            if r.status_code == 200:
                for p in r.json().get("query", {}).get("random", []):
                    pid_str, title = str(p["id"]), p.get("title", f"CurID {p['id']}")
                    article_url = f"https://en.wikipedia.org/?curid={pid_str}"
                    await manager.broadcast({"type": "wiki_progress", "queue_len": 1, "current": f"Wiki: {title}", "tld_type": "wiki", "title": title})
                    rev_r = await client.get("https://en.wikipedia.org/w/api.php", params={"action": "query", "prop": "revisions", "rvprop": "content", "rvslots": "main", "pageids": pid_str, "format": "json"}, headers={"User-Agent": "WikiDataExtractor/1.0"})
                    if rev_r.status_code == 200:
                        raw_text = rev_r.json().get("query", {}).get("pages", {}).get(pid_str, {}).get("revisions", [{}])[0].get("slots", {}).get("main", {}).get("*")
                        if raw_text:
                            cleaned = "\n\n".join(b.strip() for b in mwparserfromhell.parse(raw_text).strip_code().split('\n') if len(b.strip()) > 20)
                            if len(cleaned.split()) >= 15:
                                s4_key = f"wiki-articles/curid-{pid_str}.txt"
                                await upload_to_s4("public-documents", s4_key, cleaned.encode("utf-8"))
                                db_conn = get_db()
                                with db_conn.cursor() as cur:
                                    if not cur.execute("SELECT id FROM documents WHERE pdf_url = %s", (article_url,)).fetchone():
                                        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", cleaned) if len(s.strip()) > 10] or [cleaned[:500]]
                                        embeddings = await asyncio.to_thread(lambda: model.encode(sentences[:20]).astype("float32"))
                                        agg_emb = np.mean(embeddings, axis=0).astype("float32").tolist()

                                        cur.execute("INSERT INTO documents (domain, pdf_url, source_url, discovered_at, summary, state, county, processed, s4_path, aggregate_embedding) VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s, %s::vector) RETURNING id", ("en.wikipedia.org", article_url, article_url, datetime.utcnow().isoformat(), cleaned[:300], "US", "National", 1, s4_key, agg_emb))
                                        doc_id = cur.fetchone()[0]
                                        cur.executemany("INSERT INTO chunks (doc_id, text, granularity, embedding) VALUES (%s, %s, %s, %s::vector)", [(doc_id, s, "1_sentence", emb.tolist()) for s, emb in zip(sentences[:20], embeddings)])
                                        update_global_stats(cur)
                                        await manager.broadcast({"type": "wiki_article", "domain": "en.wikipedia.org", "pdf_url": article_url, "county": "National", "state": "US", "processed": 1, "title": title})
                                put_db(db_conn)
        db_conn = get_db()
        with db_conn.cursor() as cur:
            cur.execute("UPDATE domains SET status = 'COMPLETED', visited_count = visited_count + 1 WHERE domain = 'en.wikipedia.org'")
            update_global_stats(cur)
        put_db(db_conn)
        await manager.broadcast({"type": "domain_update", "domain": "en.wikipedia.org", "status": "COMPLETED", "county": "National", "state": "US", "tld_type": "wiki"})
        await manager.broadcast({"type": "stats", "stats": global_stats})
    except Exception as e:
        db_conn = get_db()
        db_conn.cursor().execute("UPDATE domains SET status = 'PENDING', error = %s WHERE domain = 'en.wikipedia.org'", (str(e),))
        put_db(db_conn)

@app.get("/wiki/status")
def get_wiki_status():
    conn = get_db()
    with conn.cursor() as cur:
        update_global_stats(cur)
        wiki_docs = cur.execute("SELECT COUNT(*) FROM documents WHERE domain = 'en.wikipedia.org'").fetchone()[0]
        wiki_status = (cur.execute("SELECT status FROM domains WHERE domain = 'en.wikipedia.org'").fetchone() or ["PENDING"])[0]
    put_db(conn)
    return {"state": wiki_status, "total_downloaded": wiki_docs, "total_wiki": 6800000}

@app.get("/api/catalog")
def api_catalog(page: int = Query(1, ge=1), limit: int = Query(50, ge=1, le=200)):
    offset = (page - 1) * limit
    conn = get_db()
    with conn.cursor() as cur:
        total_count = cur.execute("""
            SELECT (SELECT COUNT(*) FROM domains) + (SELECT COUNT(*) FROM documents WHERE domain = 'en.wikipedia.org')
        """).fetchone()[0] or 0

        rows = cur.execute("""
            SELECT domain, state, county, status, tld_type, 
                   (SELECT COUNT(*) FROM chunks c JOIN documents d2 ON c.doc_id = d2.id WHERE d2.domain = domains.domain) 
            FROM domains
            UNION ALL
            SELECT 'wikipedia-' || id as domain, state, county, 'COMPLETED' as status, 'wiki' as tld_type,
                   (SELECT COUNT(*) FROM chunks c WHERE c.doc_id = documents.id)
            FROM documents WHERE domain = 'en.wikipedia.org'
            LIMIT %s OFFSET %s
        """, (limit, offset)).fetchall()
    put_db(conn)
    return {
        "total": total_count,
        "items": [{
            "id": idx + offset + 1,
            "domain": r[0],
            "url": f"https://{r[0]}" if not r[0].startswith("wikipedia-") else r[0],
            "state": r[1],
            "county": r[2],
            "status": r[3],
            "tld_type": r[4],
            "chunks_count": r[5]
        } for idx, r in enumerate(rows)]
    }

@app.post("/api/catalog/scrape")
async def catalog_scrape(payload: dict):
    domain = payload.get("domain")
    if not domain:
        raise HTTPException(status_code=400, detail="Domain required")
    conn = get_db()
    row = conn.cursor().execute("SELECT state, county FROM domains WHERE domain = %s", (domain,)).fetchone()
    put_db(conn)
    if row:
        asyncio.create_task(crawl_domain(domain, row[0], row[1]))
    return {"status": "success", "message": f"Manual scrape triggered for {domain}"}

class SemanticSearchRequest(BaseModel):
    query: str
    exhaustive: bool = False
    limit: int = 20

@app.post("/search")
def search(q: SemanticSearchRequest):
    try:
        q_vec = get_model().encode([q.query]).astype("float32").tolist()[0]
        conn = get_db()
        with conn.cursor() as cur:
            rows = cur.execute(
                f"SELECT c.doc_id, c.text, c.embedding <=> %s::vector AS dist FROM chunks c ORDER BY c.embedding <=> %s::vector {'' if q.exhaustive else f'LIMIT {q.limit}'}",
                (q_vec, q_vec)
            ).fetchall()
            results, county_hits = [], {}
            for doc_id, chunk, dist in rows:
                if (doc := cur.execute("SELECT domain, pdf_url, state, county FROM documents WHERE id = %s", (doc_id,)).fetchone()):
                    d, p_url, st, cnty = doc
                    results.append({"domain": d, "pdf_url": p_url, "state": st, "county": cnty, "chunk": chunk, "score": round(float(1.0 - dist), 3)})
                    county_hits[f"{cnty}, {st}"] = county_hits.get(f"{cnty}, {st}", 0) + 1
        put_db(conn)
        return {"results": results, "county_hits": county_hits}
    except Exception as e:
        return {"results": [], "county_hits": {}, "error": str(e)}

@app.get("/graph-communities")
async def get_graph_communities(target_partitions: int = 5, k_neighbors: int = 5, sample_size: int = 150, query: str = ""):
    conn = get_db()
    rows = conn.cursor().execute("SELECT c.id, c.doc_id, c.text, c.embedding, d.pdf_url, d.state, d.county FROM chunks c JOIN documents d ON c.doc_id = d.id LIMIT 400").fetchall()
    put_db(conn)

    if not rows: return {"nodes": [], "edges": [], "community_count": 0, "modularity": 0.0, "clustering_coefficient": 0.0}
    if len(rows) > sample_size: rows = random.sample(rows, sample_size)

    try:
        vectors = np.array([json.loads(r[3]) if isinstance(r[3], str) else r[3] for r in rows], dtype="float32")
        q_vec = (lambda v: v / np.linalg.norm(v) if np.linalg.norm(v) > 0 else v)(get_model().encode([query]).astype("float32")[0]) if query.strip() else None

        G = nx.Graph()
        list(map(lambda idx_r: G.add_node(idx_r[0], full_text=idx_r[1][2], text_length=len(idx_r[1][2]), pdf_url=idx_r[1][4], state=idx_r[1][5], county=idx_r[1][6], relevance=float(np.clip((float(np.dot(vectors[idx_r[0]] / (vn := np.linalg.norm(vectors[idx_r[0]]) or 1.0), q_vec)) + 1.0) / 2.0, 0.0, 1.0)) if q_vec is not None else 0.5), enumerate(rows)))

        norms = np.linalg.norm(vectors, axis=1, keepdims=True); norms[norms == 0] = 1.0
        sims = np.dot(vectors / norms, (vectors / norms).T)
        list(map(lambda i: list(map(lambda j: sims[i][j] > 0.1 and G.add_edge(i, int(j), weight=float(sims[i][j])), np.argsort(sims[i])[::-1][1:k_neighbors+1])), range(len(rows))))

        communities = list(nx.community.louvain_communities(G, weight="weight"))
        if len(communities) > target_partitions:
            communities.sort(key=len, reverse=True)
            communities = communities[:target_partitions - 1] + [set().union(*communities[target_partitions - 1:])]

        node_comm = {n: ci for ci, cs in enumerate(communities) for n in cs}
        return {
            "nodes": [{"id": i, "group": node_comm.get(i, 0), "full_text": G.nodes[i]["full_text"], "text_length": G.nodes[i]["text_length"], "pdf_url": G.nodes[i].get("pdf_url"), "relevance": G.nodes[i].get("relevance", 0.5)} for i in G.nodes()],
            "edges": [{"source": u, "target": v, "weight": d["weight"]} for u, v, d in G.edges(data=True)],
            "community_count": len(communities),
            "modularity": round(float(nx.community.modularity(G, communities, weight="weight")), 4) if communities else 0.0,
            "clustering_coefficient": round(float(nx.average_clustering(G, weight="weight")), 4) if G.number_of_edges() > 0 else 0.0,
        }
    except Exception as e:
        return {"nodes": [], "edges": [], "community_count": 0, "modularity": 0.0, "clustering_coefficient": 0.0, "error": str(e)}

@app.get("/index-stats")
def index_stats():
    conn = get_db()
    with conn.cursor() as cur:
        update_global_stats(cur)
    put_db(conn)
    return global_stats

class MapRequest(BaseModel): counties: list[str]

@app.post("/map-status")
def map_status(req: MapRequest):
    conn = get_db()
    domains = conn.cursor().execute("SELECT county, state, status, tld_type FROM domains").fetchall()
    put_db(conn)

    gov_map, edu_map, mil_map = {}, {}, {}
    for c_name in req.counties:
        m_name = c_name.lower().replace("county", "").replace("parish", "").strip()
        g_st, e_st, m_st = "PENDING", "PENDING", "PENDING"
        for d_county, d_state, d_status, d_tld in domains:
            if not d_county: continue
            clean_org = d_county.lower().replace("county", "").replace("parish", "").strip()
            if clean_org and (m_name in clean_org or clean_org in m_name):
                tld_lower = (d_tld or "gov").strip().lower()
                if tld_lower == "edu":
                    if d_status == "COMPLETED": e_st = "COMPLETED"
                    elif d_status in ("CRAWLING", "QUEUED") and e_st != "COMPLETED": e_st = "CRAWLING"
                elif tld_lower == "mil":
                    if d_status == "COMPLETED": m_st = "COMPLETED"
                    elif d_status in ("CRAWLING", "QUEUED") and m_st != "COMPLETED": m_st = "CRAWLING"
                else:
                    if d_status == "COMPLETED": g_st = "COMPLETED"
                    elif d_status in ("CRAWLING", "QUEUED") and g_st != "COMPLETED": g_st = "CRAWLING"
        gov_map[c_name], edu_map[c_name], mil_map[c_name] = g_st, e_st, m_st
    return {"gov": gov_map, "edu": edu_map, "mil": mil_map}

@app.get("/state-stats")
def state_stats():
    conn = get_db()
    with conn.cursor() as cur:
        domains_q = cur.execute("SELECT state, tld_type, COUNT(*) FROM domains GROUP BY state, tld_type").fetchall()
        docs_q = {r[0]: r[1] for r in cur.execute("SELECT state, COUNT(*) FROM documents GROUP BY state").fetchall() if r[0]}
        chunks_q = {r[0]: r[1] for r in cur.execute("SELECT d.state, COUNT(c.id) FROM chunks c JOIN documents d ON c.doc_id = d.id GROUP BY d.state").fetchall() if r[0]}
    put_db(conn)

    abbr_to_name = {abbr.lower(): name for name, abbr in STATE_TO_ABBR.items()}
    domain_counts = {}
    for st, tld, cnt in domains_q:
        if not st: continue
        raw_st = st.strip()
        abbr = normalize_state(raw_st)
        domain_counts.setdefault(abbr, {"gov": 0, "edu": 0, "mil": 0, "total": 0})
        tld_key = "edu" if (tld or "").strip().lower() == "edu" else ("mil" if (tld or "").strip().lower() == "mil" else "gov")
        domain_counts[abbr][tld_key] += cnt
        domain_counts[abbr]["total"] += cnt

    res = {}
    for abbr, d in domain_counts.items():
        full_name = abbr_to_name.get(abbr.lower(), abbr)
        stat_obj = {
            "gov_urls": d["gov"], "edu_urls": d["edu"], "mil_urls": d["mil"], "urls": d["total"],
            "documents": docs_q.get(abbr, 0) + docs_q.get(full_name, 0) + sum(v for k, v in docs_q.items() if normalize_state(k) == abbr),
            "embeddings": chunks_q.get(abbr, 0) + chunks_q.get(full_name, 0) + sum(v for k, v in chunks_q.items() if normalize_state(k) == abbr)
        }
        res[abbr] = stat_obj
        res[full_name] = stat_obj
        res[full_name.title()] = stat_obj
    return res

@app.get("/state")
def state():
    conn = get_db()
    docs = conn.cursor().execute("SELECT domain, pdf_url, state, county, processed FROM documents ORDER BY id DESC LIMIT 50").fetchall()
    update_global_stats(conn.cursor())
    put_db(conn)
    return {"pdfs": [{"domain": d[0], "pdf_url": d[1], "state": d[2], "county": d[3], "processed": d[4]} for d in docs], "progress": global_stats}

@app.get("/", response_class=HTMLResponse)
def ui():
    return HTMLResponse(open("index.html", "r", encoding="utf-8").read()) if os.path.exists("index.html") else HTMLResponse("<h1>index.html not found</h1>", status_code=500)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)