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
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
import httpx
import networkx as nx
import numpy as np
from pgvector.psycopg import register_vector
from pydantic import BaseModel
from pypdf import PdfReader
import psycopg
from psycopg_pool import ConnectionPool
import uvicorn

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@db:5432/gov_intel"
)
CSV_PATH = "data/current-full.csv"
EDU_CSV_PATH = "data/edu-domains.csv"

db_pool = ConnectionPool(
    conninfo=DATABASE_URL, min_size=2, max_size=10, open=False
)

PYTHON_STATE_TO_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC"
}

def normalize_state(st):
    if not st:
        return "US"
    s = st.strip()
    if len(s) == 2:
        return s.upper()
    return PYTHON_STATE_TO_ABBR.get(s.lower(), s.upper())


def get_db():
  conn = db_pool.getconn()
  conn.autocommit = True
  register_vector(conn)
  return conn


def put_db(conn):
  db_pool.putconn(conn)


def get_raw_db():
  return psycopg.connect(DATABASE_URL, autocommit=True)


_model = None
global_stats = {
    "visited": 0,
    "total": 0,
    "pdfs_discovered": 0,
    "domains_crawled": 0,
    "total_domains": 0,
    "counties_completed": 0,
    "total_counties": 3144,
    "gov_crawled": 0,
    "edu_crawled": 0,
    "total_gov": 0,
    "total_edu": 0,
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
  max_retries = 15
  for attempt in range(max_retries):
    try:
      with get_raw_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1;")
        break
    except Exception as e:
      if attempt == max_retries - 1:
        raise e
      time.sleep(3)

  with get_raw_db() as conn, conn.cursor() as cur:
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

  db_pool.open()
  with get_db() as conn:
    with conn.cursor() as cur:
      cur.execute("""
                CREATE TABLE IF NOT EXISTS domains (
                    domain TEXT PRIMARY KEY, state TEXT, county TEXT, status TEXT DEFAULT 'PENDING',
                    visited_count INTEGER DEFAULT 0, error TEXT, tld_type TEXT DEFAULT 'gov'
                );
                CREATE TABLE IF NOT EXISTS documents (
                    id SERIAL PRIMARY KEY, domain TEXT, pdf_url TEXT, source_url TEXT, discovered_at TEXT, 
                    summary TEXT, state TEXT, county TEXT, processed INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS chunks (
                    id SERIAL PRIMARY KEY, doc_id INTEGER REFERENCES documents(id) ON DELETE CASCADE, 
                    text TEXT, granularity TEXT DEFAULT '1_sentence', embedding vector(384)
                );
                CREATE TABLE IF NOT EXISTS page_audit (
                    id SERIAL PRIMARY KEY, domain TEXT, url TEXT, status TEXT, pdf_count INTEGER, embedding_count INTEGER, error TEXT
                );
            """)
      cur.execute(
          "CREATE INDEX IF NOT EXISTS chunks_embedding_hnsw_idx ON chunks"
          " USING hnsw (embedding vector_cosine_ops);"
      )

      # Ensure .gov domains are loaded if missing
      cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'gov'")
      if cur.fetchone()[0] == 0 and os.path.exists(CSV_PATH):
        gov_rows = []
        with open(CSV_PATH, "r", encoding="utf-8", errors="ignore") as f:
          for row in csv.DictReader(f):
            d_name = row.get("Domain name")
            if d_name:
              gov_rows.append((
                  d_name.strip().lower(),
                  normalize_state(row.get("State", "US")),
                  (
                      row.get("Organization name")
                      or row.get("City")
                      or "Unknown"
                  ).strip(),
                  "PENDING",
                  0,
                  "",
                  "gov",
              ))
        if gov_rows:
          cur.executemany(
              """
                        INSERT INTO domains (domain, state, county, status, visited_count, error, tld_type) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (domain) DO NOTHING
                    """,
              gov_rows,
          )

      # Ensure .edu domains are loaded if missing
      cur.execute("SELECT COUNT(*) FROM domains WHERE tld_type = 'edu'")
      if cur.fetchone()[0] == 0 and os.path.exists(EDU_CSV_PATH):
        edu_rows = []
        with open(EDU_CSV_PATH, "r", encoding="utf-8", errors="ignore") as f:
          reader = csv.DictReader(f)
          for row in reader:
            url_field = (
                row.get("URL")
                or row.get("Domain name")
                or row.get("domain")
                or (list(row.values())[0] if row else None)
            )
            if url_field:
              parsed_dom = (
                  urlparse(
                      url_field if "://" in url_field else f"http://{url_field}"
                  ).netloc
                  or url_field
              )
              parsed_dom = parsed_dom.replace("www.", "").strip().lower()
              if parsed_dom and parsed_dom.endswith(".edu"):
                title = (
                    row.get("Title")
                    or row.get("Organization name")
                    or row.get("institution")
                    or "Higher Education"
                )
                raw_state = row.get("State") or row.get("state") or row.get("location") or "US"
                if raw_state == "US":
                  for val in row.values():
                    val_str = str(val).strip()
                    if val_str.upper() in PYTHON_STATE_TO_ABBR.values():
                      raw_state = val_str
                      break
                    elif val_str.title() in PYTHON_STATE_TO_ABBR:
                      raw_state = val_str
                      break
                edu_rows.append((
                    parsed_dom,
                    normalize_state(raw_state),
                    title[:100],
                    "PENDING",
                    0,
                    "",
                    "edu",
                ))
        if edu_rows:
          cur.executemany(
              """
                        INSERT INTO domains (domain, state, county, status, visited_count, error, tld_type) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (domain) DO NOTHING
                    """,
              edu_rows,
          )

      cur.execute(
          "UPDATE domains SET status = 'PENDING' WHERE status IN ('CRAWLING',"
          " 'QUEUED');"
      )
      update_global_stats(cur)


def update_global_stats(cur):
  total_docs = cur.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
  total_doms = cur.execute("SELECT COUNT(*) FROM domains").fetchone()[0]
  
  # FIXED: Count distinct completed counties and cap at 3144
  completed_doms = cur.execute(
      "SELECT LEAST(3144, COUNT(DISTINCT county)) FROM domains WHERE status = 'COMPLETED'"
  ).fetchone()[0] or 0

  crawled_doms = cur.execute(
      "SELECT COUNT(*) FROM domains WHERE status != 'PENDING'"
  ).fetchone()[0]
  total_visited = (
      cur.execute("SELECT SUM(visited_count) FROM domains").fetchone()[0] or 0
  )

  global_stats.update({
      "visited": total_visited,
      "pdfs_discovered": total_docs,
      "total_domains": total_doms,
      "domains_crawled": crawled_doms,
      "counties_completed": completed_doms,
      "total": total_doms,
      "gov_crawled": cur.execute(
          "SELECT COUNT(*) FROM domains WHERE tld_type = 'gov' AND status ="
          " 'COMPLETED'"
      ).fetchone()[0],
      "edu_crawled": cur.execute(
          "SELECT COUNT(*) FROM domains WHERE tld_type = 'edu' AND status ="
          " 'COMPLETED'"
      ).fetchone()[0],
      "total_gov": cur.execute(
          "SELECT COUNT(*) FROM domains WHERE tld_type = 'gov'"
      ).fetchone()[0],
      "total_edu": cur.execute(
          "SELECT COUNT(*) FROM domains WHERE tld_type = 'edu'"
      ).fetchone()[0],
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
      domain = None
      try:
        conn = get_db()
        try:
          row = (
              conn.cursor()
              .execute(
                  "SELECT domain, state, county, tld_type FROM domains WHERE"
                  " status = 'PENDING' AND tld_type = %s LIMIT 1",
                  (active_tld_mode,),
              )
              .fetchone()
          )
          if not row:
            row = (
                conn.cursor()
                .execute(
                    "SELECT domain, state, county, tld_type FROM domains WHERE"
                    " status = 'PENDING' LIMIT 1"
                )
                .fetchone()
            )
        finally:
          put_db(conn)

        if not row:
          await asyncio.sleep(5)
          continue
        domain, state, county, tld_type = row

        conn = get_db()
        try:
          with conn.cursor() as cur:
            pending_count = cur.execute(
                "SELECT COUNT(*) FROM domains WHERE status = 'PENDING' AND"
                " tld_type = %s",
                (tld_type,),
            ).fetchone()[0]
            cur.execute(
                "UPDATE domains SET status = 'CRAWLING' WHERE domain = %s",
                (domain,),
            )
            update_global_stats(cur)
        finally:
          put_db(conn)

        await manager.broadcast({
            "type": "queue_update",
            "queue_len": pending_count - 1,
            "current": domain,
            "tld_type": tld_type,
        })
        await manager.broadcast({
            "type": "domain_update",
            "domain": domain,
            "status": "CRAWLING",
            "county": county,
            "state": state,
            "tld_type": tld_type,
        })

        await crawl_domain(domain, state, county)

        conn = get_db()
        try:
          with conn.cursor() as cur:
            cur.execute(
                "UPDATE domains SET status = 'COMPLETED' WHERE domain = %s",
                (domain,),
            )
            update_global_stats(cur)
        finally:
          put_db(conn)

        await manager.broadcast({
            "type": "domain_update",
            "domain": domain,
            "status": "COMPLETED",
            "county": county,
            "state": state,
            "tld_type": tld_type,
        })
        await manager.broadcast({"type": "stats", "stats": global_stats})
      except Exception as e:
        if domain:
          conn = get_db()
          try:
            conn.cursor().execute(
                "UPDATE domains SET status = 'PENDING', error = %s WHERE domain"
                " = %s",
                (str(e), domain),
            )
          finally:
            put_db(conn)
        await asyncio.sleep(3)

  asyncio.create_task(bg_worker())
  yield
  db_pool.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ConnectionManager:

  def __init__(self):
    self.conns = set()

  async def connect(self, ws: WebSocket):
    await ws.accept()
    self.conns.add(ws)

  def disconnect(self, ws: WebSocket):
    self.conns.discard(ws)

  async def broadcast(self, msg: dict):
    if self.conns:
      await asyncio.gather(
          *(ws.send_text(json.dumps(msg)) for ws in self.conns),
          return_exceptions=True,
      )


manager = ConnectionManager()


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
  await manager.connect(ws)
  try:
    while True:
      await ws.receive_text()
  except (WebSocketDisconnect, Exception):
    manager.disconnect(ws)


@app.post("/crawler/toggle")
def toggle_crawler():
  global crawler_paused
  crawler_paused = not crawler_paused
  return {"paused": crawler_paused}


class TldModeReq(BaseModel):
  mode: str


@app.post("/crawler/tld-mode")
def set_tld_mode(req: TldModeReq):
  global active_tld_mode
  if req.mode in ("gov", "edu"):
    active_tld_mode = req.mode
  conn = get_db()
  try:
    with conn.cursor() as cur:
      pending_count = cur.execute(
          "SELECT COUNT(*) FROM domains WHERE status = 'PENDING' AND tld_type ="
          " %s",
          (active_tld_mode,),
      ).fetchone()[0]
  finally:
    put_db(conn)
  return {"mode": active_tld_mode, "pending_count": pending_count}


@app.get("/crawler/status")
def crawler_status():
  conn = get_db()
  try:
    with conn.cursor() as cur:
      pending_count = cur.execute(
          "SELECT COUNT(*) FROM domains WHERE status = 'PENDING' AND tld_type ="
          " %s",
          (active_tld_mode,),
      ).fetchone()[0]
  finally:
    put_db(conn)
  return {
      "paused": crawler_paused,
      "mode": active_tld_mode,
      "pending_count": pending_count,
  }


async def crawl_domain(domain: str, state: str, county: str):
  base_url = f"https://{domain}"
  visited, queue, discovered_pdfs = {base_url}, [base_url], set()
  async with httpx.AsyncClient(
      follow_redirects=True,
      timeout=8.0,
      headers={"User-Agent": "GovEduPDFCrawler/1.0"},
  ) as client:
    while queue and len(visited) < 15:
      current_url = queue.pop(0)
      conn = get_db()
      try:
        with conn.cursor() as cur:
          cur.execute(
              "UPDATE domains SET visited_count = %s WHERE domain = %s",
              (len(visited), domain),
          )
          update_global_stats(cur)
      finally:
        put_db(conn)

      try:
        resp = await client.get(current_url)
        if "text/html" not in resp.headers.get("content-type", ""):
          continue
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
          abs_url = urljoin(current_url, a["href"])
          if domain in urlparse(abs_url).netloc:
            if abs_url.lower().endswith(".pdf") and abs_url not in {
                p[0] for p in discovered_pdfs
            }:
              discovered_pdfs.add((abs_url, current_url))
              global_stats["pdfs_discovered"] += 1
              conn = get_db()
              try:
                with conn.cursor() as cur:
                  if not cur.execute(
                      "SELECT id FROM documents WHERE pdf_url = %s", (abs_url,)
                  ).fetchone():
                    cur.execute(
                        "INSERT INTO documents (domain, pdf_url, source_url,"
                        " discovered_at, summary, state, county, processed)"
                        " VALUES (%s, %s, %s, %s, %s, %s, %s, 0)",
                        (
                            domain,
                            abs_url,
                            current_url,
                            datetime.utcnow().isoformat(),
                            "Pending embedding...",
                            state,
                            county,
                        ),
                    )
              finally:
                put_db(conn)
              await manager.broadcast({
                  "type": "pdf_found",
                  "domain": domain,
                  "pdf_url": abs_url,
                  "county": county,
                  "state": state,
                  "processed": 0,
              })
            elif abs_url not in visited and abs_url not in queue:
              queue.append(abs_url)
              visited.add(abs_url)
      except Exception:
        pass


@app.get("/index-stats")
def index_stats():
  conn = get_db()
  try:
    with conn.cursor() as cur:
      return {
          "index_size": cur.execute("SELECT COUNT(*) FROM chunks").fetchone()[0],
          "processed_documents": cur.execute(
              "SELECT COUNT(*) FROM documents WHERE processed = 1"
          ).fetchone()[0],
          "total_documents": cur.execute(
              "SELECT COUNT(*) FROM documents"
          ).fetchone()[0],
          "total_embeddings": cur.execute(
              "SELECT COUNT(*) FROM chunks"
          ).fetchone()[0],
      }
  finally:
    put_db(conn)


@app.get("/process-embeddings-stream")
async def process_embeddings_stream():

  async def event_generator():
    conn = get_db()
    try:
      docs = conn.cursor().execute(
          "SELECT id, domain, pdf_url FROM documents WHERE processed = 0"
      ).fetchall()
    finally:
      put_db(conn)

    if not docs:
      payload_empty = json.dumps({
          "status": "completed",
          "message": "No pending documents.",
      })
      yield f"data: {payload_empty}\n\n"
      return

    m = get_model()
    async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
      for idx, (doc_id, domain, pdf_url) in enumerate(docs):
        try:
          msg = f"Embedding [{idx+1}/{len(docs)}]: {domain}"
          yield f"data: {json.dumps({'status': 'progress', 'message': msg})}\n\n"
          pdf_resp = await client.get(pdf_url)
          if pdf_resp.status_code != 200:
            conn = get_db()
            conn.cursor().execute(
                "UPDATE documents SET processed = 1, summary = %s WHERE id ="
                " %s",
                (f"HTTP {pdf_resp.status_code}", doc_id),
            )
            put_db(conn)
            continue

          with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(pdf_resp.content)
            tmp_path = tmp.name
          try:
            text = await asyncio.to_thread(
                lambda: "".join(
                    page.extract_text() or ""
                    for page in PdfReader(tmp_path).pages
                )
            )
          finally:
            os.unlink(tmp_path)

          clean_text = " ".join(text.split())
          sentences = [
              s.strip()
              for s in re.split(r"(?<=[.!?])\s+", clean_text)
              if len(s.strip()) > 10
          ] or [clean_text[:500]]
          embeddings = await asyncio.to_thread(
              lambda: m.encode(sentences[:20]).astype("float32")
          )

          conn = get_db()
          try:
            with conn.cursor() as cur:
              cur.execute(
                  "UPDATE documents SET summary = %s, processed = 1 WHERE id ="
                  " %s",
                  (clean_text[:300], doc_id),
              )
              cur.executemany(
                  "INSERT INTO chunks (doc_id, text, granularity, embedding)"
                  " VALUES (%s, %s, %s, %s::vector)",
                  [
                      (doc_id, s, "1_sentence", emb.tolist())
                      for s, emb in zip(sentences[:20], embeddings)
                  ],
              )
          finally:
            put_db(conn)
          await manager.broadcast({
              "type": "embedding_complete",
              "metric": {"doc_id": doc_id, "domain": domain, "pdf_url": pdf_url},
          })
        except Exception as e:
          conn = get_db()
          conn.cursor().execute(
              "UPDATE documents SET processed = 1, summary = %s WHERE id = %s",
              (str(e), doc_id),
          )
          put_db(conn)
    payload_done = json.dumps({
        "status": "completed",
        "message": "All documents processed.",
    })
    yield f"data: {payload_done}\n\n"

  return StreamingResponse(
      event_generator(), media_type="text/event-stream"
  )


class MapRequest(BaseModel):
  counties: list[str]


@app.post("/map-status")
def map_status(req: MapRequest):
  conn = get_db()
  try:
    domains = conn.cursor().execute(
        "SELECT county, state, status, tld_type FROM domains"
    ).fetchall()
  finally:
    put_db(conn)

  gov_map, edu_map = {}, {}
  for c_name in req.counties:
    m_name = c_name.lower().replace("county", "").strip()
    g_st, e_st = "PENDING", "PENDING"
    for d_county, d_state, d_status, d_tld in domains:
      if not d_county:
        continue
      clean_org = (
          d_county.lower().replace("county", "").replace("parish", "").strip()
      )
      if clean_org and (
          m_name in clean_org or clean_org in m_name or m_name == "san mateo"
      ):
        if d_tld == "edu":
          if d_status == "COMPLETED":
            e_st = "COMPLETED"
          elif (
              d_status in ("CRAWLING", "QUEUED") and e_st != "COMPLETED"
          ):
            e_st = "CRAWLING"
        else:
          if d_status == "COMPLETED":
            g_st = "COMPLETED"
          elif (
              d_status in ("CRAWLING", "QUEUED") and g_st != "COMPLETED"
          ):
            g_st = "CRAWLING"
    gov_map[c_name], edu_map[c_name] = g_st, e_st
  return {"gov": gov_map, "edu": edu_map}


@app.get("/state-stats")
def state_stats():
  conn = get_db()
  try:
    with conn.cursor() as cur:
      domains_q = cur.execute(
          "SELECT state, tld_type, COUNT(*) FROM domains GROUP BY state,"
          " tld_type"
      ).fetchall()
      domain_counts, stats = {}, {}
      for st, tld, cnt in domains_q:
        if not st:
          continue
        abbr = normalize_state(st)
        if abbr not in domain_counts:
          domain_counts[abbr] = {"gov": 0, "edu": 0, "total": 0}
        tld_key = "edu" if tld and tld.strip().lower() == "edu" else "gov"
        domain_counts[abbr][tld_key] += cnt
        domain_counts[abbr]["total"] += cnt
      docs_q = {}
      for r in cur.execute(
          "SELECT state, COUNT(*) FROM documents GROUP BY state"
      ).fetchall():
        if r[0]:
          abbr = normalize_state(r[0])
          docs_q[abbr] = docs_q.get(abbr, 0) + r[1]
      chunks_q = {}
      for r in cur.execute(
          "SELECT d.state, COUNT(c.id) FROM chunks c JOIN documents d ON"
          " c.doc_id = d.id GROUP BY d.state"
      ).fetchall():
        if r[0]:
          abbr = normalize_state(r[0])
          chunks_q[abbr] = chunks_q.get(abbr, 0) + r[1]
  finally:
    put_db(conn)

  for st in set(
      list(domain_counts.keys())
      + list(docs_q.keys())
      + list(chunks_q.keys())
  ):
    d_stat = domain_counts.get(st, {"gov": 0, "edu": 0, "total": 0})
    stats[st] = {
        "gov_urls": d_stat["gov"],
        "edu_urls": d_stat["edu"],
        "edu_domains": d_stat["edu"],
        "urls": d_stat["total"],
        "documents": docs_q.get(st, 0),
        "embeddings": chunks_q.get(st, 0),
    }
  return stats


@app.get("/graph-communities")
async def get_graph_communities(
    target_partitions: int = 5,
    k_neighbors: int = 5,
    level: str = "chunk",
    sample_size: int = 150,
):
  conn = get_db()
  try:
    rows = conn.cursor().execute(
        "SELECT c.id, c.doc_id, c.text, c.embedding, d.pdf_url, d.state,"
        " d.county FROM chunks c JOIN documents d ON c.doc_id = d.id LIMIT 300"
    ).fetchall()
  finally:
    put_db(conn)

  if not rows:
    return {
        "nodes": [],
        "edges": [],
        "community_count": 0,
        "modularity": 0.0,
        "clustering_coefficient": 0.0,
    }
  if len(rows) > sample_size:
    rows = random.sample(rows, sample_size)

  try:
    vectors = np.array(
        [json.loads(r[3]) if isinstance(r[3], str) else r[3] for r in rows],
        dtype="float32",
    )
    G = nx.Graph()
    for idx, r in enumerate(rows):
      G.add_node(
          idx,
          full_text=r[2],
          text_length=len(r[2]),
          pdf_url=r[4],
          state=r[5],
          county=r[6],
      )

    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    sims = np.dot(vectors / norms, (vectors / norms).T)
    for i in range(len(rows)):
      for j in np.argsort(sims[i])[::-1][1 : k_neighbors + 1]:
        if sims[i][j] > 0.1:
          G.add_edge(i, int(j), weight=float(sims[i][j]))

    communities = list(nx.community.louvain_communities(G, weight="weight"))
    node_comm = {n: ci for ci, cs in enumerate(communities) for n in cs}
    return {
        "nodes": [
            {
                "id": i,
                "group": node_comm.get(i, 0),
                "full_text": G.nodes[i]["full_text"],
                "text_length": G.nodes[i]["text_length"],
                "pdf_url": G.nodes[i].get("pdf_url"),
            }
            for i in G.nodes()
        ],
        "edges": [
            {"source": u, "target": v, "weight": d["weight"]}
            for u, v, d in G.edges(data=True)
        ],
        "community_count": len(communities),
        "modularity": round(
            float(
                nx.community.modularity(G, communities, weight="weight")
            ),
            4,
        )
        if communities
        else 0.0,
        "clustering_coefficient": round(
            float(nx.average_clustering(G, weight="weight")), 4
        )
        if G.number_of_edges() > 0
        else 0.0,
    }
  except Exception as e:
    return {
        "nodes": [],
        "edges": [],
        "community_count": 0,
        "modularity": 0.0,
        "clustering_coefficient": 0.0,
        "error": str(e),
    }


class Q(BaseModel):
  query: str


@app.post("/search")
def search(q: Q):
  try:
    model = get_model()
    q_vec = model.encode([q.query]).astype("float32").tolist()[0]
    conn = get_db()
    try:
      with conn.cursor() as cur:
        rows = cur.execute(
            "SELECT c.doc_id, c.text, c.embedding <=> %s::vector AS dist FROM"
            " chunks c ORDER BY c.embedding <=> %s::vector LIMIT 10",
            (q_vec, q_vec),
        ).fetchall()
        results, county_hits = [], {}
        for doc_id, chunk, dist in rows:
          doc = cur.execute(
              "SELECT domain, pdf_url, state, county FROM documents WHERE id ="
              " %s",
              (doc_id,),
          ).fetchone()
          if doc:
            d, p_url, st, cnty = doc
            results.append({
                "domain": d,
                "pdf_url": p_url,
                "state": st,
                "county": cnty,
                "chunk": chunk[:200],
    "chunk_length": len(chunk),
                "score": round(float(1.0 - dist), 3),
            })
            county_hits[f"{cnty}, {st}"] = (
                county_hits.get(f"{cnty}, {st}", 0) + 1
            )
    finally:
      put_db(conn)
    return {"results": results, "county_hits": county_hits}
  except Exception as e:
    return {"results": [], "county_hits": {}, "error": str(e)}


@app.get("/domains-status")
def domains_status():
  conn = get_db()
  try:
    return [
        {
            "domain": r[0],
            "state": r[1],
            "county": r[2],
            "status": r[3],
            "tld_type": r[4],
        }
        for r in conn.cursor().execute(
            "SELECT domain, state, county, status, tld_type FROM domains"
        ).fetchall()
    ]
  finally:
    put_db(conn)


@app.get("/state")
def state():
  conn = get_db()
  try:
    docs = conn.cursor().execute(
        "SELECT domain, pdf_url, state, county, processed FROM documents ORDER"
        " BY id DESC LIMIT 50"
    ).fetchall()
    update_global_stats(conn.cursor())
  finally:
    put_db(conn)
  return {
      "pdfs": [
          {
              "domain": d[0],
              "pdf_url": d[1],
              "state": d[2],
              "county": d[3],
              "processed": d[4],
          }
          for d in docs
      ],
      "progress": global_stats,
  }


@app.get("/")
def ui():
  if os.path.exists("index.html"):
    with open("index.html", "r", encoding="utf-8") as f:
      return HTMLResponse(f.read())
  return HTMLResponse("<h1>index.html not found</h1>", status_code=500)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)