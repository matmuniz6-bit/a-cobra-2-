#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

echo "== 1) Criando tabela document (se não existir) =="
set -a; [ -f .env ] && source .env; set +a

docker compose -f docker-compose.yml exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
CREATE TABLE IF NOT EXISTS document (
  id          BIGSERIAL PRIMARY KEY,
  tender_id   BIGINT NOT NULL REFERENCES tender(id) ON DELETE CASCADE,
  url         TEXT NOT NULL,
  source      TEXT DEFAULT 'pncp',
  fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  http_status INT,
  content_type TEXT,
  sha256      TEXT,
  size_bytes  INT,
  truncated   BOOLEAN NOT NULL DEFAULT false,
  headers     JSONB,
  body        BYTEA,
  error       TEXT
);

-- evita duplicar o mesmo doc (quando tiver hash)
CREATE UNIQUE INDEX IF NOT EXISTS document_uniq
  ON document (tender_id, url, sha256);
SQL

echo "== 2) Atualizando worker_fetch_docs.py (fetch real + grava no DB) =="

cp -a services/api/app/worker_fetch_docs.py "services/api/app/worker_fetch_docs.py.bak.$(date +%Y%m%d-%H%M%S 2>/dev/null || echo now)" 2>/dev/null || true

cat > services/api/app/worker_fetch_docs.py <<'PY'
import os, json, asyncio, logging, hashlib
import datetime as dt
import urllib.request
from urllib.parse import urlparse

import asyncpg
import redis.asyncio as redis

REDIS_URL   = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUE       = os.getenv("FETCH_QUEUE", "q:fetch_docs")
PARSE_QUEUE = os.getenv("PARSE_QUEUE", "q:parse")

DATABASE_URL = (os.getenv("DATABASE_URL", "") or "").strip()

LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_BYTES  = int(os.getenv("FETCH_MAX_BYTES", str(5 * 1024 * 1024)))  # 5MB default
TIMEOUT_S  = int(os.getenv("FETCH_TIMEOUT_S", "20"))

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fetch_docs")

def _normalize_db_dsn(dsn: str) -> str:
    # asyncpg NÃO aceita postgresql+asyncpg://
    return (
        dsn.replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgres+asyncpg://", "postgres://")
    )

def _json_default(o):
    if isinstance(o, (dt.datetime, dt.date)):
        return o.isoformat()
    return str(o)

def _fetch_url(url: str):
    # retorna: (http_status, headers_dict, content_type, body_bytes, truncated, error)
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "a-cobra/0.1 (+fetch_docs)",
                "Accept": "*/*",
            },
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
            status = getattr(resp, "status", None) or 200
            headers = dict(resp.headers.items()) if getattr(resp, "headers", None) else {}
            ctype = headers.get("Content-Type") or headers.get("content-type") or ""
            buf = bytearray()
            truncated = False
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                if len(buf) + len(chunk) > MAX_BYTES:
                    remaining = MAX_BYTES - len(buf)
                    if remaining > 0:
                        buf.extend(chunk[:remaining])
                    truncated = True
                    break
                buf.extend(chunk)
            return status, headers, ctype, bytes(buf), truncated, None
    except Exception as e:
        return None, None, None, b"", False, repr(e)

async def _db_pool():
    if not DATABASE_URL:
        log.warning("DATABASE_URL vazio — não vou gravar no Postgres.")
        return None
    dsn = _normalize_db_dsn(DATABASE_URL)
    try:
        return await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    except Exception as e:
        log.exception("Falha criando pool Postgres: %r", e)
        return None

async def _insert_doc(pool, tender_id: int, url: str, http_status, headers, content_type, body: bytes, truncated: bool, error: str | None):
    sha = hashlib.sha256(body).hexdigest() if body else None
    size_bytes = len(body) if body else 0
    now = dt.datetime.now(dt.timezone.utc)

    if pool is None:
        return None, sha, size_bytes

    try:
        row = await pool.fetchrow(
            "INSERT INTO document (tender_id,url,source,fetched_at,http_status,content_type,sha256,size_bytes,truncated,headers,body,error) "
            "VALUES ($1,$2,'pncp',$3,$4,$5,$6,$7,$8,$9,$10,$11) "
            "RETURNING id",
            int(tender_id),
            str(url),
            now,
            http_status,
            content_type,
            sha,
            int(size_bytes),
            bool(truncated),
            json.dumps(headers or {}, ensure_ascii=False) if headers is not None else None,
            body if body else None,
            error,
        )
        doc_id = int(row["id"]) if row else None
        return doc_id, sha, size_bytes
    except Exception as e:
        log.exception("Falha inserindo document no Postgres: %r", e)
        return None, sha, size_bytes

async def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    pool = await _db_pool()

    log.info("Worker fetch_docs iniciado. queue=%s redis=%s max_bytes=%s", QUEUE, REDIS_URL, MAX_BYTES)

    while True:
        item = await r.brpop(QUEUE, timeout=0)
        if not item:
            await asyncio.sleep(0.2)
            continue

        _, raw = item
        try:
            payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            payload = {"raw": raw}

        tender_id = payload.get("tender_id") or payload.get("id") or (payload.get("tender") or {}).get("id")
        id_pncp   = payload.get("id_pncp") or (payload.get("tender") or {}).get("id_pncp")
        urls      = payload.get("urls") or {}
        url       = None

        if isinstance(urls, dict):
            url = urls.get("pncp") or urls.get("url")
        if not url and isinstance(payload.get("url"), str):
            url = payload["url"]

        log.info("Consumido %s: tender_id=%s id_pncp=%s url=%s", QUEUE, tender_id, id_pncp, url)

        if not tender_id or not url:
            log.warning("Payload sem tender_id ou sem url. payload=%s", payload)
            continue

        http_status, headers, ctype, body, truncated, error = _fetch_url(url)

        doc_id, sha, size_bytes = await _insert_doc(
            pool, int(tender_id), str(url), http_status, headers, ctype, body, truncated, error
        )

        log.info("FETCH OK: doc_id=%s status=%s bytes=%s truncated=%s sha=%s err=%s",
                 doc_id, http_status, size_bytes, truncated, sha, error)

        # próximo estágio (stub por enquanto)
        if doc_id:
            msg = {
                "document_id": doc_id,
                "tender_id": int(tender_id),
                "id_pncp": id_pncp,
                "url": url,
                "sha256": sha,
                "queued_at": dt.datetime.now(dt.timezone.utc),
            }
            await r.lpush(PARSE_QUEUE, json.dumps(msg, ensure_ascii=False, default=_json_default))

if __name__ == "__main__":
    asyncio.run(main())
PY

python3 -m py_compile services/api/app/worker_fetch_docs.py
echo "[OK] worker_fetch_docs.py atualizado."

echo "== 3) Rebuild e restart do serviço fetch_docs =="
docker compose -f docker-compose.yml build fetch_docs
docker compose -f docker-compose.yml up -d fetch_docs

echo "[OK] fetch_docs rebuild+up concluído."
