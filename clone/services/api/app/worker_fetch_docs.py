import os, json, asyncio, logging, hashlib, re
import datetime as dt
import urllib.request
from urllib.parse import urlparse

import asyncpg
import redis.asyncio as redis

from .cache import invalidate_patterns, CACHE_PREFIX
from .metrics import incr_counter
from .normalize import normalize_tender
from .dedupe import fingerprint_tender, hash_metadados
from .events import log_event
REDIS_URL   = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUE       = os.getenv("FETCH_QUEUE", "q:fetch_parse")
PARSE_QUEUE = os.getenv("PARSE_QUEUE", "q:parse")
DEAD_QUEUE  = os.getenv("DEAD_QUEUE", "q:dead_fetch_docs")

DATABASE_URL = (os.getenv("DATABASE_URL", "") or "").strip()

LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_BYTES  = int(os.getenv("FETCH_MAX_BYTES", str(5 * 1024 * 1024)))  # 5MB default
TIMEOUT_S  = int(os.getenv("FETCH_TIMEOUT_S", "20"))
FETCH_MAX_RETRIES = int(os.getenv("FETCH_MAX_RETRIES", "3"))
FETCH_RETRY_BACKOFF_S = float(os.getenv("FETCH_RETRY_BACKOFF_S", "2.0"))
PNCP_API_BASE_URL = os.getenv("PNCP_API_BASE_URL", "https://pncp.gov.br/api/pncp")
PNCP_DOCS_ENABLED = os.getenv("PNCP_DOCS_ENABLED", "1").strip() not in ("0", "false", "False")
PNCP_DOCS_TIMEOUT_S = int(os.getenv("PNCP_DOCS_TIMEOUT_S", "20"))

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

def _pncp_parse_id(id_pncp: str | None):
    if not id_pncp:
        return None
    m = re.match(r"^(?P<cnpj>\d{14})-\d+-(?P<seq>\d+)/(?P<ano>\d{4})$", id_pncp.strip())
    if not m:
        return None
    cnpj = m.group("cnpj")
    seq = str(int(m.group("seq")))
    ano = m.group("ano")
    return cnpj, ano, seq

def _pncp_list_docs(cnpj: str, ano: str, seq: str):
    url = f"{PNCP_API_BASE_URL}/v1/orgaos/{cnpj}/compras/{ano}/{seq}/arquivos"
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=PNCP_DOCS_TIMEOUT_S) as resp:
            raw = resp.read()
            data = json.loads(raw.decode("utf-8")) if raw else {}
    except Exception:
        return []
    if isinstance(data, list):
        docs = data
    else:
        docs = data.get("documentos") or data.get("Documentos") or []
    urls = []
    for d in docs:
        u = d.get("url") if isinstance(d, dict) else None
        if u:
            urls.append(u)
    return urls

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

async def _insert_doc(pool, tender_id: int, url: str, http_status, headers, content_type, body: bytes, truncated: bool, error: str | None, source: str | None = None):
    sha = hashlib.sha256(body).hexdigest() if body else None
    size_bytes = len(body) if body else 0
    now = dt.datetime.now(dt.timezone.utc)

    if pool is None:
        return None, sha, size_bytes

    try:
        row = await pool.fetchrow(
            "INSERT INTO document (tender_id,url,source,fetched_at,http_status,content_type,sha256,size_bytes,truncated,headers,body,error) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) "
            "RETURNING id",
            int(tender_id),
            str(url),
            str(source or "unknown"),
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

async def _doc_exists(pool, tender_id: int, sha: str | None) -> bool:
    if pool is None or not sha:
        return False
    try:
        row = await pool.fetchrow(
            "SELECT id FROM document WHERE tender_id=$1 AND sha256=$2 LIMIT 1",
            int(tender_id),
            str(sha),
        )
        return bool(row)
    except Exception:
        return False

async def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    pool = await _db_pool()

    log.info("Worker fetch_docs iniciado. queue=%s redis=%s max_bytes=%s", QUEUE, REDIS_URL, MAX_BYTES)

    while True:
        if pool is None:
            pool = await _db_pool()
            if pool is None:
                await asyncio.sleep(1.0)
                continue

        item = await r.brpop(QUEUE, timeout=0)
        if not item:
            await asyncio.sleep(0.2)
            continue

        _, raw = item
        try:
            payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            payload = {"raw": raw}
        await incr_counter("worker.fetch_docs.consumed_total")

        tender_id = payload.get("tender_id") or payload.get("id") or (payload.get("tender") or {}).get("id")
        id_pncp   = payload.get("id_pncp") or (payload.get("tender") or {}).get("id_pncp")
        source    = payload.get("source") if isinstance(payload, dict) else None
        source_id = payload.get("source_id") if isinstance(payload, dict) else None
        urls      = payload.get("urls") or {}
        url       = None
        await log_event(
            pool,
            stage="fetch_docs",
            status="consumed",
            tender_id=int(tender_id) if str(tender_id).isdigit() else None,
            payload={"queue": QUEUE, "id_pncp": id_pncp, "source": source, "source_id": source_id},
        )

        # fallback: alguns produtores empacotam dados dentro de payload/tender
        inner = payload.get("payload")
        if isinstance(inner, dict):
            if not isinstance(urls, dict) or not urls:
                urls = inner.get("urls") or {}
            if not url and isinstance(inner.get("url"), str):
                url = inner["url"]

        if isinstance(urls, dict):
            url = url or urls.get("pncp") or urls.get("url")
        if not url and isinstance(payload.get("url"), str):
            url = payload["url"]
        log.info("Consumido %s: tender_id=%s id_pncp=%s url=%s", QUEUE, tender_id, id_pncp, url)

        # --- FK guard / resolução de tender (blindado) ---
        inner = payload.get("payload")
        if not isinstance(inner, dict):
            inner = {}

        tender_id_resolved = None
        db_error = False
        try:
            # 1) tender_id numérico existe?
            if tender_id is not None and str(tender_id).isdigit():
                tid = int(tender_id)
                row = await pool.fetchrow("SELECT 1 FROM tender WHERE id=$1", tid)
                if row:
                    tender_id_resolved = tid

            # 2) se não, tenta resolver pelo id_pncp
            idp = id_pncp
            if not (isinstance(idp, str) and idp.strip()):
                idp = inner.get("id_pncp")
            if tender_id_resolved is None and isinstance(idp, str) and idp.strip():
                row = await pool.fetchrow("SELECT id FROM tender WHERE id_pncp=$1", idp.strip())
                if row:
                    tender_id_resolved = int(row["id"])
            if tender_id_resolved is None and source and source_id:
                row = await pool.fetchrow("SELECT id FROM tender WHERE source=$1 AND source_id=$2", str(source), str(source_id))
                if row:
                    tender_id_resolved = int(row["id"])

            # 3) se ainda não existe, tenta upsert do tender usando metadata do payload
            if tender_id_resolved is None and isinstance(idp, str) and idp.strip():
                dp = inner.get("data_publicacao")
                dp_dt = None
                if isinstance(dp, str) and dp.strip():
                    x = dp.strip()
                    if x.endswith("Z"):
                        x = x[:-1] + "+00:00"
                    try:
                        dp_dt = __import__("datetime").datetime.fromisoformat(x)
                    except Exception:
                        dp_dt = None

                urls2 = inner.get("urls")
                if urls2 is None and isinstance(payload.get("urls"), dict):
                    urls2 = payload.get("urls")
                # garante tipo compatível com "$9::jsonb" (asyncpg espera string JSON)
                if isinstance(urls2, dict):
                    urls2 = json.dumps(urls2, ensure_ascii=False)
                elif isinstance(urls2, str):
                    urls2 = urls2.strip() or None
                else:
                    urls2 = None

                norm_payload = {
                    "id_pncp": idp.strip(),
                    "source": source if source else None,
                    "source_id": source_id if source_id else None,
                    "orgao": inner.get("orgao"),
                    "municipio": inner.get("municipio"),
                    "uf": inner.get("uf"),
                    "modalidade": inner.get("modalidade"),
                    "objeto": inner.get("objeto"),
                    "data_publicacao": dp_dt,
                    "status": inner.get("status"),
                    "urls": json.loads(urls2) if isinstance(urls2, str) else (urls2 or {}),
                }
                norm_payload = normalize_tender(norm_payload)
                h = hash_metadados(norm_payload)
                fp = fingerprint_tender(norm_payload)
                row = await pool.fetchrow(
                    """INSERT INTO tender (id_pncp, source, source_id, orgao, orgao_norm, municipio, municipio_norm, uf, uf_norm,
                                           modalidade, modalidade_norm, objeto, objeto_norm, fingerprint, data_publicacao, status, status_norm, urls, hash_metadados)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,$19)
                       ON CONFLICT (id_pncp) DO UPDATE SET
                         source=COALESCE(EXCLUDED.source, tender.source),
                         source_id=COALESCE(EXCLUDED.source_id, tender.source_id),
                         orgao=COALESCE(EXCLUDED.orgao, tender.orgao),
                         orgao_norm=COALESCE(EXCLUDED.orgao_norm, tender.orgao_norm),
                         municipio=COALESCE(EXCLUDED.municipio, tender.municipio),
                         municipio_norm=COALESCE(EXCLUDED.municipio_norm, tender.municipio_norm),
                         uf=COALESCE(EXCLUDED.uf, tender.uf),
                         uf_norm=COALESCE(EXCLUDED.uf_norm, tender.uf_norm),
                         modalidade=COALESCE(EXCLUDED.modalidade, tender.modalidade),
                         modalidade_norm=COALESCE(EXCLUDED.modalidade_norm, tender.modalidade_norm),
                         objeto=COALESCE(EXCLUDED.objeto, tender.objeto),
                         objeto_norm=COALESCE(EXCLUDED.objeto_norm, tender.objeto_norm),
                         fingerprint=COALESCE(EXCLUDED.fingerprint, tender.fingerprint),
                         data_publicacao=COALESCE(EXCLUDED.data_publicacao, tender.data_publicacao),
                         status=COALESCE(EXCLUDED.status, tender.status),
                         status_norm=COALESCE(EXCLUDED.status_norm, tender.status_norm),
                         urls=COALESCE(EXCLUDED.urls, tender.urls),
                         hash_metadados=COALESCE(EXCLUDED.hash_metadados, tender.hash_metadados),
                         updated_at=now()
                       RETURNING id""",
                    norm_payload.get("id_pncp"),
                    norm_payload.get("source"),
                    norm_payload.get("source_id"),
                    norm_payload.get("orgao"),
                    norm_payload.get("orgao_norm"),
                    norm_payload.get("municipio"),
                    norm_payload.get("municipio_norm"),
                    norm_payload.get("uf"),
                    norm_payload.get("uf_norm"),
                    norm_payload.get("modalidade"),
                    norm_payload.get("modalidade_norm"),
                    norm_payload.get("objeto"),
                    norm_payload.get("objeto_norm"),
                    fp,
                    norm_payload.get("data_publicacao"),
                    norm_payload.get("status"),
                    norm_payload.get("status_norm"),
                    json.dumps(norm_payload.get("urls") or {}, ensure_ascii=False),
                    h,
                )
                if row:
                    tender_id_resolved = int(row["id"])
                    try:
                        await pool.execute(
                            "INSERT INTO tender_version (tender_id, hash_metadados, payload) VALUES ($1,$2,$3::jsonb)",
                            int(tender_id_resolved),
                            h,
                            json.dumps(norm_payload, ensure_ascii=False),
                        )
                    except Exception:
                        pass
                    if fp:
                        try:
                            existing = await pool.fetchrow(
                                "SELECT id, canonical_tender_id FROM tender WHERE fingerprint=$1 AND id <> $2 ORDER BY id ASC LIMIT 1",
                                fp,
                                int(tender_id_resolved),
                            )
                            if existing:
                                canonical = int(existing["canonical_tender_id"] or existing["id"])
                                await pool.execute(
                                    "UPDATE tender SET canonical_tender_id=$1 WHERE id=$2",
                                    canonical,
                                    int(tender_id_resolved),
                                )
                                if existing["canonical_tender_id"] is None:
                                    await pool.execute(
                                        "UPDATE tender SET canonical_tender_id=$1 WHERE id=$2",
                                        canonical,
                                        int(existing["id"]),
                                    )
                        except Exception:
                            pass
        except Exception as e:
            log.exception("Falha resolvendo/garantindo tender no DB: %r", e)
            db_error = True

        if db_error:
            retries = int(payload.get("_retries", 0)) if isinstance(payload, dict) else 0
            if retries < FETCH_MAX_RETRIES:
                try:
                    await asyncio.sleep(FETCH_RETRY_BACKOFF_S * (retries + 1))
                    payload["_retries"] = retries + 1
                    await r.lpush(QUEUE, json.dumps(payload, ensure_ascii=False, default=_json_default))
                    await incr_counter("worker.fetch_docs.retry_total")
                    await log_event(
                        pool,
                        stage="fetch_docs",
                        status="retry_db_unavailable",
                        tender_id=tender_id_resolved,
                        payload={"queue": QUEUE, "retries": retries + 1},
                    )
                except Exception:
                    pass
            else:
                try:
                    dead = {"reason": "db_unavailable", "error": "db_unavailable", "payload": payload}
                    await r.lpush(DEAD_QUEUE, json.dumps(dead, ensure_ascii=False, default=_json_default))
                    await incr_counter("worker.fetch_docs.dead_total")
                    await log_event(
                        pool,
                        stage="fetch_docs",
                        status="dead_db_unavailable",
                        tender_id=tender_id_resolved,
                        payload={"queue": DEAD_QUEUE},
                    )
                except Exception:
                    pass
            continue

        if not tender_id_resolved or not url:
            # dead-letter: não perde o payload e evita FK
            try:
                msg_dead = {
                    "reason": "missing_tender_or_url",
                    "tender_id": tender_id,
                    "tender_id_resolved": tender_id_resolved,
                    "id_pncp": id_pncp,
                    "url": url,
                    "payload": payload,
                }
                await r.lpush(DEAD_QUEUE, json.dumps(msg_dead, ensure_ascii=False, default=_json_default))
                await incr_counter("worker.fetch_docs.missing_tender_or_url_total")
                await incr_counter("worker.fetch_docs.dead_total")
                await log_event(
                    pool,
                    stage="fetch_docs",
                    status="dead_missing_tender_or_url",
                    tender_id=tender_id_resolved,
                    payload={"queue": DEAD_QUEUE, "id_pncp": id_pncp},
                )
            except Exception:
                pass
            log.warning("Ignorando payload sem tender válido ou sem url. tender_id=%s resolved=%s id_pncp=%s url=%s",
                        tender_id, tender_id_resolved, id_pncp, url)
            continue

        # daqui pra frente, tender_id é garantido no DB
        tender_id = tender_id_resolved

        # se for URL do app PNCP, tenta listar e enfileirar PDFs
        if PNCP_DOCS_ENABLED and isinstance(url, str) and "pncp.gov.br/app/contratacoes" in url:
            pid = _pncp_parse_id(str(id_pncp) if id_pncp else None)
            if pid:
                cnpj, ano, seq = pid
                doc_urls = _pncp_list_docs(cnpj, ano, seq)
                if doc_urls:
                    for du in doc_urls:
                        msg = {
                            "tender_id": int(tender_id),
                            "id_pncp": id_pncp,
                            "url": du,
                            "urls": {"pncp_doc": du},
                            "queued_at": dt.datetime.now(dt.timezone.utc),
                        }
                        await r.lpush(QUEUE, json.dumps(msg, ensure_ascii=False, default=_json_default))
                    log.info("PNCP docs enfileirados: tender_id=%s total=%s", tender_id, len(doc_urls))
                    # pula fetch da página HTML se já achou docs
                    continue
        http_status, headers, ctype, body, truncated, error = _fetch_url(url)

        if error or http_status is None:
            retries = int(payload.get("_retries", 0)) if isinstance(payload, dict) else 0
            await incr_counter("worker.fetch_docs.error_total")
            if retries < FETCH_MAX_RETRIES:
                try:
                    await asyncio.sleep(FETCH_RETRY_BACKOFF_S * (retries + 1))
                    payload["_retries"] = retries + 1
                    await r.lpush(QUEUE, json.dumps(payload, ensure_ascii=False, default=_json_default))
                    await incr_counter("worker.fetch_docs.retry_total")
                    await log_event(
                        pool,
                        stage="fetch_docs",
                        status="retry",
                        tender_id=tender_id_resolved,
                        payload={"queue": QUEUE, "retries": retries + 1, "error": error},
                    )
                except Exception:
                    pass
                continue
            else:
                try:
                    dead = {"reason": "fetch_failed", "error": error, "payload": payload}
                    await r.lpush(DEAD_QUEUE, json.dumps(dead, ensure_ascii=False, default=_json_default))
                    await incr_counter("worker.fetch_docs.dead_total")
                    await log_event(
                        pool,
                        stage="fetch_docs",
                        status="dead_fetch_failed",
                        tender_id=tender_id_resolved,
                        payload={"queue": DEAD_QUEUE, "error": error},
                    )
                except Exception:
                    pass
                continue

        sha = hashlib.sha256(body).hexdigest() if body else None
        if await _doc_exists(pool, int(tender_id), sha):
            log.info("DUPLICATE: tender_id=%s sha=%s url=%s (skip)", tender_id, sha, url)
            await incr_counter("worker.fetch_docs.duplicate_total")
            await log_event(
                pool,
                stage="fetch_docs",
                status="duplicate_skip",
                tender_id=int(tender_id),
                payload={"sha256": sha, "url": url},
            )
            continue

        doc_id, sha, size_bytes = await _insert_doc(
            pool, int(tender_id), str(url), http_status, headers, ctype, body, truncated, error,
            source=payload.get("source") if isinstance(payload, dict) else None,
        )

        log.info("FETCH OK: doc_id=%s status=%s bytes=%s truncated=%s sha=%s err=%s",
                 doc_id, http_status, size_bytes, truncated, sha, error)
        if doc_id:
            await incr_counter("worker.fetch_docs.ok_total")
            await log_event(
                pool,
                stage="fetch_docs",
                status="ok",
                tender_id=int(tender_id),
                document_id=doc_id,
                payload={"http_status": http_status, "size_bytes": size_bytes, "truncated": truncated},
            )

        # próximo estágio (stub por enquanto)
        if doc_id:
            tender_q = f"tender_id={int(tender_id)}"
            await invalidate_patterns([f"{CACHE_PREFIX}:GET:/v1/documents/list?{tender_q}*"])
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
