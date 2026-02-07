#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

ts(){ date +%Y%m%d-%H%M%S 2>/dev/null || echo now; }
bk(){ [[ -f "$1" ]] && cp -a "$1" "$1.bak.$(ts)"; }

bk services/api/app/worker_triage.py

cat > services/api/app/worker_triage.py <<'PY'
import os
import json
import asyncio
import logging
import datetime as dt
import urllib.parse
import urllib.request

import asyncpg

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("worker_triage")

TRIAGE_QUEUE = os.getenv("TRIAGE_QUEUE", "q:triage")
FETCH_QUEUE  = os.getenv("FETCH_QUEUE",  "q:fetch_docs")
MIN_SCORE    = int(os.getenv("TRIAGE_MIN_SCORE", "1"))

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = (os.getenv("DATABASE_URL", "") or "").strip()

def _json_default(o):
    if isinstance(o, (dt.datetime, dt.date)):
        return o.isoformat()
    return str(o)

def _send_telegram(text: str) -> None:
    token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()
    if not token or not chat_id:
        log.warning("Telegram não configurado (faltou TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID).")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=15) as resp:
        resp.read()

def _iso(x) -> str:
    if isinstance(x, (dt.datetime, dt.date)):
        return x.isoformat()
    return str(x) if x else ""

def _fmt(info: dict, score: int | None = None) -> str:
    id_pncp = info.get("id_pncp") or "?"
    uf = info.get("uf") or "??"
    municipio = info.get("municipio") or "??"
    objeto = (info.get("objeto") or "").strip()
    dp = _iso(info.get("data_publicacao"))

    parts = [f"✅ TRIAGE OK — {id_pncp}", f"📍 {municipio}/{uf}"]
    if score is not None:
        parts.append(f"🎯 score={score}")
    if dp:
        parts.append(f"🗓 {dp}")
    if objeto:
        if len(objeto) > 180:
            objeto = objeto[:177] + "..."
        parts.append(f"🧾 {objeto}")
    return "\n".join(parts)

async def _redis_client():
    import redis.asyncio as redis_async  # type: ignore
    r = redis_async.from_url(REDIS_URL, decode_responses=True)
    return r

def _normalize_db_dsn(dsn: str) -> str:
    # asyncpg NÃO aceita "postgresql+asyncpg://"
    return (
        dsn.replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgres+asyncpg://", "postgres://")
    )

async def _db_pool():
    if not DATABASE_URL:
        log.warning("DATABASE_URL não definido (triage não vai completar dados no Postgres).")
        return None
    dsn = _normalize_db_dsn(DATABASE_URL)
    try:
        return await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    except Exception as e:
        log.exception("Falha criando pool Postgres: %r", e)
        return None

async def _db_fetch(pool, tender_id=None, id_pncp=None):
    if pool is None:
        return None
    try:
        if tender_id:
            row = await pool.fetchrow(
                "SELECT id, id_pncp, orgao, municipio, uf, modalidade, objeto, data_publicacao, status, urls "
                "FROM tender WHERE id=$1",
                int(tender_id),
            )
        elif id_pncp:
            row = await pool.fetchrow(
                "SELECT id, id_pncp, orgao, municipio, uf, modalidade, objeto, data_publicacao, status, urls "
                "FROM tender WHERE id_pncp=$1",
                str(id_pncp),
            )
        else:
            return None
        return dict(row) if row else None
    except Exception as e:
        log.exception("Falha buscando tender no Postgres: %r", e)
        return None

def _pick(payload: dict):
    # payload pode vir como {"tender": {...}} ou direto {...}
    t = payload.get("tender") if isinstance(payload.get("tender"), dict) else payload
    tender_id = t.get("id") or payload.get("id")
    id_pncp = t.get("id_pncp") or payload.get("id_pncp")
    return tender_id, id_pncp, t

def _score(t: dict) -> tuple[int, list[str]]:
    try:
        from app.triage import score_tender  # mesma imagem do api
        out = score_tender(t)
        return int(out.get("score_inicial", 0)), list(out.get("reasons", []))
    except Exception:
        return 0, []

async def main():
    r = await _redis_client()
    pool = await _db_pool()
    log.info("Worker triage iniciado. queue=%s redis=%s", TRIAGE_QUEUE, REDIS_URL)

    while True:
        try:
            item = await r.brpop(TRIAGE_QUEUE, timeout=0)  # (queue, raw)
            if not item:
                await asyncio.sleep(0.2)
                continue

            _, raw = item
            payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
            tender_id, id_pncp, t = _pick(payload)

            info = dict(t)

            # completa no DB se faltar campos
            dbinfo = await _db_fetch(pool, tender_id=tender_id, id_pncp=id_pncp)
            if dbinfo:
                info.update(dbinfo)

            score, reasons = _score(info)

            log.info("Consumido %s: id=%s id_pncp=%s score=%s reasons=%s",
                     TRIAGE_QUEUE, info.get("id") or tender_id, info.get("id_pncp") or id_pncp, score, reasons)

            # notifica telegram
            msg = _fmt(info, score=score)
            await asyncio.to_thread(_send_telegram, msg)

            # empurra pro fetch_docs se passar no filtro mínimo e tiver URL
            urls = info.get("urls") or {}
            if isinstance(urls, str):
                try:
                    urls = json.loads(urls)
                except Exception:
                    urls = {"raw": urls}

            if score >= MIN_SCORE and isinstance(urls, dict) and urls.get("pncp"):
                payload_fetch = {
                    "tender_id": info.get("id") or tender_id,
                    "id_pncp": info.get("id_pncp") or id_pncp,
                    "urls": urls,
                    "score": score,
                    "reasons": reasons,
                    "queued_at": dt.datetime.now(dt.timezone.utc),
                }
                await r.lpush(FETCH_QUEUE, json.dumps(payload_fetch, ensure_ascii=False, default=_json_default))
                log.info("Enfileirado %s: tender_id=%s id_pncp=%s", FETCH_QUEUE, payload_fetch["tender_id"], payload_fetch["id_pncp"])

        except Exception as e:
            log.exception("Erro no worker_triage: %r", e)
            await asyncio.sleep(1.0)

if __name__ == "__main__":
    asyncio.run(main())
PY

python3 -m py_compile services/api/app/worker_triage.py
echo "[OK] worker_triage.py corrigido + push p/ q:fetch_docs (backup criado)."

docker compose -f docker-compose.yml up -d --build worker
