#!/usr/bin/env bash
set -o pipefail 2>/dev/null || true
ts(){ date +%Y%m%d-%H%M%S 2>/dev/null || echo now; }

F="services/api/app/worker_triage.py"
if [[ -f "$F" ]]; then
  cp -a "$F" "${F}.bak.$(ts)" 2>/dev/null || true
  echo "[OK] backup: ${F}.bak.$(ts)"
fi

cat > "$F" <<'PY'
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

QUEUE = os.getenv("TRIAGE_QUEUE", "q:triage")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

def _send_telegram(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
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

def _fmt(info: dict) -> str:
    id_pncp = info.get("id_pncp") or "?"
    uf = info.get("uf") or "??"
    municipio = info.get("municipio") or "??"
    objeto = (info.get("objeto") or "").strip()
    dp = _iso(info.get("data_publicacao"))

    parts = [f"✅ TRIAGE OK — {id_pncp}", f"📍 {municipio}/{uf}"]
    if dp:
        parts.append(f"🗓 {dp}")
    if objeto:
        if len(objeto) > 180:
            objeto = objeto[:177] + "..."
        parts.append(f"🧾 {objeto}")
    return "\n".join(parts)

async def _redis_client():
    try:
        import redis.asyncio as redis_async  # type: ignore
        r = redis_async.from_url(REDIS_URL, decode_responses=True)
        return ("async", r)
    except Exception:
        import redis  # type: ignore
        r = redis.from_url(REDIS_URL, decode_responses=True)
        return ("sync", r)

async def _db_pool():
    if not DATABASE_URL:
        log.warning("DATABASE_URL não definido no worker (não vai buscar detalhes no Postgres).")
        return None
    try:
        return await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    except Exception as e:
        log.exception("Falha criando pool Postgres: %r", e)
        return None

async def _db_fetch(pool, tender_id=None, id_pncp=None):
    if pool is None:
        return None
    try:
        if tender_id:
            row = await pool.fetchrow(
                "SELECT id, id_pncp, orgao, municipio, uf, modalidade, objeto, data_publicacao, status "
                "FROM tender WHERE id=$1",
                int(tender_id),
            )
        elif id_pncp:
            row = await pool.fetchrow(
                "SELECT id, id_pncp, orgao, municipio, uf, modalidade, objeto, data_publicacao, status "
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
    municipio = t.get("municipio") or payload.get("municipio")
    uf = t.get("uf") or payload.get("uf")
    return tender_id, id_pncp, municipio, uf, t

async def main():
    mode, r = await _redis_client()
    pool = await _db_pool()

    log.info("Worker triage iniciado. queue=%s redis=%s mode=%s", QUEUE, REDIS_URL, mode)

    while True:
        try:
            if mode == "async":
                item = await r.brpop(QUEUE, timeout=0)  # (queue, raw)
            else:
                item = await asyncio.to_thread(r.brpop, QUEUE, 0)

            if not item:
                await asyncio.sleep(0.2)
                continue

            _, raw = item
            if raw is None:
                continue
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")

            payload = json.loads(raw) if isinstance(raw, str) else raw
            tender_id, id_pncp, municipio, uf, t = _pick(payload)

            # se faltou municipio/uf (ou outros), tenta completar no DB
            info = dict(t)
            if (not municipio) or (not uf) or (not info.get("objeto")):
                dbinfo = await _db_fetch(pool, tender_id=tender_id, id_pncp=id_pncp)
                if dbinfo:
                    info.update(dbinfo)

            msg = _fmt(info)
            log.info("Consumido %s: id=%s id_pncp=%s", QUEUE, tender_id, id_pncp)
            await asyncio.to_thread(_send_telegram, msg)

        except Exception as e:
            log.exception("Erro no worker_triage: %r", e)
            await asyncio.sleep(1.0)

if __name__ == "__main__":
    asyncio.run(main())
PY

echo "[OK] Patch aplicado: worker_triage agora completa municipio/uf pelo Postgres quando faltar."
