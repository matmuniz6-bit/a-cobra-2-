#!/usr/bin/env bash
set -o pipefail 2>/dev/null || true

# repo root (seguro)
ROOT="$(pwd)"
if command -v git >/dev/null 2>&1 && git rev-parse --show-toplevel >/dev/null 2>&1; then
  ROOT="$(git rev-parse --show-toplevel)"
fi
cd "$ROOT" || exit 0

F="services/api/app/worker_triage.py"

ts(){ date +%Y%m%d-%H%M%S 2>/dev/null || echo now; }

# backup se existir
if [[ -f "$F" ]]; then
  cp -a "$F" "${F}.bak.$(ts)" 2>/dev/null || true
  echo "[WARN] Backup criado: ${F}.bak.$(ts)"
fi

mkdir -p "$(dirname "$F")" 2>/dev/null || true

cat > "$F" <<'PY'
import os
import json
import time
import asyncio
import logging
import datetime as dt
import urllib.parse
import urllib.request

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("worker_triage")

QUEUE = os.getenv("TRIAGE_QUEUE", "q:triage")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

def _send_telegram(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        log.warning("Telegram não configurado (faltou TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID).")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }
    data = urllib.parse.urlencode(payload).encode("utf-8")

    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=15) as resp:
        _ = resp.read()  # não loga token nem resposta

def _fmt(payload: dict) -> str:
    # tenta achar campos mais comuns
    id_pncp = payload.get("id_pncp") or payload.get("tender", {}).get("id_pncp") or "?"
    uf = payload.get("uf") or payload.get("tender", {}).get("uf") or "?"
    municipio = payload.get("municipio") or payload.get("tender", {}).get("municipio") or "?"
    objeto = payload.get("objeto") or payload.get("tender", {}).get("objeto") or ""

    # data_publicacao pode vir como str ou datetime
    dp = payload.get("data_publicacao") or payload.get("tender", {}).get("data_publicacao")
    if isinstance(dp, (dt.datetime, dt.date)):
        dp_s = dp.isoformat()
    else:
        dp_s = str(dp) if dp else ""

    # mensagem curta (prova de vida)
    parts = [f"✅ TRIAGE OK — {id_pncp}"]
    parts.append(f"📍 {municipio}/{uf}")
    if dp_s:
        parts.append(f"🗓 {dp_s}")
    if objeto:
        # limita tamanho
        obj = objeto.strip()
        if len(obj) > 180:
            obj = obj[:177] + "..."
        parts.append(f"🧾 {obj}")
    return "\n".join(parts)

async def _redis_client():
    # tenta Redis assíncrono primeiro (redis>=4)
    try:
        import redis.asyncio as redis_async  # type: ignore
        r = redis_async.from_url(REDIS_URL, decode_responses=True)
        return ("async", r)
    except Exception:
        pass

    # fallback: redis sync em thread
    import redis  # type: ignore
    r = redis.from_url(REDIS_URL, decode_responses=True)
    return ("sync", r)

async def main():
    mode, r = await _redis_client()
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
            text = _fmt(payload)

            log.info("Consumido %s: %s", QUEUE, payload.get("id_pncp") or payload.get("tender", {}).get("id_pncp"))
            await asyncio.to_thread(_send_telegram, text)

        except Exception as e:
            log.exception("Erro no worker_triage: %r", e)
            await asyncio.sleep(1.0)

if __name__ == "__main__":
    asyncio.run(main())
PY

echo "[OK] worker_triage.py atualizado para Telegram probe."
