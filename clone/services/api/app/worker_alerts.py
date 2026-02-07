import os
import json
import asyncio
import datetime as dt
import urllib.parse
import urllib.request

import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
ALERTS_ENABLED = os.getenv("ALERTS_ENABLED", "1").strip() not in ("0", "false", "False")
ALERTS_POLL_S = int(os.getenv("ALERTS_POLL_S", "60"))
ALERTS_COOLDOWN_S = int(os.getenv("ALERTS_COOLDOWN_S", "300"))
ALERTS_PREFIX = os.getenv("ALERTS_PREFIX", "alerts:v1")
METRICS_PREFIX = os.getenv("METRICS_PREFIX", "metrics:v1")

ALERTS_TELEGRAM_BOT_TOKEN = (os.getenv("ALERTS_TELEGRAM_BOT_TOKEN", "") or os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
ALERTS_TELEGRAM_CHAT_ID = (os.getenv("ALERTS_TELEGRAM_CHAT_ID", "") or os.getenv("TELEGRAM_CHAT_ID", "")).strip()

QUEUE_THRESHOLDS = os.getenv(
    "ALERTS_QUEUE_THRESHOLDS",
    "q:triage=500,q:fetch_parse=200,q:parse=200,q:dead_triage=1,q:dead_fetch_docs=1,q:dead_parse=1",
).strip()

COUNTER_THRESHOLDS = os.getenv(
    "ALERTS_COUNTER_THRESHOLDS",
    "api.errors_5xx_total=5,worker.triage.dead_total=1,worker.fetch_docs.dead_total=1,worker.parse.dead_total=1",
).strip()


def _parse_thresholds(raw: str) -> dict[str, int]:
    out: dict[str, int] = {}
    if not raw:
        return out
    for part in raw.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        try:
            out[k] = int(v)
        except Exception:
            continue
    return out


async def _send_telegram(text: str) -> None:
    if not ALERTS_TELEGRAM_BOT_TOKEN or not ALERTS_TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{ALERTS_TELEGRAM_BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode(
        {"chat_id": ALERTS_TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": "true"}
    ).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=15) as resp:
        resp.read()


async def _cooldown_ok(r: redis.Redis, key: str) -> bool:
    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
    gate_key = f"{ALERTS_PREFIX}:cooldown:{key}"
    try:
        if await r.exists(gate_key):
            return False
        await r.set(gate_key, str(now), ex=ALERTS_COOLDOWN_S)
        return True
    except Exception:
        return True


async def _check_queues(r: redis.Redis, thresholds: dict[str, int]) -> list[str]:
    alerts: list[str] = []
    for q, limit in thresholds.items():
        try:
            size = int(await r.llen(q))
        except Exception:
            size = -1
        if size >= limit:
            key = f"queue:{q}"
            if await _cooldown_ok(r, key):
                alerts.append(f"ALERTA: fila {q} com {size} itens (limite {limit})")
    return alerts


async def _get_counter(r: redis.Redis, name: str) -> int:
    try:
        raw = await r.get(f"{METRICS_PREFIX}:c:{name}")
        return int(raw or 0)
    except Exception:
        return 0


async def _check_counters(r: redis.Redis, thresholds: dict[str, int]) -> list[str]:
    alerts: list[str] = []
    for name, limit in thresholds.items():
        now_val = await _get_counter(r, name)
        prev_key = f"{ALERTS_PREFIX}:last:{name}"
        try:
            prev_raw = await r.get(prev_key)
            prev_val = int(prev_raw or 0)
        except Exception:
            prev_val = 0
        delta = max(0, now_val - prev_val)
        try:
            await r.set(prev_key, str(now_val), ex=ALERTS_COOLDOWN_S * 2)
        except Exception:
            pass
        if delta >= limit:
            key = f"counter:{name}"
            if await _cooldown_ok(r, key):
                alerts.append(f"ALERTA: {name} subiu +{delta} (limite {limit})")
    return alerts


async def main():
    if not ALERTS_ENABLED:
        while True:
            await asyncio.sleep(60)
            continue
    r = redis.from_url(REDIS_URL, decode_responses=True)
    queue_thresholds = _parse_thresholds(QUEUE_THRESHOLDS)
    counter_thresholds = _parse_thresholds(COUNTER_THRESHOLDS)

    while True:
        alerts: list[str] = []
        alerts.extend(await _check_queues(r, queue_thresholds))
        alerts.extend(await _check_counters(r, counter_thresholds))
        if alerts:
            msg = "\n".join(alerts)
            try:
                await _send_telegram(msg)
            except Exception:
                pass
        await asyncio.sleep(max(5, ALERTS_POLL_S))


if __name__ == "__main__":
    asyncio.run(main())
