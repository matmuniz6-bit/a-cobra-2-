import os, json
import datetime as dt
import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUE_MAX_LEN = int(os.getenv("QUEUE_MAX_LEN", "10000"))

def _json_default(o):
    if isinstance(o, (dt.datetime, dt.date)):
        return o.isoformat()
    return str(o)

def client():
    return redis.from_url(REDIS_URL, decode_responses=True)

async def push(queue: str, payload: dict):
    r = client()
    if QUEUE_MAX_LEN > 0:
        size = await r.llen(queue)
        if int(size) >= QUEUE_MAX_LEN:
            await r.close()
            raise RuntimeError("queue_full")
    await r.lpush(queue, json.dumps(payload, ensure_ascii=False, default=_json_default))
    await r.close()

async def pop_block(queue: str, timeout: int = 5):
    r = client()
    item = await r.brpop(queue, timeout=timeout)
    await r.close()
    if not item:
        return None
    _q, raw = item
    return json.loads(raw)
