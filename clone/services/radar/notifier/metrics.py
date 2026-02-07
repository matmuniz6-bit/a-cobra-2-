import os

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
METRICS_ENABLED = os.getenv("METRICS_ENABLED", "1").strip() not in ("0", "false", "False")
METRICS_PREFIX = os.getenv("METRICS_PREFIX", "metrics:v1")
METRICS_TTL_S = int(os.getenv("METRICS_TTL_S", str(7 * 24 * 3600)))

_redis_client: redis.Redis | None = None


def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(REDIS_URL)
    return _redis_client


def incr_counter(name: str, value: int = 1) -> None:
    if not METRICS_ENABLED:
        return
    try:
        r = _get_redis()
        key = f"{METRICS_PREFIX}:c:{name}"
        r.incrby(key, int(value))
        r.expire(key, METRICS_TTL_S)
    except Exception:
        return
