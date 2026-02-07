import os
import json
import base64
import asyncio
from typing import Optional, Tuple

import redis.asyncio as redis
from fastapi import Request, Response

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

CACHE_ENABLED = os.getenv("CACHE_ENABLED", "1").strip() not in ("0", "false", "False")
CACHE_PREFIX = os.getenv("CACHE_PREFIX", "api-cache:v1")
CACHE_TTL_S = int(os.getenv("CACHE_TTL_S", "60"))
CACHE_TTL_S_MAP = os.getenv("CACHE_TTL_S_MAP", "").strip()
CACHE_MAX_BYTES = int(os.getenv("CACHE_MAX_BYTES", str(512 * 1024)))  # 512KB default
CACHE_METRICS_TTL_S = int(os.getenv("CACHE_METRICS_TTL_S", str(7 * 24 * 3600)))
CACHE_LOCK_TTL_S = int(os.getenv("CACHE_LOCK_TTL_S", "8"))
CACHE_LOCK_WAIT_MS = int(os.getenv("CACHE_LOCK_WAIT_MS", "200"))

_redis_client: Optional[redis.Redis] = None


def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(REDIS_URL)
    return _redis_client


def _normalize_query(params) -> str:
    if not params:
        return ""
    items = sorted((k, v) for k, v in params.multi_items())
    return "&".join([f"{k}={v}" for k, v in items])


def _cache_key(request: Request) -> str:
    # Varia por método, path, query e alguns headers que afetam representação.
    method = request.method.upper()
    path = request.url.path
    query = _normalize_query(request.query_params)
    accept = (request.headers.get("accept") or "").lower()
    lang = (request.headers.get("accept-language") or "").lower()
    return f"{CACHE_PREFIX}:{method}:{path}?{query}|a={accept}|l={lang}"


def _lock_key(cache_key: str) -> str:
    return f"{cache_key}:lock"


def _bypass_cache(request: Request) -> bool:
    if request.headers.get("x-cache-bypass") in ("1", "true", "True"):
        return True
    if request.headers.get("authorization"):
        return True
    if request.headers.get("cookie"):
        return True
    if request.query_params.get("cache") in ("0", "false"):
        return True
    return False


def should_attempt_cache(request: Request) -> bool:
    if not CACHE_ENABLED:
        return False
    if request.method.upper() != "GET":
        return False
    if _bypass_cache(request):
        return False
    return True


def _ttl_for_path(path: str) -> int:
    if not CACHE_TTL_S_MAP:
        return CACHE_TTL_S
    try:
        mapping = json.loads(CACHE_TTL_S_MAP)
        if isinstance(mapping, dict):
            # Longest prefix wins
            best = None
            for prefix, ttl in mapping.items():
                if isinstance(prefix, str) and path.startswith(prefix):
                    if best is None or len(prefix) > len(best[0]):
                        best = (prefix, int(ttl))
            if best:
                return max(1, int(best[1]))
    except Exception:
        return CACHE_TTL_S
    return CACHE_TTL_S


async def _incr_metric(name: str) -> None:
    if not CACHE_ENABLED:
        return
    try:
        r = _get_redis()
        key = f"{CACHE_PREFIX}:metrics:{name}"
        await r.incr(key)
        await r.expire(key, CACHE_METRICS_TTL_S)
    except Exception:
        return


async def get_cached_response(request: Request) -> Optional[Response]:
    if not CACHE_ENABLED:
        return None
    if request.method.upper() != "GET":
        return None
    if _bypass_cache(request):
        return None

    r = _get_redis()
    key = _cache_key(request)
    raw = await r.get(key)
    if not raw:
        return None

    try:
        payload = json.loads(raw.decode("utf-8"))
        body = base64.b64decode(payload["body_b64"])
        status = int(payload["status"])
        headers = payload.get("headers") or {}
        resp = Response(content=body, status_code=status)
        for k, v in headers.items():
            resp.headers[k] = v
        resp.headers["x-cache"] = "hit"
        await _incr_metric("hit")
        return resp
    except Exception:
        return None


def _should_cache_response(request: Request, response: Response, body: bytes) -> bool:
    if not CACHE_ENABLED:
        return False
    if request.method.upper() != "GET":
        return False
    if _bypass_cache(request):
        return False
    if response.status_code != 200:
        return False
    if response.headers.get("set-cookie"):
        return False
    ctype = (response.headers.get("content-type") or "").lower()
    if "application/json" not in ctype:
        return False
    if len(body) > CACHE_MAX_BYTES:
        return False
    if response.headers.get("x-cache-skip") in ("1", "true", "True"):
        return False
    return True


async def set_cached_response(request: Request, response: Response, body: bytes) -> None:
    if not _should_cache_response(request, response, body):
        return
    payload = {
        "status": response.status_code,
        "headers": {
            "content-type": response.headers.get("content-type"),
        },
        "body_b64": base64.b64encode(body).decode("ascii"),
    }
    r = _get_redis()
    key = _cache_key(request)
    ttl = _ttl_for_path(request.url.path)
    await r.set(key, json.dumps(payload, ensure_ascii=False), ex=ttl)


async def try_acquire_lock(request: Request) -> bool:
    if not CACHE_ENABLED:
        return False
    r = _get_redis()
    key = _lock_key(_cache_key(request))
    try:
        return await r.set(key, "1", nx=True, ex=CACHE_LOCK_TTL_S)
    except Exception:
        return False


async def release_lock(request: Request) -> None:
    if not CACHE_ENABLED:
        return
    r = _get_redis()
    key = _lock_key(_cache_key(request))
    try:
        await r.delete(key)
    except Exception:
        return


async def wait_for_cache_fill(request: Request) -> Optional[Response]:
    if not CACHE_ENABLED:
        return None
    await asyncio.sleep(max(0, CACHE_LOCK_WAIT_MS) / 1000)
    return await get_cached_response(request)


async def invalidate_path_prefixes(path_prefixes: list[str]) -> int:
    if not CACHE_ENABLED:
        return 0
    if not path_prefixes:
        return 0
    r = _get_redis()
    deleted = 0
    try:
        for path_prefix in path_prefixes:
            pattern = f"{CACHE_PREFIX}:GET:{path_prefix}*"
            async for key in r.scan_iter(match=pattern, count=500):
                await r.delete(key)
                deleted += 1
    except Exception:
        return deleted
    return deleted


async def invalidate_patterns(patterns: list[str]) -> int:
    if not CACHE_ENABLED:
        return 0
    if not patterns:
        return 0
    r = _get_redis()
    deleted = 0
    try:
        for pattern in patterns:
            async for key in r.scan_iter(match=pattern, count=500):
                await r.delete(key)
                deleted += 1
    except Exception:
        return deleted
    return deleted


async def get_cache_metrics() -> dict:
    if not CACHE_ENABLED:
        return {"enabled": False, "hit": 0, "miss": 0}
    r = _get_redis()
    try:
        hit = await r.get(f"{CACHE_PREFIX}:metrics:hit")
        miss = await r.get(f"{CACHE_PREFIX}:metrics:miss")
        return {
            "enabled": True,
            "hit": int(hit or 0),
            "miss": int(miss or 0),
        }
    except Exception:
        return {"enabled": True, "hit": 0, "miss": 0}
