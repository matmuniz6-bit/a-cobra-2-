import os
import time
import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse

from .cache import (
    get_cached_response,
    set_cached_response,
    _incr_metric,
    should_attempt_cache,
    get_cache_metrics,
    try_acquire_lock,
    release_lock,
    wait_for_cache_fill,
)
from .metrics import (
    incr_counter,
    incr_counter_labeled,
    observe_histogram,
    set_gauge,
    get_counters,
    get_gauges,
    get_queue_lengths,
    render_prometheus,
    DEFAULT_COUNTERS,
    DEFAULT_GAUGES,
    DEFAULT_QUEUE_LIST,
)
from .db import init_pool, close_pool, get_pool

app = FastAPI(title="a-cobra-core", version="0.1.0")

from .routes.tenders import router as tenders_router
app.include_router(tenders_router)
from .routes.queueing import router as ingest_router
app.include_router(ingest_router)
from .routes.users import router as users_router
app.include_router(users_router)
from .routes.subscriptions import router as subscriptions_router
app.include_router(subscriptions_router)
from .routes.segments import router as segments_router
app.include_router(segments_router)
from .routes.insights import router as insights_router
app.include_router(insights_router)
from .routes.documents import router as documents_router
app.include_router(documents_router)
from .routes.events import router as events_router
app.include_router(events_router)

@app.on_event("startup")
async def _startup() -> None:
    try:
        await init_pool()
    except Exception:
        pass

@app.on_event("shutdown")
async def _shutdown() -> None:
    try:
        await close_pool()
    except Exception:
        pass


DATABASE_URL = os.getenv("DATABASE_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
AUTH_REQUIRED = os.getenv("AUTH_REQUIRED", "1").strip() not in ("0", "false", "False")
API_KEYS = [k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()]
AUTH_PUBLIC_PATHS = [p.strip() for p in os.getenv(
    "AUTH_PUBLIC_PATHS",
    "/health,/health/cache,/health/queue,/metrics,/metrics/basic",
).split(",") if p.strip()]
RATE_LIMIT_RPM = int(os.getenv("RATE_LIMIT_RPM", "300"))
RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "1").strip() not in ("0", "false", "False")
RATE_LIMIT_BYPASS_KEYS = [k.strip() for k in os.getenv("RATE_LIMIT_BYPASS_KEYS", "").split(",") if k.strip()]

if AUTH_REQUIRED and not API_KEYS:
    print("WARN: AUTH_REQUIRED=1 but API_KEYS is empty. All non-public requests will be blocked.")

def _pg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _is_public_path(path: str) -> bool:
    if not path:
        return False
    return path in AUTH_PUBLIC_PATHS


def _extract_api_key(request) -> str | None:
    key = request.headers.get("x-api-key")
    if key:
        return key.strip()
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return None


_redis_client: redis.Redis | None = None

def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(REDIS_URL)
    return _redis_client

async def _rate_limit_check(key: str, path: str) -> bool:
    if not RATE_LIMIT_ENABLED:
        return True
    if not key:
        return False
    if key in RATE_LIMIT_BYPASS_KEYS:
        return True
    try:
        r = _get_redis()
        bucket = int(time.time() // 60)
        rate_key = f"ratelimit:v1:{key}:{bucket}"
        count = await r.incr(rate_key)
        await r.expire(rate_key, 120)
        return int(count) <= RATE_LIMIT_RPM
    except Exception:
        return True


class CacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        cached = await get_cached_response(request)
        if cached is not None:
            return cached

        if should_attempt_cache(request):
            await _incr_metric("miss")

        lock_acquired = False
        if should_attempt_cache(request):
            lock_acquired = bool(await try_acquire_lock(request))
            if not lock_acquired:
                filled = await wait_for_cache_fill(request)
                if filled is not None:
                    return filled

        response = await call_next(request)
        if lock_acquired and not getattr(response, "body_iterator", None):
            await release_lock(request)

        body = b""
        if getattr(response, "body_iterator", None) is not None:
            async for chunk in response.body_iterator:
                body += chunk
            new_response = StarletteResponse(
                content=body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type,
            )
            if lock_acquired:
                await set_cached_response(request, new_response, body)
                await release_lock(request)
            new_response.headers["x-cache"] = "miss"
            return new_response

        if should_attempt_cache(request):
            try:
                response.headers["x-cache"] = "miss"
            except Exception:
                pass
        return response


app.add_middleware(CacheMiddleware)

@app.middleware("http")
async def metrics_middleware(request, call_next):
    start = time.perf_counter()
    status = 500
    route = None
    try:
        response = await call_next(request)
        status = int(getattr(response, "status_code", 500) or 500)
        return response
    except Exception:
        await incr_counter("api.exceptions_total")
        raise
    finally:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        try:
            route = request.scope.get("route").path  # type: ignore[union-attr]
        except Exception:
            route = None
        if not route:
            route = request.url.path
        await incr_counter("api.requests_total")
        if status >= 500:
            await incr_counter("api.errors_5xx_total")
        elif status >= 400:
            await incr_counter("api.errors_4xx_total")
        await set_gauge("api.last_request_ms", elapsed_ms)
        await observe_histogram("api.request_duration_ms", float(elapsed_ms))
        await incr_counter_labeled(
            "api.requests_by_route_total",
            {"route": route, "status": str(status)},
        )

@app.middleware("http")
async def auth_middleware(request, call_next):
    if not AUTH_REQUIRED:
        return await call_next(request)
    if _is_public_path(request.url.path):
        return await call_next(request)
    key = _extract_api_key(request)
    if not key or key not in API_KEYS:
        return StarletteResponse(content="unauthorized", status_code=401)
    ok = await _rate_limit_check(key, request.url.path)
    if not ok:
        return StarletteResponse(content="rate_limited", status_code=429)
    return await call_next(request)

@app.get("/health")
async def health():
    checks = {"db": "unknown", "redis": "unknown"}
    ok = True

    try:
        pool = get_pool()
        if pool is None:
            pool = await init_pool()
        async with pool.acquire() as conn:
            v = await conn.fetchval("select 1;")
        checks["db"] = "ok" if v == 1 else "bad"
        ok = ok and (v == 1)
    except Exception as e:
        checks["db"] = f"error:{type(e).__name__}"
        ok = False

    try:
        r = redis.from_url(REDIS_URL)
        pong = await r.ping()
        await r.close()
        checks["redis"] = "ok" if pong else "bad"
        ok = ok and bool(pong)
    except Exception as e:
        checks["redis"] = f"error:{type(e).__name__}"
        ok = False

    return {"ok": ok, "checks": checks}


@app.get("/health/cache")
async def health_cache():
    return await get_cache_metrics()

@app.get("/health/queue")
async def health_queue():
    return await get_queue_lengths(DEFAULT_QUEUE_LIST)

@app.get("/metrics/basic")
async def metrics_basic():
    counters = await get_counters(DEFAULT_COUNTERS)
    gauges = await get_gauges(DEFAULT_GAUGES)
    queues = await get_queue_lengths(DEFAULT_QUEUE_LIST)
    cache = await get_cache_metrics()
    return {"counters": counters, "gauges": gauges, "queues": queues, "cache": cache}

@app.get("/metrics")
async def metrics_prometheus():
    payload = await render_prometheus()
    return StarletteResponse(content=payload, media_type="text/plain; version=0.0.4")
