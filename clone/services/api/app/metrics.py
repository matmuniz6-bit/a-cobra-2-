import os
from typing import Iterable

import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CACHE_PREFIX = os.getenv("CACHE_PREFIX", "api-cache:v1")
METRICS_ENABLED = os.getenv("METRICS_ENABLED", "1").strip() not in ("0", "false", "False")
METRICS_PREFIX = os.getenv("METRICS_PREFIX", "metrics:v1")
METRICS_TTL_S = int(os.getenv("METRICS_TTL_S", str(7 * 24 * 3600)))

_redis_client: redis.Redis | None = None


def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(REDIS_URL)
    return _redis_client


def _split_env_list(name: str, default: list[str]) -> list[str]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    items = [x.strip() for x in raw.split(",") if x.strip()]
    return items or default


DEFAULT_QUEUE_LIST = _split_env_list(
    "QUEUE_METRICS_LIST",
    [
        "q:triage",
        "q:fetch_parse",
        "q:parse",
        "q:parse_smoke",
        "q:dead_triage",
        "q:dead_fetch_docs",
        "q:dead_parse",
    ],
)

DEFAULT_COUNTERS = [
    "api.requests_total",
    "api.errors_4xx_total",
    "api.errors_5xx_total",
    "api.exceptions_total",
    "api.ingest.queued_total",
    "api.ingest.queue_full_total",
    "api.ingest.error_total",
    "agent.enrich.ok_total",
    "agent.enrich.error_total",
    "agent.enrich.skip_total",
    "bot.updates_total",
    "bot.messages_total",
    "bot.commands_total",
    "bot.callbacks_total",
    "bot.errors_total",
    "notifier.requests_total",
    "notifier.sent_total",
    "notifier.errors_total",
    "worker.compras_fetch.batch_ok_total",
    "worker.compras_fetch.batch_error_total",
    "worker.compras_fetch.items_total",
    "worker.compras_fetch.ingest_ok_total",
    "worker.compras_fetch.ingest_error_total",
    "data.normalization.error_total",
    "worker.triage.consumed_total",
    "worker.triage.enqueued_fetch_total",
    "worker.triage.error_total",
    "worker.triage.dead_total",
    "worker.fetch_docs.consumed_total",
    "worker.fetch_docs.ok_total",
    "worker.fetch_docs.retry_total",
    "worker.fetch_docs.error_total",
    "worker.fetch_docs.dead_total",
    "worker.fetch_docs.missing_tender_or_url_total",
    "worker.parse.consumed_total",
    "worker.parse.ok_total",
    "worker.parse.retry_total",
    "worker.parse.error_total",
    "worker.parse.dead_total",
]

DEFAULT_GAUGES = [
    "api.last_request_ms",
]

DEFAULT_LABELED_COUNTERS = [
    "api.requests_by_route_total",
]

DEFAULT_HISTOGRAMS = [
    "api.request_duration_ms",
    "agent.enrich_duration_ms",
]

_raw_buckets = _split_env_list(
    "METRICS_HISTOGRAM_BUCKETS_MS",
    ["50", "100", "200", "500", "1000", "2000", "5000"],
)
HISTOGRAM_BUCKETS_MS: list[float] = []
for _b in _raw_buckets:
    try:
        HISTOGRAM_BUCKETS_MS.append(float(_b))
    except Exception:
        continue


def _sanitize(name: str) -> str:
    out = []
    for ch in name:
        if ch.isalnum() or ch == "_":
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


def _escape_label(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")


async def incr_counter(name: str, value: int = 1) -> None:
    if not METRICS_ENABLED:
        return
    try:
        r = _get_redis()
        key = f"{METRICS_PREFIX}:c:{name}"
        await r.incrby(key, int(value))
        await r.expire(key, METRICS_TTL_S)
    except Exception:
        return


def _labels_key(labels: dict) -> str:
    parts = []
    for k in sorted(labels.keys()):
        parts.append(f"{k}={labels[k]}")
    return ",".join(parts)


async def incr_counter_labeled(name: str, labels: dict, value: int = 1) -> None:
    if not METRICS_ENABLED:
        return
    try:
        r = _get_redis()
        labels_key = _labels_key(labels)
        set_key = f"{METRICS_PREFIX}:clset:{name}"
        key = f"{METRICS_PREFIX}:cl:{name}:{labels_key}"
        await r.sadd(set_key, labels_key)
        await r.incrby(key, int(value))
        await r.expire(key, METRICS_TTL_S)
        await r.expire(set_key, METRICS_TTL_S)
    except Exception:
        return


async def set_gauge(name: str, value: int | float) -> None:
    if not METRICS_ENABLED:
        return
    try:
        r = _get_redis()
        key = f"{METRICS_PREFIX}:g:{name}"
        await r.set(key, str(value))
        await r.expire(key, METRICS_TTL_S)
    except Exception:
        return


async def get_counters(names: Iterable[str]) -> dict:
    if not METRICS_ENABLED:
        return {}
    names = list(names)
    if not names:
        return {}
    r = _get_redis()
    keys = [f"{METRICS_PREFIX}:c:{name}" for name in names]
    try:
        values = await r.mget(keys)
    except Exception:
        return {}
    out = {}
    for name, val in zip(names, values):
        out[name] = int(val or 0)
    return out


async def get_gauges(names: Iterable[str]) -> dict:
    if not METRICS_ENABLED:
        return {}
    names = list(names)
    if not names:
        return {}
    r = _get_redis()
    keys = [f"{METRICS_PREFIX}:g:{name}" for name in names]
    try:
        values = await r.mget(keys)
    except Exception:
        return {}
    out = {}
    for name, val in zip(names, values):
        if val is None:
            out[name] = None
            continue
        try:
            out[name] = float(val)
        except Exception:
            out[name] = val
    return out


async def get_queue_lengths(queues: Iterable[str]) -> dict:
    if not METRICS_ENABLED:
        return {}
    queues = list(queues)
    if not queues:
        return {}
    r = _get_redis()
    out = {}
    for q in queues:
        try:
            out[q] = int(await r.llen(q))
        except Exception:
            out[q] = None
    return out


async def get_cache_snapshot() -> dict:
    if not METRICS_ENABLED:
        return {}
    r = _get_redis()
    try:
        hit = await r.get(f"{CACHE_PREFIX}:metrics:hit")
        miss = await r.get(f"{CACHE_PREFIX}:metrics:miss")
        return {
            "cache_hit_total": int(hit or 0),
            "cache_miss_total": int(miss or 0),
        }
    except Exception:
        return {}


async def get_labeled_counters(name: str) -> dict:
    if not METRICS_ENABLED:
        return {}
    r = _get_redis()
    set_key = f"{METRICS_PREFIX}:clset:{name}"
    try:
        labels = await r.smembers(set_key)
    except Exception:
        return {}
    if not labels:
        return {}
    keys = [f"{METRICS_PREFIX}:cl:{name}:{label}" for label in labels]
    try:
        values = await r.mget(keys)
    except Exception:
        return {}
    out = {}
    for label, val in zip(labels, values):
        out[label] = int(val or 0)
    return out


def _bucket_key(name: str, le: str) -> str:
    return f"{METRICS_PREFIX}:h:{name}:bucket:{le}"


async def observe_histogram(name: str, value_ms: float, buckets: list[float] | None = None) -> None:
    if not METRICS_ENABLED:
        return
    buckets = buckets or HISTOGRAM_BUCKETS_MS
    try:
        r = _get_redis()
        for b in buckets:
            if value_ms <= b:
                await r.incr(_bucket_key(name, str(b)))
        await r.incr(_bucket_key(name, "+Inf"))
        await r.incrbyfloat(f"{METRICS_PREFIX}:h:{name}:sum", float(value_ms))
        await r.incr(f"{METRICS_PREFIX}:h:{name}:count")
        await r.expire(f"{METRICS_PREFIX}:h:{name}:sum", METRICS_TTL_S)
        await r.expire(f"{METRICS_PREFIX}:h:{name}:count", METRICS_TTL_S)
        for b in buckets:
            await r.expire(_bucket_key(name, str(b)), METRICS_TTL_S)
        await r.expire(_bucket_key(name, "+Inf"), METRICS_TTL_S)
    except Exception:
        return


async def get_histogram(name: str, buckets: list[float] | None = None) -> dict:
    if not METRICS_ENABLED:
        return {}
    buckets = buckets or HISTOGRAM_BUCKETS_MS
    r = _get_redis()
    keys = [_bucket_key(name, str(b)) for b in buckets] + [_bucket_key(name, "+Inf")]
    try:
        values = await r.mget(keys)
        sum_v = await r.get(f"{METRICS_PREFIX}:h:{name}:sum")
        count_v = await r.get(f"{METRICS_PREFIX}:h:{name}:count")
    except Exception:
        return {}
    out = {"buckets": {}, "sum": float(sum_v or 0), "count": int(count_v or 0)}
    for key, val in zip(keys, values):
        le = key.split(":bucket:", 1)[1]
        out["buckets"][le] = int(val or 0)
    return out


async def render_prometheus() -> str:
    counters = await get_counters(DEFAULT_COUNTERS)
    gauges = await get_gauges(DEFAULT_GAUGES)
    queues = await get_queue_lengths(DEFAULT_QUEUE_LIST)
    cache = await get_cache_snapshot()
    labeled = {}
    for name in DEFAULT_LABELED_COUNTERS:
        labeled[name] = await get_labeled_counters(name)
    histograms = {}
    for name in DEFAULT_HISTOGRAMS:
        histograms[name] = await get_histogram(name)

    lines: list[str] = []
    for name, val in counters.items():
        metric = _sanitize(name)
        lines.append(f"# TYPE {metric} counter")
        lines.append(f"{metric} {int(val)}")
    for name, val in gauges.items():
        metric = _sanitize(name)
        lines.append(f"# TYPE {metric} gauge")
        if val is None:
            continue
        lines.append(f"{metric} {float(val)}")
    if queues:
        metric = "queue_length"
        lines.append(f"# TYPE {metric} gauge")
    for q, val in queues.items():
        if val is None:
            continue
        lines.append(f'{metric}{{queue="{q}"}} {int(val)}')
    for name, val in cache.items():
        metric = _sanitize(name)
        lines.append(f"# TYPE {metric} counter")
        lines.append(f"{metric} {int(val)}")
    for name, items in labeled.items():
        metric = _sanitize(name)
        if not items:
            continue
        lines.append(f"# TYPE {metric} counter")
        for label_key, val in items.items():
            if not label_key:
                continue
            label_pairs = []
            for pair in label_key.split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    label_pairs.append(f'{_sanitize(k)}="{_escape_label(v)}"')
            label_str = ",".join(label_pairs)
            lines.append(f"{metric}{{{label_str}}} {int(val)}")
    for name, h in histograms.items():
        metric = _sanitize(name)
        buckets = h.get("buckets") or {}
        if not buckets:
            continue
        lines.append(f"# TYPE {metric} histogram")
        for le, val in buckets.items():
            lines.append(f'{metric}_bucket{{le="{le}"}} {int(val)}')
        lines.append(f"{metric}_sum {float(h.get('sum') or 0)}")
        lines.append(f"{metric}_count {int(h.get('count') or 0)}")
    return "\n".join(lines) + "\n"
