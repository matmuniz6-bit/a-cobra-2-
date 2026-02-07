import os
import json
import random
import datetime as dt

import asyncpg

EVENT_LOG_ENABLED = os.getenv("EVENT_LOG_ENABLED", "1").strip() not in ("0", "false", "False")
EVENT_LOG_SAMPLE = float(os.getenv("EVENT_LOG_SAMPLE", "1.0"))


def _should_log() -> bool:
    if not EVENT_LOG_ENABLED:
        return False
    if EVENT_LOG_SAMPLE >= 1.0:
        return True
    return random.random() <= EVENT_LOG_SAMPLE


async def log_event(
    pool: asyncpg.Pool | None,
    stage: str,
    status: str,
    tender_id: int | None = None,
    document_id: int | None = None,
    message: str | None = None,
    payload: dict | None = None,
) -> None:
    if pool is None:
        return
    if not _should_log():
        return
    try:
        await pool.execute(
            """
            INSERT INTO pipeline_event (tender_id, document_id, stage, status, message, payload, created_at)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)
            """,
            int(tender_id) if tender_id is not None else None,
            int(document_id) if document_id is not None else None,
            str(stage),
            str(status),
            message,
            json.dumps(payload or {}, ensure_ascii=False),
            dt.datetime.now(dt.timezone.utc),
        )
    except Exception:
        return
