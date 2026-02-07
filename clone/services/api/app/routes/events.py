import os
import asyncpg
from fastapi import APIRouter, HTTPException
from ..db import get_pool, init_pool

DATABASE_URL = os.getenv("DATABASE_URL", "")


def _pg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


router = APIRouter(prefix="/v1/events", tags=["events"])

async def _pool() -> asyncpg.Pool:
    pool = get_pool()
    if pool is None:
        pool = await init_pool()
    return pool


@router.get("")
async def list_events(
    tender_id: int | None = None,
    document_id: int | None = None,
    stage: str | None = None,
    limit: int = 50,
):
    if not DATABASE_URL:
        raise HTTPException(500, "DATABASE_URL_not_set")
    limit = max(1, min(int(limit or 50), 500))

    conditions = []
    params = []
    if tender_id is not None:
        params.append(int(tender_id))
        conditions.append(f"tender_id=${len(params)}")
    if document_id is not None:
        params.append(int(document_id))
        conditions.append(f"document_id=${len(params)}")
    if stage:
        params.append(str(stage))
        conditions.append(f"stage=${len(params)}")

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    sql = f"""
        SELECT id, tender_id, document_id, stage, status, message, payload, created_at
        FROM pipeline_event
        {where}
        ORDER BY created_at DESC
        LIMIT {limit}
    """

    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
        return [dict(r) for r in rows]
