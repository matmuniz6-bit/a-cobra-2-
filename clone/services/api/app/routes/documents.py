import os
import asyncpg
from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from ..db import get_pool, init_pool

DATABASE_URL = os.getenv("DATABASE_URL", "")

def _pg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)

router = APIRouter(prefix="/v1/documents", tags=["documents"])

async def _pool() -> asyncpg.Pool:
    pool = get_pool()
    if pool is None:
        pool = await init_pool()
    return pool

@router.get("/list")
async def list_documents(tender_id: int, limit: int = 20) -> Dict[str, Any]:
    if tender_id < 1:
        raise HTTPException(400, "invalid_tender_id")
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, tender_id, url, content_type, size_bytes, fetched_at
            FROM document
            WHERE tender_id=$1
            ORDER BY id DESC
            LIMIT $2;
            """,
            int(tender_id),
            int(limit),
        )
        items = [dict(r) for r in rows]
        return {"tender_id": tender_id, "items": items}
