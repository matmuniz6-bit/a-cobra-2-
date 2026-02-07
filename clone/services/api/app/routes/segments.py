import os
import asyncpg
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any
from ..db import get_pool, init_pool

DATABASE_URL = os.getenv("DATABASE_URL", "")

def _pg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)

router = APIRouter(prefix="/v1/segments", tags=["segments"])

class SegmentSearchIn(BaseModel):
    query: str = Field(..., min_length=2)
    limit: int = Field(5, ge=1, le=50)

async def _pool() -> asyncpg.Pool:
    pool = get_pool()
    if pool is None:
        pool = await init_pool()
    return pool

@router.post("/search")
async def search_segments(s: SegmentSearchIn) -> Dict[str, Any]:
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, document_id, tender_id, idx, text,
                   ts_rank(tsv, plainto_tsquery('portuguese', $1)) AS rank
            FROM document_segment
            WHERE tsv @@ plainto_tsquery('portuguese', $1)
            ORDER BY rank DESC
            LIMIT $2;
            """,
            s.query,
            int(s.limit),
        )
        return {"items": [dict(r) for r in rows]}
