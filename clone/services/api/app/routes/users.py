import os
import asyncpg
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from ..db import get_pool, init_pool

DATABASE_URL = os.getenv("DATABASE_URL", "")

def _pg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)

router = APIRouter(prefix="/v1/users", tags=["users"])

class UserUpsertIn(BaseModel):
    telegram_user_id: int = Field(..., ge=1)
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    language_code: Optional[str] = None

class FollowTenderIn(BaseModel):
    telegram_user_id: int = Field(..., ge=1)
    tender_id: int = Field(..., ge=1)

class UnfollowTenderIn(BaseModel):
    telegram_user_id: int = Field(..., ge=1)
    tender_id: int = Field(..., ge=1)

async def _pool() -> asyncpg.Pool:
    pool = get_pool()
    if pool is None:
        pool = await init_pool()
    return pool

@router.post("/upsert")
async def upsert_user(u: UserUpsertIn):
    payload = u.model_dump()
    pool = await _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO app_user (telegram_user_id, username, first_name, last_name, language_code, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (telegram_user_id) DO UPDATE SET
              username=EXCLUDED.username,
              first_name=EXCLUDED.first_name,
              last_name=EXCLUDED.last_name,
              language_code=EXCLUDED.language_code,
              updated_at=EXCLUDED.updated_at
            RETURNING id, telegram_user_id, username, first_name, last_name, language_code, created_at, updated_at;
            """,
            payload["telegram_user_id"],
            payload.get("username"),
            payload.get("first_name"),
            payload.get("last_name"),
            payload.get("language_code"),
            datetime.utcnow(),
        )
        if not row:
            raise HTTPException(500, "upsert_failed")
        return dict(row)

@router.post("/follow")
async def follow_tender(p: FollowTenderIn):
    pool = await _pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id FROM app_user WHERE telegram_user_id=$1",
            int(p.telegram_user_id),
        )
        if not user:
            raise HTTPException(404, "user_not_found")
        await conn.execute(
            """
            INSERT INTO tender_follow (user_id, tender_id, created_at)
            VALUES ($1,$2,$3)
            ON CONFLICT (user_id, tender_id) DO NOTHING
            """,
            int(user["id"]),
            int(p.tender_id),
            datetime.utcnow(),
        )
        return {"ok": True}

@router.post("/unfollow")
async def unfollow_tender(p: UnfollowTenderIn):
    pool = await _pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id FROM app_user WHERE telegram_user_id=$1",
            int(p.telegram_user_id),
        )
        if not user:
            raise HTTPException(404, "user_not_found")
        await conn.execute(
            "DELETE FROM tender_follow WHERE user_id=$1 AND tender_id=$2",
            int(user["id"]),
            int(p.tender_id),
        )
        return {"ok": True}
