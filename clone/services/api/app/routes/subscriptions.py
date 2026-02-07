import os
import json
import asyncpg
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional, List
from ..cache import invalidate_patterns, CACHE_PREFIX
from ..db import get_pool, init_pool

DATABASE_URL = os.getenv("DATABASE_URL", "")

def _pg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)

router = APIRouter(prefix="/v1/subscriptions", tags=["subscriptions"])

class SubscriptionCreateIn(BaseModel):
    telegram_user_id: int = Field(..., ge=1)
    filters: Dict[str, Any] = Field(default_factory=dict)
    delivery: Optional[Dict[str, Any]] = None
    frequency: Optional[str] = "realtime"

class SubscriptionUpdateIn(BaseModel):
    id: int = Field(..., ge=1)
    filters: Optional[Dict[str, Any]] = None
    delivery: Optional[Dict[str, Any]] = None
    frequency: Optional[str] = None
    is_active: Optional[bool] = None

class SubscriptionToggleIn(BaseModel):
    telegram_user_id: int = Field(..., ge=1)
    is_active: bool = True

class SubscriptionFrequencyIn(BaseModel):
    telegram_user_id: int = Field(..., ge=1)
    frequency: str = Field(..., min_length=3)

async def _pool() -> asyncpg.Pool:
    pool = get_pool()
    if pool is None:
        pool = await init_pool()
    return pool

@router.get("/list")
async def list_subscriptions(telegram_user_id: int):
    pool = await _pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id FROM app_user WHERE telegram_user_id=$1",
            int(telegram_user_id),
        )
        if not user:
            return {"items": []}
        rows = await conn.fetch(
            "SELECT id, user_id, filters, delivery, frequency, is_active, created_at, updated_at "
            "FROM user_subscription WHERE user_id=$1 ORDER BY id DESC",
            int(user["id"]),
        )
        return {"items": [dict(r) for r in rows]}

@router.post("/create")
async def create_subscription(s: SubscriptionCreateIn):
    pool = await _pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id FROM app_user WHERE telegram_user_id=$1",
            int(s.telegram_user_id),
        )
        if not user:
            raise HTTPException(404, "user_not_found")
        row = await conn.fetchrow(
            """
            INSERT INTO user_subscription (user_id, filters, delivery, frequency, is_active, created_at, updated_at)
            VALUES ($1,$2::jsonb,$3::jsonb,$4,$5,$6,$7)
            RETURNING id, user_id, filters, delivery, frequency, is_active, created_at, updated_at;
            """,
            int(user["id"]),
            json.dumps(s.filters or {}),
            json.dumps(s.delivery or {"pv": True, "channel": True}),
            s.frequency or "realtime",
            True,
            datetime.utcnow(),
            datetime.utcnow(),
        )
        if not row:
            raise HTTPException(500, "create_failed")
        user_q = f"telegram_user_id={int(s.telegram_user_id)}"
        await invalidate_patterns([f"{CACHE_PREFIX}:GET:/v1/subscriptions/list?{user_q}*"])
        return dict(row)

@router.post("/update")
async def update_subscription(s: SubscriptionUpdateIn):
    pool = await _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE user_subscription
            SET filters=COALESCE($2, filters),
                delivery=COALESCE($3, delivery),
                frequency=COALESCE($4, frequency),
                is_active=COALESCE($5, is_active),
                updated_at=$6
            WHERE id=$1
            RETURNING id, user_id, filters, delivery, frequency, is_active, created_at, updated_at;
            """,
            int(s.id),
            json.dumps(s.filters) if s.filters is not None else None,
            json.dumps(s.delivery) if s.delivery is not None else None,
            s.frequency,
            s.is_active,
            datetime.utcnow(),
        )
        if not row:
            raise HTTPException(404, "not_found")
        try:
            user_row = await conn.fetchrow(
                "SELECT telegram_user_id FROM app_user WHERE id=$1",
                int(row["user_id"]),
            )
            if user_row and user_row["telegram_user_id"]:
                user_q = f"telegram_user_id={int(user_row['telegram_user_id'])}"
                await invalidate_patterns([f"{CACHE_PREFIX}:GET:/v1/subscriptions/list?{user_q}*"])
            else:
                await invalidate_patterns([f"{CACHE_PREFIX}:GET:/v1/subscriptions/list?*"])
        except Exception:
            await invalidate_patterns([f"{CACHE_PREFIX}:GET:/v1/subscriptions/list?*"])
        return dict(row)

@router.post("/pause_all")
async def pause_all(s: SubscriptionToggleIn):
    pool = await _pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id FROM app_user WHERE telegram_user_id=$1",
            int(s.telegram_user_id),
        )
        if not user:
            raise HTTPException(404, "user_not_found")
        await conn.execute(
            "UPDATE user_subscription SET is_active=$2, updated_at=$3 WHERE user_id=$1",
            int(user["id"]),
            bool(s.is_active),
            datetime.utcnow(),
        )
        user_q = f"telegram_user_id={int(s.telegram_user_id)}"
        await invalidate_patterns([f"{CACHE_PREFIX}:GET:/v1/subscriptions/list?{user_q}*"])
        return {"ok": True, "is_active": bool(s.is_active)}

@router.post("/set_frequency")
async def set_frequency(s: SubscriptionFrequencyIn):
    pool = await _pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id FROM app_user WHERE telegram_user_id=$1",
            int(s.telegram_user_id),
        )
        if not user:
            raise HTTPException(404, "user_not_found")
        await conn.execute(
            "UPDATE user_subscription SET frequency=$2, updated_at=$3 WHERE user_id=$1",
            int(user["id"]),
            s.frequency,
            datetime.utcnow(),
        )
        user_q = f"telegram_user_id={int(s.telegram_user_id)}"
        await invalidate_patterns([f"{CACHE_PREFIX}:GET:/v1/subscriptions/list?{user_q}*"])
        return {"ok": True, "frequency": s.frequency}
