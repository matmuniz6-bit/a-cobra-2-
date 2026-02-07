import os
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "")

_pool: asyncpg.Pool | None = None


def _pg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def init_pool(min_size: int = 1, max_size: int = 5) -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(_pg_dsn(DATABASE_URL), min_size=min_size, max_size=max_size)
    return _pool


def get_pool() -> asyncpg.Pool | None:
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
