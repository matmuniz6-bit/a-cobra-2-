import os
import pathlib
import asyncio
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "")

def _pg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)

async def main():
    schema_path = pathlib.Path(__file__).with_name("schema.sql")
    sql = schema_path.read_text(encoding="utf-8")

    conn = await asyncpg.connect(_pg_dsn(DATABASE_URL))
    try:
        await conn.execute(sql)
        print("DB_INIT_OK")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
