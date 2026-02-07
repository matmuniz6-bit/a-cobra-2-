import os, asyncio
import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

async def main():
    r = redis.from_url(REDIS_URL)
    while True:
        try:
            await r.ping()
            print("worker_heartbeat")
        except Exception as e:
            print("worker_redis_error", type(e).__name__)
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
