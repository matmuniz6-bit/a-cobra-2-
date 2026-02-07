#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

set -a; [ -f .env ] && source .env; set +a

DC="docker compose -f docker-compose.yml"

TENDER_ID="${TENDER_ID:-}"
LIMIT="${LIMIT:-200}"

$DC exec -T api python - <<'PY'
import os, json, urllib.request, asyncpg, asyncio

DATABASE_URL = os.getenv("DATABASE_URL")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
EMBED_DIM = int(os.getenv("EMBED_DIM", "768"))
LIMIT = int(os.getenv("LIMIT", "200"))
TENDER_ID = os.getenv("TENDER_ID")


def embed(text: str):
    payload = json.dumps({"model": MODEL, "prompt": text}).encode("utf-8")
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/embeddings",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    vec = data.get("embedding") or []
    if len(vec) != EMBED_DIM:
        return None
    return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if TENDER_ID:
            rows = await conn.fetch(
                "SELECT id, text FROM document_segment WHERE tender_id=$1 AND embedding IS NULL LIMIT $2",
                int(TENDER_ID),
                LIMIT,
            )
        else:
            rows = await conn.fetch(
                "SELECT id, text FROM document_segment WHERE embedding IS NULL LIMIT $1",
                LIMIT,
            )
        count = 0
        for r in rows:
            vec = embed(r["text"])
            if not vec:
                continue
            await conn.execute(
                "UPDATE document_segment SET embedding=$2::vector WHERE id=$1",
                int(r["id"]),
                vec,
            )
            count += 1
        print("UPDATED", count)
    finally:
        await conn.close()

asyncio.run(main())
PY
