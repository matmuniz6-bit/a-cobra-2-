import os
from fastapi import APIRouter, Request, HTTPException
from typing import Any, Dict
from .tenders import TenderIn, upsert_tender
from ..queue import push
from ..metrics import incr_counter

router = APIRouter(prefix="/v1/ingest", tags=["ingest"])
TRIAGE_QUEUE = os.getenv("TRIAGE_QUEUE", "q:triage")

@router.post("/tender")
async def ingest_tender(t: TenderIn, request: Request):
    # 1) grava/atualiza no DB
    saved = await upsert_tender(t)

    # 2) captura force_fetch do JSON bruto (caso o Pydantic ignore extras)
    raw: Dict[str, Any] = {}
    try:
        raw = await request.json()
    except Exception:
        raw = {}

    force_fetch = bool(raw.get("force_fetch", False))

    # 3) payload limpo do schema + injeta force_fetch (redundante, proposital)
    payload = t.model_dump(mode="json")
    if force_fetch:
        payload["force_fetch"] = True
    if raw.get("source_payload"):
        payload["source_payload"] = raw.get("source_payload")

    # 4) enfileira pra triagem (TOP LEVEL + payload)
    try:
        await push(TRIAGE_QUEUE, {
            "tender_id": saved["id"],
            "id_pncp": saved["id_pncp"],
            "source": saved.get("source"),
            "source_id": saved.get("source_id"),
            "force_fetch": force_fetch,
            "payload": payload,
        })
    except RuntimeError as e:
        if str(e) == "queue_full":
            await incr_counter("api.ingest.queue_full_total")
            raise HTTPException(429, "queue_full")
        await incr_counter("api.ingest.error_total")
        raise
    await incr_counter("api.ingest.queued_total")

    return {"ok": True, "queued": TRIAGE_QUEUE, "tender": saved, "force_fetch": force_fetch}
