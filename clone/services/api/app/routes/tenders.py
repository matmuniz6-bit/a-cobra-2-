import os
import json
import asyncpg
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional
from ..dedupe import hash_metadados, fingerprint_tender
from ..metrics import incr_counter
from ..normalize import normalize_tender
from ..db import get_pool, init_pool

DATABASE_URL = os.getenv("DATABASE_URL", "")

def _pg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)

router = APIRouter(prefix="/v1/tenders", tags=["tenders"])

class TenderIn(BaseModel):
    id_pncp: str = Field(..., min_length=3)
    source: Optional[str] = None
    source_id: Optional[str] = None
    orgao: Optional[str] = None
    municipio: Optional[str] = None
    uf: Optional[str] = None
    modalidade: Optional[str] = None
    objeto: Optional[str] = None
    data_publicacao: Optional[datetime] = None
    status: Optional[str] = None
    urls: Optional[Dict[str, Any]] = None
    source_payload: Optional[Dict[str, Any]] = None

async def _pool() -> asyncpg.Pool:
    pool = get_pool()
    if pool is None:
        pool = await init_pool()
    return pool

def _normalize_source(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload)
    src = (out.get("source") or "").strip().lower()
    id_pncp = (out.get("id_pncp") or "").strip()
    source_id = (out.get("source_id") or "").strip()
    if not src:
        if id_pncp.startswith("compras:"):
            src = "compras"
        elif id_pncp:
            src = "pncp"
        else:
            src = "unknown"
    if not source_id:
        if src == "pncp":
            source_id = id_pncp or None
        elif id_pncp.startswith(f"{src}:"):
            source_id = id_pncp.split(":", 1)[1]
    if not id_pncp and source_id:
        id_pncp = f"{src}:{source_id}"
    out["source"] = src
    out["source_id"] = source_id
    out["id_pncp"] = id_pncp

    # normalize strings
    for k in ("orgao", "municipio", "modalidade", "objeto", "status"):
        if out.get(k) is not None:
            out[k] = str(out[k]).strip() or None
    if out.get("uf"):
        out["uf"] = str(out["uf"]).strip().upper() or None
    # normalize urls
    if not isinstance(out.get("urls"), dict):
        out["urls"] = {}
    return out


@router.post("/upsert")
async def upsert_tender(t: TenderIn):
    payload = t.model_dump()
    try:
        payload = _normalize_source(payload)
        payload = normalize_tender(payload)
    except Exception:
        await incr_counter("data.normalization.error_total")
        payload = t.model_dump()
    h = hash_metadados(payload)
    fp = fingerprint_tender(payload)

    pool = await _pool()
    async with pool.acquire() as conn:
        existing = None
        try:
            existing = await conn.fetchrow(
                "SELECT id, hash_metadados FROM tender WHERE id_pncp=$1",
                payload.get("id_pncp"),
            )
        except Exception:
            existing = None
        row = await conn.fetchrow(
            """
            INSERT INTO tender (id_pncp, source, source_id, orgao, orgao_norm, municipio, municipio_norm, uf, uf_norm,
                                modalidade, modalidade_norm, objeto, objeto_norm, fingerprint, data_publicacao, status, status_norm, urls, hash_metadados)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::timestamptz,$16,$17,$18::jsonb,$19)
            ON CONFLICT (id_pncp) DO UPDATE SET
              source=EXCLUDED.source,
              source_id=EXCLUDED.source_id,
              orgao=EXCLUDED.orgao,
              orgao_norm=EXCLUDED.orgao_norm,
              municipio=EXCLUDED.municipio,
              municipio_norm=EXCLUDED.municipio_norm,
              uf=EXCLUDED.uf,
              uf_norm=EXCLUDED.uf_norm,
              modalidade=EXCLUDED.modalidade,
              modalidade_norm=EXCLUDED.modalidade_norm,
              objeto=EXCLUDED.objeto,
              objeto_norm=EXCLUDED.objeto_norm,
              fingerprint=EXCLUDED.fingerprint,
              data_publicacao=EXCLUDED.data_publicacao,
              status=EXCLUDED.status,
              status_norm=EXCLUDED.status_norm,
              urls=EXCLUDED.urls,
              hash_metadados=EXCLUDED.hash_metadados,
              updated_at=now()
            RETURNING id, id_pncp, source, source_id, hash_metadados, updated_at;
            """,
            payload["id_pncp"],
            payload.get("source"),
            payload.get("source_id"),
            payload.get("orgao"),
            payload.get("orgao_norm"),
            payload.get("municipio"),
            payload.get("municipio_norm"),
            payload.get("uf"),
            payload.get("uf_norm"),
            payload.get("modalidade"),
            payload.get("modalidade_norm"),
            payload.get("objeto"),
            payload.get("objeto_norm"),
            fp,
            payload.get("data_publicacao"),
            payload.get("status"),
            payload.get("status_norm"),
            json.dumps(payload.get("urls") or {}),
            h,
        )
        if not row:
            raise HTTPException(500, "upsert_failed")
        # raw payload per source
        src_payload = payload.get("source_payload") or payload
        try:
            await conn.execute(
                """
                INSERT INTO tender_source_payload (tender_id, source, source_id, payload)
                VALUES ($1,$2,$3,$4::jsonb)
                """,
                int(row["id"]),
                payload.get("source") or "unknown",
                payload.get("source_id"),
                json.dumps(src_payload, ensure_ascii=False),
            )
        except Exception:
            pass
        saved = dict(row)
        # versioning (insert on create or change)
        try:
            prev_hash = existing["hash_metadados"] if existing else None
            if (existing is None) or (prev_hash != h):
                await conn.execute(
                    """
                    INSERT INTO tender_version (tender_id, hash_metadados, payload)
                    VALUES ($1,$2,$3::jsonb)
                    """,
                    int(saved["id"]),
                    h,
                    json.dumps(payload, ensure_ascii=False),
                )
        except Exception:
            pass
        # dedupe cross-source by fingerprint
        if fp:
            try:
                existing = await conn.fetchrow(
                    "SELECT id, canonical_tender_id FROM tender WHERE fingerprint=$1 AND id <> $2 ORDER BY id ASC LIMIT 1",
                    fp,
                    int(saved["id"]),
                )
                if existing:
                    canonical = int(existing["canonical_tender_id"] or existing["id"])
                    await conn.execute(
                        "UPDATE tender SET canonical_tender_id=$1 WHERE id=$2",
                        canonical,
                        int(saved["id"]),
                    )
                    if existing["canonical_tender_id"] is None:
                        await conn.execute(
                            "UPDATE tender SET canonical_tender_id=$1 WHERE id=$2",
                            canonical,
                            int(existing["id"]),
                        )
            except Exception:
                pass
        return saved
