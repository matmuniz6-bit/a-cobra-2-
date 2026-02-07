import hashlib
import json
from typing import Any, Dict

def _norm(v: Any) -> Any:
    # deixa o hash estável e serializável
    try:
        # datetime/date -> ISO
        import datetime as _dt
        if isinstance(v, (_dt.datetime, _dt.date)):
            return v.isoformat()
    except Exception:
        pass
    return v

def hash_metadados(payload: Dict[str, Any]) -> str:
    # Canonicaliza apenas campos relevantes (evita ruído)
    key = {
        "id_pncp": payload.get("id_pncp"),
        "source": payload.get("source"),
        "source_id": payload.get("source_id"),
        "orgao": payload.get("orgao"),
        "municipio": payload.get("municipio"),
        "uf": payload.get("uf"),
        "modalidade": payload.get("modalidade"),
        "objeto": payload.get("objeto"),
        "data_publicacao": _norm(payload.get("data_publicacao")),
        "status": payload.get("status"),
        "urls": payload.get("urls"),
    }
    raw = json.dumps(key, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def fingerprint_tender(payload: Dict[str, Any]) -> str | None:
    # Fingerprint para dedupe cross-source (evita IDs e status)
    key = {
        "orgao_norm": payload.get("orgao_norm"),
        "municipio_norm": payload.get("municipio_norm"),
        "uf_norm": payload.get("uf_norm"),
        "modalidade_norm": payload.get("modalidade_norm"),
        "objeto_norm": payload.get("objeto_norm"),
        "data_publicacao": _norm(payload.get("data_publicacao")),
    }
    if not any(key.values()):
        return None
    raw = json.dumps(key, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
