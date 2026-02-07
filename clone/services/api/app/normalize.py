import re
import unicodedata
from typing import Any, Dict, Tuple


def _strip(s: str | None) -> str | None:
    if s is None:
        return None
    out = str(s).strip()
    return out or None


def _squash_ws(s: str | None) -> str | None:
    if s is None:
        return None
    out = re.sub(r"\s+", " ", str(s)).strip()
    return out or None


def _no_accents(s: str | None) -> str | None:
    if s is None:
        return None
    nfkd = unicodedata.normalize("NFKD", str(s))
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def _upper(s: str | None) -> str | None:
    if s is None:
        return None
    out = str(s).strip().upper()
    return out or None


def normalize_uf(uf: str | None) -> str | None:
    u = _upper(uf)
    if not u:
        return None
    if len(u) == 2 and u.isalpha():
        return u
    return None


def split_municipio_uf(raw: str | None) -> Tuple[str | None, str | None]:
    if not raw:
        return None, None
    text = _squash_ws(raw) or ""
    # patterns: "Cidade/UF" or "Cidade - UF"
    m = re.search(r"^(?P<city>.+?)[\s/-]+(?P<uf>[A-Za-z]{2})$", text)
    if m:
        return _strip(m.group("city")), normalize_uf(m.group("uf"))
    return text, None


def normalize_modalidade(raw: str | None) -> str | None:
    s = _no_accents(_strip(raw))
    if not s:
        return None
    s = s.lower()
    if "preg" in s:
        return "PREGAO"
    if "concorr" in s:
        return "CONCORRENCIA"
    if "dispensa" in s:
        return "DISPENSA"
    if "inexig" in s:
        return "INEXIGIBILIDADE"
    if "convite" in s:
        return "CONVITE"
    if "tomada" in s or "precos" in s or "preços" in s:
        return "TOMADA_PRECOS"
    if "rdc" in s:
        return "RDC"
    if "leil" in s:
        return "LEILAO"
    return "OUTRA"


def normalize_status(raw: str | None) -> str | None:
    s = _no_accents(_strip(raw))
    if not s:
        return None
    s = s.lower()
    if any(k in s for k in ["aberta", "aberto", "abertura", "publicada"]):
        return "OPEN"
    if any(k in s for k in ["em andamento", "andamento", "processando"]):
        return "IN_PROGRESS"
    if any(k in s for k in ["encerrada", "finalizada", "homologada"]):
        return "CLOSED"
    if any(k in s for k in ["cancelada", "anulada", "revogada"]):
        return "CANCELED"
    if any(k in s for k in ["suspensa", "suspenso"]):
        return "SUSPENDED"
    if any(k in s for k in ["deserta", "fracassada"]):
        return "FAILED"
    return "UNKNOWN"


def normalize_objeto(raw: str | None) -> str | None:
    s = _squash_ws(raw)
    if not s:
        return None
    return s


def normalize_orgao(raw: str | None) -> str | None:
    return _squash_ws(raw)


def normalize_municipio(raw: str | None) -> str | None:
    city, _uf = split_municipio_uf(raw)
    return _squash_ws(city)


def normalize_tender(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload)
    # municipio/uf split
    municipio_raw = out.get("municipio")
    city, uf_from_city = split_municipio_uf(municipio_raw if isinstance(municipio_raw, str) else None)
    uf = normalize_uf(out.get("uf")) or uf_from_city

    out["orgao_norm"] = normalize_orgao(out.get("orgao"))
    out["municipio_norm"] = normalize_municipio(city)
    out["uf_norm"] = uf
    out["modalidade_norm"] = normalize_modalidade(out.get("modalidade"))
    out["status_norm"] = normalize_status(out.get("status"))
    out["objeto_norm"] = normalize_objeto(out.get("objeto"))

    # ensure base fields cleaned
    out["orgao"] = _strip(out.get("orgao"))
    out["municipio"] = _strip(city)
    out["uf"] = uf
    out["modalidade"] = _strip(out.get("modalidade"))
    out["status"] = _strip(out.get("status"))
    out["objeto"] = normalize_objeto(out.get("objeto"))
    return out
