import os
import json
import time
import urllib.request
from typing import Any, Dict, Optional

from .metrics import incr_counter, observe_histogram

AGENT_ENABLED = os.getenv("AGENT_ENABLED", "0").strip() in ("1", "true", "True")
AGENT_URL = (os.getenv("AGENT_URL", "") or "").strip()
AGENT_TIMEOUT_S = int(os.getenv("AGENT_TIMEOUT_S", "15"))
AGENT_MIN_CHARS = int(os.getenv("AGENT_MIN_CHARS", "300"))
AGENT_MAX_CHARS = int(os.getenv("AGENT_MAX_CHARS", "4000"))
AGENT_FORCE = os.getenv("AGENT_FORCE", "0").strip() in ("1", "true", "True")
AGENT_MATERIA_ALLOWED = [
    "saude",
    "educacao",
    "limpeza",
    "ti",
    "obras",
    "servicos",
    "materiais",
    "vigilancia",
    "manutencao",
    "alimentacao",
    "transporte",
    "seguranca",
    "administrativo",
    "outros",
]

# Fallback to Ollama if AGENT_URL not set
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_CHAT_MODEL = os.getenv("OLLAMA_CHAT_MODEL", "")
OLLAMA_TIMEOUT_S = int(os.getenv("OLLAMA_TIMEOUT_S", "30"))


def _truncate(text: str) -> str:
    if not text:
        return ""
    text = text.strip()
    if len(text) <= AGENT_MAX_CHARS:
        return text
    return text[:AGENT_MAX_CHARS]


def _safe_json_load(raw: str) -> Optional[dict]:
    if not raw:
        return None
    raw = raw.strip()

    def _repair_keys(s: str) -> str:
        # quote unquoted keys like {materia:"x"} -> {"materia":"x"}
        import re
        return re.sub(r'([,{]\\s*)([A-Za-z_][A-Za-z0-9_]*)\\s*:', r'\\1\"\\2\":', s)

    candidates: list[str] = [raw]

    if "```" in raw:
        parts = [p for p in raw.split("```") if p.strip()]
        for p in parts:
            p = p.strip()
            if p.startswith("json"):
                p = p[4:].strip()
            candidates.append(p)

    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidates.append(raw[start : end + 1])

    for cand in candidates:
        try:
            obj = json.loads(cand)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
        try:
            fixed = _repair_keys(cand)
            obj = json.loads(fixed)
            if isinstance(obj, dict):
                return obj
        except Exception:
            # Fallback: try Python literal eval (handles null->None)
            try:
                import ast
                py_fixed = fixed.replace("null", "None")
                obj = ast.literal_eval(py_fixed)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                continue

    return None


def _parse_agent_output(raw: str) -> Optional[dict]:
    if not raw:
        return None
    raw = raw.strip()
    if "{" in raw and "}" in raw:
        raw = raw[raw.find("{") : raw.rfind("}") + 1]
    # repair unquoted keys and try JSON first
    import re
    fixed = re.sub(r'([,{]\\s*)([A-Za-z_][A-Za-z0-9_]*)\\s*:', r'\\1\"\\2\":', raw)
    try:
        obj = json.loads(fixed)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    # fallback to safe loader
    return _safe_json_load(raw)


def _call_agent_url(payload: dict) -> Optional[dict]:
    req = urllib.request.Request(
        AGENT_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=AGENT_TIMEOUT_S) as resp:
        raw = resp.read().decode("utf-8", "ignore")
    return _parse_agent_output(raw)


def _call_ollama(text: str, meta: dict) -> Optional[dict]:
    if not OLLAMA_CHAT_MODEL:
        return None
    prompt = (
        "Responda APENAS com JSON valido (uma linha), sem texto extra. "
        "Schema: {\"materia\":string,\"categoria\":string,\"confidence\":number,\"tags\":[string]}. "
        "Use valores em minusculo, sem acentos. Se incerto, use null. "
        "materia deve ser UMA das opcoes: "
        + ", ".join(AGENT_MATERIA_ALLOWED)
        + ". "
        "Use no maximo 3 palavras por campo.\n\n"
        f"Metadados: {json.dumps(meta, ensure_ascii=False)}\n\n"
        f"Texto:\n{text}"
    )
    payload = {
        "model": OLLAMA_CHAT_MODEL,
        "messages": [
            {"role": "system", "content": "Voce classifica materia de licitacoes."},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
    }
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/chat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT_S) as resp:
        data = json.loads(resp.read().decode("utf-8", "ignore"))
    content = ((data.get("message") or {}).get("content") or "").strip()
    return _parse_agent_output(content)


def _normalize_result(raw: dict) -> dict:
    import unicodedata

    def _fold(s: str) -> str:
        nfkd = unicodedata.normalize("NFKD", s)
        return "".join(c for c in nfkd if not unicodedata.combining(c))

    materia = raw.get("materia") or raw.get("category") or raw.get("categoria")
    categoria = raw.get("categoria") or raw.get("category")
    conf = raw.get("confidence") or raw.get("conf")
    tags = raw.get("tags") or []

    if isinstance(materia, str):
        materia = _fold(materia.strip().lower())
        if "\n" in materia:
            materia = materia.splitlines()[0].strip()
        if len(materia) > 80:
            materia = None
        if materia and materia not in AGENT_MATERIA_ALLOWED:
            materia = None
        materia = materia or None
    else:
        materia = None
    if isinstance(categoria, str):
        categoria = _fold(categoria.strip().lower())
        if "\n" in categoria:
            categoria = categoria.splitlines()[0].strip()
        if len(categoria) > 80:
            categoria = None
        if categoria and categoria not in AGENT_MATERIA_ALLOWED:
            categoria = None
        categoria = categoria or None
    else:
        categoria = None
    try:
        conf = float(conf) if conf is not None else None
    except Exception:
        conf = None
    if not isinstance(tags, list):
        tags = []
    tags = [_fold(str(t).strip().lower()) for t in tags if str(t).strip()]
    tags = [t for t in tags if len(t) <= 40][:10]

    return {
        "materia": materia,
        "categoria": categoria,
        "confidence": conf,
        "tags": tags,
    }


def _should_skip(existing: dict | None) -> bool:
    if AGENT_FORCE:
        return False
    if not existing:
        return False
    return bool(existing.get("materia") or existing.get("categoria"))


async def enrich_tender(pool, tender_id: int, text: str, meta: dict, existing: dict | None = None) -> None:
    if not AGENT_ENABLED:
        return
    if not text or len(text) < AGENT_MIN_CHARS:
        await incr_counter("agent.enrich.skip_total")
        return
    if _should_skip(existing):
        await incr_counter("agent.enrich.skip_total")
        return

    payload = {
        "tender_id": int(tender_id),
        "text": _truncate(text),
        "meta": meta or {},
    }

    t0 = time.perf_counter()
    try:
        if AGENT_URL:
            raw = _call_agent_url(payload)
        else:
            raw = _call_ollama(payload["text"], payload["meta"])
        if not raw:
            await incr_counter("agent.enrich.error_total")
            return
        result = _normalize_result(raw)
        if not (result.get("materia") or result.get("categoria") or result.get("tags")):
            await incr_counter("agent.enrich.error_total")
            return

        if pool is None:
            await incr_counter("agent.enrich.error_total")
            return
        try:
            await pool.execute(
                """
                UPDATE tender
                SET materia=$2, categoria=$3, materia_confidence=$4, materia_source=$5,
                    materia_tags=$6::jsonb, materia_updated_at=now(), updated_at=now()
                WHERE id=$1
                """,
                int(tender_id),
                result.get("materia"),
                result.get("categoria"),
                result.get("confidence"),
                "agent" if AGENT_URL or OLLAMA_CHAT_MODEL else None,
                json.dumps(result.get("tags") or [], ensure_ascii=False),
            )
            await incr_counter("agent.enrich.ok_total")
        except Exception:
            await incr_counter("agent.enrich.error_total")
            return
    except Exception:
        await incr_counter("agent.enrich.error_total")
        return
    finally:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        await observe_histogram("agent.enrich_duration_ms", float(elapsed_ms))
