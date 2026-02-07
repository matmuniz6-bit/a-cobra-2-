import os
import json
import urllib.request
import asyncpg
import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any
from ..db import get_pool, init_pool

DATABASE_URL = os.getenv("DATABASE_URL", "")
EMBEDDINGS_ENABLED = os.getenv("EMBEDDINGS_ENABLED", "0").strip() in ("1", "true", "True")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
OLLAMA_CHAT_MODEL = os.getenv("OLLAMA_CHAT_MODEL", "")
EMBED_DIM = int(os.getenv("EMBED_DIM", "768"))
EMBED_TIMEOUT_S = int(os.getenv("EMBED_TIMEOUT_S", "15"))
OLLAMA_TIMEOUT_S = int(os.getenv("OLLAMA_TIMEOUT_S", "30"))

def _pg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)

router = APIRouter(prefix="/v1/insights", tags=["insights"])

def _vector_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"

def _embed_text(text: str) -> list[float] | None:
    if not EMBEDDINGS_ENABLED or not text:
        return None
    try:
        payload = json.dumps({"model": OLLAMA_EMBED_MODEL, "prompt": text}).encode("utf-8")
        req = urllib.request.Request(
            f"{OLLAMA_URL}/api/embeddings",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=EMBED_TIMEOUT_S) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        vec = data.get("embedding") or []
        if len(vec) != EMBED_DIM:
            return None
        return vec
    except Exception:
        return None

def _first_line_short(text: str, max_len: int = 220) -> str:
    if not text:
        return ""
    line = text.strip().splitlines()[0].strip()
    line = " ".join(line.split())
    return line[:max_len]

def _clean_object_text(val: str) -> str:
    if not val:
        return ""
    # remove ruidos comuns de cabecalho
    for token in ("http", "E-mail", "CEP:"):
        if token in val:
            val = val.split(token)[0]
    val = re.sub(r"E-mail\s*:\s*\S+", "", val, flags=re.IGNORECASE)
    val = re.sub(r"http\S+", "", val, flags=re.IGNORECASE)
    val = re.sub(r"CEP\s*:\s*\S+", "", val, flags=re.IGNORECASE)
    if "OBJETO" in val:
        val = val.split("OBJETO")[-1]
    val = val.replace("Contrataç oão", "Contratação")
    if "Contrata" in val:
        val = val[val.find("Contrata") :]
    return " ".join(val.split()).strip()

def _heuristic_summary(text: str) -> list[str]:
    if not text:
        return []
    # Normaliza espacos para facilitar regex
    norm = " ".join(text[:12000].split())
    bullets = []

    def clean_upper(val: str, stop_tokens: list[str], max_len: int = 120) -> str:
        for token in stop_tokens:
            if token in val:
                val = val.split(token)[0]
        val = " ".join(val.split()).strip()
        return val[:max_len]

    def pick(pattern: str, label: str, max_len: int = 220):
        m = re.search(pattern, norm, flags=re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if val:
                bullets.append(f"{label}: {val[:max_len]}")

    m_obj = re.search(r"OBJETO\s*[:\-]?\s*(.{20,1200}?)\s*(?:VALOR|DATA|CRIT[ÉE]RIO|MODALIDADE|$)", norm, flags=re.IGNORECASE)
    if m_obj:
        val = _clean_object_text(m_obj.group(1))
        if len(val) < 60:
            m_alt = re.search(r"(Contrata[^.]{60,220})", norm, flags=re.IGNORECASE)
            if m_alt:
                val = _clean_object_text(m_alt.group(1))
        if val:
            bullets.append(f"Objeto: {val[:220]}")
    pick(r"VALOR\s+(?:TOTAL\s+)?ESTIMADO.*?(R\$\s*[0-9\.]+,[0-9]{2}[^\n]{0,80})", "Valor")
    m_sess = re.search(r"DATA\s+DA\s+SESS[ÃA]O\s+P[ÚU]BLICA\s*[:\-]?\s*([0-9]{2}/[0-9]{2}/[0-9]{4}(?:\s+[^\s]{1,5}\s+[0-9]{2}:[0-9]{2}h?)?)", norm, flags=re.IGNORECASE)
    if m_sess:
        bullets.append(f"Sessao: {m_sess.group(1).strip()[:60]}")
    m_mod = re.search(r"MODALIDADE\s*[:\-]?\s*([A-ZÇÃÕ\s]{4,60})", norm, flags=re.IGNORECASE)
    if m_mod:
        val = clean_upper(m_mod.group(1), ["CRIT", "MODO", "PREFER"])
        if val:
            bullets.append(f"Modalidade: {val}")
    m_crit = re.search(r"CRIT[ÉE]RIO\s+DE\s+JULGAMENTO\s*[:\-]?\s*([A-ZÇÃÕ\s]{4,60})", norm, flags=re.IGNORECASE)
    if m_crit:
        val = clean_upper(m_crit.group(1), ["MODO", "PREFER"])
        if val:
            bullets.append(f"Criterio: {val}")
    m_org = re.search(r"(DEPARTAMENTO\s+NACIONAL\s+DE\s+INFRAESTRUTURA\s+DE\s+TRANSPORTES[^\n]{0,120})", norm, flags=re.IGNORECASE)
    if m_org:
        val = clean_upper(m_org.group(1), ["EDITAL", "PREG", "OBJETO"], max_len=140)
        if val:
            bullets.append(f"Orgao: {val}")

    return bullets[:10]

def _ollama_summarize(text: str) -> list[str] | None:
    if not OLLAMA_CHAT_MODEL or not text:
        return None
    # limita o contexto para reduzir tempo e custo
    text = text[:2000]
    prompt = (
        "Responda APENAS com 6 a 10 linhas em formato de bullet iniciando por '-'. "
        "Cada linha com no maximo 14 palavras. "
        "Foque em objeto, orgao, modalidade, datas, valor e exigencias. "
        "Nao copie trechos longos do edital.\n\n"
        f"Texto:\n{text}"
    )
    try:
        payload = {
            "model": OLLAMA_CHAT_MODEL,
            "messages": [
                {"role": "system", "content": "Você é um assistente que resume editais de licitação."},
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
            data = json.loads(resp.read().decode("utf-8"))
        content = ((data.get("message") or {}).get("content") or "").strip()
        if not content:
            return None
        # quebra em bullets simples
        raw_lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
        lines = []
        for ln in raw_lines:
            if ln.startswith(("-", "•")):
                ln = ln.lstrip("-•").strip()
            ln = " ".join(ln.split())
            if ln:
                lines.append(ln)
        if not lines:
            return None
        # se veio um bloco unico gigante, descarta
        if len(lines) == 1 and len(lines[0]) > 300:
            return None
        trimmed = [ln[:220] for ln in lines[:10]]
        return trimmed
    except Exception:
        return None

def _summary_looks_useful(bullets: list[str]) -> bool:
    if not bullets:
        return False
    joined = " ".join(bullets).lower()
    if any(x in joined for x in ("binario", "content type", "bytes")):
        return False
    has_obj = "objeto" in joined or "contrat" in joined
    has_val = "r$" in joined or "valor" in joined
    has_data = "data" in joined or "sess" in joined
    useful_hits = sum([has_obj, has_val, has_data])
    # rejeita quando so tem cabecalho/contato
    if "e-mail" in joined or "http" in joined:
        return useful_hits >= 2
    return has_obj or (has_val and has_data)

async def _tender_quality(conn, tender_id: int) -> dict:
    row = await conn.fetchrow(
        """
        SELECT
          avg(texto_quality) AS avg_quality,
          max(texto_chars) AS max_chars,
          count(*) AS docs
        FROM document
        WHERE tender_id=$1
        """,
        int(tender_id),
    )
    if not row:
        return {"avg_quality": 0.0, "max_chars": 0, "docs": 0}
    return {
        "avg_quality": float(row["avg_quality"] or 0.0),
        "max_chars": int(row["max_chars"] or 0),
        "docs": int(row["docs"] or 0),
    }

def _summary_confidence(fields: dict, quality: dict) -> float:
    field_hits = sum(1 for k in ("objeto", "valor", "sessao", "prazo_proposta", "modalidade", "orgao") if fields.get(k))
    fields_score = min(1.0, field_hits / 6.0)
    q = float(quality.get("avg_quality") or 0.0)
    chars = min(1.0, float(quality.get("max_chars") or 0) / 20000.0)
    score = (0.5 * fields_score) + (0.3 * q) + (0.2 * chars)
    return round(min(1.0, max(0.0, score)), 3)

def _extract_structured(text: str) -> Dict[str, Any]:
    if not text:
        return {}
    norm = " ".join(text[:20000].split())
    out: Dict[str, Any] = {}

    def pick(pattern: str):
        m = re.search(pattern, norm, flags=re.IGNORECASE)
        return m.group(1).strip() if m else None

    obj = pick(r"OBJETO\s*[:\-]?\s*(.{20,1200}?)\s*(?:VALOR|DATA|CRIT[ÉE]RIO|MODALIDADE|$)")
    if obj:
        obj = _clean_object_text(obj)
        if obj:
            out["objeto"] = obj[:400]

    # valor estimado/total/global
    val_global = pick(r"VALOR\s+GLOBAL\s*(R\$\s*[0-9\.]+,[0-9]{2}[^\n]{0,80})")
    val_total = pick(r"VALOR\s+TOTAL\s*(?:ESTIMADO\s+DA\s+CONTRATA[ÇC][AÃ]O\s*)?(R\$\s*[0-9\.]+,[0-9]{2}[^\n]{0,80})")
    val_estimado = pick(r"VALOR\s+(?:TOTAL\s+)?ESTIMADO.*?(R\$\s*[0-9\.]+,[0-9]{2}[^\n]{0,80})")
    if val_global:
        out["valor_global"] = val_global[:120]
    if val_total:
        out["valor_total"] = val_total[:120]
    if val_estimado:
        out["valor_estimado"] = val_estimado[:120]
    # valor preferido
    out["valor"] = out.get("valor_global") or out.get("valor_total") or out.get("valor_estimado")

    sess = pick(r"DATA\s+DA\s+SESS[ÃA]O\s+P[ÚU]BLICA\s*[:\-]?\s*([0-9]{2}/[0-9]{2}/[0-9]{4}[^\n]{0,40})")
    if sess:
        for token in ("CRIT", "MODO", "PREFER"):
            if token in sess:
                sess = sess.split(token)[0].strip()
        out["sessao"] = sess[:80]

    prazo = pick(r"PRAZO\s+FINAL\s+PARA\s+PROPOSTA\S*\s*[:\-]?\s*([0-9]{2}/[0-9]{2}/[0-9]{4}[^\n]{0,40})")
    if prazo:
        out["prazo_proposta"] = prazo[:80]

    mod = pick(r"MODALIDADE\s*[:\-]?\s*([A-ZÇÃÕ\s]{4,80})")
    if mod:
        mod = mod.split("CRIT")[0].strip()
        out["modalidade"] = mod[:80]

    org = pick(r"(DEPARTAMENTO\s+NACIONAL\s+DE\s+INFRAESTRUTURA\s+DE\s+TRANSPORTES[^\n]{0,120})")
    if org:
        out["orgao"] = " ".join(org.split())[:140]

    return out

def _ollama_answer(question: str, evidence: list[dict]) -> str | None:
    if not OLLAMA_CHAT_MODEL or not question or not evidence:
        return None
    # Limita o contexto para evitar respostas enormes
    chunks = []
    for ev in evidence[:3]:
        text = (ev.get("text") or "").strip()
        if not text:
            continue
        chunks.append(text[:400])
    if not chunks:
        return None
    joined = "\n\n---\n\n".join(chunks)
    prompt = (
        "Responda de forma direta e curta em 1 a 3 frases. "
        "Se nao houver certeza, diga que nao consta. "
        "Use apenas o texto fornecido.\n\n"
        f"Pergunta:\n{question}\n\n"
        f"Trechos:\n{joined}"
    )
    try:
        payload = {
            "model": OLLAMA_CHAT_MODEL,
            "messages": [
                {"role": "system", "content": "Voce responde perguntas sobre editais com base em trechos."},
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
            data = json.loads(resp.read().decode("utf-8"))
        content = ((data.get("message") or {}).get("content") or "").strip()
        return content or None
    except Exception:
        return None

def _heuristic_answer(question: str, evidence: list[dict]) -> str | None:
    if not question or not evidence:
        return None
    q = question.lower()
    joined = " ".join((ev.get("text") or "") for ev in evidence[:5])
    joined = " ".join(joined.split())
    if "sess" in q and "data" in q:
        m = re.search(r"DATA\s+DA\s+SESS[ÃA]O\s+P[ÚU]BLICA\s*[:\-]?\s*([0-9]{2}/[0-9]{2}/[0-9]{4}[^\n]{0,40})", joined, flags=re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            for token in ("CRIT", "MODO", "PREFER"):
                if token in val:
                    val = val.split(token)[0].strip()
            return f"Data da sessao publica: {val}."
    if "valor" in q:
        m = re.search(r"VALOR\s+(?:TOTAL\s+)?ESTIMADO.*?(R\$\s*[0-9\.]+,[0-9]{2}[^\n]{0,80})", joined, flags=re.IGNORECASE)
        if m:
            return f"Valor estimado: {m.group(1).strip()}."
    if "objeto" in q:
        m = re.search(r"OBJETO\s*[:\-]?\s*(.{20,400})", joined, flags=re.IGNORECASE)
        if m:
            val = _clean_object_text(m.group(1))
            if len(val) < 60:
                m2 = re.search(r"objeto da presente licita[çc][ãa]o [ée] (.{20,400})", joined, flags=re.IGNORECASE)
                if m2:
                    val = _clean_object_text(m2.group(1))
            if val:
                return f"Objeto: {val[:220]}."
    return None

async def _pool() -> asyncpg.Pool:
    pool = get_pool()
    if pool is None:
        pool = await init_pool()
    return pool

class SummaryIn(BaseModel):
    tender_id: int = Field(..., ge=1)
    limit: int = Field(8, ge=3, le=20)

class ChecklistIn(BaseModel):
    tender_id: int = Field(..., ge=1)

class QaIn(BaseModel):
    tender_id: int = Field(..., ge=1)
    question: str = Field(..., min_length=3)
    limit: int = Field(5, ge=1, le=10)

@router.post("/summary")
async def summary(s: SummaryIn) -> Dict[str, Any]:
    pool = await _pool()
    async with pool.acquire() as conn:
        # prioriza trechos com sinais de resumo (objeto/valor/data/etc.)
        rows = await conn.fetch(
            """
            SELECT id, text
            FROM document_segment
            WHERE tender_id=$1
              AND (
                text ILIKE '%OBJETO%' OR
                text ILIKE '%VALOR%' OR
                text ILIKE '%DATA%' OR
                text ILIKE '%SESSÃO%' OR
                text ILIKE '%SESSAO%' OR
                text ILIKE '%CRIT%' OR
                text ILIKE '%MODALIDADE%'
              )
            ORDER BY id ASC
            LIMIT $2;
            """,
            int(s.tender_id),
            int(s.limit),
        )
        if not rows and EMBEDDINGS_ENABLED:
            qvec = _embed_text("resumo do edital")
            if qvec:
                rows = await conn.fetch(
                    """
                    SELECT id, text
                    FROM document_segment
                    WHERE tender_id=$1 AND embedding IS NOT NULL
                    ORDER BY embedding <=> $2::vector
                    LIMIT $3;
                    """,
                    int(s.tender_id),
                    _vector_literal(qvec),
                    int(s.limit),
                )
        if not rows:
            rows = await conn.fetch(
                """
                SELECT id, text
                FROM document_segment
                WHERE tender_id=$1
                ORDER BY id ASC
                LIMIT $2;
                """,
                int(s.tender_id),
                int(s.limit),
            )
        raw = "\n\n".join([r["text"] for r in rows[:6]])
        fields = _extract_structured(raw)
        bullets = []
        if fields.get("objeto"):
            bullets.append(f"Objeto: {fields['objeto']}")
        if fields.get("valor"):
            bullets.append(f"Valor: {fields['valor']}")
        if fields.get("sessao"):
            bullets.append(f"Sessao: {fields['sessao']}")
        if fields.get("prazo_proposta"):
            bullets.append(f"Prazo proposta: {fields['prazo_proposta']}")
        if fields.get("modalidade"):
            bullets.append(f"Modalidade: {fields['modalidade']}")
        if fields.get("orgao"):
            bullets.append(f"Orgao: {fields['orgao']}")
        if not bullets:
            bullets = _ollama_summarize(raw)
        if bullets and not _summary_looks_useful(bullets):
            bullets = None
        if not bullets:
            bullets = _heuristic_summary(raw)
        if not bullets:
            # fallback enxuto para nao despejar texto bruto
            bullets = [_first_line_short(r["text"]) for r in rows if (r["text"] or "").strip()]
        quality = await _tender_quality(conn, s.tender_id)
        confidence = _summary_confidence(fields, quality)
        return {
            "tender_id": s.tender_id,
            "bullets": bullets,
            "confidence": confidence,
            "quality": quality,
        }

@router.post("/extract")
async def extract_fields(s: SummaryIn) -> Dict[str, Any]:
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, text
            FROM document_segment
            WHERE tender_id=$1
              AND (
                text ILIKE '%OBJETO%' OR
                text ILIKE '%VALOR%' OR
                text ILIKE '%DATA%' OR
                text ILIKE '%SESSÃO%' OR
                text ILIKE '%SESSAO%' OR
                text ILIKE '%CRIT%' OR
                text ILIKE '%MODALIDADE%'
              )
            ORDER BY id ASC
            LIMIT $2;
            """,
            int(s.tender_id),
            int(s.limit),
        )
        raw = "\n\n".join([r["text"] for r in rows])
        fields = _extract_structured(raw)
        quality = await _tender_quality(conn, s.tender_id)
        confidence = _summary_confidence(fields, quality)
        return {"tender_id": s.tender_id, "fields": fields, "confidence": confidence, "quality": quality}

@router.post("/checklist")
async def checklist(c: ChecklistIn) -> Dict[str, Any]:
    # checklist base (stub) — futuramente alimentado por extração
    items = [
        {"title": "Proposta comercial", "priority": "alta"},
        {"title": "Habilitação jurídica", "priority": "alta"},
        {"title": "Regularidade fiscal", "priority": "alta"},
        {"title": "Qualificação técnica", "priority": "media"},
        {"title": "Qualificação econômico-financeira", "priority": "media"},
        {"title": "Declarações obrigatórias", "priority": "media"},
    ]
    return {"tender_id": c.tender_id, "items": items}

@router.post("/qa")
async def qa(q: QaIn) -> Dict[str, Any]:
    # busca simples por trechos relevantes
    pool = await _pool()
    async with pool.acquire() as conn:
        question_l = q.question.lower()
        rows = []
        if "sess" in question_l and "data" in question_l:
            rows = await conn.fetch(
                """
                SELECT id, document_id, tender_id, idx, text, 1.0 AS score
                FROM document_segment
                WHERE tender_id=$1 AND text ILIKE '%DATA DA SESS%'
                ORDER BY id ASC
                LIMIT $2;
                """,
                int(q.tender_id),
                int(q.limit),
            )
        elif "valor" in question_l:
            rows = await conn.fetch(
                """
                SELECT id, document_id, tender_id, idx, text, 1.0 AS score
                FROM document_segment
                WHERE tender_id=$1 AND text ILIKE '%VALOR%ESTIMADO%'
                ORDER BY id ASC
                LIMIT $2;
                """,
                int(q.tender_id),
                int(q.limit),
            )
        elif "objeto" in question_l:
            rows = await conn.fetch(
                """
                SELECT id, document_id, tender_id, idx, text, 1.0 AS score
                FROM document_segment
                WHERE tender_id=$1 AND text ILIKE '%OBJETO%'
                ORDER BY id ASC
                LIMIT $2;
                """,
                int(q.tender_id),
                int(q.limit),
            )
        if EMBEDDINGS_ENABLED:
            qvec = _embed_text(q.question)
            if qvec:
                more = await conn.fetch(
                    """
                    SELECT id, document_id, tender_id, idx, text,
                           1 - (embedding <=> $1::vector) AS score
                    FROM document_segment
                    WHERE tender_id=$2 AND embedding IS NOT NULL
                    ORDER BY embedding <=> $1::vector
                    LIMIT $3;
                    """,
                    _vector_literal(qvec),
                    int(q.tender_id),
                    int(q.limit),
                )
                rows = rows + list(more)
        if not rows:
            rows = await conn.fetch(
                """
                SELECT id, document_id, tender_id, idx, text,
                       ts_rank(tsv, plainto_tsquery('portuguese', $1)) AS rank
                FROM document_segment
                WHERE tender_id=$2 AND tsv @@ plainto_tsquery('portuguese', $1)
                ORDER BY rank DESC
                LIMIT $3;
                """,
                q.question,
                int(q.tender_id),
                int(q.limit),
            )
        # remove duplicados por id mantendo ordem
        seen = set()
        deduped = []
        for r in rows:
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            deduped.append(r)
        rows = deduped
        if not rows:
            return {"tender_id": q.tender_id, "answer": "Não encontrei trechos relevantes.", "evidence": []}
        evidence = [dict(r) for r in rows]
        # para perguntas diretas, prefira heuristica antes do LLM
        answer = _heuristic_answer(q.question, evidence)
        if not answer:
            answer = _ollama_answer(q.question, evidence)
        if not answer:
            answer = "Encontrei trechos relacionados. Revise os destaques abaixo."
        return {"tender_id": q.tender_id, "answer": answer, "evidence": evidence}
