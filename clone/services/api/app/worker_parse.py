import os, json, asyncio, logging
import datetime as dt
import json
import tempfile
import subprocess
import time
import urllib.request
import zipfile

import asyncpg
import redis.asyncio as redis

from .metrics import incr_counter
from .events import log_event
from .agent_enrich import enrich_tender

REDIS_URL   = os.getenv("REDIS_URL", "redis://redis:6379/0")
PARSE_QUEUE = os.getenv("PARSE_QUEUE", "q:parse")
DATABASE_URL = (os.getenv("DATABASE_URL", "") or "").strip()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_CHARS = int(os.getenv("PARSE_MAX_CHARS", "200000"))
DROP_BODY = os.getenv("PARSE_DROP_BODY", "1").strip() not in ("0", "false", "False")
SEGMENT_CHARS = int(os.getenv("SEGMENT_CHARS", "800"))
SEGMENT_OVERLAP = int(os.getenv("SEGMENT_OVERLAP", "100"))
PARSE_OCR = os.getenv("PARSE_OCR", "0").strip() in ("1", "true", "True")
OCR_MIN_TEXT = int(os.getenv("OCR_MIN_TEXT", "200"))
OCR_MIN_QUALITY = float(os.getenv("OCR_MIN_QUALITY", "0.25"))
OCR_MAX_BYTES = int(os.getenv("OCR_MAX_BYTES", str(20 * 1024 * 1024)))
OCR_TIMEOUT_S = int(os.getenv("OCR_TIMEOUT_S", "120"))
OCR_LANG = os.getenv("OCR_LANG", "por+eng")
OCR_JOBS = os.getenv("OCR_JOBS", "2")
OCR_DPI = int(os.getenv("OCR_DPI", "150"))
OCR_MAX_PAGES = int(os.getenv("OCR_MAX_PAGES", "12"))
OCR_PAGE_TIMEOUT_S = int(os.getenv("OCR_PAGE_TIMEOUT_S", "60"))
OCR_MODE = os.getenv("OCR_MODE", "pages")  # pages | ocrmypdf | auto
COMPRESS_PDF = os.getenv("COMPRESS_PDF", "0").strip() in ("1", "true", "True")
COMPRESS_PDF_MIN_BYTES = int(os.getenv("COMPRESS_PDF_MIN_BYTES", str(5 * 1024 * 1024)))
EMBEDDINGS_ENABLED = os.getenv("EMBEDDINGS_ENABLED", "0").strip() in ("1", "true", "True")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
EMBED_DIM = int(os.getenv("EMBED_DIM", "768"))
EMBED_TIMEOUT_S = int(os.getenv("EMBED_TIMEOUT_S", "15"))
TABLE_EXTRACT_ENABLED = os.getenv("TABLE_EXTRACT_ENABLED", "0").strip() in ("1", "true", "True")
DOC_CONVERT_ENABLED = os.getenv("DOC_CONVERT_ENABLED", "0").strip() in ("1", "true", "True")
DOC_CONVERT_STRATEGY = os.getenv("DOC_CONVERT_STRATEGY", "auto")
PARSE_QUEUE_LIST = [q.strip() for q in os.getenv("PARSE_QUEUE_LIST", "").split(",") if q.strip()]
PARSE_SMOKE_QUEUE = os.getenv("PARSE_SMOKE_QUEUE", "q:parse_smoke")
PARSE_SMOKE_DISABLE_OCR = os.getenv("PARSE_SMOKE_DISABLE_OCR", "1").strip() not in ("0", "false", "False")
PARSE_SMOKE_DISABLE_EMBEDDINGS = os.getenv("PARSE_SMOKE_DISABLE_EMBEDDINGS", "1").strip() not in ("0", "false", "False")
PARSE_SMOKE_DROP_BODY = os.getenv("PARSE_SMOKE_DROP_BODY", "1").strip() not in ("0", "false", "False")
PARSE_SMOKE_MAX_CHARS = int(os.getenv("PARSE_SMOKE_MAX_CHARS", "20000"))
PARSE_MAX_RETRIES = int(os.getenv("PARSE_MAX_RETRIES", "3"))
PARSE_RETRY_BACKOFF_S = float(os.getenv("PARSE_RETRY_BACKOFF_S", "2.0"))
PARSE_DEAD_QUEUE = os.getenv("PARSE_DEAD_QUEUE", "q:dead_parse")
POST_OCR_GATE_ENABLED = os.getenv("POST_OCR_GATE_ENABLED", "0").strip() in ("1", "true", "True")
POST_OCR_GATE_KEYWORDS = os.getenv("POST_OCR_GATE_KEYWORDS", "").strip()
POST_OCR_GATE_REGEX = os.getenv("POST_OCR_GATE_REGEX", "").strip()
TELEGRAM_NOTIFY_STAGE = (os.getenv("TELEGRAM_NOTIFY_STAGE", "triage") or "").strip().lower()
TELEGRAM_UF_CHANNELS = (os.getenv("TELEGRAM_UF_CHANNELS", "") or "").strip()
TRIAGE_UF_ALLOWLIST = (os.getenv("TRIAGE_UF_ALLOWLIST", "") or "").strip()
TRIAGE_MUNICIPIO_ALLOWLIST = (os.getenv("TRIAGE_MUNICIPIO_ALLOWLIST", "") or "").strip()
BOT_USERNAME = (os.getenv("BOT_USERNAME", "") or "").strip()

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("parse")

def _json_default(o):
    if isinstance(o, (dt.datetime, dt.date)):
        return o.isoformat()
    return str(o)

def _normalize_db_dsn(dsn: str) -> str:
    return (
        dsn.replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgres+asyncpg://", "postgres://")
    )

def _extract_text(body: bytes | None, content_type: str | None, max_chars: int = MAX_CHARS) -> str:
    if not body:
        return ""
    if isinstance(body, memoryview):
        body = body.tobytes()

    ctype = (content_type or "").lower()
    is_zip = ("zip" in ctype) or (body[:2] == b"PK")

    # ZIP: tenta extrair PDFs internos (PNCP costuma enviar .zip com editais)
    if is_zip:
        try:
            from io import BytesIO
            with zipfile.ZipFile(BytesIO(body)) as zf:
                names = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
                if not names:
                    return ""
                parts = []
                total = 0
                for name in names:
                    try:
                        with zf.open(name) as f:
                            pdf_bytes = f.read()
                        txt = _extract_text(pdf_bytes, "application/pdf")
                        if txt:
                            block = f"[ARQUIVO] {name}\n{txt}"
                            parts.append(block)
                            total += len(block)
                            if total >= max_chars:
                                break
                    except Exception:
                        continue
                return ("\n\n".join(parts)).strip()[:max_chars]
        except Exception:
            return ""

    # texto puro / json / html simples (stub)  [json_pretty_print]
    if "text/" in ctype or "application/json" in ctype or "application/xml" in ctype:
        # JSON: tenta pretty-print (melhor pra downstream)
        if "application/json" in ctype:
            try:
                raw = body.decode("utf-8", errors="ignore")
                obj = json.loads(raw)
                txt = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
            except Exception:
                # fallback: texto cru
                try:
                    txt = body.decode("utf-8", errors="ignore")
                except Exception:
                    txt = body.decode("latin-1", errors="ignore")
        else:
            try:
                txt = body.decode("utf-8", errors="ignore")
            except Exception:
                txt = body.decode("latin-1", errors="ignore")

        # se for HTML, faz um "strip" bem básico (sem depender de libs)
        if "text/html" in ctype:
            import re
            txt = re.sub(r"<script.*?</script>", " ", txt, flags=re.S|re.I)
            txt = re.sub(r"<style.*?</style>", " ", txt, flags=re.S|re.I)
            txt = re.sub(r"<[^>]+>", " ", txt)
            txt = re.sub(r"\s+", " ", txt).strip()

        return txt[:max_chars]

    # PDF: tenta extrair texto com pdfplumber (melhor em layout)
    is_pdf = "pdf" in ctype or (body[:4] == b"%PDF")
    if is_pdf:
        try:
            from io import BytesIO
            import pdfplumber

            parts = []
            total = 0
            with pdfplumber.open(BytesIO(body)) as pdf:
                for page in pdf.pages:
                    txt = page.extract_text() or ""
                    if txt:
                        parts.append(txt)
                        total += len(txt)
                        if total >= max_chars:
                            break
            text = "\n\n".join(parts).strip()
            if text:
                return text[:max_chars]
        except Exception:
            pass
        # fallback: pypdf
        try:
            from io import BytesIO
            from pypdf import PdfReader

            reader = PdfReader(BytesIO(body))
            parts = []
            total = 0
            for page in reader.pages:
                txt = page.extract_text() or ""
                if txt:
                    parts.append(txt)
                    total += len(txt)
                if total >= max_chars:
                    break
            text = "\n\n".join(parts).strip()
            if text:
                return text[:max_chars]
        except Exception:
            pass
        # sem texto, deixa vazio para forçar OCR mais adiante
        return ""

    return f"[BINARIO] content_type={ctype or 'desconhecido'} bytes={len(body)}"[:max_chars]

def _ocr_pdf(body: bytes, max_chars: int = MAX_CHARS) -> str:
    # OCR opcional (depende de ocrmypdf + tesseract instalados)
    if not PARSE_OCR or not body or len(body) > OCR_MAX_BYTES:
        return ""
    try:
        with tempfile.TemporaryDirectory() as td:
            in_pdf = os.path.join(td, "in.pdf")
            out_pdf = os.path.join(td, "out.pdf")
            sidecar = os.path.join(td, "out.txt")
            with open(in_pdf, "wb") as f:
                f.write(body)

            cmd = [
                "ocrmypdf",
                "-l",
                OCR_LANG,
                "--jobs",
                OCR_JOBS,
                "--skip-text",
                "--sidecar",
                sidecar,
                in_pdf,
                out_pdf,
            ]
            subprocess.run(cmd, check=True, timeout=OCR_TIMEOUT_S, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if os.path.exists(sidecar):
                with open(sidecar, "r", encoding="utf-8", errors="ignore") as f:
                    return f.read()[:max_chars]
    except Exception:
        return ""
    return ""

def _ocr_pdf_pages(body: bytes, max_chars: int = MAX_CHARS) -> str:
    # OCR por página (fallback mais controlado)
    if not PARSE_OCR or not body or len(body) > OCR_MAX_BYTES:
        return ""
    try:
        with tempfile.TemporaryDirectory() as td:
            in_pdf = os.path.join(td, "in.pdf")
            with open(in_pdf, "wb") as f:
                f.write(body)
            prefix = os.path.join(td, "page")
            cmd = ["pdftoppm", "-png", "-r", str(OCR_DPI), in_pdf, prefix]
            subprocess.run(cmd, check=True, timeout=OCR_TIMEOUT_S, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            texts = []
            pages = sorted([p for p in os.listdir(td) if p.startswith("page-") and p.endswith(".png")])
            for i, name in enumerate(pages):
                if i >= OCR_MAX_PAGES:
                    break
                img = os.path.join(td, name)
                try:
                    r = subprocess.run(
                        ["tesseract", img, "stdout", "-l", OCR_LANG],
                        check=True,
                        timeout=OCR_PAGE_TIMEOUT_S,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                    )
                    txt = r.stdout.decode("utf-8", "ignore").strip()
                    if txt:
                        texts.append(txt)
                    if sum(len(t) for t in texts) >= max_chars:
                        break
                except Exception:
                    continue
            out = "\n\n".join(texts).strip()
            return out[:max_chars]
    except Exception:
        return ""
    return ""

def _zip_first_pdf(body: bytes) -> bytes | None:
    if not body or body[:2] != b"PK":
        return None
    try:
        from io import BytesIO
        with zipfile.ZipFile(BytesIO(body)) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
            if not names:
                return None
            with zf.open(names[0]) as f:
                return f.read()
    except Exception:
        return None
    return None

def _compress_pdf_light(body: bytes) -> bytes | None:
    if not body or len(body) < COMPRESS_PDF_MIN_BYTES:
        return None
    if not COMPRESS_PDF:
        return None
    try:
        with tempfile.TemporaryDirectory() as td:
            in_pdf = os.path.join(td, "in.pdf")
            out_pdf = os.path.join(td, "out.pdf")
            with open(in_pdf, "wb") as f:
                f.write(body)
            cmd = [
                "gs",
                "-sDEVICE=pdfwrite",
                "-dCompatibilityLevel=1.4",
                "-dPDFSETTINGS=/printer",
                "-dNOPAUSE",
                "-dBATCH",
                "-dQUIET",
                f"-sOutputFile={out_pdf}",
                in_pdf,
            ]
            subprocess.run(cmd, check=True, timeout=OCR_TIMEOUT_S, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if os.path.exists(out_pdf):
                with open(out_pdf, "rb") as f:
                    return f.read()
    except Exception:
        return None
    return None

def _segment_text(text: str) -> list[str]:
    if not text:
        return []
    segs: list[str] = []
    size = max(200, SEGMENT_CHARS)
    overlap = max(0, min(SEGMENT_OVERLAP, size - 1))
    start = 0
    n = len(text)
    while start < n:
        end = min(n, start + size)
        seg = text[start:end].strip()
        if seg:
            segs.append(seg)
        if end >= n:
            break
        start = end - overlap
    return segs


def _text_quality(text: str) -> float:
    if not text:
        return 0.0
    total = len(text)
    printable = sum(1 for c in text if c.isprintable())
    alnum = sum(1 for c in text if c.isalnum())
    # score heuristic: printable ratio * (alnum ratio + small boost)
    pr = printable / total if total else 0.0
    ar = alnum / total if total else 0.0
    return round(pr * (ar + 0.1), 4)

def _fold(text: str | None) -> str:
    if not text:
        return ""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", str(text))
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()

def _parse_uf_channels(raw: str) -> dict:
    out = {}
    if not raw:
        return out
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        uf, cid = part.split(":", 1)
        uf = uf.strip().upper()
        cid = cid.strip()
        if uf and cid:
            out[uf] = cid
    return out

def _parse_uf_allowlist(raw: str) -> set[str]:
    if not raw:
        return set()
    return set([p.strip().upper() for p in raw.split(",") if p.strip()])

def _parse_municipio_allowlist(raw: str) -> set[str]:
    if not raw:
        return set()
    return set(_fold(p) for p in raw.split(",") if p.strip())

def _send_telegram(text: str, chat_id: str | int | None = None, reply_markup: dict | None = None) -> None:
    token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    chat_id = str(chat_id or "").strip()
    if not token or not chat_id:
        return
    import urllib.parse, urllib.request
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
    except Exception:
        return

def _iso(x) -> str:
    if isinstance(x, (dt.datetime, dt.date)):
        return x.isoformat()
    return str(x) if x else ""

def _short(s: str | None, n: int = 200) -> str:
    if not s:
        return ""
    s = str(s).strip()
    if len(s) <= n:
        return s
    return s[: max(0, n - 3)] + "..."

def _fmt(info: dict, score: int | None = None) -> str:
    id_pncp = info.get("id_pncp") or "?"
    uf = info.get("uf") or "??"
    municipio = info.get("municipio") or "??"
    orgao = info.get("orgao") or "?"
    modalidade = info.get("modalidade") or "?"
    status = info.get("status") or "?"
    objeto = _short(info.get("objeto") or "", 220)
    dp = _iso(info.get("data_publicacao"))

    parts = [
        f"✅ OPORTUNIDADE — {id_pncp}",
        f"Órgão: {orgao}",
        f"Local: {municipio}/{uf}",
        f"Modalidade: {modalidade}",
        f"Status: {status}",
    ]
    if dp:
        parts.append(f"Publicação: {dp}")
    if score is not None:
        parts.append(f"Score: {score}")
    if objeto:
        parts.append(f"Resumo: {objeto}")
    return "\n".join(parts)

def _normalize_filters(filters) -> dict:
    if filters is None:
        return {}
    if isinstance(filters, dict):
        return filters
    if isinstance(filters, str):
        try:
            parsed = json.loads(filters)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}

def _match_list(value: str | None, allowed) -> bool:
    if not allowed:
        return True
    if isinstance(allowed, str):
        allowed = [allowed]
    allowed_norm = [str(x).strip().upper() for x in allowed if str(x).strip()]
    if "ALL" in allowed_norm:
        return True
    if not value:
        return False
    return str(value).strip().upper() in allowed_norm

def _match_keywords(text: str, keywords) -> bool:
    if not keywords:
        return True
    if isinstance(keywords, str):
        keywords = [keywords]
    text_l = (text or "").lower()
    import re
    for kw in keywords:
        k = str(kw).strip().lower()
        if not k:
            continue
        if re.search(rf"\b{re.escape(k)}\b", text_l):
            return True
    return False

def _normalize_list(value) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(v) for v in value if str(v).strip()]
    if isinstance(value, str):
        return [value]
    return [str(value)]

def _matches_filters(info: dict, filters: dict) -> bool:
    filters = _normalize_filters(filters)
    if not filters:
        return True
    uf_ok = _match_list(info.get("uf"), filters.get("uf"))
    mun_ok = _match_list(info.get("municipio"), filters.get("municipio"))
    mod_allowed = _normalize_list(filters.get("modalidade"))
    info_mod_norm = _fold(info.get("modalidade"))
    mod_allowed_norm = [_fold(m) for m in (mod_allowed or []) if _fold(m)]
    mod_ok = _match_list(info_mod_norm, mod_allowed_norm if mod_allowed_norm else None)
    obj = info.get("objeto") or ""
    kw_ok = _match_keywords(_fold(obj), [_fold(k) for k in _normalize_list(filters.get("keywords")) or []])
    cat_ok = _match_keywords(_fold(obj), [_fold(k) for k in _normalize_list(filters.get("categoria")) or []])
    mat_allowed = _normalize_list(filters.get("materia") or filters.get("categoria"))
    info_mat_norm = _fold(info.get("materia") or info.get("categoria"))
    mat_allowed_norm = [_fold(m) for m in (mat_allowed or []) if _fold(m)]
    mat_ok = _match_list(info_mat_norm, mat_allowed_norm if mat_allowed_norm else None)
    rep = (filters.get("republicacoes") or "").lower()
    rep_ok = True
    if rep in ("new_only", "new"):
        rep_flag = (info.get("republicacao") or info.get("is_republication") or "").lower()
        if rep_flag in ("true", "1", "yes", "sim"):
            rep_ok = False
    return uf_ok and mun_ok and mod_ok and kw_ok and cat_ok and mat_ok and rep_ok

async def _db_active_subscriptions(pool):
    if pool is None:
        return []
    try:
        rows = await pool.fetch(
            """
            SELECT us.id, us.filters, us.delivery, us.frequency, us.is_active, au.telegram_user_id
            FROM user_subscription us
            JOIN app_user au ON au.id=us.user_id
            WHERE us.is_active=true
            ORDER BY us.id DESC
            """
        )
        return [dict(r) for r in rows]
    except Exception:
        return []
def _post_ocr_gate(text: str) -> bool:
    if not POST_OCR_GATE_ENABLED:
        return True
    if not text:
        return False
    text_l = text.lower()
    if POST_OCR_GATE_KEYWORDS:
        kws = [k.strip().lower() for k in POST_OCR_GATE_KEYWORDS.split(",") if k.strip()]
        for kw in kws:
            if kw in text_l:
                return True
    if POST_OCR_GATE_REGEX:
        try:
            import re
            if re.search(POST_OCR_GATE_REGEX, text, flags=re.IGNORECASE | re.MULTILINE):
                return True
        except Exception:
            return False
    return False

def _doc_type(content_type: str | None, body: bytes | None) -> str:
    ctype = (content_type or "").lower()
    if "pdf" in ctype or (body or b"")[:4] == b"%PDF":
        return "pdf"
    if "zip" in ctype or (body or b"")[:2] == b"PK":
        return "zip"
    if "json" in ctype:
        return "json"
    if "html" in ctype:
        return "html"
    if "text/" in ctype or "xml" in ctype:
        return "text"
    return "binary"

def _should_ocr(doc_type: str, text: str, quality: float) -> bool:
    if not PARSE_OCR:
        return False
    if doc_type not in ("pdf", "zip"):
        return False
    if len(text or "") < OCR_MIN_TEXT:
        return True
    if quality < OCR_MIN_QUALITY:
        return True
    return False

async def _store_artifact(pool, document_id: int, kind: str, payload: dict | list | str | None) -> None:
    if pool is None:
        return
    try:
        await pool.execute(
            """
            INSERT INTO document_artifact (document_id, kind, payload)
            VALUES ($1,$2,$3::jsonb)
            ON CONFLICT (document_id, kind) DO UPDATE SET
              payload=EXCLUDED.payload,
              created_at=now()
            """,
            int(document_id),
            str(kind),
            json.dumps(payload, ensure_ascii=False),
        )
    except Exception:
        return

def _extract_tables_pdf(body: bytes) -> list[dict]:
    if not TABLE_EXTRACT_ENABLED or not body:
        return []
    max_bytes = int(os.getenv("TABLE_EXTRACT_MAX_BYTES", str(10 * 1024 * 1024)))
    max_pages = int(os.getenv("TABLE_EXTRACT_MAX_PAGES", "5"))
    flavor = os.getenv("TABLE_EXTRACT_FLAVOR", "stream")
    if len(body) > max_bytes:
        return []
    try:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "in.pdf")
            with open(path, "wb") as f:
                f.write(body)
            # Camelot (preferido)
            try:
                import camelot  # type: ignore
                tables = camelot.read_pdf(path, pages=f"1-{max_pages}", flavor=flavor)
                out = []
                for t in tables:
                    try:
                        rows = t.df.values.tolist()
                    except Exception:
                        rows = []
                    if rows:
                        out.append({"page": getattr(t, "page", None), "rows": rows})
                if out:
                    return out
            except Exception:
                pass
            # Tabula (fallback)
            try:
                import tabula  # type: ignore
                dfs = tabula.read_pdf(path, pages=f"1-{max_pages}", multiple_tables=True)
                out = []
                for df in dfs or []:
                    try:
                        rows = df.fillna("").astype(str).values.tolist()
                    except Exception:
                        rows = []
                    if rows:
                        out.append({"page": None, "rows": rows})
                return out
            except Exception:
                return []
    except Exception:
        return []

def _doc_convert_unstructured(body: bytes, content_type: str | None) -> dict | None:
    if not DOC_CONVERT_ENABLED or not body:
        return None
    try:
        from unstructured.partition.auto import partition  # type: ignore
    except Exception:
        return None
    try:
        with tempfile.TemporaryDirectory() as td:
            ext = ".bin"
            doc_type = _doc_type(content_type, body)
            if doc_type == "pdf":
                ext = ".pdf"
            elif doc_type == "html":
                ext = ".html"
            elif doc_type == "json":
                ext = ".json"
            path = os.path.join(td, f"in{ext}")
            with open(path, "wb") as f:
                f.write(body)
            strategy = DOC_CONVERT_STRATEGY
            if DOC_CONVERT_STRATEGY == "auto":
                strategy = "hi_res" if doc_type == "pdf" else "fast"
            elements = partition(filename=path, strategy=strategy)
            texts = [getattr(e, "text", "") for e in elements if getattr(e, "text", "")]
            md = "\n\n".join(texts).strip()
            return {"markdown": md, "elements": [str(e) for e in elements[:200]]}
    except Exception:
        return None

def _doc_convert_docling(body: bytes, content_type: str | None) -> dict | None:
    if not DOC_CONVERT_ENABLED or not body:
        return None
    try:
        from docling.document_converter import DocumentConverter  # type: ignore
    except Exception:
        return None

def _doc_convert_fallback(text: str | None) -> dict | None:
    if not DOC_CONVERT_ENABLED:
        return None
    if not text:
        return None
    # Simple AI-ready fallback: store plain text as markdown.
    return {"markdown": text}
    try:
        with tempfile.TemporaryDirectory() as td:
            ext = ".bin"
            doc_type = _doc_type(content_type, body)
            if doc_type == "pdf":
                ext = ".pdf"
            path = os.path.join(td, f"in{ext}")
            with open(path, "wb") as f:
                f.write(body)
            conv = DocumentConverter()
            doc = conv.convert(path)
            md = getattr(doc, "document", None)
            if hasattr(md, "to_markdown"):
                return {"markdown": md.to_markdown()}
            return {"markdown": str(doc)}
    except Exception:
        return None

def _vector_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"

def _embed_text(text: str, enabled: bool = True) -> list[float] | None:
    if not enabled or not EMBEDDINGS_ENABLED or not text:
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

async def _db_pool():
    if not DATABASE_URL:
        log.warning("DATABASE_URL vazio — não vou ler/gravar no Postgres.")
        return None
    dsn = _normalize_db_dsn(DATABASE_URL)
    try:
        return await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    except Exception as e:
        log.exception("Falha criando pool Postgres: %r", e)
        return None

async def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    pool = await _db_pool()
    uf_channels = _parse_uf_channels(TELEGRAM_UF_CHANNELS)
    uf_allow = _parse_uf_allowlist(TRIAGE_UF_ALLOWLIST)
    mun_allow = _parse_municipio_allowlist(TRIAGE_MUNICIPIO_ALLOWLIST)

    queues = PARSE_QUEUE_LIST or [PARSE_SMOKE_QUEUE, PARSE_QUEUE]
    # remove duplicados preservando ordem
    seen = set()
    queues = [q for q in queues if q and not (q in seen or seen.add(q))]

    log.info("Worker parse iniciado. queues=%s redis=%s max_chars=%s", ",".join(queues), REDIS_URL, MAX_CHARS)

    while True:
        if pool is None:
            pool = await _db_pool()
            if pool is None:
                await asyncio.sleep(1.0)
                continue

        item = await r.brpop(queues, timeout=0)
        if not item:
            await asyncio.sleep(0.2)
            continue

        queue_name, raw = item
        is_smoke = queue_name == PARSE_SMOKE_QUEUE
        parse_ocr = PARSE_OCR and not (is_smoke and PARSE_SMOKE_DISABLE_OCR)
        embed_enabled = EMBEDDINGS_ENABLED and not (is_smoke and PARSE_SMOKE_DISABLE_EMBEDDINGS)
        drop_body = DROP_BODY or (is_smoke and PARSE_SMOKE_DROP_BODY)
        max_chars = min(MAX_CHARS, PARSE_SMOKE_MAX_CHARS) if is_smoke else MAX_CHARS
        try:
            msg = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            msg = {"_raw": raw}
        await incr_counter("worker.parse.consumed_total")
        await log_event(
            pool,
            stage="parse",
            status="consumed",
            document_id=msg.get("document_id") if isinstance(msg, dict) else None,
            payload={"queue": queue_name},
        )

        doc_id = msg.get("document_id")
        if not doc_id:
            log.warning("Job sem document_id: %s", msg)
            await incr_counter("worker.parse.error_total")
            await log_event(
                pool,
                stage="parse",
                status="error_missing_document_id",
                payload={"queue": PARSE_QUEUE},
            )
            continue

        # tudo dentro de try pra não derrubar o worker
        try:
            if pool is None:
                log.warning("Sem Postgres (pool=None). Ignorando doc_id=%s", doc_id)
                continue

            row = await pool.fetchrow(
                "SELECT id, tender_id, url, content_type, body, texto_extraido FROM document WHERE id=$1",
                int(doc_id),
            )
            if not row:
                log.warning("Document não encontrado: id=%s", doc_id)
                continue

            # Se o body já foi descartado, reutiliza texto_extraido para segmentar
            ocr_used = False
            if row["body"] is None and row["texto_extraido"]:
                text = row["texto_extraido"]
            else:
                body = row["body"] or b""
                text = _extract_text(body, row["content_type"], max_chars=max_chars)
                # OCR se PDF (content-type ou assinatura) e pouco texto
                is_pdf = (row["content_type"] and "pdf" in row["content_type"].lower()) or (body[:4] == b"%PDF")
                is_zip = (row["content_type"] and "zip" in row["content_type"].lower()) or (body[:2] == b"PK")
                text_quality = _text_quality(text or "")
                doc_type = _doc_type(row["content_type"], body)
                if parse_ocr and _should_ocr(doc_type, text or "", text_quality):
                    t0 = time.perf_counter()
                    log.info("OCR start doc_id=%s mode=%s", doc_id, OCR_MODE)
                    ocr_text = ""
                    ocr_body = body
                    if is_zip:
                        ocr_body = _zip_first_pdf(body) or b""
                    if ocr_body:
                        compressed = _compress_pdf_light(ocr_body)
                        if compressed:
                            ocr_body = compressed
                    if ocr_body:
                        if OCR_MODE == "pages":
                            ocr_text = _ocr_pdf_pages(ocr_body, max_chars=max_chars)
                        elif OCR_MODE == "ocrmypdf":
                            ocr_text = _ocr_pdf(ocr_body, max_chars=max_chars)
                        else:  # auto
                            ocr_text = _ocr_pdf(ocr_body, max_chars=max_chars)
                            if not ocr_text:
                                ocr_text = _ocr_pdf_pages(ocr_body, max_chars=max_chars)
                    log.info("OCR done doc_id=%s chars=%s elapsed=%.1fs", doc_id, len(ocr_text or ""), time.perf_counter() - t0)
                    if ocr_text:
                        text = ocr_text
                        ocr_used = True

            # grava texto (stub) + timestamp
            text_chars = len(text or "")
            text_quality = _text_quality(text or "")
            if row["body"] is not None and drop_body:
                await pool.execute(
                    "UPDATE document SET texto_extraido=$2, texto_path=$3, baixado_em=COALESCE(baixado_em, $4), body=NULL, texto_chars=$5, texto_quality=$6, ocr_used=$7 WHERE id=$1",
                    int(doc_id),
                    text,
                    None,
                    dt.datetime.now(dt.timezone.utc),
                    int(text_chars),
                    float(text_quality),
                    bool(ocr_used),
                )
            elif row["body"] is not None:
                await pool.execute(
                    "UPDATE document SET texto_extraido=$2, texto_path=$3, baixado_em=COALESCE(baixado_em, $4), texto_chars=$5, texto_quality=$6, ocr_used=$7 WHERE id=$1",
                    int(doc_id),
                    text,
                    None,
                    dt.datetime.now(dt.timezone.utc),
                    int(text_chars),
                    float(text_quality),
                    bool(ocr_used),
                )

            if not _post_ocr_gate(text):
                await log_event(
                    pool,
                    stage="parse",
                    status="drop_post_ocr_gate",
                    tender_id=int(row["tender_id"]),
                    document_id=int(row["id"]),
                    payload={"reason": "post_ocr_gate"},
                )
                log.info("POST_OCR_GATE drop: doc_id=%s tender_id=%s", row["id"], row["tender_id"])
                continue

            # enriquecimento por agente (materia/categoria)
            if not is_smoke and text:
                try:
                    trow = await pool.fetchrow(
                        """
                        SELECT id, id_pncp, source, source_id, orgao, municipio, uf, modalidade, objeto,
                               materia, categoria, data_publicacao, status, urls
                        FROM tender
                        WHERE id=$1
                        """,
                        int(row["tender_id"]),
                    )
                except Exception:
                    trow = None
                meta = {}
                existing = None
                if trow:
                    meta = {
                        "id_pncp": trow.get("id_pncp"),
                        "source": trow.get("source"),
                        "source_id": trow.get("source_id"),
                        "orgao": trow.get("orgao"),
                        "municipio": trow.get("municipio"),
                        "uf": trow.get("uf"),
                        "modalidade": trow.get("modalidade"),
                        "objeto": trow.get("objeto"),
                    }
                    existing = {"materia": trow.get("materia"), "categoria": trow.get("categoria")}
                await enrich_tender(pool, int(row["tender_id"]), text, meta, existing=existing)

                # notificação Telegram pós-OCR (opcional)
                if TELEGRAM_NOTIFY_STAGE == "parse" and trow:
                    info = dict(trow)
                    uf = (info.get("uf") or "").upper()
                    if uf_allow and uf not in uf_allow:
                        pass
                    else:
                        mun_norm = _fold(info.get("municipio") or "")
                        if mun_allow and mun_norm and mun_norm not in mun_allow:
                            pass
                        else:
                            # dedupe por tender + usuário
                            subs = await _db_active_subscriptions(pool)
                            msg = _fmt(info, score=None)
                            sent_users = set()
                            for sub in subs:
                                if not sub.get("telegram_user_id"):
                                    continue
                                if str(sub.get("frequency") or "realtime").lower() not in ("realtime", "rt"):
                                    continue
                                if not _matches_filters(info, sub.get("filters") or {}):
                                    continue
                                delivery = sub.get("delivery") or {"pv": True, "channel": True}
                                if isinstance(delivery, str):
                                    try:
                                        delivery = json.loads(delivery)
                                    except Exception:
                                        delivery = {"pv": True, "channel": True}
                                uid = int(sub["telegram_user_id"])
                                if uid in sent_users:
                                    continue
                                if delivery.get("pv", True):
                                    try:
                                        key = f"tg_sent:parse:{info.get('id')}:{uid}"
                                        ok = await r.set(key, "1", nx=True, ex=24 * 3600)
                                    except Exception:
                                        ok = True
                                    if ok:
                                        try:
                                            await asyncio.to_thread(_send_telegram, msg, uid)
                                        except Exception:
                                            pass
                                sent_users.add(uid)

                            # canal por UF (broadcast)
                            channel_id = uf_channels.get(uf)
                            if channel_id:
                                wants_channel = False
                                for sub in subs:
                                    if not _matches_filters(info, sub.get("filters") or {}):
                                        continue
                                    delivery = sub.get("delivery") or {"pv": True, "channel": True}
                                    if isinstance(delivery, str):
                                        try:
                                            delivery = json.loads(delivery)
                                        except Exception:
                                            delivery = {"pv": True, "channel": True}
                                    if delivery.get("channel", True):
                                        wants_channel = True
                                        break
                                if not wants_channel:
                                    channel_id = None
                            if channel_id:
                                try:
                                    key = f"tg_sent:parse:chan:{uf}:{info.get('id')}"
                                    ok = await r.set(key, "1", nx=True, ex=24 * 3600)
                                except Exception:
                                    ok = True
                                if ok:
                                    url = None
                                    if isinstance(info.get("urls"), dict):
                                        url = info["urls"].get("pncp") or info["urls"].get("compras") or info["urls"].get("url")
                                    tender_id_link = info.get("id")
                                    bot_link = None
                                    if BOT_USERNAME and tender_id_link:
                                        bot_link = f"https://t.me/{BOT_USERNAME}?start=qa_{tender_id_link}"
                                    follow_link = None
                                    if BOT_USERNAME and tender_id_link:
                                        follow_link = f"https://t.me/{BOT_USERNAME}?start=follow_{tender_id_link}"
                                    buttons = []
                                    rowb = []
                                    if url:
                                        rowb.append({"text": "Abrir", "url": url})
                                    if bot_link:
                                        rowb.append({"text": "Resumo", "url": bot_link})
                                    if rowb:
                                        buttons.append(rowb)
                                    row2 = []
                                    if bot_link:
                                        row2.append({"text": "Checklist", "url": bot_link})
                                    if follow_link:
                                        row2.append({"text": "Seguir", "url": follow_link})
                                    if row2:
                                        buttons.append(row2)
                                    reply_markup = {"inline_keyboard": buttons} if buttons else None
                                    try:
                                        await asyncio.to_thread(_send_telegram, msg, channel_id, reply_markup)
                                    except Exception:
                                        pass

            # artefatos (tabelas/markdown/estruturado)
            if not is_smoke:
                try:
                    body_bytes = row["body"] or b""
                    if body_bytes and _doc_type(row["content_type"], body_bytes) == "pdf":
                        tables = _extract_tables_pdf(body_bytes)
                        if tables:
                            await _store_artifact(pool, int(doc_id), "tables", tables)
                    # unstructured/docling (markdown e estrutura)
                    conv = _doc_convert_unstructured(body_bytes, row["content_type"])
                    if conv is None:
                        conv = _doc_convert_docling(body_bytes, row["content_type"])
                    if conv is None:
                        conv = _doc_convert_fallback(text)
                    if isinstance(conv, dict) and (conv.get("markdown") or conv.get("elements")):
                        await _store_artifact(pool, int(doc_id), "doc_convert", conv)
                except Exception:
                    pass

            # segmentos para busca simples
            segs = _segment_text(text or "")
            if segs:
                await pool.execute(
                    "DELETE FROM document_segment WHERE document_id=$1",
                    int(doc_id),
                )
                for i, seg in enumerate(segs):
                    vec = _embed_text(seg, enabled=embed_enabled)
                    vec_lit = _vector_literal(vec) if vec else None
                    await pool.execute(
                        """
                        INSERT INTO document_segment (document_id, tender_id, idx, text, tsv, embedding)
                        VALUES ($1,$2,$3,$4, to_tsvector('portuguese', $4), $5::vector)
                        """,
                        int(doc_id),
                        int(row["tender_id"]),
                        int(i),
                        seg,
                        vec_lit,
                    )

            log.info("PARSE OK: doc_id=%s tender_id=%s chars=%s url=%s",
                     row["id"], row["tender_id"], len(text or ""), row["url"])
            await incr_counter("worker.parse.ok_total")
            await log_event(
                pool,
                stage="parse",
                status="ok",
                tender_id=int(row["tender_id"]),
                document_id=int(row["id"]),
                payload={"chars": len(text or "")},
            )

        except Exception as e:
            log.exception("Falha no parse doc_id=%s: %r", doc_id, e)
            await incr_counter("worker.parse.error_total")
            retries = int(msg.get("_retries", 0)) if isinstance(msg, dict) else 0
            if retries < PARSE_MAX_RETRIES:
                try:
                    await asyncio.sleep(PARSE_RETRY_BACKOFF_S * (retries + 1))
                    msg["_retries"] = retries + 1
                    await r.lpush(queue_name, json.dumps(msg, ensure_ascii=False, default=_json_default))
                    await incr_counter("worker.parse.retry_total")
                    await log_event(
                        pool,
                        stage="parse",
                        status="retry",
                        document_id=doc_id,
                        payload={"queue": queue_name, "retries": retries + 1, "error": repr(e)},
                    )
                except Exception:
                    pass
            else:
                try:
                    dead = {"reason": "parse_failed", "error": repr(e), "payload": msg}
                    await r.lpush(PARSE_DEAD_QUEUE, json.dumps(dead, ensure_ascii=False, default=_json_default))
                    await incr_counter("worker.parse.dead_total")
                    await log_event(
                        pool,
                        stage="parse",
                        status="dead_parse_failed",
                        document_id=doc_id,
                        payload={"queue": PARSE_DEAD_QUEUE, "error": repr(e)},
                    )
                except Exception:
                    pass

if __name__ == "__main__":
    asyncio.run(main())
