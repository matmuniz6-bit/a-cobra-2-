import os
import re
import unicodedata
import json
import asyncio
import logging
import datetime as dt
import urllib.parse
import urllib.request

import asyncpg

from .metrics import incr_counter
from .events import log_event

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("worker_triage")

TRIAGE_QUEUE = os.getenv("TRIAGE_QUEUE", "q:triage")
FETCH_QUEUE  = os.getenv("FETCH_QUEUE",  "q:fetch_parse")
MIN_SCORE    = int(os.getenv("TRIAGE_MIN_SCORE", "1"))
TRIAGE_MAX_RETRIES = int(os.getenv("TRIAGE_MAX_RETRIES", "3"))
TRIAGE_RETRY_BACKOFF_S = float(os.getenv("TRIAGE_RETRY_BACKOFF_S", "2.0"))
TRIAGE_DEAD_QUEUE = os.getenv("TRIAGE_DEAD_QUEUE", "q:dead_triage")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = (os.getenv("DATABASE_URL", "") or "").strip()
BOT_USERNAME = (os.getenv("BOT_USERNAME", "") or "").strip()
TELEGRAM_UF_CHANNELS = (os.getenv("TELEGRAM_UF_CHANNELS", "") or "").strip()
TRIAGE_UF_ALLOWLIST = (os.getenv("TRIAGE_UF_ALLOWLIST", "") or "").strip()
TRIAGE_MUNICIPIO_ALLOWLIST = (os.getenv("TRIAGE_MUNICIPIO_ALLOWLIST", "") or "").strip()
TELEGRAM_NOTIFY_STAGE = (os.getenv("TELEGRAM_NOTIFY_STAGE", "triage") or "").strip().lower()

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
    parts = [p.strip().upper() for p in raw.split(",") if p.strip()]
    return set(parts)
def _parse_municipio_allowlist(raw: str) -> set[str]:
    if not raw:
        return set()
    return set(_fold(p) for p in raw.split(",") if p.strip())

def _json_default(o):
    if isinstance(o, (dt.datetime, dt.date)):
        return o.isoformat()
    return str(o)

def _send_telegram(text: str, chat_id: str | int | None = None, reply_markup: dict | None = None) -> None:
    token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    chat_id = str(chat_id or "").strip()
    if not token or not chat_id:
        log.warning("Telegram não configurado (faltou TELEGRAM_BOT_TOKEN ou chat_id).")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
    except Exception as e:
        log.warning("Falha enviando Telegram chat_id=%s err=%r", chat_id, e)
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

async def _redis_client():
    import redis.asyncio as redis_async  # type: ignore
    r = redis_async.from_url(REDIS_URL, decode_responses=True)
    return r

def _normalize_db_dsn(dsn: str) -> str:
    # asyncpg NÃO aceita "postgresql+asyncpg://"
    return (
        dsn.replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgres+asyncpg://", "postgres://")
    )

async def _db_pool():
    if not DATABASE_URL:
        log.warning("DATABASE_URL não definido (triage não vai completar dados no Postgres).")
        return None
    dsn = _normalize_db_dsn(DATABASE_URL)
    try:
        return await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    except Exception as e:
        log.exception("Falha criando pool Postgres: %r", e)
        return None

async def _db_fetch(pool, tender_id=None, id_pncp=None, source=None, source_id=None):
    if pool is None:
        return None
    try:
        if tender_id:
            row = await pool.fetchrow(
                "SELECT id, id_pncp, source, source_id, orgao, municipio, uf, modalidade, objeto, data_publicacao, status, urls, materia, categoria "
                "FROM tender WHERE id=$1",
                int(tender_id),
            )
        elif id_pncp:
            row = await pool.fetchrow(
                "SELECT id, id_pncp, source, source_id, orgao, municipio, uf, modalidade, objeto, data_publicacao, status, urls, materia, categoria "
                "FROM tender WHERE id_pncp=$1",
                str(id_pncp),
            )
        elif source and source_id:
            row = await pool.fetchrow(
                "SELECT id, id_pncp, source, source_id, orgao, municipio, uf, modalidade, objeto, data_publicacao, status, urls, materia, categoria "
                "FROM tender WHERE source=$1 AND source_id=$2",
                str(source),
                str(source_id),
            )
        else:
            return None
        return dict(row) if row else None
    except Exception as e:
        log.exception("Falha buscando tender no Postgres: %r", e)
        return None

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

def _fold(text: str | None) -> str:
    if not text:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(text))
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()

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
    except Exception as e:
        log.exception("Falha buscando subscriptions: %r", e)
        return []

def _pick(payload: dict):
    # Aceita formatos:
    #  - {"tender": {...}}
    #  - {"payload": {...}}
    #  - direto {...}
    t = None
    if isinstance(payload, dict) and isinstance(payload.get("tender"), dict):
        t = payload["tender"]
    elif isinstance(payload, dict) and isinstance(payload.get("payload"), dict):
        t = payload["payload"]
    else:
        t = payload

    tender_id = (
        (t.get("id") if isinstance(t, dict) else None)
        or (payload.get("tender_id") if isinstance(payload, dict) else None)
        or (payload.get("id") if isinstance(payload, dict) else None)
    )
    id_pncp = (
        (t.get("id_pncp") if isinstance(t, dict) else None)
        or (payload.get("id_pncp") if isinstance(payload, dict) else None)
    )
    return tender_id, id_pncp, t
def _score(t: dict) -> tuple[int, list[str]]:
    try:
        from app.triage import score_tender  # mesma imagem do api
        out = score_tender(t)
        return int(out.get("score_inicial", 0)), list(out.get("reasons", []))
    except Exception:
        return 0, []

async def main():
    r = await _redis_client()
    pool = await _db_pool()
    log.info("Worker triage iniciado. queue=%s redis=%s", TRIAGE_QUEUE, REDIS_URL)
    uf_channels = _parse_uf_channels(TELEGRAM_UF_CHANNELS)
    uf_allow = _parse_uf_allowlist(TRIAGE_UF_ALLOWLIST)
    mun_allow = _parse_municipio_allowlist(TRIAGE_MUNICIPIO_ALLOWLIST)

    while True:
        payload = None
        try:
            item = await r.brpop(TRIAGE_QUEUE, timeout=0)  # (queue, raw)
            if not item:
                await asyncio.sleep(0.2)
                continue

            _, raw = item

            payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
            await incr_counter("worker.triage.consumed_total")

            tender_id, id_pncp, t = _pick(payload)
            info = dict(t) if isinstance(t, dict) else {}
            source = (payload.get("source") if isinstance(payload, dict) else None) or info.get("source")
            source_id = (payload.get("source_id") if isinstance(payload, dict) else None) or info.get("source_id")
            await log_event(
                pool,
                stage="triage",
                status="consumed",
                tender_id=tender_id if tender_id else None,
                payload={"queue": TRIAGE_QUEUE, "id_pncp": id_pncp},
            )

            # contract_v5_clean: force_fetch pode vir no topo, dentro do tender, ou dentro do payload
            inner_payload = payload.get("payload") if isinstance(payload, dict) and isinstance(payload.get("payload"), dict) else None
            inner_tender  = payload.get("tender")  if isinstance(payload, dict) and isinstance(payload.get("tender"), dict) else None
            force_fetch = bool(
                (payload.get("force_fetch") if isinstance(payload, dict) else False)
                or (info.get("force_fetch") if isinstance(info, dict) else False)
                or (inner_payload.get("force_fetch") if isinstance(inner_payload, dict) else False)
                or (inner_tender.get("force_fetch") if isinstance(inner_tender, dict) else False)
            )

            # completa no DB se faltar campos
            dbinfo = await _db_fetch(pool, tender_id=tender_id, id_pncp=id_pncp, source=source, source_id=source_id)
            if dbinfo:
                info.update(dbinfo)

            score, reasons = _score(info)

            uf = (info.get("uf") or "").upper()
            if uf_allow and uf not in uf_allow and not force_fetch:
                await log_event(
                    pool,
                    stage="triage",
                    status="drop_uf_allowlist",
                    tender_id=info.get("id") or tender_id,
                    payload={"uf": uf, "allowlist": sorted(list(uf_allow))},
                )
                log.info("Drop UF allowlist: id=%s uf=%s", info.get("id") or tender_id, uf)
                continue
            if mun_allow and not force_fetch:
                mun_norm = _fold(info.get("municipio") or "")
                if mun_norm and mun_norm not in mun_allow:
                    await log_event(
                        pool,
                        stage="triage",
                        status="drop_municipio_allowlist",
                        tender_id=info.get("id") or tender_id,
                        payload={"municipio": info.get("municipio"), "allowlist": sorted(list(mun_allow))},
                    )
                    log.info("Drop municipio allowlist: id=%s municipio=%s", info.get("id") or tender_id, info.get("municipio"))
                    continue

            log.info("Consumido %s: id=%s id_pncp=%s score=%s reasons=%s",
                     TRIAGE_QUEUE, info.get("id") or tender_id, info.get("id_pncp") or id_pncp, score, reasons)

            # notificação Telegram pode ser deslocada para o parse (pós-OCR)
            if TELEGRAM_NOTIFY_STAGE == "triage":
                msg = _fmt(info, score=score)
                subs = await _db_active_subscriptions(pool)
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
                            await asyncio.to_thread(_send_telegram, msg, uid)
                        except Exception:
                            pass
                    sent_users.add(uid)
                if not sent_users:
                    log.info("Sem assinaturas compatíveis para notificar.")

                # canal por UF (broadcast)
                channel_id = uf_channels.get(uf)
                if channel_id:
                    # Only send if at least one subscription wants channel delivery
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
                        key = f"chan_sent:{uf}:{info.get('id') or tender_id}"
                        ok = await r.set(key, "1", nx=True, ex=24 * 3600)
                    except Exception:
                        ok = True
                    if ok:
                        url = None
                        if isinstance(info.get("urls"), dict):
                            url = info["urls"].get("pncp") or info["urls"].get("compras") or info["urls"].get("url")
                        tender_id_link = info.get("id") or tender_id
                        bot_link = None
                        if BOT_USERNAME and tender_id_link:
                            bot_link = f"https://t.me/{BOT_USERNAME}?start=qa_{tender_id_link}"
                        follow_link = None
                        if BOT_USERNAME and tender_id_link:
                            follow_link = f"https://t.me/{BOT_USERNAME}?start=follow_{tender_id_link}"
                        buttons = []
                        row = []
                        if url:
                            row.append({"text": "Abrir", "url": url})
                        if bot_link:
                            row.append({"text": "Resumo", "url": bot_link})
                        if row:
                            buttons.append(row)
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

            # empurra pro fetch_docs se passar no filtro mínimo e tiver URL
            # contract_urls_fallback_v3: urls pode vir do DB (info.urls) ou do payload (tender/payload)
            # contract_triage_v3_fix_v2: urls pode vir do DB (info), do tender (t) ou do payload recebido
            urls = info.get("urls") or (t.get("urls") if isinstance(t, dict) else None)
            if not urls and isinstance(payload, dict):
                if isinstance(payload.get("tender"), dict) and payload["tender"].get("urls"):
                    urls = payload["tender"].get("urls")
                elif isinstance(payload.get("payload"), dict) and payload["payload"].get("urls"):
                    urls = payload["payload"].get("urls")
                elif payload.get("urls"):
                    urls = payload.get("urls")
            urls = urls or {}
            if isinstance(urls, str):
                try:
                    urls = json.loads(urls)
                except Exception:
                    urls = {"raw": urls}
            def _pick_url(u: dict | None) -> str | None:
                if not isinstance(u, dict):
                    return None
                return u.get("pncp") or u.get("compras") or u.get("url") or u.get("sistema_origem")

            pick_url = _pick_url(urls if isinstance(urls, dict) else None)

            if (force_fetch or score >= MIN_SCORE) and isinstance(pick_url, str) and pick_url.strip():

                # resolve URL PNCP (pra facilitar downstream)
                pncp_url = None
                try:
                    if isinstance(urls, dict):
                        pncp_url = urls.get("pncp")
                except Exception:
                    pncp_url = None
                payload_fetch = {
                    "force_fetch": force_fetch,
                    "tender_id": info.get("id") or tender_id,
                    "id_pncp": info.get("id_pncp") or id_pncp,
                    "source": info.get("source") or source,
                    "source_id": info.get("source_id") or source_id,
                    "urls": urls,
                    "score": score,
                    "reasons": reasons,
                    "queued_at": dt.datetime.now(dt.timezone.utc),
                }
                payload_fetch["url"] = pncp_url or pick_url
                await r.lpush(FETCH_QUEUE, json.dumps(payload_fetch, ensure_ascii=False, default=_json_default))
                log.info("Enfileirado %s: tender_id=%s id_pncp=%s", FETCH_QUEUE, payload_fetch["tender_id"], payload_fetch["id_pncp"])
                await incr_counter("worker.triage.enqueued_fetch_total")
                await log_event(
                    pool,
                    stage="triage",
                    status="enqueued_fetch",
                    tender_id=payload_fetch.get("tender_id"),
                    payload={"queue": FETCH_QUEUE, "score": score},
                )

        except Exception as e:
            log.exception("Erro no worker_triage: %r", e)
            await incr_counter("worker.triage.error_total")
            retries = int(payload.get("_retries", 0)) if isinstance(payload, dict) else 0
            if payload is not None and retries < TRIAGE_MAX_RETRIES:
                try:
                    await asyncio.sleep(TRIAGE_RETRY_BACKOFF_S * (retries + 1))
                    payload["_retries"] = retries + 1
                    await r.lpush(TRIAGE_QUEUE, json.dumps(payload, ensure_ascii=False, default=_json_default))
                except Exception:
                    pass
            elif payload is not None:
                try:
                    dead = {"reason": "triage_failed", "error": repr(e), "payload": payload}
                    await r.lpush(TRIAGE_DEAD_QUEUE, json.dumps(dead, ensure_ascii=False, default=_json_default))
                    await incr_counter("worker.triage.dead_total")
                    await log_event(
                        pool,
                        stage="triage",
                        status="dead",
                        tender_id=(payload or {}).get("tender_id") if isinstance(payload, dict) else None,
                        payload={"queue": TRIAGE_DEAD_QUEUE, "error": repr(e)},
                    )
                except Exception:
                    pass
            await asyncio.sleep(1.0)

if __name__ == "__main__":
    asyncio.run(main())
