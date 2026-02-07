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

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("worker_daily")

DATABASE_URL = (os.getenv("DATABASE_URL", "") or "").strip()
DAILY_POLL_S = int(os.getenv("DAILY_POLL_S", "3600"))
DAILY_LOOKBACK_H = int(os.getenv("DAILY_LOOKBACK_H", "24"))
DAILY_MAX_ITEMS = int(os.getenv("DAILY_MAX_ITEMS", "8"))


def _send_telegram(text: str, chat_id: str | int | None = None) -> None:
    token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    chat_id = str(chat_id or "").strip()
    if not token or not chat_id:
        log.warning("Telegram não configurado (faltou TELEGRAM_BOT_TOKEN ou chat_id).")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=15) as resp:
        resp.read()


def _normalize_db_dsn(dsn: str) -> str:
    return (
        dsn.replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgres+asyncpg://", "postgres://")
    )


async def _db_pool():
    if not DATABASE_URL:
        log.warning("DATABASE_URL não definido (daily não vai rodar).")
        return None
    dsn = _normalize_db_dsn(DATABASE_URL)
    try:
        return await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    except Exception as e:
        log.exception("Falha criando pool Postgres: %r", e)
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


def _short(text: str, n: int = 90) -> str:
    t = (text or "").strip()
    if len(t) <= n:
        return t
    return t[: n - 3] + "..."


async def _db_active_daily_subscriptions(pool):
    if pool is None:
        return []
    try:
        rows = await pool.fetch(
            """
            SELECT us.id, us.filters, us.frequency, us.is_active,
                   au.id AS user_id, au.telegram_user_id
            FROM user_subscription us
            JOIN app_user au ON au.id=us.user_id
            WHERE us.is_active=true AND us.frequency='daily'
            ORDER BY us.id DESC
            """
        )
        return [dict(r) for r in rows]
    except Exception as e:
        log.exception("Falha buscando subscriptions daily: %r", e)
        return []


async def _db_recent_tenders(pool, since_dt: dt.datetime):
    if pool is None:
        return []
    try:
        rows = await pool.fetch(
            """
            SELECT id, id_pncp, municipio, uf, modalidade, objeto, data_publicacao, urls, materia, categoria
            FROM tender
            WHERE data_publicacao >= $1
            ORDER BY data_publicacao DESC
            """,
            since_dt,
        )
        return [dict(r) for r in rows]
    except Exception as e:
        log.exception("Falha buscando tenders recentes: %r", e)
        return []


async def _already_sent_today(pool, user_id: int) -> bool:
    if pool is None:
        return False
    try:
        row = await pool.fetchrow(
            """
            SELECT 1
            FROM alert
            WHERE user_id=$1 AND type='daily_summary'
              AND sent_at >= date_trunc('day', now())
            LIMIT 1
            """,
            int(user_id),
        )
        return bool(row)
    except Exception as e:
        log.exception("Falha checando resumo diário: %r", e)
        return False


async def _record_sent(pool, user_id: int, payload: dict):
    if pool is None:
        return
    try:
        await pool.execute(
            """
            INSERT INTO alert (user_id, type, payload, sent_at, created_at)
            VALUES ($1, 'daily_summary', $2::jsonb, $3, $4)
            """,
            int(user_id),
            json.dumps(payload or {}),
            dt.datetime.now(dt.timezone.utc),
            dt.datetime.now(dt.timezone.utc),
        )
    except Exception as e:
        log.exception("Falha gravando alert: %r", e)


def _fmt_daily(items: list[dict]) -> str:
    if not items:
        return "Resumo diário: nenhum edital novo nas últimas 24h."
    lines = ["Resumo diário — últimas 24h:"]
    for it in items:
        obj = _short(it.get("objeto") or "")
        muni = it.get("municipio") or "?"
        uf = it.get("uf") or "?"
        id_pncp = it.get("id_pncp") or ""
        url = ""
        urls = it.get("urls") or {}
        if isinstance(urls, str):
            try:
                urls = json.loads(urls)
            except Exception:
                urls = {}
        if isinstance(urls, dict):
            url = urls.get("pncp") or ""
        line = f"- {muni}/{uf} • {obj}"
        if id_pncp:
            line += f" ({id_pncp})"
        if url:
            line += f"\n  {url}"
        lines.append(line)
    return "\n".join(lines)


async def main():
    pool = await _db_pool()
    log.info("Worker daily iniciado. poll_s=%s lookback_h=%s", DAILY_POLL_S, DAILY_LOOKBACK_H)

    while True:
        try:
            if pool is None:
                await asyncio.sleep(10)
                pool = await _db_pool()
                continue

            subs = await _db_active_daily_subscriptions(pool)
            if not subs:
                await asyncio.sleep(DAILY_POLL_S)
                continue

            since_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=DAILY_LOOKBACK_H)
            tenders = await _db_recent_tenders(pool, since_dt)

            # agrupa por usuário, envia no máximo 1 resumo diário por usuário por dia
            by_user = {}
            for sub in subs:
                uid = sub.get("user_id")
                if not uid:
                    continue
                by_user.setdefault(uid, []).append(sub)

            for user_id, user_subs in by_user.items():
                if await _already_sent_today(pool, int(user_id)):
                    continue
                telegram_user_id = user_subs[0].get("telegram_user_id")
                if not telegram_user_id:
                    continue

                matched = []
                for t in tenders:
                    for sub in user_subs:
                        if _matches_filters(t, sub.get("filters") or {}):
                            matched.append(t)
                            break
                    if len(matched) >= DAILY_MAX_ITEMS:
                        break

                msg = _fmt_daily(matched)
                await asyncio.to_thread(_send_telegram, msg, telegram_user_id)
                await _record_sent(pool, int(user_id), {"count": len(matched), "lookback_h": DAILY_LOOKBACK_H})

            await asyncio.sleep(DAILY_POLL_S)
        except Exception as e:
            log.exception("Erro no worker_daily: %r", e)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
