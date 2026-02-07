import os
import json
import asyncio
import datetime as dt
import urllib.parse
import urllib.request

from .metrics import incr_counter

COMPRAS_API_BASE = os.getenv("COMPRAS_API_BASE", "https://compras.dados.gov.br").rstrip("/")
COMPRAS_LIST_PATH = os.getenv("COMPRAS_LIST_PATH", "/licitacoes/v1/licitacoes.json")
COMPRAS_DETAIL_PATH = os.getenv("COMPRAS_DETAIL_PATH", "/licitacoes/id/licitacao/{id}.json")
COMPRAS_POLL_S = float(os.getenv("COMPRAS_POLL_S", "3600"))
COMPRAS_MAX_PAGES = int(os.getenv("COMPRAS_MAX_PAGES", "10"))
COMPRAS_MAX_ITEMS = int(os.getenv("COMPRAS_MAX_ITEMS", "500"))
COMPRAS_DATE_FIELD = os.getenv("COMPRAS_DATE_FIELD", "data_abertura_proposta")
COMPRAS_DATA_INICIAL = os.getenv("COMPRAS_DATA_INICIAL", "")
COMPRAS_DATA_FINAL = os.getenv("COMPRAS_DATA_FINAL", "")
COMPRAS_UASG = os.getenv("COMPRAS_UASG", "")

CORE_API_URL = os.getenv("CORE_API_URL", os.getenv("API_BASE_URL", "http://api:8080"))
CORE_API_KEY = (os.getenv("CORE_API_KEY", "") or os.getenv("API_KEY", "") or "").strip()


def _today_ymd() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%d")


def _date_range() -> tuple[str, str]:
    if COMPRAS_DATA_INICIAL and COMPRAS_DATA_FINAL:
        return COMPRAS_DATA_INICIAL, COMPRAS_DATA_FINAL
    today = _today_ymd()
    return today, today


def _http_get(url: str, timeout: int = 30):
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return json.loads(raw.decode("utf-8")) if raw else {}


def _http_post(url: str, payload: dict, timeout: int = 30):
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if CORE_API_KEY:
        headers["x-api-key"] = CORE_API_KEY
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return json.loads(raw.decode("utf-8")) if raw else {}


def _build_list_url() -> str:
    data_inicial, data_final = _date_range()
    params = {}
    if COMPRAS_DATE_FIELD:
        params[f"{COMPRAS_DATE_FIELD}_min"] = data_inicial
        params[f"{COMPRAS_DATE_FIELD}_max"] = data_final
    if COMPRAS_UASG:
        params["uasg"] = COMPRAS_UASG
    query = urllib.parse.urlencode(params)
    return f"{COMPRAS_API_BASE}{COMPRAS_LIST_PATH}?{query}" if query else f"{COMPRAS_API_BASE}{COMPRAS_LIST_PATH}"


def _get_items(payload: dict) -> list[dict]:
    if isinstance(payload.get("_embedded"), dict):
        for k in ("licitacoes", "licitacao", "items"):
            v = payload["_embedded"].get(k)
            if isinstance(v, list):
                return v
    for k in ("licitacoes", "items", "licitacao"):
        v = payload.get(k)
        if isinstance(v, list):
            return v
    return []


def _next_link(payload: dict) -> str | None:
    links = payload.get("_links") or payload.get("links") or {}
    if isinstance(links, dict):
        next_link = links.get("next") or links.get("proximo") or None
        if isinstance(next_link, dict) and next_link.get("href"):
            return str(next_link["href"])
        if isinstance(next_link, str):
            return next_link
    return None


def _normalize_id(item: dict) -> str | None:
    for key in ("identificador", "id", "numero_processo", "numero_aviso"):
        val = item.get(key)
        if val:
            return str(val)
    return None


def _map_tender(detail: dict, fallback: dict, ident: str) -> dict:
    obj = detail.get("objeto") or fallback.get("objeto") or ""
    data_pub = detail.get("data_publicacao") or fallback.get("data_publicacao")
    modalidade = detail.get("modalidade") or fallback.get("modalidade")
    uasg = detail.get("uasg") or fallback.get("uasg")
    status = detail.get("situacao_aviso") or fallback.get("situacao_aviso")
    url_html = f"{COMPRAS_API_BASE}/licitacoes/id/licitacao/{ident}.html"
    url_json = f"{COMPRAS_API_BASE}/licitacoes/id/licitacao/{ident}.json"

    urls = {
        "compras": url_html,
        "api": url_json,
        "url": url_html,
    }

    payload = {
        "id_pncp": f"compras:{ident}",
        "source": "compras",
        "source_id": str(ident),
        "orgao": f"UASG {uasg}" if uasg else None,
        "municipio": None,
        "uf": None,
        "modalidade": str(modalidade) if modalidade is not None else None,
        "objeto": obj or None,
        "data_publicacao": data_pub,
        "status": status,
        "urls": urls,
        "force_fetch": False,
        "source_payload": {"list_item": fallback, "detail": detail},
    }
    return payload


async def _fetch_once():
    url = _build_list_url()
    pages = 0
    total = 0

    while url and pages < COMPRAS_MAX_PAGES and total < COMPRAS_MAX_ITEMS:
        payload = await asyncio.to_thread(_http_get, url)
        items = _get_items(payload)
        if not items:
            break
        for item in items:
            ident = _normalize_id(item)
            if not ident:
                continue
            detail_url = f"{COMPRAS_API_BASE}{COMPRAS_DETAIL_PATH}".format(id=ident)
            try:
                detail = await asyncio.to_thread(_http_get, detail_url)
            except Exception:
                detail = {}
            tender_payload = _map_tender(detail if isinstance(detail, dict) else {}, item, ident)
            try:
                await asyncio.to_thread(_http_post, f"{CORE_API_URL}/v1/ingest/tender", tender_payload)
                await incr_counter("worker.compras_fetch.ingest_ok_total")
            except Exception:
                await incr_counter("worker.compras_fetch.ingest_error_total")
            total += 1
            if total >= COMPRAS_MAX_ITEMS:
                break
        pages += 1
        url = _next_link(payload)
        if url and url.startswith("/"):
            url = f"{COMPRAS_API_BASE}{url}"
    return total


async def main():
    while True:
        try:
            processed = await _fetch_once()
            await incr_counter("worker.compras_fetch.batch_ok_total")
            if processed:
                await incr_counter("worker.compras_fetch.items_total", processed)
        except Exception:
            await incr_counter("worker.compras_fetch.batch_error_total")
        await asyncio.sleep(COMPRAS_POLL_S)


if __name__ == "__main__":
    asyncio.run(main())
