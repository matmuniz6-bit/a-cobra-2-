import os
import json
import time
import logging
import datetime as dt
import urllib.parse
import urllib.request

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pncp_fetch")

PNCP_BASE_URL = os.getenv("PNCP_BASE_URL", "https://pncp.gov.br/api/consulta")
PNCP_MODALIDADE_IDS = os.getenv("PNCP_MODALIDADE_IDS", "8")
PNCP_PAGE_SIZE = int(os.getenv("PNCP_PAGE_SIZE", "50"))
PNCP_PAGE_SIZE_MIN = int(os.getenv("PNCP_PAGE_SIZE_MIN", "10"))
PNCP_MAX_PAGES = int(os.getenv("PNCP_MAX_PAGES", "20"))
PNCP_MAX_ITEMS = int(os.getenv("PNCP_MAX_ITEMS", "500"))
PNCP_SLEEP_S = float(os.getenv("PNCP_SLEEP_S", "1"))
PNCP_POLL_S = float(os.getenv("PNCP_POLL_S", "3600"))
PNCP_BACKOFF_S = float(os.getenv("PNCP_BACKOFF_S", "10"))

PNCP_DATA_INICIAL = os.getenv("PNCP_DATA_INICIAL", "")
PNCP_DATA_FINAL = os.getenv("PNCP_DATA_FINAL", "")

PNCP_UF = os.getenv("PNCP_UF", "")
PNCP_CODIGO_MUNICIPIO_IBGE = os.getenv("PNCP_CODIGO_MUNICIPIO_IBGE", "")
PNCP_CNPJ = os.getenv("PNCP_CNPJ", "")
PNCP_CODIGO_UNIDADE_ADMINISTRATIVA = os.getenv("PNCP_CODIGO_UNIDADE_ADMINISTRATIVA", "")
PNCP_CODIGO_MODO_DISPUTA = os.getenv("PNCP_CODIGO_MODO_DISPUTA", "")
PNCP_ID_USUARIO = os.getenv("PNCP_ID_USUARIO", "")

CORE_API_URL = os.getenv("CORE_API_URL", os.getenv("API_BASE_URL", "http://api:8080"))
CORE_API_KEY = (os.getenv("CORE_API_KEY", "") or os.getenv("API_KEY", "") or "").strip()


def _today_ymd() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%d")


def _date_range() -> tuple[str, str]:
    if PNCP_DATA_INICIAL and PNCP_DATA_FINAL:
        return PNCP_DATA_INICIAL, PNCP_DATA_FINAL
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


def _build_url(modalidade_id: str, page: int, data_inicial: str, data_final: str) -> str:
    page_size = max(PNCP_PAGE_SIZE_MIN, PNCP_PAGE_SIZE)
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "codigoModalidadeContratacao": modalidade_id,
        "pagina": str(page),
        "tamanhoPagina": str(page_size),
    }
    if PNCP_UF:
        params["uf"] = PNCP_UF
    if PNCP_CODIGO_MUNICIPIO_IBGE:
        params["codigoMunicipioIbge"] = PNCP_CODIGO_MUNICIPIO_IBGE
    if PNCP_CNPJ:
        params["cnpj"] = PNCP_CNPJ
    if PNCP_CODIGO_UNIDADE_ADMINISTRATIVA:
        params["codigoUnidadeAdministrativa"] = PNCP_CODIGO_UNIDADE_ADMINISTRATIVA
    if PNCP_CODIGO_MODO_DISPUTA:
        params["codigoModoDisputa"] = PNCP_CODIGO_MODO_DISPUTA
    if PNCP_ID_USUARIO:
        params["idUsuario"] = PNCP_ID_USUARIO

    return f"{PNCP_BASE_URL}/v1/contratacoes/publicacao?{urllib.parse.urlencode(params)}"


def _map_item(item: dict) -> dict:
    numero = item.get("numeroControlePNCP") or ""
    orgao = (item.get("orgaoEntidade") or {}).get("razaoSocial")
    unidade = item.get("unidadeOrgao") or {}
    municipio = unidade.get("municipioNome")
    uf = unidade.get("ufSigla")
    modalidade = item.get("modalidadeNome")
    objeto = item.get("objetoCompra") or ""
    info = item.get("informacaoComplementar") or ""
    if info:
        objeto = f"{objeto} | {info}" if objeto else info
    data_pub = item.get("dataPublicacaoPncp")
    status = item.get("situacaoCompraNome")

    urls = {
        "pncp": f"https://pncp.gov.br/app/contratacoes/{numero}" if numero else None,
        "sistema_origem": item.get("linkSistemaOrigem"),
        "processo": item.get("linkProcessoEletronico"),
    }
    urls = {k: v for k, v in urls.items() if v}

    return {
        "id_pncp": numero,
        "source": "pncp",
        "source_id": numero,
        "orgao": orgao,
        "municipio": municipio,
        "uf": uf,
        "modalidade": modalidade,
        "objeto": objeto,
        "data_publicacao": data_pub,
        "status": status,
        "urls": urls,
        "force_fetch": False,
        "source_payload": item,
    }


def _fetch_once():
    data_inicial, data_final = _date_range()
    modalidades = [m.strip() for m in PNCP_MODALIDADE_IDS.split(",") if m.strip()]
    total_items = 0

    for mid in modalidades:
        page = 1
        pages = 0
        while page <= PNCP_MAX_PAGES:
            url = _build_url(mid, page, data_inicial, data_final)
            log.info("PNCP fetch: modalidade=%s page=%s url=%s", mid, page, url)
            try:
                payload = _http_get(url)
            except Exception as e:
                log.warning("PNCP fetch error: %r (backoff %ss)", e, PNCP_BACKOFF_S)
                time.sleep(PNCP_BACKOFF_S)
                break
            data = payload.get("data") or []
            if not data:
                break
            for item in data:
                mapped = _map_item(item)
                if not mapped.get("id_pncp"):
                    continue
                try:
                    _http_post(f"{CORE_API_URL}/v1/ingest/tender", mapped)
                    total_items += 1
                except Exception as e:
                    log.warning("Ingest failed id_pncp=%s err=%r", mapped.get("id_pncp"), e)
                if total_items >= PNCP_MAX_ITEMS:
                    log.info("PNCP max items reached: %s", total_items)
                    return
            pages += 1
            page += 1
            time.sleep(PNCP_SLEEP_S)
        log.info("PNCP fetch done modalidade=%s pages=%s", mid, pages)
    log.info("PNCP fetch total items=%s", total_items)


def main():
    log.info("PNCP worker started base=%s", PNCP_BASE_URL)
    while True:
        try:
            _fetch_once()
        except Exception as e:
            log.exception("PNCP worker error: %r", e)
        time.sleep(PNCP_POLL_S)


if __name__ == "__main__":
    main()
