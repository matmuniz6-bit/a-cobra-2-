#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DC=(docker compose -f "${ROOT_DIR}/docker-compose.yml")

API_BASE="${API_BASE:-http://localhost:8080}"
if [ -z "${API_KEY:-}" ] && [ -z "${CORE_API_KEY:-}" ] && [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  . "${ROOT_DIR}/.env"
  set +a
fi
AUTH_KEY="${API_KEY:-${CORE_API_KEY:-}}"
H_AUTH=()
if [ -n "${AUTH_KEY}" ]; then
  H_AUTH=(-H "x-api-key: ${AUTH_KEY}")
fi

IDPNCP="${1:-PCA-82777228000157-2025-1}"
URLPNCP="${2:-https://pncp.gov.br/api/pncp/v1/orgaos/82777228000157/pca/2025/1/consolidado}"

echo "== 1) POST /v1/ingest/tender =="
curl -fsS -X POST "$API_BASE/v1/ingest/tender" \
  -H 'Content-Type: application/json' \
  "${H_AUTH[@]}" \
  -d @- <<JSON
{
  "id_pncp": "$IDPNCP",
  "orgao": "MUNICIPIO DE OURO",
  "municipio": "Ouro",
  "uf": "SC",
  "modalidade": "PCA",
  "objeto": "PCA 2025 - consolidado",
  "status": "publicado",
  "urls": {"pncp": "$URLPNCP"},
  "force_fetch": true
}
JSON
echo
echo

echo "== 2) Redis LLEN filas =="
"${DC[@]}" exec -T redis sh -lc '
for q in q:triage q:fetch_parse q:parse q:dead_fetch_docs; do
  echo -n "$q = "
  redis-cli llen "$q" || true
done
'
echo

echo "== 3) Logs (tail) worker + fetch_docs + parse =="
"${DC[@]}" logs --tail=120 worker || true
echo "----"
"${DC[@]}" logs --tail=120 fetch_docs || true
echo "----"
"${DC[@]}" logs --tail=120 parse || true
echo

echo "== 4) DB: tender + último doc desse id_pncp =="
"${DC[@]}" exec -T db psql -U acobra -d acobra -c \
"select id,id_pncp,created_at,updated_at from tender where id_pncp='${IDPNCP}' order by id desc limit 1;"

"${DC[@]}" exec -T db psql -U acobra -d acobra -c \
"with t as (select id from tender where id_pncp='${IDPNCP}' order by id desc limit 1)
 select d.id,d.tender_id,d.http_status,d.content_type,
        length(coalesce(d.texto_extraido,'')) as texto_len,
        left(replace(coalesce(d.texto_extraido,''), E'\n', '\\n'), 220) as preview_escaped,
        d.error
 from document d join t on t.id=d.tender_id
 order by d.id desc limit 1;"
