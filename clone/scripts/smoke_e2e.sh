#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

DC="docker compose -f docker-compose.yml"

set -a
[ -f .env ] && source .env
set +a

boot_timeout_s=${SMOKE_BOOT_TIMEOUT_S:-60}
deadline_s=${SMOKE_DEADLINE_S:-300}

wait_db() {
  local start=$SECONDS
  while [ $((SECONDS - start)) -lt "${boot_timeout_s}" ]; do
    if $DC exec -T db psql -U "${POSTGRES_USER:-acobra}" -d "${POSTGRES_DB:-acobra}" -Atc "select 1;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_api() {
  local start=$SECONDS
  while [ $((SECONDS - start)) -lt "${boot_timeout_s}" ]; do
    if $DC exec -T api sh -c "curl -fsS http://localhost:8080/health >/dev/null"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

if ! wait_db; then
  echo "[FAIL] DB not ready after ${boot_timeout_s}s"
  exit 1
fi

if ! wait_api; then
  echo "[FAIL] API not ready after ${boot_timeout_s}s"
  exit 1
fi

id_pncp="SMOKE-$(date +%s)"

payload=$(printf '{"id_pncp":"%s","orgao":"Teste","municipio":"Sao Paulo","uf":"SP","modalidade":"pregao","objeto":"Teste E2E","data_publicacao":"2026-02-03T12:00:00Z","status":"aberto","urls":{"pncp":"https://example.com"},"force_fetch":true}' "$id_pncp")
api_key_header=""
if [ -n "${API_KEY:-}" ]; then
  api_key_header="-H \"x-api-key: ${API_KEY}\""
fi

$DC exec -T api sh -c "printf '%s' '$payload' | curl -sS -X POST http://localhost:8080/v1/ingest/tender -H 'Content-Type: application/json' ${api_key_header} -d @-" >/tmp/smoke_ingest.json

echo "== Ingest response =="
cat /tmp/smoke_ingest.json

sleep 5

tender_id=$($DC exec -T db psql -U "${POSTGRES_USER:-acobra}" -d "${POSTGRES_DB:-acobra}" -Atc "select id from tender where id_pncp='${id_pncp}' order by id desc limit 1;")
if [ -z "$tender_id" ]; then
  echo "[FAIL] tender not found"
  exit 1
fi

doc_id=""
texto_len="0"
requeued=0
deadline=$((SECONDS + deadline_s))
while [ $SECONDS -lt $deadline ]; do
  doc_row=$($DC exec -T db psql -U "${POSTGRES_USER:-acobra}" -d "${POSTGRES_DB:-acobra}" -Atc "select id,coalesce(length(texto_extraido),0) from document where tender_id=${tender_id} order by id desc limit 1;")
  doc_id=$(printf '%s' "$doc_row" | cut -d'|' -f1)
  texto_len=$(printf '%s' "$doc_row" | cut -d'|' -f2)
  if [ -n "$doc_id" ] && [ "${texto_len:-0}" -le 0 ] && [ "$requeued" -eq 0 ]; then
    # Prioriza este doc no início da fila (LIFO)
    smoke_q="${PARSE_SMOKE_QUEUE:-q:parse_smoke}"
    $DC exec -T redis redis-cli LPUSH "$smoke_q" "{\"document_id\":${doc_id}}"
    requeued=1
  fi
  if [ -n "$doc_id" ] && [ "${texto_len:-0}" -gt 0 ]; then
    break
  fi
  sleep 2
done

if [ -z "$doc_id" ] || [ "${texto_len:-0}" -le 0 ]; then
  echo "[FAIL] document/texto_extraido not found (tender_id=${tender_id})"
  exit 1
fi

echo "[OK] smoke e2e passed (tender_id=${tender_id}, doc_id=${doc_id}, texto_len=${texto_len})"
