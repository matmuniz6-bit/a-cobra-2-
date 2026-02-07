#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

DC="docker compose -f docker-compose.yml"

# carrega .env se existir
set -a
[ -f .env ] && source .env
set +a

echo "== 1) Subindo db + redis =="
$DC up -d db redis

echo "== 2) Esperando Postgres ficar pronto =="
for i in {1..60}; do
  if $DC exec -T db pg_isready -U "${POSTGRES_USER:-acobra}" -d "${POSTGRES_DB:-acobra}" >/dev/null 2>&1; then
    echo "[OK] Postgres aceitando conexoes."
    break
  fi
  sleep 1
done

echo "== 3) Garantindo colunas no schema (idempotente) =="
./scripts/migrate_document_table.sh
./scripts/migrate_embeddings.sh

echo "== 4) Subindo resto =="
$DC up -d api worker fetch_docs parse bot

echo "== 5) Status =="
$DC ps

echo "== 6) Logs recentes (2m) =="
$DC logs --since 2m --tail=200 worker fetch_docs || true
