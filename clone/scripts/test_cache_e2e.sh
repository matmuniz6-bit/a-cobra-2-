#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

DC="docker compose -f docker-compose.yml"

set -a
[ -f .env ] && source .env
set +a

echo "== 1) Primeira chamada (MISS esperado) =="
resp1=$($DC exec -T api sh -c "curl -sS -D - http://localhost:8080/health/cache -o /dev/null")
echo "$resp1" | grep -i "x-cache" || true

echo "== 2) Segunda chamada (HIT esperado) =="
resp2=$($DC exec -T api sh -c "curl -sS -D - http://localhost:8080/health/cache -o /dev/null")
echo "$resp2" | grep -i "x-cache" || true

echo "== 3) Métricas =="
$DC exec -T api sh -c "curl -sS http://localhost:8080/health/cache"

echo "[OK] cache e2e done"
