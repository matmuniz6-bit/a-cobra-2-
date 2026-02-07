#!/usr/bin/env bash
set -euo pipefail

echo "== wake_codespace =="
docker compose up -d --build
docker compose ps

echo
echo "== health =="
curl -fsS http://localhost:8080/health >/dev/null && echo "[OK] api /health"

echo
echo "== queues =="
docker exec -i a-cobra-redis-1 sh -lc '
for q in q:triage q:fetch_parse q:parse q:dead_fetch_docs; do
  echo -n "$q="; redis-cli --raw LLEN "$q"
done
'
