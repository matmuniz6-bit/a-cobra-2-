#!/usr/bin/env bash
set -o pipefail 2>/dev/null || true

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 0

echo "=== CORE logs ==="
docker compose -f docker-compose.yml logs -f --tail=200 &
CORE_PID=$!

if [[ -f "$ROOT/services/radar/docker-compose.yml" ]]; then
  echo "=== RADAR logs ==="
  ( cd "$ROOT/services/radar" && docker compose logs -f --tail=200 ) &
  RADAR_PID=$!
fi

wait $CORE_PID ${RADAR_PID:-}
