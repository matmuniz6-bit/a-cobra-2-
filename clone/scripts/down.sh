#!/usr/bin/env bash
set -o pipefail 2>/dev/null || true

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 0

echo "[RADAR] down"
if [[ -f "$ROOT/services/radar/docker-compose.yml" ]]; then
  ( cd "$ROOT/services/radar" && docker compose down )
fi

echo "[CORE] down"
docker compose -f docker-compose.yml down
