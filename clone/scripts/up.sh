#!/usr/bin/env bash
set -o pipefail 2>/dev/null || true

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 0

echo "[CORE] up"
docker compose -f docker-compose.yml up -d --build

if [[ -f "$ROOT/services/radar/docker-compose.yml" ]]; then
  echo "[RADAR] up"
  ( cd "$ROOT/services/radar" && docker compose up -d --build )
fi
