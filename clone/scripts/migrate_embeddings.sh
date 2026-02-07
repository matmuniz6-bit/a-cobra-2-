#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

set -a; [ -f .env ] && source .env; set +a

DC="docker compose -f docker-compose.yml"

$DC exec -T db psql -U "${POSTGRES_USER:-acobra}" -d "${POSTGRES_DB:-acobra}" <<'SQL'
CREATE EXTENSION IF NOT EXISTS vector;
ALTER TABLE document_segment ADD COLUMN IF NOT EXISTS embedding vector(768);
CREATE INDEX IF NOT EXISTS idx_document_segment_embedding ON document_segment USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
SQL

echo "[OK] Migração de embeddings concluída."
