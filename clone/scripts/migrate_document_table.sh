#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

set -a; [ -f .env ] && source .env; set +a

docker compose -f docker-compose.yml exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
-- Garante colunas esperadas (sem quebrar se já existir)

ALTER TABLE document
  ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'pncp';

ALTER TABLE document
  ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE document
  ADD COLUMN IF NOT EXISTS http_status INT;

ALTER TABLE document
  ADD COLUMN IF NOT EXISTS content_type TEXT;

ALTER TABLE document
  ADD COLUMN IF NOT EXISTS sha256 TEXT;

ALTER TABLE document
  ADD COLUMN IF NOT EXISTS size_bytes BIGINT;

ALTER TABLE document
  ADD COLUMN IF NOT EXISTS truncated BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE document
  ADD COLUMN IF NOT EXISTS headers JSONB;

ALTER TABLE document
  ADD COLUMN IF NOT EXISTS body BYTEA;

ALTER TABLE document
  ADD COLUMN IF NOT EXISTS error TEXT;

-- índice útil (não deve falhar; NULL em sha256 não conflita)
CREATE UNIQUE INDEX IF NOT EXISTS document_uniq
  ON document (tender_id, url, sha256);
SQL

echo "[OK] Migração da tabela document concluída."
