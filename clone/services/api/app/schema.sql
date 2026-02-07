CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS tender (
  id BIGSERIAL PRIMARY KEY,
  id_pncp TEXT NOT NULL UNIQUE,
  source TEXT NOT NULL DEFAULT 'pncp',
  source_id TEXT,
  canonical_tender_id BIGINT REFERENCES tender(id) ON DELETE SET NULL,
  orgao TEXT,
  orgao_norm TEXT,
  municipio TEXT,
  municipio_norm TEXT,
  uf TEXT,
  uf_norm TEXT,
  modalidade TEXT,
  modalidade_norm TEXT,
  objeto TEXT,
  objeto_norm TEXT,
  fingerprint TEXT,
  data_publicacao TIMESTAMPTZ,
  status TEXT,
  status_norm TEXT,
  materia TEXT,
  categoria TEXT,
  materia_confidence NUMERIC,
  materia_source TEXT,
  materia_tags JSONB,
  materia_updated_at TIMESTAMPTZ,
  urls JSONB,
  hash_metadados TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotent column additions for existing DBs (before indexes)
ALTER TABLE tender ADD COLUMN IF NOT EXISTS materia TEXT;
ALTER TABLE tender ADD COLUMN IF NOT EXISTS categoria TEXT;
ALTER TABLE tender ADD COLUMN IF NOT EXISTS materia_confidence NUMERIC;
ALTER TABLE tender ADD COLUMN IF NOT EXISTS materia_source TEXT;
ALTER TABLE tender ADD COLUMN IF NOT EXISTS materia_tags JSONB;
ALTER TABLE tender ADD COLUMN IF NOT EXISTS materia_updated_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_tender_data_publicacao ON tender (data_publicacao);
CREATE INDEX IF NOT EXISTS idx_tender_hash_metadados ON tender (hash_metadados);
CREATE INDEX IF NOT EXISTS idx_tender_source ON tender (source, source_id);
CREATE INDEX IF NOT EXISTS idx_tender_modalidade_norm ON tender (modalidade_norm);
CREATE INDEX IF NOT EXISTS idx_tender_status_norm ON tender (status_norm);
CREATE INDEX IF NOT EXISTS idx_tender_uf_norm ON tender (uf_norm);
CREATE INDEX IF NOT EXISTS idx_tender_fingerprint ON tender (fingerprint);
CREATE INDEX IF NOT EXISTS idx_tender_canonical ON tender (canonical_tender_id);
CREATE INDEX IF NOT EXISTS idx_tender_materia ON tender (materia);
CREATE INDEX IF NOT EXISTS idx_tender_categoria ON tender (categoria);

CREATE TABLE IF NOT EXISTS document (
  id BIGSERIAL PRIMARY KEY,
  tender_id BIGINT NOT NULL REFERENCES tender(id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  source TEXT,
  fetched_at TIMESTAMPTZ,
  http_status INT,
  content_type TEXT,
  sha256 TEXT,
  size_bytes BIGINT,
  tamanho BIGINT,
  truncated BOOLEAN,
  headers JSONB,
  body BYTEA,
  error TEXT,
  baixado_em TIMESTAMPTZ,
  texto_path TEXT,
  texto_extraido TEXT,
  texto_chars INT,
  texto_quality NUMERIC,
  ocr_used BOOLEAN,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_document_tender_id ON document (tender_id);
CREATE INDEX IF NOT EXISTS idx_document_sha256 ON document (sha256);

CREATE TABLE IF NOT EXISTS analysis (
  id BIGSERIAL PRIMARY KEY,
  tender_id BIGINT NOT NULL REFERENCES tender(id) ON DELETE CASCADE,
  resumo_curto TEXT,
  campos_extraidos JSONB,
  score_relevancia NUMERIC,
  riscos JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_analysis_tender_id ON analysis (tender_id);

CREATE TABLE IF NOT EXISTS subscription (
  id BIGSERIAL PRIMARY KEY,
  client_id TEXT NOT NULL,
  filtros JSONB,
  horarios JSONB,
  seguir_urls JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_subscription_client_id ON subscription (client_id);

-- Base futura (produto/Telegram)
CREATE TABLE IF NOT EXISTS app_user (
  id BIGSERIAL PRIMARY KEY,
  telegram_user_id BIGINT UNIQUE NOT NULL,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  language_code TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_app_user_telegram ON app_user (telegram_user_id);

CREATE TABLE IF NOT EXISTS plan (
  id BIGSERIAL PRIMARY KEY,
  code TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  limits JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entitlement (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  plan_id BIGINT REFERENCES plan(id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'active',
  starts_at TIMESTAMPTZ,
  ends_at TIMESTAMPTZ,
  meta JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_entitlement_user_id ON entitlement (user_id);

CREATE TABLE IF NOT EXISTS user_subscription (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  filters JSONB,
  delivery JSONB,
  frequency TEXT,
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_subscription_user_id ON user_subscription (user_id);

CREATE TABLE IF NOT EXISTS tender_follow (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  tender_id BIGINT NOT NULL REFERENCES tender(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, tender_id)
);

CREATE TABLE IF NOT EXISTS alert (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT REFERENCES app_user(id) ON DELETE SET NULL,
  tender_id BIGINT REFERENCES tender(id) ON DELETE SET NULL,
  type TEXT NOT NULL,
  payload JSONB,
  sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alert_user_id ON alert (user_id);

CREATE TABLE IF NOT EXISTS audit_event (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT REFERENCES app_user(id) ON DELETE SET NULL,
  event_type TEXT NOT NULL,
  event_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_event_user_id ON audit_event (user_id);

-- Raw payloads per source (auditability)
CREATE TABLE IF NOT EXISTS tender_source_payload (
  id BIGSERIAL PRIMARY KEY,
  tender_id BIGINT REFERENCES tender(id) ON DELETE SET NULL,
  source TEXT NOT NULL,
  source_id TEXT,
  payload JSONB,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tender_source_payload_tender_id ON tender_source_payload (tender_id);
CREATE INDEX IF NOT EXISTS idx_tender_source_payload_source ON tender_source_payload (source, source_id);

-- Tender versions (history)
CREATE TABLE IF NOT EXISTS tender_version (
  id BIGSERIAL PRIMARY KEY,
  tender_id BIGINT NOT NULL REFERENCES tender(id) ON DELETE CASCADE,
  hash_metadados TEXT,
  payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tender_version_tender_id ON tender_version (tender_id);
CREATE INDEX IF NOT EXISTS idx_tender_version_created_at ON tender_version (created_at);

-- Pipeline events (triage -> fetch -> parse)
CREATE TABLE IF NOT EXISTS pipeline_event (
  id BIGSERIAL PRIMARY KEY,
  tender_id BIGINT REFERENCES tender(id) ON DELETE SET NULL,
  document_id BIGINT REFERENCES document(id) ON DELETE SET NULL,
  stage TEXT NOT NULL,
  status TEXT NOT NULL,
  message TEXT,
  payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_event_tender_id ON pipeline_event (tender_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_event_document_id ON pipeline_event (document_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_event_stage ON pipeline_event (stage);
CREATE INDEX IF NOT EXISTS idx_pipeline_event_created_at ON pipeline_event (created_at);

-- Segmentos de texto para busca simples (FTS)
CREATE TABLE IF NOT EXISTS document_segment (
  id BIGSERIAL PRIMARY KEY,
  document_id BIGINT NOT NULL REFERENCES document(id) ON DELETE CASCADE,
  tender_id BIGINT REFERENCES tender(id) ON DELETE SET NULL,
  idx INT NOT NULL,
  text TEXT NOT NULL,
  tsv tsvector,
  embedding vector(768)
);

CREATE INDEX IF NOT EXISTS idx_document_segment_doc_id ON document_segment (document_id);
CREATE INDEX IF NOT EXISTS idx_document_segment_tsv ON document_segment USING GIN (tsv);
CREATE INDEX IF NOT EXISTS idx_document_segment_embedding ON document_segment USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Artefatos derivados (markdown estruturado, tabelas, etc.)
CREATE TABLE IF NOT EXISTS document_artifact (
  id BIGSERIAL PRIMARY KEY,
  document_id BIGINT NOT NULL REFERENCES document(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (document_id, kind)
);

CREATE INDEX IF NOT EXISTS idx_document_artifact_doc_id ON document_artifact (document_id);
