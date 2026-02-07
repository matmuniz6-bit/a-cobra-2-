# Data Contract (Cliente)

## Objetivo
Padronizar campos, origem e garantias de dados para consumo externo.

## Entidades

1. Tender (licitacao)
Campos principais:
- `id`: identificador interno
- `id_pncp`: identificador global legado
- `source`: origem do dado (`pncp` | `compras` | `unknown`)
- `source_id`: id original na fonte
- `orgao`, `orgao_norm`
- `municipio`, `municipio_norm`
- `uf`, `uf_norm`
- `modalidade`, `modalidade_norm`
- `objeto`, `objeto_norm`
- `data_publicacao`
- `status`, `status_norm`
- `urls`
- `canonical_tender_id`
- `fingerprint`
- `created_at`, `updated_at`

Garantias:
- `source` e `source_id` sempre presentes para registros novos.
- `*_norm` pode ser `NULL` se a fonte nao fornecer ou a normalizacao falhar.
- `canonical_tender_id` pode ser `NULL` se nao houver dedupe.

2. Document (documento)
Campos principais:
- `id`, `tender_id`
- `url`, `source`
- `http_status`, `content_type`, `sha256`, `size_bytes`, `truncated`
- `texto_extraido`, `texto_chars`, `texto_quality`, `ocr_used`
- `created_at`

Garantias:
- `texto_quality` varia de `0.0` a `1.0` (heuristico).
- `ocr_used` indica se OCR foi aplicado.

## Auditoria e historico
- `tender_source_payload` guarda o payload bruto recebido.
- `tender_version` guarda snapshots por mudanca.
- `pipeline_event` registra o fluxo `triage -> fetch_docs -> parse`.

## SLA e atualizacao
- Ingestao depende da disponibilidade das fontes externas.
- Atualizacoes geram nova linha em `tender_version` quando ha mudanca.
