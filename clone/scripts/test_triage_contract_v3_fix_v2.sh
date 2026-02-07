#!/usr/bin/env bash
set -euo pipefail
DC=(docker compose -f docker-compose.yml)

echo "== pausa fetch_docs pra inspecionar fila =="
"${DC[@]}" stop fetch_docs >/dev/null || true

echo "== envia msg pra q:triage (force_fetch topo + tender.urls.pncp) =="
"${DC[@]}" exec -T worker python - <<'PY'
import os, json, time, redis
r = redis.from_url(os.getenv("REDIS_URL","redis://redis:6379/0"), decode_responses=True)
q = os.getenv("TRIAGE_QUEUE","q:triage")

id_pncp = f"TESTE-CONTRATO-V3FIX2-{int(time.time())}"
msg = {
  "force_fetch": True,
  "tender": {
    "id": None,
    "id_pncp": id_pncp,
    "orgao": "MUNICIPIO TESTE",
    "municipio": "Taubaté",
    "uf": "ZZ",  # score tende a 0
    "modalidade": "PCA",
    "objeto": "teste contrato v3 fix v2",
    "status": "publicado",
    "urls": {"pncp": "https://pncp.gov.br/api/pncp/v1/orgaos/82777228000157/pca/2025/1/consolidado"}
  }
}
r.lpush(q, json.dumps(msg, ensure_ascii=False))
print("OK enviado:", id_pncp)
PY

echo "== logs worker (tail) =="
"${DC[@]}" logs --tail=120 worker || true

echo "== confere fila q:fetch_parse (tem que ter 1 item com campo url) =="
"${DC[@]}" exec -T redis sh -lc '
echo -n "LLEN q:fetch_parse = "; redis-cli llen q:fetch_parse
echo "TOP q:fetch_parse:"; redis-cli lrange q:fetch_parse 0 0
' || true

echo "== retoma fetch_docs =="
"${DC[@]}" up -d fetch_docs >/dev/null || true
echo "OK"
