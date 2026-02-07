#!/usr/bin/env bash
set -euo pipefail
DC=(docker compose -f docker-compose.yml)

echo "== 0) parar consumidores (parse + fetch_docs) =="
"${DC[@]}" stop parse fetch_docs >/dev/null || true

echo "== 1) limpar q:fetch_parse só pra teste ficar limpo =="
"${DC[@]}" exec -T redis sh -lc 'redis-cli del q:fetch_parse >/dev/null; echo "OK del q:fetch_parse"'

echo "== 2) enviar msg pra q:triage (force_fetch topo + tender.urls.pncp) =="
"${DC[@]}" exec -T worker python - <<'PY'
import os, json, time, redis
r = redis.from_url(os.getenv("REDIS_URL","redis://redis:6379/0"), decode_responses=True)
q = os.getenv("TRIAGE_QUEUE","q:triage")

id_pncp = f"TESTE-HOLD-FETCHPARSE-{int(time.time())}"
msg = {
  "force_fetch": True,
  "tender": {
    "id": None,
    "id_pncp": id_pncp,
    "orgao": "MUNICIPIO TESTE",
    "municipio": "Taubaté",
    "uf": "ZZ",
    "modalidade": "PCA",
    "objeto": "teste hold fetch_parse",
    "status": "publicado",
    "urls": {"pncp": "https://pncp.gov.br/api/pncp/v1/orgaos/82777228000157/pca/2025/1/consolidado"}
  }
}
r.lpush(q, json.dumps(msg, ensure_ascii=False))
print("OK enviado:", id_pncp)
PY

echo "== 3) esperar 1s e checar q:fetch_parse (agora tem que ficar 1) =="
sleep 1
"${DC[@]}" exec -T redis sh -lc '
echo -n "LLEN q:fetch_parse = "; redis-cli llen q:fetch_parse
echo "TOP q:fetch_parse ="; redis-cli lrange q:fetch_parse 0 0
'

echo "== 4) voltar consumidores =="
"${DC[@]}" up -d parse fetch_docs >/dev/null || true
echo "OK"
