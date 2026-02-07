#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_fetch_docs.py"

python3 - <<'PY'
from pathlib import Path
import re

p = Path("services/api/app/worker_fetch_docs.py")
s = p.read_text(encoding="utf-8")

# 1) garantir DEAD_QUEUE no topo (perto de QUEUE/PARSE_QUEUE)
if 'DEAD_QUEUE' not in s:
    s = re.sub(
        r'^(QUEUE\s*=.*\nPARSE_QUEUE\s*=.*\n)',
        r'\1DEAD_QUEUE  = os.getenv("DEAD_QUEUE", "q:dead_fetch_docs")\n',
        s,
        flags=re.M
    )

# 2) substituir o bloco simples "if not tender_id or not url" por um bloco robusto
old = """          if not tender_id or not url:
              log.warning("Payload sem tender_id ou sem url. payload=%s", payload)
              continue
"""

if old not in s:
    print("[ERRO] bloco antigo não encontrado exatamente. Mostre as linhas ~120-160 do worker_fetch_docs.py.")
    raise SystemExit(1)

new = """          # --- FK guard / resolução de tender ---
          inner = payload.get("payload")
          if not isinstance(inner, dict):
              inner = {}

          # tenta resolver tender_id via DB (pelo próprio id e/ou id_pncp)
          tender_id_resolved = None
          try:
              if tender_id is not None and str(tender_id).isdigit():
                  tid = int(tender_id)
                  row = await pool.fetchrow("SELECT 1 FROM tender WHERE id=$1", tid)
                  if row:
                      tender_id_resolved = tid

              if tender_id_resolved is None and isinstance(id_pncp, str) and id_pncp.strip():
                  row = await pool.fetchrow("SELECT id FROM tender WHERE id_pncp=$1", id_pncp.strip())
                  if row:
                      tender_id_resolved = int(row["id"])

              # se ainda não achou e temos metadata no payload, faz upsert do tender
              if tender_id_resolved is None:
                  idp = (id_pncp or inner.get("id_pncp"))
                  if isinstance(idp, str) and idp.strip():
                      import datetime as _dt
                      dp = inner.get("data_publicacao")
                      dp_dt = None
                      if isinstance(dp, str) and dp.strip():
                          x = dp.strip()
                          if x.endswith("Z"):
                              x = x[:-1] + "+00:00"
                          try:
                              dp_dt = _dt.datetime.fromisoformat(x)
                          except Exception:
                              dp_dt = None

                      urls2 = inner.get("urls")
                      if not isinstance(urls2, dict):
                          urls2 = payload.get("urls") if isinstance(payload.get("urls"), dict) else None

                      row = await pool.fetchrow(
                          \"\"\"INSERT INTO tender (id_pncp, orgao, municipio, uf, modalidade, objeto, data_publicacao, status, urls, hash_metadados)
                             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                             ON CONFLICT (id_pncp) DO UPDATE SET
                               orgao=COALESCE(EXCLUDED.orgao, tender.orgao),
                               municipio=COALESCE(EXCLUDED.municipio, tender.municipio),
                               uf=COALESCE(EXCLUDED.uf, tender.uf),
                               modalidade=COALESCE(EXCLUDED.modalidade, tender.modalidade),
                               objeto=COALESCE(EXCLUDED.objeto, tender.objeto),
                               data_publicacao=COALESCE(EXCLUDED.data_publicacao, tender.data_publicacao),
                               status=COALESCE(EXCLUDED.status, tender.status),
                               urls=COALESCE(EXCLUDED.urls, tender.urls),
                               hash_metadados=COALESCE(EXCLUDED.hash_metadados, tender.hash_metadados),
                               updated_at=now()
                             RETURNING id\"\"\",
                          idp.strip(),
                          inner.get("orgao"),
                          inner.get("municipio"),
                          inner.get("uf"),
                          inner.get("modalidade"),
                          inner.get("objeto"),
                          dp_dt,
                          inner.get("status"),
                          urls2,
                          inner.get("hash_metadados"),
                      )
                      if row:
                          tender_id_resolved = int(row["id"])
          except Exception as e:
              log.exception("Falha resolvendo/garantindo tender no DB: %r", e)

          if not tender_id_resolved or not url:
              # joga numa dead-letter queue pra não perder o payload e não poluir com FK
              try:
                  msg_dead = {
                      "reason": "missing_tender_or_url",
                      "tender_id": tender_id,
                      "tender_id_resolved": tender_id_resolved,
                      "id_pncp": id_pncp,
                      "url": url,
                      "payload": payload,
                  }
                  await r.lpush(DEAD_QUEUE, json.dumps(msg_dead, ensure_ascii=False, default=_json_default))
              except Exception:
                  pass

              log.warning("Ignorando payload sem tender válido ou sem url. tender_id=%s resolved=%s id_pncp=%s url=%s",
                          tender_id, tender_id_resolved, id_pncp, url)
              continue
"""

s = s.replace(old, new)

# 3) garantir que a inserção do document usa o tender_id resolvido
s = s.replace(
    "pool, int(tender_id), str(url), http_status, headers, ctype, body, truncated, error",
    "pool, int(tender_id_resolved), str(url), http_status, headers, ctype, body, truncated, error"
)

# 4) garantir que a msg pro PARSE_QUEUE usa o tender_id resolvido
s = s.replace(
    '"tender_id": int(tender_id),',
    '"tender_id": int(tender_id_resolved),'
)

p.write_text(s, encoding="utf-8")
print("[OK] patch aplicado:", p)
PY

echo "[OK] agora rebuild/restart do fetch_docs:"
echo "docker compose -f docker-compose.yml up -d --build fetch_docs"
