#!/usr/bin/env bash
set -euo pipefail

F="services/api/app/worker_fetch_docs.py"
python - <<'PY'
from pathlib import Path
p = Path("services/api/app/worker_fetch_docs.py")
s = p.read_text(encoding="utf-8")

old = """          urls      = payload.get("urls") or {}
          url       = None

          if isinstance(urls, dict):
              url = urls.get("pncp") or urls.get("url")
          if not url and isinstance(payload.get("url"), str):
              url = payload["url"]
"""

if old not in s:
    print("[ERRO] bloco alvo não encontrado. Mostre as linhas ~120-140 do arquivo.")
    raise SystemExit(1)

new = """          urls      = payload.get("urls") or {}
          url       = None

          def _pick_url(obj):
              if isinstance(obj, dict):
                  u = obj.get("url")
                  if isinstance(u, str) and u.strip():
                      return u.strip()

                  uu = obj.get("urls")
                  if isinstance(uu, dict):
                      for k in ("pncp","url","edital","documento"):
                          v = uu.get(k)
                          if isinstance(v, str) and v.strip():
                              return v.strip()
              return None

          url = (
              _pick_url(payload)
              or _pick_url(payload.get("payload"))
              or _pick_url(payload.get("tender"))
              or _pick_url((payload.get("payload") or {}).get("tender"))
          )
"""

p.write_text(s.replace(old, new), encoding="utf-8")
print("[OK] patch aplicado em", p)
PY

echo
echo "Rebuild + restart fetch_docs..."
docker compose -f docker-compose.yml up -d --build fetch_docs
