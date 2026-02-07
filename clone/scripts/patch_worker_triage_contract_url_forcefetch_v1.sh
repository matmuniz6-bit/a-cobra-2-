#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_triage.py"
test -f "$FILE" || { echo "[ERRO] não achei $FILE"; exit 1; }

python3 - <<'PY'
from pathlib import Path
import re, datetime as dt

p = Path("services/api/app/worker_triage.py")
src = p.read_text(encoding="utf-8")

if "contract_url_forcefetch_v1" in src:
    print("[OK] Já patchado (contract_url_forcefetch_v1).")
    raise SystemExit(0)

out = src

# 1) force_fetch: aceitar no topo (info) OU dentro do payload
#    troca a linha atual:
#      force_fetch = bool(isinstance(payload, dict) and payload.get("force_fetch"))
pat_force = re.compile(r'^(?P<ind>[ \t]*)force_fetch\s*=\s*bool\(\s*isinstance\(payload,\s*dict\)\s*and\s*payload\.get\("force_fetch"\)\s*\)\s*$',
                       re.M)

m = pat_force.search(out)
if not m:
    print("[ERRO] Não achei a linha force_fetch atual para substituir.")
    print("Procure por: force_fetch = bool(isinstance(payload, dict) and payload.get(\"force_fetch\"))")
    raise SystemExit(1)

ind = m.group("ind")
out = pat_force.sub(
    ind + 'force_fetch = bool(info.get("force_fetch") or (isinstance(payload, dict) and payload.get("force_fetch")))  # contract_url_forcefetch_v1\n',
    out,
    count=1
)

# 2) Antes do IF de enqueue, criar pncp_url e usar no gate (mais simples que urls.get dentro do if)
pat_if = re.compile(
    r'^(?P<ind>[ \t]*)if\s*\(\s*force_fetch\s*or\s*score\s*>=\s*MIN_SCORE\s*\)\s*and\s*isinstance\(urls,\s*dict\)\s*and\s*urls\.get\("pncp"\)\s*:\s*$',
    re.M
)

m2 = pat_if.search(out)
if not m2:
    print("[ERRO] Não achei o IF de enqueue esperado.")
    print('Procure por: if (force_fetch or score >= MIN_SCORE) and isinstance(urls, dict) and urls.get("pncp"):')
    raise SystemExit(1)

ind2 = m2.group("ind")
replacement = (
    ind2 + 'pncp_url = urls.get("pncp") if isinstance(urls, dict) else None  # contract_url_forcefetch_v1\n'
    + ind2 + 'if (force_fetch or score >= MIN_SCORE) and isinstance(pncp_url, str) and pncp_url.strip():\n'
)
out = pat_if.sub(replacement, out, count=1)

# 3) Inserir "url": pncp_url dentro do payload_fetch (se ainda não existir)
#    tenta inserir logo após "id_pncp": id_pncp,
if '"url":' not in out:
    out2 = re.sub(
        r'(\n(?P<ind>[ \t]*)"id_pncp"\s*:\s*id_pncp\s*,\s*\n)',
        r'\1\g<ind>"url": pncp_url,\n',
        out,
        count=1
    )
    out = out2

bak = p.with_suffix(p.suffix + f".bak.contract.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(src, encoding="utf-8")
p.write_text(out, encoding="utf-8")

print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup criado em {bak}")
PY

echo
echo "[OK] Rebuild do worker (triage roda aqui):"
docker compose -f docker-compose.yml up -d --build worker
