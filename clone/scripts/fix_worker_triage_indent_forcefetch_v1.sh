#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_triage.py"
test -f "$FILE" || { echo "[ERRO] não achei $FILE"; exit 1; }

python3 - <<'PY'
from pathlib import Path
import datetime as dt
import re

p = Path("services/api/app/worker_triage.py")
orig = p.read_text(encoding="utf-8").splitlines(True)

bak = p.with_suffix(p.suffix + f".bak.fixtriage.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text("".join(orig), encoding="utf-8")

lines = orig[:]

# anchors
i_payload = next((i for i,l in enumerate(lines) if "payload = json.loads(raw)" in l), None)
i_pick = next((i for i,l in enumerate(lines) if "_pick(payload)" in l and "tender_id" in l), None)
if i_payload is None or i_pick is None or i_pick <= i_payload:
    raise SystemExit("[ERRO] Não achei anchors 'payload = json.loads(raw)' e 'tender_id, id_pncp, t = _pick(payload)'")

# remove bloco force_fetch quebrado entre payload e _pick(payload)
block = lines[i_payload+1:i_pick]
new_block = []
for l in block:
    # remove qualquer resto do force_fetch (inclusive linhas de continuação)
    if "force_fetch" in l:
        continue
    if re.search(r'^\s*\(isinstance\(payload,\s*dict\)', l):
        continue
    if re.search(r'^\s*or\s+\(isinstance\(payload,\s*dict\)', l):
        continue
    if re.search(r'^\s*\)\s*$', l) and any("force_fetch" in x for x in block):
        continue
    new_block.append(l)

lines[i_payload+1:i_pick] = new_block

# acha 'info = dict(t)'
i_info = next((i for i,l in enumerate(lines) if re.search(r'^\s*info\s*=\s*dict\(\s*t\s*\)\s*$', l)), None)
if i_info is None:
    raise SystemExit("[ERRO] Não achei 'info = dict(t)'")

# não duplica
if not any("contract_force_fetch_v2" in l for l in lines):
    indent = re.match(r'^(\s*)', lines[i_info]).group(1)
    ins = [
        f"{indent}# contract_force_fetch_v2: permite bypass do score gate\n",
        f"{indent}force_fetch = bool(\n",
        f"{indent}    (isinstance(payload, dict) and payload.get('force_fetch'))\n",
        f"{indent}    or (isinstance(info, dict) and info.get('force_fetch'))\n",
        f"{indent}    or (isinstance(payload, dict) and isinstance(payload.get('payload'), dict) and payload['payload'].get('force_fetch'))\n",
        f"{indent})\n",
        "\n",
    ]
    lines[i_info+1:i_info+1] = ins

p.write_text("".join(lines), encoding="utf-8")
print("[OK] Corrigido:", p)
print("[OK] Backup:", bak)
PY

echo
echo "== sanity: python -m py_compile =="
python3 -m py_compile "$FILE"

echo
echo "== rebuild + subir worker =="
docker compose -f docker-compose.yml up -d --build worker

echo
docker compose -f docker-compose.yml logs --tail=80 worker
