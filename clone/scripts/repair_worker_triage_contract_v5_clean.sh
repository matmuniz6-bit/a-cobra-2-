#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_triage.py"
test -f "$FILE" || { echo "[ERRO] não achei $FILE"; exit 1; }

python3 - <<'PY'
from pathlib import Path
import datetime as dt
import re

p = Path("services/api/app/worker_triage.py")
src0 = p.read_text(encoding="utf-8").splitlines(True)

bak = p.with_suffix(p.suffix + f".bak.contract_v5_clean.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text("".join(src0), encoding="utf-8")

lines = src0[:]

# 0) garantir import json
has_json = any(re.match(r'^\s*import\s+json\s*$', l) for l in lines)
if not has_json:
    # insere após o primeiro bloco de imports
    ins_at = 0
    for i,l in enumerate(lines):
        if l.startswith("import ") or l.startswith("from "):
            ins_at = i+1
        else:
            break
    lines.insert(ins_at, "import json\n")

# 1) substituir def _pick(...) por versão que aceita {tender:{...}} / {payload:{...}} / direto
src = "".join(lines)
m = re.search(r"(?ms)^def _pick\(payload: dict\):\n.*?\n(?=^def _score|\Z)", src)
if not m:
    raise SystemExit("[ERRO] Não achei def _pick(payload: dict):")

new_pick = (
"def _pick(payload: dict):\n"
"    # Aceita formatos:\n"
"    #  - {\"tender\": {...}}\n"
"    #  - {\"payload\": {...}}\n"
"    #  - direto {...}\n"
"    t = None\n"
"    if isinstance(payload, dict) and isinstance(payload.get(\"tender\"), dict):\n"
"        t = payload[\"tender\"]\n"
"    elif isinstance(payload, dict) and isinstance(payload.get(\"payload\"), dict):\n"
"        t = payload[\"payload\"]\n"
"    else:\n"
"        t = payload\n"
"\n"
"    tender_id = (t.get(\"id\") if isinstance(t, dict) else None) or (payload.get(\"id\") if isinstance(payload, dict) else None)\n"
"    id_pncp   = (t.get(\"id_pncp\") if isinstance(t, dict) else None) or (payload.get(\"id_pncp\") if isinstance(payload, dict) else None)\n"
"    return tender_id, id_pncp, t\n"
)
src = src[:m.start()] + new_pick + src[m.end():]
lines = src.splitlines(True)

# 2) reescrever trecho do loop: do pós payload=json.loads(raw) até antes do "# completa no DB"
i_payload = None
i_db = None
for i,l in enumerate(lines):
    if "payload = json.loads(raw)" in l:
        i_payload = i
        continue
    if i_payload is not None and ("# completa no DB" in l or "dbinfo = await _db_fetch" in l):
        i_db = i
        break

if i_payload is None:
    raise SystemExit("[ERRO] Não achei âncora: payload = json.loads(raw)")
if i_db is None:
    raise SystemExit("[ERRO] Não achei âncora do DB: '# completa no DB' ou 'dbinfo = await _db_fetch'")

indent = lines[i_payload][:len(lines[i_payload]) - len(lines[i_payload].lstrip())]

block = []
block.append("\n")
block.append(f"{indent}tender_id, id_pncp, t = _pick(payload)\n")
block.append(f"{indent}info = dict(t) if isinstance(t, dict) else {{}}\n")
block.append("\n")
block.append(f"{indent}# contract_v5_clean: force_fetch pode vir no topo, dentro do tender, ou dentro do payload\n")
block.append(f"{indent}inner_payload = payload.get(\"payload\") if isinstance(payload, dict) and isinstance(payload.get(\"payload\"), dict) else None\n")
block.append(f"{indent}inner_tender  = payload.get(\"tender\")  if isinstance(payload, dict) and isinstance(payload.get(\"tender\"), dict) else None\n")
block.append(f"{indent}force_fetch = bool(\n")
block.append(f"{indent}    (payload.get(\"force_fetch\") if isinstance(payload, dict) else False)\n")
block.append(f"{indent}    or (info.get(\"force_fetch\") if isinstance(info, dict) else False)\n")
block.append(f"{indent}    or (inner_payload.get(\"force_fetch\") if isinstance(inner_payload, dict) else False)\n")
block.append(f"{indent}    or (inner_tender.get(\"force_fetch\") if isinstance(inner_tender, dict) else False)\n")
block.append(f"{indent})\n")
block.append("\n")

# substitui tudo entre i_payload+1 e i_db
lines[i_payload+1:i_db] = block

p.write_text("".join(lines), encoding="utf-8")
print(f"[OK] Repair v5 aplicado em {p}")
print(f"[OK] Backup em {bak}")
PY

echo
echo "== sanity: py_compile (host) =="
python3 -m py_compile services/api/app/worker_triage.py
echo "[OK] py_compile passou"
