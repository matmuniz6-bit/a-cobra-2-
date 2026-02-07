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

# backup do estado atual
bak = p.with_suffix(p.suffix + f".bak.rm_orphan.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text("".join(src0), encoding="utf-8")

lines = src0[:]

# remove bloco que começa com "# contract_triage_v3:" até fechar o ")" (ou até "# completa no DB")
start = None
end = None
for i,l in enumerate(lines):
    if "# contract_triage_v3:" in l:
        start = i
        break

if start is None:
    print("[INFO] não achei '# contract_triage_v3:' — nada a remover.")
else:
    # acha fim: primeiro "# completa no DB" depois do start, OU primeiro line que seja só ")"
    for j in range(start+1, min(start+80, len(lines))):
        if "# completa no DB" in lines[j]:
            end = j
            break
        if re.match(r'^\s*\)\s*$', lines[j]):
            # inclui o ")" e no máximo uma linha em branco depois
            end = j + 1
            if end < len(lines) and lines[end].strip() == "":
                end += 1
            break

    if end is None:
        raise SystemExit("[ERRO] achei '# contract_triage_v3:' mas não consegui achar o fim do bloco.")
    del lines[start:end]
    print(f"[OK] Removido bloco órfão contract_triage_v3: linhas {start+1}..{end}")

p.write_text("".join(lines), encoding="utf-8")
print(f"[OK] Escrevi {p}")
print(f"[OK] Backup {bak}")
PY

echo
echo "== sanity: py_compile =="
python3 -m py_compile services/api/app/worker_triage.py
echo "[OK] py_compile passou"
