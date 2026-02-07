#!/usr/bin/env bash
set -euo pipefail

patch_file() {
  local f="$1"
  test -f "$f" || { echo "[ERRO] arquivo não existe: $f"; exit 1; }
}

patch_file services/api/app/worker_triage.py
patch_file services/api/app/worker_fetch_docs.py

python3 - <<'PY'
import re
from pathlib import Path

def backup_write(path: Path, new: str):
    bak = path.with_suffix(path.suffix + ".bak")
    if not bak.exists():
        bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    path.write_text(new, encoding="utf-8")

# ----------------------------
# 1) worker_triage.py
#   - default do FETCH_QUEUE: q:fetch_docs -> q:fetch_parse
# ----------------------------
p = Path("services/api/app/worker_triage.py")
txt = p.read_text(encoding="utf-8")
orig = txt

# troca APENAS o default do getenv de FETCH_QUEUE
txt, n1 = re.subn(
    r'(\bFETCH_QUEUE\s*=\s*os\.getenv\(\s*"FETCH_QUEUE"\s*,\s*")q:fetch_docs("\s*\))',
    r"\1q:fetch_parse\2",
    txt,
    flags=re.M,
)

if txt != orig:
    backup_write(p, txt)
    print(f"[OK] patched {p} (FETCH_QUEUE default) matches: {n1}")
else:
    print(f"[OK] no changes needed in {p}")

# ----------------------------
# 2) worker_fetch_docs.py
#   Queremos que ele aceite:
#     - FETCH_QUEUE (preferencial)
#     - QUEUE (legado)
#   e default final = q:fetch_parse
#
#   Casos comuns:
#     a) QUEUE = os.getenv("QUEUE", "q:fetch_docs")
#     b) QUEUE = os.getenv("FETCH_QUEUE", "q:fetch_docs")
#     c) FETCH_QUEUE = os.getenv("FETCH_QUEUE", "q:fetch_docs") e depois QUEUE = FETCH_QUEUE
# ----------------------------
p = Path("services/api/app/worker_fetch_docs.py")
txt = p.read_text(encoding="utf-8")
orig = txt

# Caso (a): QUEUE = os.getenv("QUEUE", "q:fetch_docs")  -> robusto com fallback
txt, n2 = re.subn(
    r'^\s*QUEUE\s*=\s*os\.getenv\(\s*"QUEUE"\s*,\s*"q:fetch_docs"\s*\)\s*$',
    'QUEUE = os.getenv("FETCH_QUEUE") or os.getenv("QUEUE") or "q:fetch_parse"',
    txt,
    flags=re.M,
)

# Caso (b): QUEUE = os.getenv("FETCH_QUEUE", "q:fetch_docs") -> só troca default
txt, n3 = re.subn(
    r'(\bQUEUE\s*=\s*os\.getenv\(\s*"FETCH_QUEUE"\s*,\s*")q:fetch_docs("\s*\))',
    r"\1q:fetch_parse\2",
    txt,
    flags=re.M,
)

# Caso (c): FETCH_QUEUE default q:fetch_docs -> q:fetch_parse
txt, n4 = re.subn(
    r'(\bFETCH_QUEUE\s*=\s*os\.getenv\(\s*"FETCH_QUEUE"\s*,\s*")q:fetch_docs("\s*\))',
    r"\1q:fetch_parse\2",
    txt,
    flags=re.M,
)

if txt != orig:
    backup_write(p, txt)
    print(f"[OK] patched {p} (QUEUE/FETCH_QUEUE defaults) matches: a={n2}, b={n3}, c={n4}")
else:
    print(f"[OK] no changes needed in {p}")
PY

echo
echo "== PROVA (grep) =="
grep -nE 'TRIAGE_QUEUE|FETCH_QUEUE|QUEUE' services/api/app/worker_triage.py | sed -n '1,120p' || true
echo
grep -nE 'TRIAGE_QUEUE|FETCH_QUEUE|QUEUE' services/api/app/worker_fetch_docs.py | sed -n '1,160p' || true

echo
echo "[OK] patch aplicado. Backups: *.bak (se ainda não existiam)."
