#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_triage.py"
test -f "$FILE" || { echo "[ERRO] não achei $FILE"; exit 1; }

python3 - <<'PY'
from pathlib import Path
import datetime as dt
import re

p = Path("services/api/app/worker_triage.py")
src0 = p.read_text(encoding="utf-8")

if "repair_triage_contract_v4" in src0:
    print("[OK] Já patchado (repair_triage_contract_v4 encontrado).")
    raise SystemExit(0)

bak = p.with_suffix(p.suffix + f".bak.repairtriage.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(src0, encoding="utf-8")

src = src0

# 0) garante import json (caso alguém tenha bagunçado)
if re.search(r'^\s*import\s+json\s*$', src, flags=re.M) is None:
    m = re.search(r'^(?:from\s+\S+\s+import\s+[^\n]+\n|import\s+[^\n]+\n)+', src, flags=re.M)
    if m:
        src = src[:m.end()] + "import json\n" + src[m.end():]
    else:
        src = "import json\n" + src

# 1) substitui _pick para aceitar {tender:{...}} / {payload:{...}} / direto {...}
pick_re = re.compile(r"(?ms)^def _pick\(payload: dict\):\n.*?\n(?=^def _score|\Z)")
m = pick_re.search(src)
if not m:
    raise SystemExit("[ERRO] Não achei def _pick(payload: dict):")

new_pick = (
"def _pick(payload: dict):\n"
"    # Aceita formatos:\n"
"    #  - {\"tender\": {...}}\n"
"    #  - {\"payload\": {...}}\n"
"    #  - {...} (direto)\n"
"    t = None\n"
"    if isinstance(payload.get(\"tender\"), dict):\n"
"        t = payload[\"tender\"]\n"
"    elif isinstance(payload.get(\"payload\"), dict):\n"
"        t = payload[\"payload\"]\n"
"    else:\n"
"        t = payload\n"
"\n"
"    tender_id = (t.get(\"id\") if isinstance(t, dict) else None) or payload.get(\"id\")\n"
"    id_pncp   = (t.get(\"id_pncp\") if isinstance(t, dict) else None) or payload.get(\"id_pncp\")\n"
"    return tender_id, id_pncp, t\n"
)
src = pick_re.sub(new_pick, src, count=1)

# 2) hard-reset do miolo (payload/jsonloads/_pick/info/force_fetch) dentro do main loop
lines = src.splitlines(True)

# acha "payload = json.loads(raw" dentro do while
i_payload = next((i for i,l in enumerate(lines) if re.search(r'^\s*payload\s*=\s*json\.loads\(raw\)', l)), None)
if i_payload is None:
    raise SystemExit("[ERRO] Não achei a linha: payload = json.loads(raw) ...")

# acha a próxima linha "info = dict(t)" depois do _pick
i_info = None
for j in range(i_payload, min(len(lines), i_payload+80)):
    if re.search(r'^\s*info\s*=\s*dict\(\s*t\s*\)', lines[j]):
        i_info = j
        break
if i_info is None:
    raise SystemExit("[ERRO] Não achei a linha: info = dict(t) perto do payload/json.loads.")

indent = re.match(r'^(\s*)', lines[i_payload]).group(1)

new_block = [
    f"{indent}payload = json.loads(raw) if isinstance(raw, str) else (raw or {{}})\n",
    "\n",
    f"{indent}tender_id, id_pncp, t = _pick(payload)\n",
    "\n",
    f"{indent}info = dict(t) if isinstance(t, dict) else {{}}\n",
    "\n",
    f"{indent}# repair_triage_contract_v4: force_fetch pode vir no topo ou dentro do tender/payload\n",
    f"{indent}force_fetch = bool(\n",
    f"{indent}    (isinstance(payload, dict) and payload.get('force_fetch'))\n",
    f"{indent}    or (isinstance(info, dict) and info.get('force_fetch'))\n",
    f"{indent})\n",
    "\n",
]

# substitui do payload até info (inclusive)
lines[i_payload:i_info+1] = new_block

# marca no arquivo (pra idempotência)
if "repair_triage_contract_v4" not in "".join(lines):
    # insere um marcador perto do topo (após imports)
    for k in range(min(60, len(lines))):
        if lines[k].startswith("log = "):
            lines.insert(k+1, "# repair_triage_contract_v4\n")
            break

p.write_text("".join(lines), encoding="utf-8")
print("[OK] Patch aplicado:", p)
print("[OK] Backup:", bak)
PY

echo
echo "== sanity: py_compile =="
if ! python3 -m py_compile "$FILE"; then
  echo
  echo "== ERRO AINDA EXISTE — contexto 150..210 =="
  nl -ba "$FILE" | sed -n '150,210p'
  exit 1
fi

echo
echo "== rebuild + subir worker =="
docker compose -f docker-compose.yml up -d --build worker

echo
docker compose -f docker-compose.yml logs --tail=120 worker
