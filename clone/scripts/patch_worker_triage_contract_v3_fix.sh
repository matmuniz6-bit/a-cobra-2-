#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_triage.py"
test -f "$FILE" || { echo "[ERRO] não achei $FILE"; exit 1; }

python3 - <<'PY'
from pathlib import Path
import re
import datetime as dt

p = Path("services/api/app/worker_triage.py")
src0 = p.read_text(encoding="utf-8")

if "contract_triage_v3_fix" in src0:
    print("[OK] Já patchado (contract_triage_v3_fix encontrado).")
    raise SystemExit(0)

src = src0

# 0) garante import json
if re.search(r'^\s*import\s+json\s*$', src, flags=re.M) is None:
    m = re.search(r'^(?:from\s+\S+\s+import\s+[^\n]+\n|import\s+[^\n]+\n)+', src, flags=re.M)
    if m:
        src = src[:m.end()] + "import json\n" + src[m.end():]
    else:
        src = "import json\n" + src

# 1) _pick: aceitar {tender:{...}} e {payload:{...}} além do direto
pat_pick = re.compile(r"(?ms)^def _pick\(payload: dict\):\n.*?\n(?=^\s*def |\Z)")
m = pat_pick.search(src)
if not m:
    raise SystemExit("[ERRO] Não achei def _pick(payload: dict):")

new_pick = (
"def _pick(payload: dict):\n"
"    # Aceita formatos:\n"
"    #  - { ...tender... }\n"
"    #  - {\"tender\": {...}}\n"
"    #  - {\"payload\": {...}} (compat upstream)\n"
"    inner = None\n"
"    if isinstance(payload.get(\"tender\"), dict):\n"
"        inner = payload.get(\"tender\")\n"
"    elif isinstance(payload.get(\"payload\"), dict):\n"
"        inner = payload.get(\"payload\")\n"
"    t = inner if isinstance(inner, dict) else payload\n"
"    tender_id = (t.get(\"id\") if isinstance(t, dict) else None) or payload.get(\"id\")\n"
"    id_pncp = (t.get(\"id_pncp\") if isinstance(t, dict) else None) or payload.get(\"id_pncp\")\n"
"    return tender_id, id_pncp, (t if isinstance(t, dict) else {})\n"
)
src = src[:m.start()] + new_pick + src[m.end():]

# 2) remover quaisquer linhas "force_fetch = ..." (pra recolocar no lugar certo)
lines = src.splitlines(True)
out = []
for line in lines:
    if re.match(r'^\s*force_fetch\s*=\s*', line):
        continue
    out.append(line)
lines = out

# 3) inserir force_fetch logo após "info = dict(t)"
inserted = False
for i, line in enumerate(lines):
    if re.search(r'^\s*info\s*=\s*dict\(t\)\s*$', line):
        indent = line[:len(line)-len(line.lstrip())]
        block = [
            "\n",
            f"{indent}# contract_triage_v3_fix: force_fetch pode vir no topo, dentro do tender, ou dentro de payload\n",
            f"{indent}inner_payload = payload.get(\"payload\") if isinstance(payload, dict) and isinstance(payload.get(\"payload\"), dict) else None\n",
            f"{indent}force_fetch = bool(\n",
            f"{indent}    (isinstance(payload, dict) and payload.get(\"force_fetch\"))\n",
            f"{indent}    or (isinstance(info, dict) and info.get(\"force_fetch\"))\n",
            f"{indent}    or (isinstance(inner_payload, dict) and inner_payload.get(\"force_fetch\"))\n",
            f"{indent})\n",
        ]
        lines[i+1:i+1] = block
        inserted = True
        break
if not inserted:
    raise SystemExit("[ERRO] Não achei a âncora: info = dict(t)")

src = "".join(lines)

# 4) urls: não depender só do DB; usar t (do _pick) e payload também
# troca a linha: urls = info.get("urls") or {}
m_urls = re.search(r'^(?P<indent>[ \t]*)urls\s*=\s*info\.get\("urls"\)\s*or\s*\{\}\s*$', src, flags=re.M)
if not m_urls:
    raise SystemExit('[ERRO] Não achei a linha: urls = info.get("urls") or {}')

indent = m_urls.group("indent")
replacement = (
f"{indent}# contract_triage_v3_fix: urls pode vir do DB (info), do tender (t) ou do payload recebido\n"
f"{indent}urls = info.get(\"urls\") or (t.get(\"urls\") if isinstance(t, dict) else None)\n"
f"{indent}if not urls and isinstance(payload, dict):\n"
f"{indent}    if isinstance(payload.get(\"tender\"), dict) and payload[\"tender\"].get(\"urls\"):\n"
f"{indent}        urls = payload[\"tender\"].get(\"urls\")\n"
f"{indent}    elif isinstance(payload.get(\"payload\"), dict) and payload[\"payload\"].get(\"urls\"):\n"
f"{indent}        urls = payload[\"payload\"].get(\"urls\")\n"
f"{indent}    elif payload.get(\"urls\"):\n"
f"{indent}        urls = payload.get(\"urls\")\n"
f"{indent}urls = urls or {{}}\n"
)
src = re.sub(r'^[ \t]*urls\s*=\s*info\.get\("urls"\)\s*or\s*\{\}\s*$', replacement, src, flags=re.M, count=1)

# 5) backup + write
bak = p.with_suffix(p.suffix + f".bak.contract_v3_fix.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(src0, encoding="utf-8")
p.write_text(src, encoding="utf-8")
print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup criado em {bak}")
PY

echo
echo "[OK] Rebuild do worker (triage) pra garantir que o container usa o código novo:"
docker compose -f docker-compose.yml up -d --build worker
