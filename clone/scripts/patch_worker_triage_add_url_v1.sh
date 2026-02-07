#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_triage.py"
test -f "$FILE" || { echo "[ERRO] não achei $FILE"; exit 1; }

python3 - <<'PY'
from pathlib import Path
import re, datetime as dt

p = Path("services/api/app/worker_triage.py")
src = p.read_text(encoding="utf-8")

# já patchado?
if re.search(r'payload_fetch\[\s*[\'"]url[\'"]\s*\]\s*=\s*pncp_url', src):
    print("[OK] worker_triage.py já tem payload_fetch['url']=pncp_url (não vou duplicar).")
    raise SystemExit(0)

lines = src.splitlines(True)

# 1) achar onde o payload_fetch é criado
# tentativas comuns:
# payload_fetch = { ... }
pf_idx = None
for i, line in enumerate(lines):
    if re.search(r'^\s*payload_fetch\s*=\s*\{', line):
        pf_idx = i
        break

if pf_idx is None:
    print("[ERRO] Não achei 'payload_fetch = {' no worker_triage.py.")
    print("Rode: nl -ba services/api/app/worker_triage.py | sed -n '1,260p'")
    raise SystemExit(1)

indent = lines[pf_idx][:len(lines[pf_idx]) - len(lines[pf_idx].lstrip())]

# 2) precisamos ter acesso a `urls` (dict) pra tirar o pncp_url
# vamos inserir o cálculo logo ANTES do payload_fetch = {
# procurando a variável urls nas linhas anteriores (se não existir, ainda inserimos defensivo)
insert_at = pf_idx

pncp_block = [
    f"{indent}# resolve URL PNCP (pra facilitar downstream)\n",
    f"{indent}pncp_url = None\n",
    f"{indent}try:\n",
    f"{indent}    if isinstance(urls, dict):\n",
    f"{indent}        pncp_url = urls.get('pncp')\n",
    f"{indent}except Exception:\n",
    f"{indent}    pncp_url = None\n",
]

# evita duplicar se já existir pncp_url
window = "".join(lines[max(0, pf_idx-40):pf_idx+5])
if "pncp_url" not in window:
    lines[insert_at:insert_at] = pncp_block
    pf_idx += len(pncp_block)

# 3) depois que o payload_fetch é criado, inserir payload_fetch["url"] = pncp_url
# achar o fechamento do dict (primeiro "}" no mesmo nível) e inserir logo após
close_idx = None
level = 0
for j in range(pf_idx, min(len(lines), pf_idx + 220)):
    line = lines[j]
    if "payload_fetch" in line and "{" in line:
        level += line.count("{") - line.count("}")
        continue
    level += line.count("{") - line.count("}")
    if level <= 0 and re.match(r'^\s*\}\s*$', line):
        close_idx = j
        break

if close_idx is None:
    # fallback: achar a primeira linha depois do payload_fetch que não seja continuação do dict
    for j in range(pf_idx, min(len(lines), pf_idx + 220)):
        if re.search(r'^\s*await\s+.*lpush\(', lines[j]):
            close_idx = j - 1
            break

if close_idx is None:
    print("[ERRO] Não achei o fechamento do dict do payload_fetch.")
    print("Mostre o trecho: nl -ba services/api/app/worker_triage.py | sed -n '1,260p'")
    raise SystemExit(1)

ins = [
    f"{indent}payload_fetch['url'] = pncp_url\n",
]

# inserir logo após o fechamento do dict (linha seguinte)
lines[close_idx+1:close_idx+1] = ins

out = "".join(lines)
bak = p.with_suffix(p.suffix + f".bak.addurl.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(src, encoding="utf-8")
p.write_text(out, encoding="utf-8")

print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup criado em {bak}")
PY

echo
echo "[OK] Rebuild do worker (triage) e api:"
docker compose -f docker-compose.yml up -d --build worker api
