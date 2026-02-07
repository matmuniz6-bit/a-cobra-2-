#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_triage.py"
test -f "$FILE" || { echo "[ERRO] não achei $FILE"; exit 1; }

python3 - <<'PY'
from pathlib import Path
import datetime as dt

p = Path("services/api/app/worker_triage.py")
src = p.read_text(encoding="utf-8").splitlines(True)

txt = "".join(src)
# idempotência: se já tiver url sendo setada no payload_fetch, não mexe
if 'payload_fetch["url"]' in txt or "payload_fetch['url']" in txt:
    print("[OK] Já existe payload_fetch['url'] no worker_triage.py (não vou duplicar).")
    raise SystemExit(0)

# 1) achar onde payload_fetch é construído
i0 = None
for i, line in enumerate(src):
    if "payload_fetch" in line and "=" in line and "{" in line and line.lstrip().startswith("payload_fetch"):
        i0 = i
        break

if i0 is None:
    print("[ERRO] Não achei a criação do payload_fetch = {...} no worker_triage.py")
    print("Dica: rode `grep -n \"payload_fetch\" -n services/api/app/worker_triage.py`")
    raise SystemExit(1)

indent = src[i0][:len(src[i0]) - len(src[i0].lstrip())]

# 2) achar o fechamento do dict do payload_fetch (linha com '}' no mesmo nível de indent)
i1 = None
for j in range(i0 + 1, min(len(src), i0 + 200)):
    if src[j].startswith(indent) and src[j].lstrip().startswith("}"):
        i1 = j
        break

if i1 is None:
    print("[ERRO] Não achei o fim do dict do payload_fetch perto da criação.")
    print(f"Mostre o trecho com: nl -ba {p} | sed -n '{max(1,i0-20)}p'")
    raise SystemExit(1)

block = [
    f"{indent}# contrato: sempre explicitar a URL principal (pncp) no payload do fetch\n",
    f"{indent}# evita depender do fetch_docs 'adivinhar' dentro de urls{{}}\n",
    f"{indent}try:\n",
    f"{indent}    _u = payload_fetch.get('urls')\n",
    f"{indent}    if isinstance(_u, dict):\n",
    f"{indent}        payload_fetch['url'] = (_u.get('pncp') or _u.get('url') or _u.get('link'))\n",
    f"{indent}    elif isinstance(_u, str):\n",
    f"{indent}        payload_fetch['url'] = _u.strip() or None\n",
    f"{indent}    else:\n",
    f"{indent}        payload_fetch['url'] = None\n",
    f"{indent}except Exception:\n",
    f"{indent}    payload_fetch['url'] = None\n",
]

out = src[:i1+1] + block + src[i1+1:]

bak = p.with_suffix(p.suffix + f".bak.emiturl.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text("".join(src), encoding="utf-8")
p.write_text("".join(out), encoding="utf-8")

print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup criado em {bak}")
PY

echo
echo "[OK] Rebuild dos serviços que usam services/api:"
docker compose -f docker-compose.yml up -d --build worker api
