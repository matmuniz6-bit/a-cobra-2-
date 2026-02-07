#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_fetch_docs.py"

python3 - <<'PY'
from pathlib import Path
import datetime as dt

p = Path("services/api/app/worker_fetch_docs.py")
s = p.read_text(encoding="utf-8").splitlines(True)  # mantém \n

# 1) acha a linha urls2 = inner.get("urls")
i0 = None
for i, line in enumerate(s):
    if 'urls2 = inner.get("urls")' in line:
        i0 = i
        break
if i0 is None:
    print('[ERRO] Não achei a linha: urls2 = inner.get("urls")')
    raise SystemExit(1)

# 2) acha a próxima linha "row = await pool.fetchrow("
i1 = None
for j in range(i0 + 1, min(len(s), i0 + 80)):
    if 'row = await pool.fetchrow(' in s[j].lstrip():
        i1 = j
        break
if i1 is None:
    print('[ERRO] Não achei "row = await pool.fetchrow(" depois do urls2.')
    print('Mostre as linhas ao redor com: nl -ba services/api/app/worker_fetch_docs.py | sed -n "160,220p"')
    raise SystemExit(1)

# indent do bloco (mesma indentação do urls2)
indent = s[i0].split('u')[0]  # pega espaços antes de "urls2"

new_block = [
    f'{indent}urls2 = inner.get("urls")\n',
    f'{indent}if urls2 is None and isinstance(payload.get("urls"), dict):\n',
    f'{indent}    urls2 = payload.get("urls")\n',
    f'{indent}# garante tipo compatível com "$9::jsonb" (asyncpg espera string JSON)\n',
    f'{indent}if isinstance(urls2, dict):\n',
    f'{indent}    urls2 = json.dumps(urls2, ensure_ascii=False)\n',
    f'{indent}elif isinstance(urls2, str):\n',
    f'{indent}    urls2 = urls2.strip() or None\n',
    f'{indent}else:\n',
    f'{indent}    urls2 = None\n',
    '\n',
]

bak = p.with_suffix(p.suffix + f".bak.urls2fix.{dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S')}")
bak.write_text("".join(s), encoding="utf-8")

s2 = s[:i0] + new_block + s[i1:]  # mantém a linha do row intacta
p.write_text("".join(s2), encoding="utf-8")

print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup criado em {bak}")
print("[OK] Trecho resultante (para conferência):")
start = max(0, i0 - 3)
end = min(len(s2), i0 + 25)
for k in range(start, end):
    print(f"{k+1:4d}  {s2[k].rstrip()}")
PY

echo
echo "[OK] Rebuild do serviço fetch_docs:"
docker compose -f docker-compose.yml up -d --build fetch_docs
