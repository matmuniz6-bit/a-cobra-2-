#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_fetch_docs.py"

python3 - <<'PY'
from pathlib import Path
import re

p = Path("services/api/app/worker_fetch_docs.py")
s = p.read_text(encoding="utf-8")

# pega o bloco urls2 atual e troca por um que SEMPRE converte dict -> JSON string
pat = re.compile(
    r"""
(?P<indent>^[ \t]+)urls2\s*=\s*inner\.get\("urls"\)\s*\n
(?:(?P=indent).*\n){0,10}?
(?P=indent)row\s*=\s*await\s+pool\.fetchrow\(
""",
    re.M | re.X
)

m = pat.search(s)
if not m:
    print("[ERRO] Não consegui localizar o bloco urls2 antes do pool.fetchrow().")
    print("Procure manualmente por: urls2 = inner.get(\"urls\")")
    raise SystemExit(1)

indent = m.group("indent")

new_block = (
f'{indent}urls2 = inner.get("urls")\n'
f'{indent}if urls2 is None and isinstance(payload.get("urls"), dict):\n'
f'{indent}    urls2 = payload.get("urls")\n'
f'{indent}# garante tipo compatível com "$9::jsonb" (precisa ser string JSON)\n'
f'{indent}if isinstance(urls2, dict):\n'
f'{indent}    urls2 = json.dumps(urls2, ensure_ascii=False)\n'
f'{indent}elif isinstance(urls2, str):\n'
f'{indent}    urls2 = urls2.strip() or None\n'
f'{indent}else:\n'
f'{indent}    urls2 = None\n\n'
f'{indent}row = await pool.fetchrow(\n'
)

# substitui do começo do urls2 até a linha do row = await pool.fetchrow(
start = m.start()
# encontra a posição exata do "row = await pool.fetchrow(" capturado no match
row_line_pos = s.find(f"{indent}row = await pool.fetchrow(", start)
if row_line_pos == -1:
    print("[ERRO] Não achei a linha row = await pool.fetchrow(")
    raise SystemExit(1)

# corta do start até row_line_pos e injeta new_block
s2 = s[:start] + new_block + s[row_line_pos + len(f"{indent}row = await pool.fetchrow(\n"):]

bak = p.with_suffix(p.suffix + ".bak.urls2fix")
bak.write_text(s, encoding="utf-8")
p.write_text(s2, encoding="utf-8")
print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup criado em {bak}")
PY

echo
echo "[OK] Agora rebuild do fetch_docs:"
docker compose -f docker-compose.yml up -d --build fetch_docs
