#!/usr/bin/env bash
set -euo pipefail
FILE="services/api/app/worker_fetch_docs.py"

python3 - <<'PY'
from pathlib import Path
import re, datetime

p = Path("services/api/app/worker_fetch_docs.py")
s = p.read_text(encoding="utf-8", errors="ignore").splitlines(True)

# 1) localizar o início do bloco "FK guard" (pra não encostar no urls do começo do loop)
start = None
for i,l in enumerate(s):
    if "FK guard" in l or "missing_tender_or_url" in l or "DEAD_QUEUE" in l:
        start = i
        break
if start is None:
    # fallback: começa perto do INSERT INTO tender
    for i,l in enumerate(s):
        if "INSERT INTO tender" in l:
            start = max(0, i-80)
            break
if start is None:
    print("[ERRO] não achei marcador do FK guard nem 'INSERT INTO tender'.")
    raise SystemExit(1)

# 2) achar a região do INSERT INTO tender
ins = None
for i in range(start, len(s)):
    if "INSERT INTO tender" in s[i]:
        ins = i
        break
if ins is None:
    print("[ERRO] não achei 'INSERT INTO tender' depois do bloco FK guard.")
    raise SystemExit(1)

# backup
bak = p.with_suffix(p.suffix + f".bak.{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text("".join(s), encoding="utf-8")

# 3) garantir que o SQL do INSERT tenha $9::jsonb (se houver $9)
for i in range(ins, min(len(s), ins+120)):
    if "$9" in s[i] and "::jsonb" not in s[i]:
        s[i] = s[i].replace("$9", "$9::jsonb")
    if "RETURNING" in s[i]:
        break

# 4) achar a variável que está recebendo .get("urls") DENTRO do FK guard e converter para string JSON
# (para não quebrar o pick_url no começo do loop)
urls_var = None
urls_line = None
for i in range(start, ins):
    m = re.search(r'^\s*(\w+)\s*=\s*.*get\("urls"\)', s[i])
    if m:
        urls_var = m.group(1)
        urls_line = i

if urls_var is None:
    # fallback: tenta achar uma linha 'urls =' depois do start (mas antes do INSERT)
    for i in range(start, ins):
        if re.search(r'^\s*urls\s*=', s[i]):
            urls_var = "urls"
            urls_line = i
            break

if urls_var is None or urls_line is None:
    print("[ERRO] não achei atribuição de urls no bloco FK guard (get(\"urls\")).")
    print("Rode o diag e me mande as linhas do upsert que eu ajusto o patch 100%.")
    raise SystemExit(1)

# evitar duplicar
window = "".join(s[urls_line: min(len(s), urls_line+12)])
if "json.dumps" not in window and "ensure_ascii" not in window:
    indent = re.match(r'^(\s*)', s[urls_line]).group(1)
    insert = (
        f"{indent}# garante tipo compatível com asyncpg (evita dict em bind)\n"
        f"{indent}if isinstance({urls_var}, dict):\n"
        f"{indent}    {urls_var} = json.dumps({urls_var}, ensure_ascii=False)\n"
    )
    s.insert(urls_line+1, insert)

p.write_text("".join(s), encoding="utf-8")
print(f"[OK] patch aplicado em {p}")
print(f"[OK] backup criado em {bak}")
PY

