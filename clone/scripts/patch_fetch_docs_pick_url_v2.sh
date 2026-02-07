#!/usr/bin/env bash
set -euo pipefail

python - <<'PY'
from pathlib import Path
import re

p = Path("services/api/app/worker_fetch_docs.py")
s = p.read_text(encoding="utf-8")

# vamos substituir APENAS o bloco de extração da url, mantendo o resto intacto
pattern = re.compile(
    r"""(?P<indent>\s*)urls\s*=\s*payload\.get\("urls"\)\s*or\s*\{\}\s*\n"""
    r"""(?P=indent)url\s*=\s*None\s*\n\s*\n"""
    r"""(?P=indent)if\s+isinstance\(urls,\s*dict\):\s*\n"""
    r"""(?P=indent)\s+url\s*=\s*urls\.get\("pncp"\)\s*or\s*urls\.get\("url"\)\s*\n"""
    r"""(?P=indent)if\s+not\s+url\s+and\s+isinstance\(payload\.get\("url"\),\s*str\):\s*\n"""
    r"""(?P=indent)\s+url\s*=\s*payload\["url"\]\s*\n""",
    re.M
)

m = pattern.search(s)
if not m:
    print("[ERRO] não achei o bloco de extração de url exatamente como esperado.")
    print("Mostre as linhas 120-140 do arquivo pra eu ajustar o pattern.")
    raise SystemExit(1)

indent = m.group("indent")

new_block = (
f"""{indent}urls      = payload.get("urls") or {{}}
{indent}url       = None

{indent}# fallback: alguns produtores empacotam dados dentro de payload/tender
{indent}inner = payload.get("payload")
{indent}if isinstance(inner, dict):
{indent}    if not isinstance(urls, dict) or not urls:
{indent}        urls = inner.get("urls") or {{}}
{indent}    if not url and isinstance(inner.get("url"), str):
{indent}        url = inner["url"]

{indent}if isinstance(urls, dict):
{indent}    url = url or urls.get("pncp") or urls.get("url")
{indent}if not url and isinstance(payload.get("url"), str):
{indent}    url = payload["url"]
"""
)

s2 = s[:m.start()] + new_block + s[m.end():]
p.write_text(s2, encoding="utf-8")
print("[OK] patch aplicado em", p)
PY
