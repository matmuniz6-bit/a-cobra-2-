#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_triage.py"
test -f "$FILE" || { echo "[ERRO] não achei $FILE"; exit 1; }

python3 - <<'PY'
from pathlib import Path
import re
import datetime as dt

p = Path("services/api/app/worker_triage.py")
src = p.read_text(encoding="utf-8")

if "triage_contract_url_v1" in src:
    print("[OK] Já parece patchado (triage_contract_url_v1 encontrado).")
    raise SystemExit(0)

# 0) garantir import json (caso ainda não tenha)
if re.search(r'^\s*import\s+json\s*$', src, flags=re.M) is None:
    m = re.search(r'^(?:from\s+\S+\s+import\s+[^\n]+\n|import\s+[^\n]+\n)+', src, flags=re.M)
    if m:
        src = src[:m.end()] + "import json\n" + src[m.end():]
    else:
        src = "import json\n" + src

# 1) descobrir o nome da variável "outer" (ex: info) a partir de: payload = info.get("payload")
m_payload = re.search(
    r'^(?P<indent>[ \t]*)payload\s*=\s*(?P<outer>\w+)\.get\(\s*[\'"]payload[\'"]\s*\)\s*$',
    src, flags=re.M
)
if not m_payload:
    print("[ERRO] Não achei a âncora: payload = <outer>.get(\"payload\")")
    print("Rode: nl -ba services/api/app/worker_triage.py | sed -n '1,240p'")
    raise SystemExit(1)

outer = m_payload.group("outer")
indent = m_payload.group("indent")

# 2) patch force_fetch: antes era só payload.get("force_fetch")
pat_force = re.compile(
    r'^(?P<indent>[ \t]*)force_fetch\s*=\s*bool\(\s*isinstance\(payload,\s*dict\)\s*and\s*payload\.get\(\s*[\'"]force_fetch[\'"]\s*\)\s*\)\s*$',
    flags=re.M
)

force_line = f'{indent}force_fetch = bool({outer}.get("force_fetch") or (isinstance(payload, dict) and payload.get("force_fetch")))\n'
if pat_force.search(src):
    src = pat_force.sub(force_line, src, count=1)
else:
    # se não existir a linha, insere logo após payload=
    insert_at = m_payload.end()
    src = src[:insert_at] + "\n" + force_line + src[insert_at:]

# 3) garantir fallback de urls a partir do payload (quando tender não existe no DB ainda)
m_urls = re.search(r'^(?P<indent>[ \t]*)urls\s*=\s*.*get\(\s*[\'"]urls[\'"]\s*\).*$',
                   src, flags=re.M)
if not m_urls:
    print("[ERRO] Não achei uma linha atribuindo urls = ...get(\"urls\")...")
    print("Rode: grep -n \"urls =\" -n services/api/app/worker_triage.py | head -n 30")
    raise SystemExit(1)

indent_urls = m_urls.group("indent")
fallback_urls_block = (
f"{indent_urls}# triage_contract_url_v1: fallback urls do payload quando tender ainda não existe no DB\n"
f"{indent_urls}if (not isinstance(urls, dict)) and isinstance(payload, dict) and isinstance(payload.get('urls'), dict):\n"
f"{indent_urls}    urls = payload.get('urls')\n"
)

# injeta fallback logo após a linha urls =
pos = m_urls.end()
src = src[:pos] + "\n" + fallback_urls_block + src[pos:]

# 4) inserir campo "url" no payload_fetch (pra fetch_docs não depender de urls['pncp'])
# busca a linha '"force_fetch": force_fetch,' dentro do dict payload_fetch
m_pf = re.search(r'^(?P<indent>[ \t]*)"force_fetch"\s*:\s*force_fetch\s*,\s*$',
                 src, flags=re.M)
if not m_pf:
    print("[ERRO] Não achei a linha: \"force_fetch\": force_fetch, dentro do payload_fetch.")
    print("Rode: grep -n '\"force_fetch\"' -n services/api/app/worker_triage.py | head -n 30")
    raise SystemExit(1)

indent_pf = m_pf.group("indent")
url_line = f'{indent_pf}"url": (urls.get("pncp") if isinstance(urls, dict) else None),\n'
src = src[:m_pf.end()] + "\n" + url_line + src[m_pf.end():]

# 5) backup + write
bak = p.with_suffix(p.suffix + f".bak.triage_contract_url_v1.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(p.read_text(encoding="utf-8"), encoding="utf-8")
p.write_text(src, encoding="utf-8")

print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup criado em {bak}")
PY

echo
echo "[OK] Rebuild do worker (triage):"
docker compose -f docker-compose.yml up -d --build worker
