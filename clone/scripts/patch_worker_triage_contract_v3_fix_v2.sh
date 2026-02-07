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

if "contract_triage_v3_fix_v2" in src0:
    print("[OK] Já patchado (contract_triage_v3_fix_v2 encontrado).")
    raise SystemExit(0)

src = src0

# 0) garante import json
if re.search(r'^\s*import\s+json\s*$', src, flags=re.M) is None:
    m = re.search(r'^(?:from\s+\S+\s+import\s+[^\n]+\n|import\s+[^\n]+\n)+', src, flags=re.M)
    if m:
        src = src[:m.end()] + "import json\n" + src[m.end():]
    else:
        src = "import json\n" + src

# 1) substituir _pick(...) por versão que aceita tender/payload/direto
pat_pick = re.compile(r"(?ms)^def _pick\(\s*payload[^)]*\)\s*:\n.*?\n(?=^\s*def |\Z)")
m = pat_pick.search(src)
if not m:
    raise SystemExit("[ERRO] Não achei o bloco def _pick(...):")

new_pick = (
"def _pick(payload: dict):\n"
"    # contract_triage_v3_fix_v2\n"
"    # Aceita formatos:\n"
"    #  - {...} (direto)\n"
"    #  - {\"tender\": {...}}\n"
"    #  - {\"payload\": {...}}\n"
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

# 2) remover qualquer linha antiga de force_fetch = ... (pra não ficar bugada antes do info)
src = re.sub(r'(?m)^[ \t]*force_fetch\s*=.*\n', '', src)

# 3) inserir force_fetch no lugar certo: logo após "info = dict(t)"
m_info = re.search(r'^(?P<indent>[ \t]*)info\s*=\s*dict\(t\)\s*$', src, flags=re.M)
if not m_info:
    raise SystemExit("[ERRO] Não achei a âncora: info = dict(t)")

indent = m_info.group("indent")
force_block = (
"\n"
f"{indent}# contract_triage_v3_fix_v2: force_fetch pode vir no topo, dentro do tender, ou dentro de payload\n"
f"{indent}inner_payload = payload.get(\"payload\") if isinstance(payload, dict) and isinstance(payload.get(\"payload\"), dict) else None\n"
f"{indent}inner_tender  = payload.get(\"tender\")  if isinstance(payload, dict) and isinstance(payload.get(\"tender\"), dict) else None\n"
f"{indent}force_fetch = bool(\n"
f"{indent}    (payload.get(\"force_fetch\") if isinstance(payload, dict) else False)\n"
f"{indent}    or (t.get(\"force_fetch\") if isinstance(t, dict) else False)\n"
f"{indent}    or (inner_payload.get(\"force_fetch\") if isinstance(inner_payload, dict) else False)\n"
f"{indent}    or (inner_tender.get(\"force_fetch\") if isinstance(inner_tender, dict) else False)\n"
f"{indent})\n"
)
src = re.sub(r'^(?P<indent>[ \t]*)info\s*=\s*dict\(t\)\s*$', lambda m: m.group(0) + force_block, src, flags=re.M, count=1)

# 4) substituir o bloco de urls de forma resiliente:
#    acha a primeira atribuição de urls que menciona info.get("urls") (com ou sem comentários)
lines = src.splitlines(True)

idx_urls = None
for i, line in enumerate(lines):
    if "urls" in line and "=" in line and "info.get" in line and "urls" in line and ("get(\"urls\")" in line or "get('urls')" in line):
        idx_urls = i
        break

if idx_urls is None:
    # fallback: procura seção "empurra pro fetch" e pega a primeira linha que começa com "urls ="
    for i, line in enumerate(lines):
        if "empurra pro fetch" in line:
            for j in range(i, min(i+40, len(lines))):
                if re.match(r'^\s*urls\s*=', lines[j]):
                    idx_urls = j
                    break
            break

if idx_urls is None:
    raise SystemExit("[ERRO] Não achei onde o triage define urls (âncora). Rode: nl -ba services/api/app/worker_triage.py | sed -n '140,220p'")

# define o fim do bloco: antes do primeiro pncp_url = ... (ou antes do if que usa pncp_url)
idx_end = None
for j in range(idx_urls+1, min(idx_urls+60, len(lines))):
    if re.match(r'^\s*pncp_url\s*=', lines[j]):
        idx_end = j
        break
    if "pncp_url" in lines[j] and re.match(r'^\s*if\s*\(?.*pncp_url.*:\s*$', lines[j]):
        idx_end = j
        break

if idx_end is None:
    raise SystemExit("[ERRO] Não achei o ponto onde pncp_url/if começa após urls. Rode: nl -ba services/api/app/worker_triage.py | sed -n '160,220p'")

indent_urls = lines[idx_urls][:len(lines[idx_urls]) - len(lines[idx_urls].lstrip())]

new_urls_block = [
    f"{indent_urls}# contract_triage_v3_fix_v2: urls pode vir do DB (info), do tender (t) ou do payload recebido\n",
    f"{indent_urls}urls = info.get(\"urls\") or (t.get(\"urls\") if isinstance(t, dict) else None)\n",
    f"{indent_urls}if not urls and isinstance(payload, dict):\n",
    f"{indent_urls}    if isinstance(payload.get(\"tender\"), dict) and payload[\"tender\"].get(\"urls\"):\n",
    f"{indent_urls}        urls = payload[\"tender\"].get(\"urls\")\n",
    f"{indent_urls}    elif isinstance(payload.get(\"payload\"), dict) and payload[\"payload\"].get(\"urls\"):\n",
    f"{indent_urls}        urls = payload[\"payload\"].get(\"urls\")\n",
    f"{indent_urls}    elif payload.get(\"urls\"):\n",
    f"{indent_urls}        urls = payload.get(\"urls\")\n",
    f"{indent_urls}urls = urls or {{}}\n",
    f"{indent_urls}if isinstance(urls, str):\n",
    f"{indent_urls}    try:\n",
    f"{indent_urls}        urls = json.loads(urls)\n",
    f"{indent_urls}    except Exception:\n",
    f"{indent_urls}        urls = {{\"raw\": urls}}\n",
]

lines[idx_urls:idx_end] = new_urls_block
src = "".join(lines)

# 5) backup + write
bak = p.with_suffix(p.suffix + f".bak.contract_v3_fix_v2.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(src0, encoding="utf-8")
p.write_text(src, encoding="utf-8")
print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup criado em {bak}")
PY

echo
echo "[OK] Rebuild do worker (triage) pra garantir que o container usa o código novo:"
docker compose -f docker-compose.yml up -d --build worker
