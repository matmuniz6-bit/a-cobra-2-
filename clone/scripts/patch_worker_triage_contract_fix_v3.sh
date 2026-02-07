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

if "contract_triage_v3" in src0:
    print("[OK] Já patchado (contract_triage_v3 encontrado).")
    raise SystemExit(0)

lines = src0.splitlines(True)

# ------------------------------------------------------------
# 1) Atualiza _pick() para aceitar payload/tender/payload(payload)
# ------------------------------------------------------------
def replace_pick(text: str) -> str:
    pat = re.compile(r"(?ms)^def _pick\(payload: dict\):\n.*?\n(?=^\s*def |\Z)")
    m = pat.search(text)
    if not m:
        raise SystemExit("[ERRO] Não achei def _pick(payload: dict): para substituir.")
    new = (
        "def _pick(payload: dict):\n"
        "    # Aceita mensagens no formato:\n"
        "    #  - { ...campos do tender... }\n"
        "    #  - {\"tender\": {...}}\n"
        "    #  - {\"payload\": {...}}  (compat)\n"
        "    inner = None\n"
        "    if isinstance(payload.get(\"tender\"), dict):\n"
        "        inner = payload.get(\"tender\")\n"
        "    elif isinstance(payload.get(\"payload\"), dict):\n"
        "        inner = payload.get(\"payload\")\n"
        "    t = inner if isinstance(inner, dict) else payload\n"
        "    tender_id = (t.get(\"id\") if isinstance(t, dict) else None) or payload.get(\"id\")\n"
        "    id_pncp = (t.get(\"id_pncp\") if isinstance(t, dict) else None) or payload.get(\"id_pncp\")\n"
        "    return tender_id, id_pncp, (t if isinstance(t, dict) else {})\n"
        "\n"
    )
    return text[:m.start()] + new + text[m.end():]

src = replace_pick("".join(lines))
lines = src.splitlines(True)

# ------------------------------------------------------------
# 2) Remover force_fetch antigo que usa 'info' antes de definir
# ------------------------------------------------------------
out = []
removed_force_fetch = False
for i, line in enumerate(lines):
    if "force_fetch" in line and "info.get" in line and "contract_url_forcefetch_v1" in line:
        removed_force_fetch = True
        continue
    # também remove qualquer linha que seja "force_fetch = bool(info.get(..."
    if re.search(r'^\s*force_fetch\s*=\s*bool\(\s*info\.get\(', line):
        removed_force_fetch = True
        continue
    out.append(line)

lines = out

# ------------------------------------------------------------
# 3) Inserir force_fetch correto logo após "info = dict(t)"
#    (lê do topo + inner tender + inner payload)
# ------------------------------------------------------------
inserted = False
for i, line in enumerate(lines):
    if re.search(r'^\s*info\s*=\s*dict\(t\)\s*$', line):
        indent = line[:len(line)-len(line.lstrip())]
        block = [
            "\n",
            f"{indent}# contract_triage_v3: force_fetch (top-level ou dentro do tender/payload)\n",
            f"{indent}inner_payload = payload.get(\"payload\") if isinstance(payload, dict) and isinstance(payload.get(\"payload\"), dict) else None\n",
            f"{indent}force_fetch = bool(\n",
            f"{indent}    (isinstance(payload, dict) and payload.get(\"force_fetch\"))\n",
            f"{indent}    or (isinstance(info, dict) and info.get(\"force_fetch\"))\n",
            f"{indent}    or (isinstance(inner_payload, dict) and inner_payload.get(\"force_fetch\"))\n",
            f"{indent})\n",
            "\n",
        ]
        lines[i+1:i+1] = block
        inserted = True
        break

if not inserted:
    raise SystemExit("[ERRO] Não achei a âncora: info = dict(t)")

# ------------------------------------------------------------
# 4) Melhorar resolução de urls: DB (info.urls) OU tender payload OU payload aninhado
#    Substitui o bloco a partir de 'urls = info.get("urls")' até 'pncp_url = ...'
# ------------------------------------------------------------
text = "".join(lines)

if "contract_urls_fallback_v3" in text:
    print("[OK] urls fallback já patchado.")
else:
    # encontra início
    m0 = re.search(r'^\s*urls\s*=\s*info\.get\("urls"\)\s*or\s*\{\}\s*$', text, flags=re.M)
    if not m0:
        # fallback: aceita "urls = info.get("urls") or {}" com espaços diferentes
        m0 = re.search(r'^\s*urls\s*=\s*info\.get\("urls"\)\s*or\s*\{\}\s*$', text, flags=re.M)
    if not m0:
        raise SystemExit('[ERRO] Não achei a linha: urls = info.get("urls") or {}')

    # encontra fim (linha pncp_url = ...)
    m1 = re.search(r'^\s*pncp_url\s*=\s*urls\.get\("pncp"\).*$', text[m0.end():], flags=re.M)
    if not m1:
        raise SystemExit('[ERRO] Não achei a linha pncp_url = urls.get("pncp") ... após o bloco urls.')
    start = m0.start()
    end = m0.end() + m1.end()

    # indent base do bloco urls
    indent = re.match(r'^(\s*)', text[start:]).group(1)

    new_block = (
        f"{indent}# contract_urls_fallback_v3: urls pode vir do DB (info.urls) ou do payload (tender/payload)\n"
        f"{indent}urls = info.get(\"urls\")\n"
        f"{indent}if not urls and isinstance(info.get(\"payload\"), dict):\n"
        f"{indent}    urls = info[\"payload\"].get(\"urls\")\n"
        f"{indent}if not urls and isinstance(payload, dict) and isinstance(payload.get(\"payload\"), dict):\n"
        f"{indent}    urls = payload[\"payload\"].get(\"urls\")\n"
        f"{indent}if not urls and isinstance(payload, dict):\n"
        f"{indent}    urls = payload.get(\"urls\")\n"
        f"{indent}urls = urls or {{}}\n"
        f"{indent}if isinstance(urls, str):\n"
        f"{indent}    try:\n"
        f"{indent}        urls = json.loads(urls)\n"
        f"{indent}    except Exception:\n"
        f"{indent}        urls = {{\"raw\": urls}}\n"
        f"{indent}pncp_url = urls.get(\"pncp\") if isinstance(urls, dict) else None\n"
    )

    text = text[:start] + new_block + text[end:]

# ------------------------------------------------------------
# 5) Grava backup + escreve
# ------------------------------------------------------------
bak = p.with_suffix(p.suffix + f".bak.contract_v3.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(src0, encoding="utf-8")
p.write_text(text, encoding="utf-8")

print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup criado em {bak}")

# mostra trechos críticos
print("\n[OK] Trechos para conferência:")
show = []
for idx, line in enumerate(text.splitlines(), 1):
    if "def _pick" in line or "contract_triage_v3" in line or "contract_urls_fallback_v3" in line:
        show.append(idx)
if show:
    lo = max(1, min(show) - 8)
    hi = min(len(text.splitlines()), max(show) + 30)
    for i in range(lo, hi+1):
        print(f"{i:4d} | {text.splitlines()[i-1]}")
PY

echo
echo "[OK] Rebuild apenas do worker (triage):"
docker compose -f docker-compose.yml up -d --build worker
