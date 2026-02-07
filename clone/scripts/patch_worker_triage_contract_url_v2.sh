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

if "triage_contract_url_v2" in src0:
    print("[OK] Já patchado (triage_contract_url_v2 encontrado).")
    raise SystemExit(0)

src = src0

# (A) garante import json
if re.search(r'^\s*import\s+json\s*$', src, flags=re.M) is None:
    m = re.search(r'^(?:from\s+\S+\s+import\s+[^\n]+\n|import\s+[^\n]+\n)+', src, flags=re.M)
    if m:
        src = src[:m.end()] + "import json\n" + src[m.end():]
    else:
        src = "import json\n" + src

# (B) acha o nome da variável que recebe json.loads(...)
outer = None

m = re.search(r'^(?P<var>\w+)\s*=\s*json\.loads\(', src, flags=re.M)
if m:
    outer = m.group("var")

# fallback: tenta achar algo como payload = X.get("payload") em qualquer formato
if not outer:
    m2 = re.search(r'payload\s*=\s*(?P<var>\w+)\.get\(\s*[\'"]payload[\'"]\s*\)', src)
    if m2:
        outer = m2.group("var")

if not outer:
    print("[ERRO] Não consegui inferir o 'outer' (dict da mensagem).")
    print("Preciso achar uma linha tipo: info = json.loads(...)  (ou algo equivalente).")
    print("Rode: nl -ba services/api/app/worker_triage.py | sed -n '1,220p'")
    raise SystemExit(1)

# (C) patch force_fetch: pega TOPO ou payload
# caso exista a linha antiga (sua grep mostrou exatamente isso)
pat_force = re.compile(
    r'^(?P<indent>[ \t]*)force_fetch\s*=\s*bool\(\s*isinstance\(payload,\s*dict\)\s*and\s*payload\.get\(\s*[\'"]force_fetch[\'"]\s*\)\s*\)\s*$',
    flags=re.M
)

m_force = pat_force.search(src)
if m_force:
    indent = m_force.group("indent")
    new_line = (
        f'{indent}# triage_contract_url_v2: force_fetch pode vir do topo ou do payload\n'
        f'{indent}force_fetch = bool((isinstance({outer}, dict) and {outer}.get("force_fetch")) '
        f'or (isinstance(payload, dict) and payload.get("force_fetch")))\n'
    )
    src = pat_force.sub(new_line, src, count=1)
else:
    # se não achou, tenta inserir logo após a primeira ocorrência de "payload ="
    m_payload = re.search(r'^(?P<indent>[ \t]*)payload\s*=\s*.+$', src, flags=re.M)
    if not m_payload:
        print("[ERRO] Não achei nenhuma linha 'payload = ...' pra inserir force_fetch.")
        print("Rode: grep -n '^\\s*payload\\s*=' -n services/api/app/worker_triage.py")
        raise SystemExit(1)
    indent = m_payload.group("indent")
    ins = (
        f'{indent}# triage_contract_url_v2: force_fetch pode vir do topo ou do payload\n'
        f'{indent}force_fetch = bool((isinstance({outer}, dict) and {outer}.get("force_fetch")) '
        f'or (isinstance(payload, dict) and payload.get("force_fetch")))\n'
    )
    pos = m_payload.end()
    src = src[:pos] + "\n" + ins + src[pos:]

# (D) fallback de urls vindo do payload (quando tender ainda não existe no DB)
# acha "urls = ..." e injeta fallback logo depois
m_urls = re.search(r'^(?P<indent>[ \t]*)urls\s*=\s*.+$', src, flags=re.M)
if not m_urls:
    print("[ERRO] Não achei linha 'urls = ...' no worker_triage.py.")
    print("Rode: grep -n '^\\s*urls\\s*=' -n services/api/app/worker_triage.py")
    raise SystemExit(1)

indent_urls = m_urls.group("indent")
fallback_block = (
    f'{indent_urls}# triage_contract_url_v2: fallback urls do payload quando tender ainda não existe no DB\n'
    f'{indent_urls}if (not isinstance(urls, dict)) and isinstance(payload, dict) and isinstance(payload.get("urls"), dict):\n'
    f'{indent_urls}    urls = payload.get("urls")\n'
)

pos_urls = m_urls.end()
src = src[:pos_urls] + "\n" + fallback_block + src[pos_urls:]

# (E) inserir campo "url" no payload_fetch (logo após "force_fetch": force_fetch,)
m_pf = re.search(r'^(?P<indent>[ \t]*)"force_fetch"\s*:\s*force_fetch\s*,\s*$', src, flags=re.M)
if not m_pf:
    print('[ERRO] Não achei a linha: "force_fetch": force_fetch, dentro do payload_fetch.')
    print("Rode: grep -n '\"force_fetch\"' -n services/api/app/worker_triage.py | head -n 30")
    raise SystemExit(1)

indent_pf = m_pf.group("indent")
url_line = f'{indent_pf}"url": (urls.get("pncp") if isinstance(urls, dict) else None),\n'

# evita duplicar caso exista algo parecido
after = src[m_pf.end():m_pf.end()+300]
if '"url"' not in after:
    src = src[:m_pf.end()] + "\n" + url_line + src[m_pf.end():]

# (F) salva com backup
bak = p.with_suffix(p.suffix + f".bak.triage_contract_url_v2.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(src0, encoding="utf-8")
p.write_text(src, encoding="utf-8")

print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup criado em {bak}")
print(f"[OK] outer_detectado={outer}")
PY

echo
echo "[OK] Rebuild do worker:"
docker compose -f docker-compose.yml up -d --build worker
