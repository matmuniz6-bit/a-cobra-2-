#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_parse.py"
test -f "$FILE" || { echo "[ERRO] não achei $FILE"; exit 1; }

python3 - <<'PY'
from pathlib import Path
import re, datetime

p = Path("services/api/app/worker_parse.py")
s = p.read_text(encoding="utf-8")

# 1) garante import json
if re.search(r'^\s*import\s+json\s*$', s, flags=re.M) is None:
    # tenta inserir após imports padrões
    m = re.search(r'^(import\s+[^\n]+\n)+', s, flags=re.M)
    if m:
        ins_at = m.end()
        s = s[:ins_at] + "import json\n" + s[ins_at:]
    else:
        s = "import json\n" + s

# 2) acha o "if" que inicia o branch de PDF (pra inserir JSON antes)
m_pdf = re.search(r'^(?P<indent>[ \t]*)if\s+.*content_type.*pdf.*:\s*$', s, flags=re.M|re.I)
if not m_pdf:
    m_pdf = re.search(r'^(?P<indent>[ \t]*)if\s+.*application\/pdf.*:\s*$', s, flags=re.M|re.I)

if not m_pdf:
    print("[ERRO] Não achei o ponto de decisão do parse por content_type (pdf).")
    print('Mostre o trecho com: nl -ba services/api/app/worker_parse.py | sed -n "1,220p"')
    raise SystemExit(1)

indent = m_pdf.group("indent")

json_block = f"""
{indent}# --- JSON support (PNCP API etc.) ---
{indent}if isinstance(content_type, str) and content_type.lower().startswith("application/json"):
{indent}    try:
{indent}        raw = row["body"]
{indent}        if raw is None:
{indent}            raise ValueError("body is None")
{indent}        txt = raw.decode("utf-8", errors="replace")
{indent}        # tenta pretty-print se for JSON válido
{indent}        try:
{indent}            obj = json.loads(txt)
{indent}            txt = json.dumps(obj, ensure_ascii=False, indent=2)
{indent}        except Exception:
{indent}            pass
{indent}        if isinstance(MAX_CHARS, int) and MAX_CHARS > 0:
{indent}            txt = txt[:MAX_CHARS]
{indent}        await pool.execute(
{indent}            "UPDATE document SET texto_extraido=$1, texto_path=NULL, error=NULL WHERE id=$2",
{indent}            txt, doc_id
{indent}        )
{indent}        log.info("PARSE OK: doc_id=%s tender_id=%s chars=%s url=%s (json)", doc_id, tender_id, len(txt), url)
{indent}        continue
{indent}    except Exception as e:
{indent}        log.exception("PARSE JSON falhou: doc_id=%s err=%r", doc_id, e)
{indent}        await pool.execute("UPDATE document SET error=$1 WHERE id=$2", f"parse_json_failed: {{}!r}".format(e), doc_id)
{indent}        continue

"""

# 3) injeta antes do bloco PDF
pos = m_pdf.start()
if "JSON support (PNCP API etc.)" not in s:
    s = s[:pos] + json_block + s[pos:]

# backup + grava
bak = p.with_suffix(p.suffix + f".bak.json.{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(p.read_text(encoding="utf-8"), encoding="utf-8")
p.write_text(s, encoding="utf-8")
print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup: {bak}")
PY

echo
echo "[OK] Agora rebuild + subir parse:"
docker compose -f docker-compose.yml up -d --build parse

echo
echo "[OK] Tail logs parse:"
docker compose -f docker-compose.yml logs -f --tail=200 parse
