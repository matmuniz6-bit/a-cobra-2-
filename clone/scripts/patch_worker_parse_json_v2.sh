#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_parse.py"
test -f "$FILE" || { echo "[ERRO] não achei $FILE"; exit 1; }

python3 - <<'PY'
from pathlib import Path
import re
import datetime as dt

p = Path("services/api/app/worker_parse.py")
s = p.read_text(encoding="utf-8")

# evita duplicar
if "JSON support (PNCP API etc.)" in s:
    print("[OK] Já existe bloco JSON no worker_parse.py (não vou duplicar).")
    raise SystemExit(0)

# 1) garante import json
if re.search(r'^\s*import\s+json\s*$', s, flags=re.M) is None:
    # tenta inserir após o último import do topo
    m = re.search(r'^(?:from\s+\S+\s+import\s+[^\n]+\n|import\s+[^\n]+\n)+', s, flags=re.M)
    if m:
        ins_at = m.end()
        s = s[:ins_at] + "import json\n" + s[ins_at:]
    else:
        s = "import json\n" + s

# 2) achar ponto seguro: linha que atribui content_type (row["content_type"] ou row.get("content_type"))
anchor = None
for pat in [
    r'^(?P<indent>[ \t]*)content_type\s*=\s*row\[\s*[\'"]content_type[\'"]\s*\]\s*$',
    r'^(?P<indent>[ \t]*)content_type\s*=\s*row\.get\(\s*[\'"]content_type[\'"]\s*\)\s*$',
]:
    m = re.search(pat, s, flags=re.M)
    if m:
        anchor = m
        break

if not anchor:
    print("[ERRO] Não achei a linha onde content_type é definido no worker_parse.py.")
    print('Rode: nl -ba services/api/app/worker_parse.py | sed -n "1,240p"')
    raise SystemExit(1)

indent = anchor.group("indent")
insert_pos = anchor.end()

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
{indent}        await pool.execute(
{indent}            "UPDATE document SET error=$1 WHERE id=$2",
{indent}            f"parse_json_failed: {e!r}", doc_id
{indent}        )
{indent}        continue

"""

# injeta logo após a linha content_type = ...
s = s[:insert_pos] + "\n" + json_block + s[insert_pos:]

bak = p.with_suffix(p.suffix + f".bak.json.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(p.read_text(encoding="utf-8"), encoding="utf-8")
p.write_text(s, encoding="utf-8")

print(f"[OK] Patch aplicado em {p}")
print(f"[OK] Backup: {bak}")
PY

echo
echo "[OK] Rebuild + subir parse:"
docker compose -f docker-compose.yml up -d --build parse

echo
echo "[OK] Tail logs parse:"
docker compose -f docker-compose.yml logs -f --tail=200 parse
