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
if "json_pretty_print" in s:
    print("[OK] Já parece patchado (json_pretty_print encontrado).")
    raise SystemExit(0)

# garante import json
if re.search(r'^\s*import\s+json\s*$', s, flags=re.M) is None:
    m = re.search(r'^(?:from\s+\S+\s+import\s+[^\n]+\n|import\s+[^\n]+\n)+', s, flags=re.M)
    if m:
        s = s[:m.end()] + "import json\n" + s[m.end():]
    else:
        s = "import json\n" + s

old = (
'    # texto puro / json / html simples (stub)\n'
'    if "text/" in ctype or "application/json" in ctype or "application/xml" in ctype:\n'
'        try:\n'
'            txt = body.decode("utf-8", errors="ignore")\n'
'        except Exception:\n'
'            txt = body.decode("latin-1", errors="ignore")\n'
)

if old not in s:
    print("[ERRO] Não achei o bloco esperado do _extract_text().")
    print('Mostre o topo com: nl -ba services/api/app/worker_parse.py | sed -n "1,90p"')
    raise SystemExit(1)

new = (
'    # texto puro / json / html simples (stub)  [json_pretty_print]\n'
'    if "text/" in ctype or "application/json" in ctype or "application/xml" in ctype:\n'
'        # JSON: tenta pretty-print (melhor pra downstream)\n'
'        if "application/json" in ctype:\n'
'            try:\n'
'                raw = body.decode("utf-8", errors="ignore")\n'
'                obj = json.loads(raw)\n'
'                txt = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)\n'
'            except Exception:\n'
'                # fallback: texto cru\n'
'                try:\n'
'                    txt = body.decode("utf-8", errors="ignore")\n'
'                except Exception:\n'
'                    txt = body.decode("latin-1", errors="ignore")\n'
'        else:\n'
'            try:\n'
'                txt = body.decode("utf-8", errors="ignore")\n'
'            except Exception:\n'
'                txt = body.decode("latin-1", errors="ignore")\n'
)

bak = p.with_suffix(p.suffix + f".bak.jsonpretty.{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}")
bak.write_text(s, encoding="utf-8")  # backup do estado atual

s2 = s.replace(old, new)
p.write_text(s2, encoding="utf-8")

print("[OK] Patch aplicado:", p)
print("[OK] Backup:", bak)
PY

docker compose -f docker-compose.yml up -d --build parse
