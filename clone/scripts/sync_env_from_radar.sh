#!/usr/bin/env bash
set -o pipefail 2>/dev/null || true

ROOT_ENV=".env"
RADAR_ENV="services/radar/.env"

ts(){ date +%Y%m%d-%H%M%S 2>/dev/null || echo now; }

if [[ ! -f "$ROOT_ENV" ]]; then
  echo "[ERRO] Não achei $ROOT_ENV na raiz."
  exit 0
fi
if [[ ! -f "$RADAR_ENV" ]]; then
  echo "[ERRO] Não achei $RADAR_ENV."
  exit 0
fi

cp -a "$ROOT_ENV" "${ROOT_ENV}.bak.$(ts)" 2>/dev/null || true

python3 - <<'PY'
from pathlib import Path
import re

root = Path(".env")
radar = Path("services/radar/.env")

root_txt = root.read_text(encoding="utf-8", errors="replace")
radar_txt = radar.read_text(encoding="utf-8", errors="replace")

def has(txt, k):
    return re.search(rf"^{re.escape(k)}=.*$", txt, flags=re.M) is not None

def get(txt, k):
    m = re.search(rf"^{re.escape(k)}=(.*)$", txt, flags=re.M)
    return m.group(1).strip() if m else None

keys = ["TELEGRAM_CHAT_ID", "WEBHOOK_TOKEN"]
added = []

for k in keys:
    if has(root_txt, k):
        continue
    v = get(radar_txt, k)
    if v is None or v == "":
        continue
    if root_txt and not root_txt.endswith("\n"):
        root_txt += "\n"
    root_txt += f"{k}={v}\n"
    added.append(k)

root.write_text(root_txt, encoding="utf-8")

print("[OK] Sync concluído.")
print("[OK] Adicionados:", ", ".join(added) if added else "(nenhum — já existia ou não estava no radar)")
PY
