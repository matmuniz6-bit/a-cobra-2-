#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

F="services/api/app/worker_triage.py"
test -f "$F" || { echo "[ERRO] não achei $F"; exit 1; }

ts(){ date +%Y%m%d-%H%M%S 2>/dev/null || echo now; }
cp -a "$F" "$F.bak.$(ts)"

python3 - <<'PY'
from pathlib import Path
import re

p = Path("services/api/app/worker_triage.py")
txt = p.read_text(encoding="utf-8", errors="replace")

# 1) garante config FETCH_QUEUE / THRESHOLD
if 'FETCH_QUEUE =' not in txt:
    txt = re.sub(
        r'(QUEUE\s*=\s*os\.getenv\("TRIAGE_QUEUE",[^\n]*\)\n)',
        r'\1FETCH_QUEUE = os.getenv("FETCH_QUEUE", "q:fetch_docs")\nTRIAGE_THRESHOLD = int(os.getenv("TRIAGE_THRESHOLD", "4"))\n',
        txt,
        count=1
    )

# 2) garante json default (datetime/date)
if "def _json_default" not in txt:
    # tenta inserir perto do _iso
    if "def _iso(" in txt:
        txt = txt.replace(
            "def _iso(x) -> str:\n",
            "def _json_default(o):\n"
            "    if isinstance(o, (dt.datetime, dt.date)):\n"
            "        return o.isoformat()\n"
            "    return str(o)\n\n"
            "def _iso(x) -> str:\n"
        )
    else:
        # fallback: coloca depois do logger
        txt = re.sub(
            r'(log\s*=\s*logging\.getLogger\("worker_triage"\)\n)',
            r'\1\ndef _json_default(o):\n    if isinstance(o, (dt.datetime, dt.date)):\n        return o.isoformat()\n    return str(o)\n\n',
            txt,
            count=1
        )

# 3) garante que o DB fetch traz urls (pra fetch_docs)
def add_urls_to_select(block: str) -> str:
    # adiciona ", urls" se não tiver
    return block.replace("status ", "status, urls ").replace("status\n", "status, urls\n")

txt2 = txt
# duas queries: por id e por id_pncp
txt2 = re.sub(
    r'(SELECT id, id_pncp, orgao, municipio, uf, modalidade, objeto, data_publicacao, status\s+FROM tender WHERE id=\$1)',
    lambda m: add_urls_to_select(m.group(1)),
    txt2
)
txt2 = re.sub(
    r'(SELECT id, id_pncp, orgao, municipio, uf, modalidade, objeto, data_publicacao, status\s+FROM tender WHERE id_pncp=\$1)',
    lambda m: add_urls_to_select(m.group(1)),
    txt2
)
txt = txt2

# 4) injeta enqueue pro fetch_docs (idempotente)
if "Enfileirado" not in txt and "FETCH_QUEUE" in txt:
    needle = "await asyncio.to_thread(_send_telegram, msg)"
    if needle in txt:
        inject = (
            "\n            # se passou no limiar, manda pro fetch_docs\n"
            "            try:\n"
            "                score = int(info.get('score_inicial') or info.get('score') or 0)\n"
            "            except Exception:\n"
            "                score = 0\n"
            "\n"
            "            if score >= TRIAGE_THRESHOLD:\n"
            "                fetch_payload = {\n"
            "                    'tender_id': info.get('id') or tender_id,\n"
            "                    'id_pncp': id_pncp,\n"
            "                    'urls': info.get('urls') or t.get('urls') or {},\n"
            "                }\n"
            "                raw_fetch = json.dumps(fetch_payload, ensure_ascii=False, default=_json_default)\n"
            "                if mode == 'async':\n"
            "                    await r.lpush(FETCH_QUEUE, raw_fetch)\n"
            "                else:\n"
            "                    await asyncio.to_thread(r.lpush, FETCH_QUEUE, raw_fetch)\n"
            "                log.info('Enfileirado %s: id=%s id_pncp=%s score=%s', FETCH_QUEUE, fetch_payload.get('tender_id'), id_pncp, score)\n"
            "\n"
        )
        txt = txt.replace(needle, inject + "            " + needle)

p.write_text(txt, encoding="utf-8")
print("PATCH_TRIAGE_ENQUEUE_FETCH_DOCS_OK")
PY

echo "[OK] Patch aplicado em $F (backup criado)."
