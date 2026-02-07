#!/usr/bin/env bash
set -euo pipefail

FILE="services/api/app/worker_fetch_docs.py"

python3 - <<'PY'
from pathlib import Path
import re, datetime as dt

p = Path("services/api/app/worker_fetch_docs.py")
s = p.read_text(encoding="utf-8")

# backup
ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
bak = p.with_suffix(p.suffix + f".bak.{ts}")
bak.write_text(s, encoding="utf-8")

# 1) garantir DEAD_QUEUE logo após PARSE_QUEUE (independente de espaços)
if "DEAD_QUEUE" not in s:
    m = re.search(r'^(?P<l1>[ \t]*QUEUE[ \t]*=.*\n)(?P<l2>[ \t]*PARSE_QUEUE[ \t]*=.*\n)', s, flags=re.M)
    if not m:
        raise SystemExit("[ERRO] não achei as linhas QUEUE/PARSE_QUEUE no topo para inserir DEAD_QUEUE.")
    insert = m.group(0) + 'DEAD_QUEUE  = os.getenv("DEAD_QUEUE", "q:dead_fetch_docs")\n'
    s = s.replace(m.group(0), insert)

# 2) substituir o bloco do "sem tender_id ou sem url" por versão robusta (regex)
pattern = re.compile(
    r'(?P<indent>^[ \t]*)if[ \t]+not[ \t]+tender_id[ \t]+or[ \t]+not[ \t]+url[ \t]*:\s*\n'
    r'(?P=indent)[ \t]*log\.warning\("Payload sem tender_id ou sem url\. payload=%s",[ \t]*payload\)\s*\n'
    r'(?P=indent)[ \t]*continue\s*\n',
    flags=re.M
)

m = pattern.search(s)
if not m:
    # mostra um pedaço útil pra bater o olho sem pedir nada do usuário
    lines = s.splitlines()
    # tenta achar a região do "Payload sem"
    idx = next((i for i,l in enumerate(lines) if "Payload sem tender_id" in l), None)
    if idx is None:
        idx = 120
    a = max(0, idx-15); b = min(len(lines), idx+20)
    snippet = "\n".join(f"{i+1:4d}  {lines[i]}" for i in range(a,b))
    print("[ERRO] não achei o bloco if-not-tender/url para patch.\n--- trecho aproximado ---\n" + snippet)
    raise SystemExit(1)

indent = m.group("indent")

new_block = f"""{indent}# --- FK guard / resolução de tender (blindado) ---
{indent}inner = payload.get("payload")
{indent}if not isinstance(inner, dict):
{indent}    inner = {{}}

{indent}tender_id_resolved = None
{indent}try:
{indent}    # 1) tender_id numérico existe?
{indent}    if tender_id is not None and str(tender_id).isdigit():
{indent}        tid = int(tender_id)
{indent}        row = await pool.fetchrow("SELECT 1 FROM tender WHERE id=$1", tid)
{indent}        if row:
{indent}            tender_id_resolved = tid

{indent}    # 2) se não, tenta resolver pelo id_pncp
{indent}    idp = id_pncp
{indent}    if not (isinstance(idp, str) and idp.strip()):
{indent}        idp = inner.get("id_pncp")
{indent}    if tender_id_resolved is None and isinstance(idp, str) and idp.strip():
{indent}        row = await pool.fetchrow("SELECT id FROM tender WHERE id_pncp=$1", idp.strip())
{indent}        if row:
{indent}            tender_id_resolved = int(row["id"])

{indent}    # 3) se ainda não existe, tenta upsert do tender usando metadata do payload
{indent}    if tender_id_resolved is None and isinstance(idp, str) and idp.strip():
{indent}        dp = inner.get("data_publicacao")
{indent}        dp_dt = None
{indent}        if isinstance(dp, str) and dp.strip():
{indent}            x = dp.strip()
{indent}            if x.endswith("Z"):
{indent}                x = x[:-1] + "+00:00"
{indent}            try:
{indent}                dp_dt = __import__("datetime").datetime.fromisoformat(x)
{indent}            except Exception:
{indent}                dp_dt = None

{indent}        urls2 = inner.get("urls")
{indent}        if not isinstance(urls2, dict):
{indent}            urls2 = payload.get("urls") if isinstance(payload.get("urls"), dict) else None

{indent}        row = await pool.fetchrow(
{indent}            \"\"\"INSERT INTO tender (id_pncp, orgao, municipio, uf, modalidade, objeto, data_publicacao, status, urls, hash_metadados)
{indent}               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
{indent}               ON CONFLICT (id_pncp) DO UPDATE SET
{indent}                 orgao=COALESCE(EXCLUDED.orgao, tender.orgao),
{indent}                 municipio=COALESCE(EXCLUDED.municipio, tender.municipio),
{indent}                 uf=COALESCE(EXCLUDED.uf, tender.uf),
{indent}                 modalidade=COALESCE(EXCLUDED.modalidade, tender.modalidade),
{indent}                 objeto=COALESCE(EXCLUDED.objeto, tender.objeto),
{indent}                 data_publicacao=COALESCE(EXCLUDED.data_publicacao, tender.data_publicacao),
{indent}                 status=COALESCE(EXCLUDED.status, tender.status),
{indent}                 urls=COALESCE(EXCLUDED.urls, tender.urls),
{indent}                 hash_metadados=COALESCE(EXCLUDED.hash_metadados, tender.hash_metadados),
{indent}                 updated_at=now()
{indent}               RETURNING id\"\"\",
{indent}            idp.strip(),
{indent}            inner.get("orgao"),
{indent}            inner.get("municipio"),
{indent}            inner.get("uf"),
{indent}            inner.get("modalidade"),
{indent}            inner.get("objeto"),
{indent}            dp_dt,
{indent}            inner.get("status"),
{indent}            urls2,
{indent}            inner.get("hash_metadados"),
{indent}        )
{indent}        if row:
{indent}            tender_id_resolved = int(row["id"])
{indent}except Exception as e:
{indent}    log.exception("Falha resolvendo/garantindo tender no DB: %r", e)

{indent}if not tender_id_resolved or not url:
{indent}    # dead-letter: não perde o payload e evita FK
{indent}    try:
{indent}        msg_dead = {{
{indent}            "reason": "missing_tender_or_url",
{indent}            "tender_id": tender_id,
{indent}            "tender_id_resolved": tender_id_resolved,
{indent}            "id_pncp": id_pncp,
{indent}            "url": url,
{indent}            "payload": payload,
{indent}        }}
{indent}        await r.lpush(DEAD_QUEUE, json.dumps(msg_dead, ensure_ascii=False, default=_json_default))
{indent}    except Exception:
{indent}        pass
{indent}    log.warning("Ignorando payload sem tender válido ou sem url. tender_id=%s resolved=%s id_pncp=%s url=%s",
{indent}                tender_id, tender_id_resolved, id_pncp, url)
{indent}    continue

{indent}# daqui pra frente, tender_id é garantido no DB
{indent}tender_id = tender_id_resolved
"""

s = s[:m.start()] + new_block + s[m.end():]

# sanity check
if "tender_id_resolved" not in s or "DEAD_QUEUE" not in s:
    raise SystemExit("[ERRO] patch não parece ter sido aplicado corretamente (tokens não encontrados).")

p.write_text(s, encoding="utf-8")
print("[OK] patch aplicado em", p)
print("[OK] backup criado em", bak)
PY
