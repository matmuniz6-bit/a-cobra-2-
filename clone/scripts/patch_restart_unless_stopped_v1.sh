#!/usr/bin/env bash
set -euo pipefail

FILE="${1:-docker-compose.yml}"

test -f "$FILE" || { echo "[ERRO] não achei: $FILE"; exit 1; }

TS="$(date +%Y%m%d-%H%M%S)"
BAK="$FILE.bak.$TS"
cp -a "$FILE" "$BAK"

python3 - <<PY
import re
from pathlib import Path

path = Path("$FILE")
lines = path.read_text(encoding="utf-8").splitlines(True)

# acha "services:"
svc_idx = None
for i, ln in enumerate(lines):
    if re.match(r'^\s*services:\s*$', ln):
        svc_idx = i
        break

if svc_idx is None:
    raise SystemExit("[ERRO] não achei bloco 'services:' no compose")

out = []
i = 0

# helper: detecta service key com 2 espaços (padrão compose)
svc_key_re = re.compile(r'^  ([A-Za-z0-9_.-]+):\s*$')
restart_re = re.compile(r'^\s{4}restart:\s*', re.M)

while i < len(lines):
    out.append(lines[i])

    # entrou em services, começa a monitorar services keys
    if i == svc_idx:
        i += 1
        # percorre o bloco de services até sair dele
        while i < len(lines):
            ln = lines[i]

            # saiu do bloco services quando volta pra indent 0 (ou EOF)
            if re.match(r'^[^\s#]', ln) and not re.match(r'^\s*$', ln):
                # é um novo top-level key (ex: networks:, volumes:)
                break

            m = svc_key_re.match(ln)
            if m:
                # capturar bloco do service
                svc_start = i
                svc_name = m.group(1)

                # acha fim do bloco (próximo service key ou saída de services)
                j = i + 1
                while j < len(lines):
                    l2 = lines[j]
                    if re.match(r'^[^\s#]', l2) and not re.match(r'^\s*$', l2):
                        break
                    if svc_key_re.match(l2):
                        break
                    j += 1

                block = ''.join(lines[svc_start+1:j])

                out.append(ln)  # a própria linha "  service:"
                # se já tem restart no bloco, não mexe
                if re.search(r'^\s{4}restart:\s*', block, flags=re.M):
                    pass
                else:
                    out.append("    restart: unless-stopped\n")

                # cola o resto do bloco (linhas após o header do service)
                out.extend(lines[svc_start+1:j])

                i = j
                continue

            # linhas dentro de services mas fora de service (comentários, vazias, etc)
            out.append(ln)
            i += 1

        # terminou services: volta pro fluxo normal sem perder a linha atual (que já é top-level)
        continue

    i += 1

new_text = ''.join(out)

# sanity: evitar duplicar o arquivo (caso de bug)
if len(new_text) < 20:
    raise SystemExit("[ERRO] patch gerou conteúdo muito curto (abortando)")

path.write_text(new_text, encoding="utf-8")
print(f"[OK] patch aplicado em {path} | backup: {Path('$BAK').name}")
PY

echo
echo "== PROVA (linhas com restart:) =="
grep -nE '^\s{4}restart:\s' "$FILE" || true

echo
echo "[OK] pronto. Agora rode: docker compose up -d"
