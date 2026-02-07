#!/usr/bin/env bash
set -euo pipefail

# ========= Config =========
PROJECT_DIR="${PROJECT_DIR:-/workspaces/a-cobra}"
OUT_ROOT="${OUT_ROOT:-/tmp}"
BUNDLE_PREFIX="${BUNDLE_PREFIX:-a-cobra-bundle}"
KEEP_BACKUPS="${KEEP_BACKUPS:-1}"

DB_CONTAINER="${DB_CONTAINER:-a-cobra-db-1}"
DB_NAME="${DB_NAME:-acobra}"
DB_USER="${DB_USER:-acobra}"

REDIS_CONTAINER="${REDIS_CONTAINER:-a-cobra-redis-1}"
DUMP_REDIS="${DUMP_REDIS:-0}" # 1 para gerar dump.rdb via redis-cli --rdb

SLEEP_SECS="${SLEEP_SECS:-2}"

# ========= Helpers =========
wait_step() { sleep "$SLEEP_SECS"; }
log() { echo -e "\n== $* =="; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[ERRO] comando ausente: $1"
    exit 1
  }
}

TS="$(date +%Y%m%d-%H%M%S)"
BUNDLE_DIR="$OUT_ROOT/${BUNDLE_PREFIX}-${TS}"
ZIP_PATH="$OUT_ROOT/${BUNDLE_PREFIX}-${TS}.zip"
LOG_PATH="$OUT_ROOT/${BUNDLE_PREFIX}-${TS}.log"

mkdir -p "$BUNDLE_DIR"
exec > >(tee -a "$LOG_PATH") 2>&1

log "0) Preflight"
need_cmd docker
need_cmd git
need_cmd python3

cd "$PROJECT_DIR"
echo "PWD=$(pwd)"
git status --porcelain || true
df -h . || true
wait_step

log "1) Congelar serviços que escrevem (para consistência do dump)"
# mantém db/redis rodando; pausa produtores/consumidores
docker compose stop api worker fetch_docs parse bot >/dev/null 2>&1 || true
docker compose ps || true
wait_step

log "2) Dump do Postgres (custom format + globals)"
mkdir -p "$BUNDLE_DIR/backups"

# dump do banco (formato custom, bom para pg_restore)
echo "[DB] pg_dump -> backups/db_${DB_NAME}_${TS}.dump.gz"
docker exec -i "$DB_CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" -Fc \
  | gzip -9 > "$BUNDLE_DIR/backups/db_${DB_NAME}_${TS}.dump.gz"
wait_step

# globals (roles, etc.) – opcional, mas útil
echo "[DB] pg_dumpall --globals-only -> backups/globals_${TS}.sql.gz"
docker exec -i "$DB_CONTAINER" pg_dumpall -U "$DB_USER" --globals-only \
  | gzip -9 > "$BUNDLE_DIR/backups/globals_${TS}.sql.gz"
wait_step

log "3) (Opcional) Dump do Redis"
if [[ "$DUMP_REDIS" == "1" ]]; then
  need_cmd gzip
  echo "[REDIS] redis-cli --rdb -> backups/redis_${TS}.rdb.gz"
  docker exec -i "$REDIS_CONTAINER" sh -lc "redis-cli --rdb /tmp/dump.rdb >/dev/null && cat /tmp/dump.rdb" \
    | gzip -9 > "$BUNDLE_DIR/backups/redis_${TS}.rdb.gz"
  wait_step
else
  echo "[SKIP] DUMP_REDIS=0"
fi

log "4) Copiar repo (sem lixo pesado) para dentro do bundle"
mkdir -p "$BUNDLE_DIR/repo"
# copia com python para ter exclusões mais controladas
python3 - <<PY
import os, shutil
src = os.environ.get("PROJECT_DIR", "$PROJECT_DIR")
dst = os.path.join("$BUNDLE_DIR", "repo")

EXCLUDE_DIRS = {
  ".git", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
  ".venv", "venv", "node_modules", ".next", "dist", "build",
  ".codespaces", ".devcontainer"
}
EXCLUDE_FILES_SUFFIX = {".pyc", ".pyo", ".log"}

def should_skip(path: str) -> bool:
  base = os.path.basename(path)
  if base in EXCLUDE_DIRS:
    return True
  for suf in EXCLUDE_FILES_SUFFIX:
    if base.endswith(suf):
      return True
  return False

def copytree_filtered(src, dst):
  for root, dirs, files in os.walk(src):
    # filtra dirs in-place
    dirs[:] = [d for d in dirs if not should_skip(os.path.join(root, d))]
    rel = os.path.relpath(root, src)
    out_dir = os.path.join(dst, rel) if rel != "." else dst
    os.makedirs(out_dir, exist_ok=True)
    for f in files:
      p = os.path.join(root, f)
      if should_skip(p):
        continue
      shutil.copy2(p, os.path.join(out_dir, f))

copytree_filtered(src, dst)
print("OK: repo copiado para", dst)
PY
wait_step

log "5) Manifest + SHA256"
( cd "$BUNDLE_DIR" && find . -type f -maxdepth 3 | sort ) > "$BUNDLE_DIR/MANIFEST.txt"
if command -v sha256sum >/dev/null 2>&1; then
  ( cd "$BUNDLE_DIR" && sha256sum $(find . -type f -maxdepth 3 | sort) ) > "$BUNDLE_DIR/SHA256SUMS.txt" || true
fi
wait_step

log "6) Zipar bundle"
if ! command -v zip >/dev/null 2>&1; then
  echo "[INFO] zip não encontrado. Tentando instalar..."
  if command -v sudo >/dev/null 2>&1; then
    sudo apt-get update -y && sudo apt-get install -y zip
  else
    echo "[ERRO] sem zip e sem sudo para instalar. Instale zip ou use outra forma de compactar."
    exit 1
  fi
fi

rm -f "$ZIP_PATH"
( cd "$BUNDLE_DIR" && zip -r "$ZIP_PATH" . >/dev/null )
ls -lah "$ZIP_PATH"
wait_step

log "7) Limpeza de backups antigos (opcional, mantém só os mais recentes)"
# Só limpa bundles antigos no /tmp para não apagar coisas do repo sem querer
set +e
ls -1t "$OUT_ROOT"/${BUNDLE_PREFIX}-*.zip 2>/dev/null | tail -n +$((KEEP_BACKUPS+1)) | xargs -r rm -f
set -e
wait_step

log "8) Restaurar serviços"
docker compose start db redis >/dev/null 2>&1 || true
docker compose start api worker fetch_docs parse bot >/dev/null 2>&1 || true
docker compose ps || true

echo
echo "[OK] Bundle criado:"
echo "ZIP=$ZIP_PATH"
echo "LOG=$LOG_PATH"
