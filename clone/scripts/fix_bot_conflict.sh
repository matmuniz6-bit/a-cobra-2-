#!/usr/bin/env bash
set -euo pipefail
DC=(docker compose -f docker-compose.yml)

echo "1) Parando/removendo bot local (se existir)..."
"${DC[@]}" stop bot || true
"${DC[@]}" rm -f bot || true

echo
echo "2) WebhookInfo + deleteWebhook(drop_pending_updates=true)..."
"${DC[@]}" run --rm --no-deps bot sh -lc '
set -e
echo "== getWebhookInfo ==";
curl -sS "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/getWebhookInfo" | python -m json.tool;
echo;
echo "== deleteWebhook ==";
curl -sS "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/deleteWebhook?drop_pending_updates=true" | python -m json.tool;
'

echo
echo "3) Subindo bot (uma instância só)..."
"${DC[@]}" up -d bot
"${DC[@]}" ps bot

echo
echo "4) Logs do bot:"
"${DC[@]}" logs -f --tail=200 bot
