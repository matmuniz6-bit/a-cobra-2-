import os
import re
import json
import html
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import requests

from .metrics import incr_counter

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "").strip()

URL_RE = re.compile(r"(https?://\\S+)")

def send_telegram(text: str, url: str | None = None) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Faltou TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID")

    api = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }

    if url:
        payload["reply_markup"] = json.dumps({
            "inline_keyboard": [[{"text": "🔗 Abrir oportunidade", "url": url}]]
        })

    r = requests.post(api, data=payload, timeout=20)
    r.raise_for_status()

def normalize_payload(raw: bytes, content_type: str) -> tuple[str, str | None]:
    # Tenta JSON
    url = None
    text = None
    if raw:
        raw_str = raw.decode("utf-8", "replace").strip()

        if "application/json" in (content_type or "").lower() or raw_str.startswith("{"):
            try:
                data = json.loads(raw_str)
                # Campos comuns (pode variar)
                url = data.get("url") or data.get("watch_url") or data.get("link")
                title = data.get("title") or data.get("watch_title") or data.get("subject") or "Alerta de oportunidade"
                body = data.get("text") or data.get("message") or data.get("body") or ""

                if isinstance(body, (dict, list)):
                    body = json.dumps(body, ensure_ascii=False)

                title = html.escape(str(title))
                body = html.escape(str(body))
                url = str(url).strip() if url else None

                text = f"🚨 <b>{title}</b>\\n{body}".strip()
            except Exception:
                # Cai pra texto cru
                text = html.escape(raw_str)
        else:
            text = html.escape(raw_str)

    if not text:
        text = "🚨 <b>Alerta recebido</b> (sem corpo de mensagem)."

    # Se não veio URL no JSON, tenta pescar do texto
    if not url:
        m = URL_RE.search(html.unescape(text))
        if m:
            url = m.group(1)

    return text, url

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: str):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def do_GET(self):
        incr_counter("notifier.requests_total")
        if self.path.startswith("/health"):
            return self._send(200, "ok")
        return self._send(200, "notifier up")

    def do_POST(self):
        incr_counter("notifier.requests_total")
        parsed = urlparse(self.path)
        if parsed.path != "/hook":
            return self._send(404, "not found")

        qs = parse_qs(parsed.query)
        token = (qs.get("token") or [""])[0]

        if not WEBHOOK_TOKEN:
            return self._send(403, "webhook token not configured")
        if token != WEBHOOK_TOKEN:
            return self._send(403, "forbidden")

        length = int(self.headers.get("content-length") or "0")
        raw = self.rfile.read(length) if length > 0 else b""
        ctype = self.headers.get("content-type", "")

        try:
            msg, url = normalize_payload(raw, ctype)
            send_telegram(msg, url)
            incr_counter("notifier.sent_total")
            return self._send(200, "sent")
        except Exception as e:
            incr_counter("notifier.errors_total")
            return self._send(500, f"error: {e}")

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[notifier] listening on :{port}")
    server.serve_forever()
