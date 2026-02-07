import json
import urllib.request
import urllib.error
import os

API_KEY = (os.getenv("API_KEY", "") or "").strip()


def _request(method: str, url: str, payload: dict | None = None, timeout: int = 10):
    data = None
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["x-api-key"] = API_KEY
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return raw.decode("utf-8", "ignore")


def post(url: str, payload: dict, timeout: int = 10):
    return _request("POST", url, payload=payload, timeout=timeout)


def get(url: str, timeout: int = 10):
    return _request("GET", url, payload=None, timeout=timeout)
