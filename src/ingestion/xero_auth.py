"""Persistent Xero OAuth auth server for headless/container operation.

Runs on 0.0.0.0:9877 (alongside Monzo auth on 9876).
Provides browser-based auth flow — no webbrowser.open() or input() needed.

Routes:
    GET /              — status page with token state + "Start Auth" button
    GET /auth/xero     — redirects to Xero OAuth URL
    GET /oauth/callback — receives Xero redirect, exchanges code for tokens
    GET /auth/status   — JSON status for polling
"""

import json
import secrets
import signal
import sys
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import settings
from src.ingestion.xero import (
    TOKEN_FILE, XERO_AUTH_URL, XERO_TOKEN_URL, XERO_SCOPES,
    _load_tokens, _save_tokens, get_connections,
)

_pending_state: str | None = None


def _token_status() -> dict:
    """Check current Xero token state."""
    tokens = _load_tokens()
    if not tokens or not tokens.get("access_token"):
        return {"authenticated": False, "reason": "no_tokens"}

    try:
        resp = requests.get(
            "https://api.xero.com/connections",
            headers={"Authorization": f"Bearer {tokens['access_token']}", "Content-Type": "application/json"},
            timeout=10,
        )
        if resp.ok:
            conns = resp.json()
            if conns:
                return {
                    "authenticated": True,
                    "tenant_name": conns[0].get("tenantName"),
                    "tenant_id": conns[0].get("tenantId"),
                    "authenticated_at": tokens.get("authenticated_at"),
                }
        return {"authenticated": False, "reason": "token_invalid"}
    except Exception as e:
        return {"authenticated": False, "reason": f"error: {e}"}


class AuthHandler(BaseHTTPRequestHandler):
    """HTTP handler for Xero OAuth auth server."""

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/":
            self._handle_index()
        elif path == "/auth/xero":
            self._handle_start_auth()
        elif path == "/oauth/callback":
            self._handle_callback()
        elif path == "/auth/status":
            self._handle_status()
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")

    def _handle_index(self):
        """Status page with auth button."""
        status = _token_status()
        auth_ok = status.get("authenticated", False)

        if auth_ok:
            ts = status.get("authenticated_at")
            when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else "unknown"
            tenant = status.get("tenant_name", "?")
            status_html = f'<p class="ok">Authenticated (org: {tenant})</p><p>Since: {when}</p>'
        else:
            reason = status.get("reason", "unknown")
            status_html = f'<p class="err">Not authenticated ({reason})</p>'

        html = f"""<!DOCTYPE html>
<html><head><title>Finance Sync — Xero Auth</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body {{ font-family: system-ui, sans-serif; max-width: 480px; margin: 40px auto; padding: 0 20px; background: #0a0a0f; color: #e0e0e0; }}
h1 {{ color: #13b5ea; }}
.ok {{ color: #4ade80; font-weight: bold; }}
.err {{ color: #f87171; font-weight: bold; }}
a.btn {{ display: inline-block; padding: 12px 24px; background: #13b5ea; color: #0a0a0f;
         text-decoration: none; border-radius: 6px; font-weight: bold; margin-top: 16px; }}
a.btn:hover {{ background: #0e9bc7; }}
</style></head>
<body>
<h1>Finance Sync</h1>
<h2>Xero Authentication</h2>
{status_html}
<a class="btn" href="/auth/xero">Start Authentication</a>
</body></html>"""

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(html.encode())

    def _handle_start_auth(self):
        """Redirect to Xero OAuth."""
        global _pending_state
        _pending_state = secrets.token_urlsafe(16)

        auth_url = XERO_AUTH_URL + "?" + urlencode({
            "response_type": "code",
            "client_id": settings.xero_client_id,
            "redirect_uri": settings.xero_redirect_uri,
            "scope": XERO_SCOPES,
            "state": _pending_state,
        })

        self.send_response(302)
        self.send_header("Location", auth_url)
        self.end_headers()

    def _handle_callback(self):
        """Receive Xero OAuth callback, exchange code for tokens."""
        global _pending_state
        qs = parse_qs(urlparse(self.path).query)
        code = qs.get("code", [None])[0]
        state = qs.get("state", [None])[0]

        if not code:
            self._send_html(400, "<h2>Error</h2><p>No authorisation code received.</p>")
            return

        if state != _pending_state:
            self._send_html(400, "<h2>Error</h2><p>OAuth state mismatch.</p>")
            return

        _pending_state = None

        try:
            resp = requests.post(XERO_TOKEN_URL, data={
                "grant_type": "authorization_code",
                "client_id": settings.xero_client_id,
                "client_secret": settings.xero_client_secret,
                "redirect_uri": settings.xero_redirect_uri,
                "code": code,
            }, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            data["authenticated_at"] = time.time()

            # Fetch tenant ID
            connections = get_connections(data["access_token"])
            if connections:
                data["tenant_id"] = connections[0]["tenantId"]
                tenant_name = connections[0].get("tenantName", "Unknown")
            else:
                tenant_name = "No organisation connected"

            _save_tokens(data)
        except Exception as e:
            self._send_html(500, f"<h2>Error</h2><p>Token exchange failed: {e}</p>")
            return

        html = f"""<!DOCTYPE html>
<html><head><title>Xero Connected</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body {{ font-family: system-ui, sans-serif; max-width: 480px; margin: 40px auto; padding: 0 20px; background: #0a0a0f; color: #e0e0e0; }}
h1 {{ color: #13b5ea; }}
.ok {{ color: #4ade80; font-weight: bold; font-size: 1.2em; }}
</style></head>
<body>
<h1>Finance Sync</h1>
<p class="ok">Connected to Xero: {tenant_name}</p>
<p>You can close this tab.</p>
</body></html>"""

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(html.encode())

    def _handle_status(self):
        """JSON endpoint for polling auth status."""
        status = _token_status()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(status).encode())

    def _send_html(self, code: int, body: str):
        html = f"""<!DOCTYPE html>
<html><head><title>Finance Sync</title>
<style>body {{ font-family: system-ui, sans-serif; max-width: 480px; margin: 40px auto; padding: 0 20px; background: #0a0a0f; color: #e0e0e0; }}</style>
</head><body>{body}<p><a href="/">Back to status</a></p></body></html>"""
        self.send_response(code)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(html.encode())

    def log_message(self, format, *args):
        print(f"[xero-auth] {self.address_string()} - {format % args}")


def main():
    port = 9877
    server = HTTPServer(("0.0.0.0", port), AuthHandler)
    print(f"Xero auth server listening on 0.0.0.0:{port}")
    print(f"Token file: {TOKEN_FILE.resolve()}")

    def shutdown(sig, frame):
        print("\nShutting down Xero auth server...")
        server.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    server.serve_forever()


if __name__ == "__main__":
    main()
