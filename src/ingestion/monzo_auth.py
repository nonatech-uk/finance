"""Persistent Monzo OAuth auth server for headless/container operation.

Runs on 0.0.0.0:9876 (behind https://finance.mees.st reverse proxy).
Provides browser-based auth flow — no webbrowser.open() or input() needed.

Routes:
    GET /              — status page with token state + "Start Auth" button
    GET /auth/monzo    — redirects to Monzo OAuth URL
    GET /oauth/callback — receives Monzo redirect, exchanges code for tokens
    GET /auth/status   — JSON status for polling from the approval page
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
from src.ingestion.monzo import TOKEN_FILE, _load_tokens, _save_tokens

# Module-level state for the current auth flow
_pending_state: str | None = None


def _token_status() -> dict:
    """Check current token state."""
    tokens = _load_tokens()
    if not tokens or not tokens.get("access_token"):
        return {"authenticated": False, "reason": "no_tokens"}

    try:
        resp = requests.get(
            f"{settings.monzo_api_base}/ping/whoami",
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
            timeout=10,
        )
        if resp.ok:
            info = resp.json()
            if info.get("authenticated"):
                return {
                    "authenticated": True,
                    "user_id": info.get("user_id"),
                    "authenticated_at": tokens.get("authenticated_at"),
                }
        return {"authenticated": False, "reason": "token_invalid"}
    except Exception as e:
        return {"authenticated": False, "reason": f"error: {e}"}


class AuthHandler(BaseHTTPRequestHandler):
    """HTTP handler for Monzo OAuth auth server."""

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/":
            self._handle_index()
        elif path == "/auth/monzo":
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
            status_html = f'<p class="ok">Authenticated (user: {status.get("user_id", "?")})</p><p>Since: {when}</p>'
        else:
            reason = status.get("reason", "unknown")
            status_html = f'<p class="err">Not authenticated ({reason})</p>'

        html = f"""<!DOCTYPE html>
<html><head><title>Finance Sync — Monzo Auth</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body {{ font-family: system-ui, sans-serif; max-width: 480px; margin: 40px auto; padding: 0 20px; background: #0a0a0f; color: #e0e0e0; }}
h1 {{ color: #7c9aff; }}
.ok {{ color: #4ade80; font-weight: bold; }}
.err {{ color: #f87171; font-weight: bold; }}
a.btn {{ display: inline-block; padding: 12px 24px; background: #7c9aff; color: #0a0a0f;
         text-decoration: none; border-radius: 6px; font-weight: bold; margin-top: 16px; }}
a.btn:hover {{ background: #5b7de8; }}
</style></head>
<body>
<h1>Finance Sync</h1>
<h2>Monzo Authentication</h2>
{status_html}
<a class="btn" href="/auth/monzo">Start Authentication</a>
</body></html>"""

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(html.encode())

    def _handle_start_auth(self):
        """Redirect to Monzo OAuth."""
        global _pending_state
        _pending_state = secrets.token_urlsafe(16)

        auth_url = settings.monzo_auth_url + "?" + urlencode({
            "client_id": settings.monzo_client_id,
            "redirect_uri": settings.monzo_redirect_uri,
            "response_type": "code",
            "state": _pending_state,
        })

        self.send_response(302)
        self.send_header("Location", auth_url)
        self.end_headers()

    def _handle_callback(self):
        """Receive Monzo OAuth callback, exchange code, show approval page."""
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

        # Exchange code for tokens
        try:
            resp = requests.post(settings.monzo_token_url, data={
                "grant_type": "authorization_code",
                "client_id": settings.monzo_client_id,
                "client_secret": settings.monzo_client_secret,
                "redirect_uri": settings.monzo_redirect_uri,
                "code": code,
            }, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            data["authenticated_at"] = time.time()
            _save_tokens(data)
        except Exception as e:
            self._send_html(500, f"<h2>Error</h2><p>Token exchange failed: {e}</p>")
            return

        # Show "approve in app" page with auto-polling
        html = """<!DOCTYPE html>
<html><head><title>Approve in Monzo App</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body { font-family: system-ui, sans-serif; max-width: 480px; margin: 40px auto; padding: 0 20px; background: #0a0a0f; color: #e0e0e0; }
h1 { color: #7c9aff; }
.waiting { color: #fbbf24; font-weight: bold; }
.ok { color: #4ade80; font-weight: bold; }
#status { font-size: 1.2em; }
</style></head>
<body>
<h1>Finance Sync</h1>
<h2>Almost done!</h2>
<p>Open the <strong>Monzo app</strong> on your phone and approve the login request.</p>
<p id="status" class="waiting">Waiting for approval...</p>
<script>
async function poll() {
    try {
        const r = await fetch('/auth/status');
        const d = await r.json();
        if (d.authenticated) {
            document.getElementById('status').className = 'ok';
            document.getElementById('status').textContent = 'Authenticated! You can close this tab.';
            return;
        }
    } catch(e) {}
    setTimeout(poll, 3000);
}
setTimeout(poll, 5000);
</script>
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
        print(f"[auth] {self.address_string()} - {format % args}")


def main():
    port = 9876
    server = HTTPServer(("0.0.0.0", port), AuthHandler)
    print(f"Monzo auth server listening on 0.0.0.0:{port}")
    print(f"Token file: {TOKEN_FILE.resolve()}")

    def shutdown(sig, frame):
        print("\nShutting down auth server...")
        server.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    server.serve_forever()


if __name__ == "__main__":
    main()
