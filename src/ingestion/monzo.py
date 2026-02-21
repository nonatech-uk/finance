"""Monzo API client: OAuth flow and transaction fetching."""

import json
import os
import secrets
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlencode, urlparse, parse_qs

import requests

from config.settings import settings

TOKEN_FILE = Path(os.environ.get("MONZO_TOKEN_FILE", "tokens.json"))
SCA_WINDOW_SECONDS = 270  # 4.5 minutes — leave 30s safety margin


class AuthRequiredError(Exception):
    """Raised when Monzo authentication is needed but headless mode prevents interactive flow."""
    pass


class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Captures the OAuth callback code from Monzo's redirect."""

    code: Optional[str] = None
    state: Optional[str] = None

    def do_GET(self):
        qs = parse_qs(urlparse(self.path).query)
        _OAuthCallbackHandler.code = qs.get("code", [None])[0]
        _OAuthCallbackHandler.state = qs.get("state", [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<h2>Authorised - you can close this tab.</h2>")

    def log_message(self, format, *args):
        pass  # suppress request logs


def _load_tokens() -> Optional[dict]:
    if TOKEN_FILE.exists():
        return json.loads(TOKEN_FILE.read_text())
    return None


def _save_tokens(data: dict):
    TOKEN_FILE.write_text(json.dumps(data, indent=2))


def authenticate(headless: bool = False) -> str:
    """Run OAuth flow or refresh existing token. Returns access_token.

    Args:
        headless: If True, only attempt token refresh. Raises AuthRequiredError
                  if a full interactive OAuth flow would be needed.
    """
    tokens = _load_tokens()

    # Try refresh first
    if tokens and tokens.get("refresh_token"):
        print("Attempting token refresh...")
        resp = requests.post(settings.monzo_token_url, data={
            "grant_type": "refresh_token",
            "client_id": settings.monzo_client_id,
            "client_secret": settings.monzo_client_secret,
            "refresh_token": tokens["refresh_token"],
        }, timeout=30)
        if resp.ok:
            data = resp.json()
            data["authenticated_at"] = time.time()
            _save_tokens(data)
            print("Token refreshed.")
            return data["access_token"]
        print(f"Refresh failed ({resp.status_code}).")

    if headless:
        raise AuthRequiredError(
            "Monzo token refresh failed and interactive auth is not available. "
            "Re-authenticate via the web UI."
        )

    # Full OAuth flow (interactive only)
    print("Starting fresh OAuth flow.")
    state = secrets.token_urlsafe(16)
    auth_url = settings.monzo_auth_url + "?" + urlencode({
        "client_id": settings.monzo_client_id,
        "redirect_uri": settings.monzo_redirect_uri,
        "response_type": "code",
        "state": state,
    })

    parsed = urlparse(settings.monzo_redirect_uri)
    host = parsed.hostname or "0.0.0.0"
    port = parsed.port or 9876

    server = HTTPServer((host, port), _OAuthCallbackHandler)

    print(f"\nOpening Monzo auth in browser...\n  {auth_url}\n")
    webbrowser.open(auth_url)
    print("Waiting for callback...")
    server.handle_request()
    server.server_close()

    if _OAuthCallbackHandler.state != state:
        raise RuntimeError("OAuth state mismatch — possible CSRF.")
    if not _OAuthCallbackHandler.code:
        raise RuntimeError("No authorisation code received.")

    # Exchange code for token
    resp = requests.post(settings.monzo_token_url, data={
        "grant_type": "authorization_code",
        "client_id": settings.monzo_client_id,
        "client_secret": settings.monzo_client_secret,
        "redirect_uri": settings.monzo_redirect_uri,
        "code": _OAuthCallbackHandler.code,
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    data["authenticated_at"] = time.time()
    _save_tokens(data)

    print("\nToken received. Approve access in the Monzo app now.")
    input("Press Enter once you've approved in the app...")

    # Verify token works
    whoami = requests.get(
        f"{settings.monzo_api_base}/ping/whoami",
        headers={"Authorization": f"Bearer {data['access_token']}"},
        timeout=30,
    )
    whoami.raise_for_status()
    info = whoami.json()
    if not info.get("authenticated"):
        raise RuntimeError("Token not authenticated — did you approve in the app?")

    print(f"Authenticated as user {info.get('user_id')}")
    return data["access_token"]


def list_accounts(access_token: str, account_type: Optional[str] = None) -> List[dict]:
    """Fetch Monzo accounts."""
    params = {}
    if account_type:
        params["account_type"] = account_type
    resp = requests.get(
        f"{settings.monzo_api_base}/accounts",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params,
        timeout=30,
    )
    if resp.status_code == 401:
        raise AuthRequiredError("Monzo access token expired (401). Re-authenticate.")
    resp.raise_for_status()
    return resp.json()["accounts"]


def fetch_transactions(
    access_token: str,
    account_id: str,
    since: Optional[datetime] = None,
    auth_time: Optional[float] = None,
) -> List[dict]:
    """
    Fetch all transactions for an account using monthly windows.

    Walks forward from `since` (or 2015-01-01) to now in monthly chunks.
    Handles pagination within each chunk via cursor.
    Respects rate limits with backoff.
    """
    headers = {"Authorization": f"Bearer {access_token}"}

    if since is None:
        since = datetime(2015, 1, 1, tzinfo=timezone.utc)
    if auth_time is None:
        tokens = _load_tokens()
        auth_time = tokens.get("authenticated_at", time.time()) if tokens else time.time()

    now = datetime.now(timezone.utc)
    all_txns = []
    window_start = since

    while window_start < now:
        # Check SCA window
        elapsed = time.time() - auth_time
        if elapsed > SCA_WINDOW_SECONDS:
            cutoff = datetime.now(timezone.utc) - timedelta(days=90)
            if window_start < cutoff:
                print(f"\n  SCA window expired ({elapsed:.0f}s). "
                      f"Skipping to last 90 days.")
                window_start = cutoff

        window_end = min(window_start + timedelta(days=30), now)
        cursor = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        before = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        chunk_txns = []
        while True:
            params = {
                "account_id": account_id,
                "since": cursor,
                "before": before,
                "limit": 100,
            }
            resp = _api_get(f"{settings.monzo_api_base}/transactions", headers, params)
            batch = resp.json().get("transactions", [])
            chunk_txns.extend(batch)

            if len(batch) < 100:
                break
            # Cursor-based pagination: use last txn ID
            cursor = batch[-1]["id"]

        all_txns.extend(chunk_txns)
        elapsed = time.time() - auth_time
        print(f"  {window_start.strftime('%Y-%m')} → {len(chunk_txns):>4} txns  "
              f"(total: {len(all_txns):>6}, elapsed: {elapsed:.0f}s)")

        window_start = window_end

    return all_txns


def _api_get(url: str, headers: dict, params: dict, max_retries: int = 5) -> requests.Response:
    """GET with exponential backoff on 429."""
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"    Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        if resp.status_code == 401:
            raise AuthRequiredError("Monzo access token expired (401). Re-authenticate.")
        if resp.status_code == 400:
            # Might be a >1 year span issue, log and return empty
            print(f"    400 error: {resp.text[:200]}")
            return _empty_response()
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Rate limited {max_retries} times, giving up.")


class _empty_response:
    """Stub for returning empty transaction list on 400 errors."""
    def json(self):
        return {"transactions": []}
