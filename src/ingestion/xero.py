"""Xero API client: OAuth2 flow and Bank Transaction push."""

import json
import secrets
import time
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, urlparse, parse_qs

import requests

from config.settings import settings

XERO_AUTH_URL = "https://login.xero.com/identity/connect/authorize"
XERO_TOKEN_URL = "https://identity.xero.com/connect/token"
XERO_API_BASE = "https://api.xero.com/api.xro/2.0"
XERO_CONNECTIONS_URL = "https://api.xero.com/connections"
XERO_SCOPES = "openid profile email accounting.transactions accounting.contacts accounting.settings offline_access"

TOKEN_FILE = Path(settings.xero_token_file)


class AuthRequiredError(Exception):
    """Raised when Xero authentication is needed but headless mode prevents interactive flow."""
    pass


class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Captures the OAuth callback code from Xero's redirect."""

    code: Optional[str] = None
    state: Optional[str] = None

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/oauth/callback":
            qs = parse_qs(urlparse(self.path).query)
            _OAuthCallbackHandler.code = qs.get("code", [None])[0]
            _OAuthCallbackHandler.state = qs.get("state", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h2>Authorised \xe2\x80\x94 you can close this tab.</h2>")
        else:
            # Ignore favicon/other requests
            self.send_response(204)
            self.end_headers()

    def log_message(self, format, *args):
        pass


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
        print("Attempting Xero token refresh...")
        resp = requests.post(XERO_TOKEN_URL, data={
            "grant_type": "refresh_token",
            "client_id": settings.xero_client_id,
            "client_secret": settings.xero_client_secret,
            "refresh_token": tokens["refresh_token"],
        }, timeout=30)
        if resp.ok:
            data = resp.json()
            data["authenticated_at"] = time.time()
            # Preserve tenant_id from previous tokens
            if tokens.get("tenant_id"):
                data["tenant_id"] = tokens["tenant_id"]
            _save_tokens(data)
            print("Xero token refreshed.")
            return data["access_token"]
        print(f"Xero refresh failed ({resp.status_code}): {resp.text[:200]}")

    if headless:
        raise AuthRequiredError(
            "Xero token refresh failed and interactive auth is not available. "
            "Re-authenticate via: python scripts/xero_setup.py"
        )

    # Full OAuth flow (interactive only)
    print("Starting Xero OAuth flow.")
    state = secrets.token_urlsafe(16)
    auth_url = XERO_AUTH_URL + "?" + urlencode({
        "response_type": "code",
        "client_id": settings.xero_client_id,
        "redirect_uri": settings.xero_redirect_uri,
        "scope": XERO_SCOPES,
        "state": state,
    })

    parsed = urlparse(settings.xero_redirect_uri)
    host = parsed.hostname or "0.0.0.0"
    port = parsed.port or 9877

    server = HTTPServer((host, port), _OAuthCallbackHandler)

    print(f"\nOpening Xero auth in browser...\n  {auth_url}\n")
    webbrowser.open(auth_url)
    print("Waiting for callback...")
    # Loop until we get the actual OAuth callback (ignoring favicon etc.)
    _OAuthCallbackHandler.code = None
    _OAuthCallbackHandler.state = None
    while _OAuthCallbackHandler.code is None:
        server.handle_request()
    server.server_close()

    if _OAuthCallbackHandler.state != state:
        raise RuntimeError("OAuth state mismatch — possible CSRF.")
    if not _OAuthCallbackHandler.code:
        raise RuntimeError("No authorisation code received.")

    # Exchange code for token
    resp = requests.post(XERO_TOKEN_URL, data={
        "grant_type": "authorization_code",
        "client_id": settings.xero_client_id,
        "client_secret": settings.xero_client_secret,
        "redirect_uri": settings.xero_redirect_uri,
        "code": _OAuthCallbackHandler.code,
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    data["authenticated_at"] = time.time()

    # Fetch tenant ID
    connections = get_connections(data["access_token"])
    if connections:
        data["tenant_id"] = connections[0]["tenantId"]
        print(f"Connected to: {connections[0].get('tenantName', 'Unknown')}")
    else:
        print("Warning: no Xero organisations connected.")

    _save_tokens(data)
    print("Xero authentication complete.")
    return data["access_token"]


def get_tenant_id() -> str:
    """Get the Xero tenant ID from stored tokens or settings."""
    if settings.xero_tenant_id:
        return settings.xero_tenant_id
    tokens = _load_tokens()
    if tokens and tokens.get("tenant_id"):
        return tokens["tenant_id"]
    raise RuntimeError("No Xero tenant ID configured. Run: python scripts/xero_setup.py")


def _api_headers(access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Xero-Tenant-Id": get_tenant_id(),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _api_get(url: str, access_token: str, params: dict | None = None, max_retries: int = 5) -> requests.Response:
    """GET with exponential backoff on 429."""
    headers = _api_headers(access_token)
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"  Xero rate limited, waiting {retry_after}s...")
            time.sleep(retry_after)
            continue
        if resp.status_code == 401:
            raise AuthRequiredError("Xero access token expired (401). Re-authenticate.")
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Xero rate limited {max_retries} times, giving up.")


def _api_put(url: str, access_token: str, data: dict, max_retries: int = 5) -> requests.Response:
    """PUT with exponential backoff on 429."""
    headers = _api_headers(access_token)
    for attempt in range(max_retries):
        resp = requests.put(url, headers=headers, json=data, timeout=30)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"  Xero rate limited, waiting {retry_after}s...")
            time.sleep(retry_after)
            continue
        if resp.status_code == 401:
            raise AuthRequiredError("Xero access token expired (401). Re-authenticate.")
        if resp.status_code == 400:
            # Xero returns 400 with per-element validation errors — return for caller to parse
            return resp
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Xero rate limited {max_retries} times, giving up.")


def _api_post(url: str, access_token: str, data: dict, max_retries: int = 5) -> requests.Response:
    """POST with exponential backoff on 429."""
    headers = _api_headers(access_token)
    for attempt in range(max_retries):
        resp = requests.post(url, headers=headers, json=data, timeout=30)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"  Xero rate limited, waiting {retry_after}s...")
            time.sleep(retry_after)
            continue
        if resp.status_code == 401:
            raise AuthRequiredError("Xero access token expired (401). Re-authenticate.")
        if resp.status_code == 400:
            return resp
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Xero rate limited {max_retries} times, giving up.")


def get_connections(access_token: str) -> list[dict]:
    """Fetch connected Xero organisations."""
    resp = requests.get(
        XERO_CONNECTIONS_URL,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def get_organisation(access_token: str) -> dict:
    """Fetch the connected organisation details."""
    resp = _api_get(f"{XERO_API_BASE}/Organisation", access_token)
    return resp.json()["Organisations"][0]


def get_accounts(access_token: str) -> list[dict]:
    """Fetch chart of accounts."""
    resp = _api_get(f"{XERO_API_BASE}/Accounts", access_token)
    return resp.json()["Accounts"]


def get_bank_accounts(access_token: str) -> list[dict]:
    """Fetch bank accounts only."""
    resp = _api_get(f"{XERO_API_BASE}/Accounts", access_token, params={"where": 'Type=="BANK"'})
    return resp.json()["Accounts"]


def get_tax_rates(access_token: str) -> list[dict]:
    """Fetch available tax rates."""
    resp = _api_get(f"{XERO_API_BASE}/TaxRates", access_token)
    return resp.json()["TaxRates"]


def create_bank_transactions(access_token: str, transactions: list[dict]) -> dict:
    """Create bank transactions in Xero (batch, up to 50 at a time).

    Each transaction dict should have the Xero BankTransaction structure:
    {
        "Type": "SPEND" or "RECEIVE",
        "Contact": {"Name": "Merchant Name"},
        "BankAccount": {"AccountID": "xxx"},
        "Date": "2026-01-15",
        "LineItems": [{"Description": "...", "AccountCode": "400", "LineAmount": 15.00, "TaxType": "NONE"}],
        "Reference": "finance-system-uuid",
        "Status": "AUTHORISED"
    }
    """
    resp = _api_put(
        f"{XERO_API_BASE}/BankTransactions",
        access_token,
        {"BankTransactions": transactions},
    )
    return resp.json()


def build_bank_transaction(
    txn_type: str,
    merchant: str,
    bank_account_id: str,
    date: str,
    amount: float,
    account_code: str,
    reference: str,
    description: str = "",
) -> dict:
    """Build a Xero BankTransaction dict from our fields."""
    return {
        "Type": txn_type,
        "Contact": {"Name": merchant or "Unknown"},
        "BankAccount": {"AccountID": bank_account_id},
        "Date": date,
        "LineItems": [{
            "Description": description or merchant or "Transaction",
            "AccountCode": account_code,
            "LineAmount": round(amount, 2),
            "TaxType": "NONE",
        }],
        "Reference": reference,
        "Status": "AUTHORISED",
    }


def get_bank_transactions(access_token: str, bank_account_id: str | None = None) -> list[dict]:
    """Fetch all non-deleted bank transactions from Xero (paginated, 100/page)."""
    all_txns = []
    page = 1
    while True:
        params: dict = {"page": page}
        where_parts = ['Status!="DELETED"']
        if bank_account_id:
            where_parts.append(f'BankAccount.AccountID==Guid("{bank_account_id}")')
        params["where"] = " AND ".join(where_parts)
        resp = _api_get(f"{XERO_API_BASE}/BankTransactions", access_token, params=params)
        batch = resp.json().get("BankTransactions", [])
        if not batch:
            break
        all_txns.extend(batch)
        page += 1
    return all_txns


def delete_bank_transactions(access_token: str, xero_transaction_ids: list[str]) -> dict:
    """Delete bank transactions in Xero by setting Status to DELETED (batch, up to 50)."""
    txns = [{"BankTransactionID": tid, "Status": "DELETED"} for tid in xero_transaction_ids]
    resp = _api_post(
        f"{XERO_API_BASE}/BankTransactions",
        access_token,
        {"BankTransactions": txns},
    )
    return resp.json()
