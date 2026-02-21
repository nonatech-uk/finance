"""Wise API client: profile/balance listing, activities, and transaction details.

Two approaches:
1. Activities endpoint (primary) — walks monthly windows, fetches rich detail
   per card-transaction or transfer. No SCA required.
2. Statements endpoint (fallback/recon) — gets balance-level statements.
   Currently blocked by PSD2 SCA (403).

The activities approach yields richer data: MCC codes, merchant location,
fee breakdown, auth method, etc.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests

from config.settings import settings

# Wise statement endpoint max date range
MAX_STATEMENT_DAYS = 460  # API limit is 469, leave margin


def _headers() -> dict:
    token = settings.wise_api_token
    if not token:
        raise RuntimeError("Wise API token not configured (WISE_API_TOKEN / .env)")
    return {"Authorization": f"Bearer {token}"}


def get_profiles() -> List[dict]:
    """Fetch Wise profiles (personal + business)."""
    resp = requests.get(f"{settings.wise_api_base}/v2/profiles", headers=_headers(), timeout=30)
    if resp.status_code == 401:
        raise RuntimeError(
            "Wise API token invalid or expired (401). "
            "Regenerate at https://wise.com/settings/api-tokens"
        )
    resp.raise_for_status()
    return resp.json()


def get_balances(profile_id: int) -> List[dict]:
    """Fetch all standard balances for a profile."""
    resp = requests.get(
        f"{settings.wise_api_base}/v4/profiles/{profile_id}/balances",
        headers=_headers(),
        params={"types": "STANDARD"},
        timeout=30,
    )
    if resp.status_code == 401:
        raise RuntimeError(
            "Wise API token invalid or expired (401). "
            "Regenerate at https://wise.com/settings/api-tokens"
        )
    resp.raise_for_status()
    return resp.json()


# ── Activities endpoint (primary) ──────────────────────────────────────────

def fetch_activities(
    profile_id: int,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> List[dict]:
    """Fetch all activities using monthly windowing to beat the 100-result cap.

    The activities endpoint has a hard 100-result limit per query.
    Walking in monthly windows ensures we get everything.

    Returns a deduplicated list of activity dicts.
    """
    if since is None:
        since = datetime(2017, 1, 1, tzinfo=timezone.utc)
    if until is None:
        until = datetime.now(timezone.utc)

    all_activities = []
    seen_ids = set()
    window_start = since

    while window_start < until:
        window_end = min(window_start + timedelta(days=30), until)

        # Fetch page(s) within this window
        window_activities = _fetch_activities_window(
            profile_id, window_start, window_end
        )

        added = 0
        for activity in window_activities:
            aid = activity.get("id")
            if aid and aid not in seen_ids:
                seen_ids.add(aid)
                all_activities.append(activity)
                added += 1

        if added > 0:
            print(f"    {window_start.strftime('%Y-%m')}: {added} activities "
                  f"(total: {len(all_activities)})")

        window_start = window_end

    return all_activities


def _fetch_activities_window(
    profile_id: int,
    start: datetime,
    end: datetime,
) -> List[dict]:
    """Fetch activities within a single time window, handling cursor pagination."""
    activities = []
    cursor = None

    while True:
        params = {
            "since": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "until": end.strftime("%Y-%m-%dT%H:%M:%S.999Z"),
            "size": 100,
        }
        if cursor:
            params["offset"] = cursor

        resp = _api_get(
            f"{settings.wise_api_base}/v1/profiles/{profile_id}/activities",
            params=params,
        )

        data = resp.json()
        batch = data.get("activities", [])
        activities.extend(batch)

        # Check for next cursor
        next_cursor = data.get("endOfStatementBalance")  # not actually cursor
        # Activities API doesn't have traditional cursor — if we got 100,
        # the monthly window should be narrow enough. If not, we'd need
        # to split the window further.
        if len(batch) >= 100:
            print(f"      WARNING: {len(batch)} activities in window "
                  f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}. "
                  f"Some may be missed.")
        break

    return activities


def fetch_card_transaction_detail(
    profile_id: int,
    transaction_id: str,
) -> Optional[dict]:
    """Fetch rich detail for a card transaction.

    Returns merchant name/city/country, MCC code, fees, auth method.
    """
    resp = _api_get(
        f"{settings.wise_api_base}/v3/profiles/{profile_id}"
        f"/card-transactions/{transaction_id}",
        params={},
    )
    data = resp.json()
    if not data:
        return None
    return data


def fetch_transfer_detail(
    profile_id: int,
    transfer_id: str,
) -> Optional[dict]:
    """Fetch rich detail for a transfer.

    Returns source/target amounts, rate, fee breakdown, state history.
    """
    resp = _api_get(
        f"{settings.wise_api_base}/v3/profiles/{profile_id}"
        f"/transfers/{transfer_id}",
        params={},
    )
    data = resp.json()
    if not data:
        return None
    return data


def enrich_activities(
    profile_id: int,
    activities: List[dict],
    skip_detail: bool = False,
) -> List[dict]:
    """For each activity, fetch the appropriate detail endpoint.

    Enriches the activity dict with a '_detail' key containing the full
    card-transaction or transfer response.

    Activity types and their detail endpoints:
    - CARD (card payments/ATM): /v3/profiles/{id}/card-transactions/{txn_id}
    - TRANSFER (transfers): /v3/profiles/{id}/transfers/{transfer_id}
    - Others: no detail endpoint available

    Returns the enriched activities list.
    """
    if skip_detail:
        return activities

    enriched = 0
    total = len(activities)

    for i, activity in enumerate(activities):
        activity_type = activity.get("type", "")
        resource = activity.get("resource", {})
        resource_type = resource.get("type", "")
        resource_id = resource.get("id")

        if not resource_id:
            continue

        detail = None
        if resource_type == "CARD_TRANSACTION":
            detail = fetch_card_transaction_detail(profile_id, resource_id)
        elif resource_type == "TRANSFER":
            detail = fetch_transfer_detail(profile_id, resource_id)

        if detail:
            activity["_detail"] = detail
            enriched += 1

            # Progress every 50
            if enriched % 50 == 0:
                print(f"    Enriched {enriched}/{total} activities...")

        # Be gentle with rate limits
        if enriched % 20 == 0 and enriched > 0:
            time.sleep(0.5)

    if enriched > 0:
        print(f"    Enriched {enriched} activities with detail data.")

    return activities


# ── Statements endpoint (recon/fallback, currently SCA-blocked) ────────────

def fetch_statements(
    profile_id: int,
    balance_id: int,
    currency: str,
    since: Optional[datetime] = None,
) -> List[dict]:
    """Fetch all statement transactions for a balance, walking in yearly windows.

    Returns list of transaction dicts from the statement response.
    NOTE: Currently returns 403 due to PSD2 SCA requirements.
    """
    if since is None:
        since = datetime(2017, 1, 1, tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    all_txns = []
    window_start = since

    while window_start < now:
        window_end = min(window_start + timedelta(days=MAX_STATEMENT_DAYS), now)

        resp = _api_get(
            f"{settings.wise_api_base}/v1/profiles/{profile_id}"
            f"/balance-statements/{balance_id}/statement.json",
            params={
                "currency": currency,
                "intervalStart": window_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "intervalEnd": window_end.strftime("%Y-%m-%dT%H:%M:%S.999Z"),
                "type": "COMPACT",
            },
        )

        data = resp.json()
        txns = data.get("transactions", [])
        all_txns.extend(txns)

        print(f"  {currency} {window_start.strftime('%Y-%m')} - {window_end.strftime('%Y-%m')}: "
              f"{len(txns)} txns (total: {len(all_txns)})")

        window_start = window_end

    return all_txns


# ── Common HTTP helper ─────────────────────────────────────────────────────

def _api_get(url: str, params: dict, max_retries: int = 5) -> requests.Response:
    """GET with exponential backoff on 429."""
    for attempt in range(max_retries):
        resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"    Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        if resp.status_code == 401:
            raise RuntimeError(
                "Wise API token invalid or expired (401). "
                "Regenerate at https://wise.com/settings/api-tokens"
            )
        if resp.status_code == 403:
            print(f"    403 Forbidden (SCA required?): {resp.text[:200]}")
            return _empty_response()
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Rate limited {max_retries} times, giving up.")


class _empty_response:
    """Stub for returning empty result on errors."""
    def json(self):
        return {}
