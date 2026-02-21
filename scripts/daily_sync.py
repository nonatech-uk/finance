#!/usr/bin/env python3
"""Daily transaction sync orchestrator.

Fetches Monzo + Wise transactions, runs cleaning and dedup pipelines,
pings healthcheck URLs on success.

Called by systemd timer:
    podman exec finance-sync python scripts/daily_sync.py

Env vars:
    HEALTHCHECK_MONZO_URL  — pinged after successful Monzo sync
    HEALTHCHECK_WISE_URL   — pinged after successful Wise sync
"""

import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings


def ping_healthcheck(url: str | None, label: str):
    """Ping a healthcheck URL. Silently skips if not configured."""
    if not url:
        return
    try:
        subprocess.run(
            ["curl", "-fsS", "-m", "10", "--retry", "3", url],
            capture_output=True, timeout=60,
        )
        print(f"  [{label}] Healthcheck pinged.")
    except Exception as e:
        print(f"  [{label}] Healthcheck ping failed: {e}")


def sync_wise() -> dict:
    """Sync Wise transactions for the last 30 days."""
    from src.ingestion.wise import get_profiles, get_balances, fetch_activities, enrich_activities
    from scripts.wise_bulk_load import parse_activity, write_transactions, build_api_fx_events

    since = datetime.now(timezone.utc) - timedelta(days=30)

    profiles = get_profiles()
    profile = next(p for p in profiles if p["type"] == "PERSONAL")
    profile_id = profile["id"]
    print(f"  Wise profile: {profile_id}")

    activities = fetch_activities(profile_id, since=since)
    print(f"  Fetched {len(activities)} activities")

    activities = enrich_activities(profile_id, activities)

    txns = []
    for activity in activities:
        parsed = parse_activity(activity)
        if parsed:
            txns.append(parsed)

    print(f"  Parsed {len(txns)} transactions")

    if not txns:
        return {"inserted": 0, "skipped": 0, "fx_events": 0}

    conn = psycopg2.connect(settings.dsn)
    try:
        result = write_transactions(txns, conn)
        fx_stats = build_api_fx_events(txns, conn)
        result["fx_events"] = fx_stats["fx_events"]
        return result
    finally:
        conn.close()


def sync_monzo() -> dict:
    """Sync Monzo transactions for the last 30 days."""
    from src.ingestion.monzo import authenticate, list_accounts, fetch_transactions, AuthRequiredError
    from src.ingestion.writer import write_monzo_transactions

    access_token = authenticate(headless=True)
    auth_time = time.time()

    accounts = list_accounts(access_token)
    since = datetime.now(timezone.utc) - timedelta(days=30)

    total_inserted = 0
    total_skipped = 0

    for acc in accounts:
        if acc.get("closed"):
            continue

        acc_id = acc["id"]
        print(f"  Account: {acc_id}")
        txns = fetch_transactions(access_token, acc_id, since=since, auth_time=auth_time)
        print(f"  Fetched {len(txns)} transactions")

        if txns:
            result = write_monzo_transactions(txns, acc_id)
            total_inserted += result["inserted"]
            total_skipped += result["skipped"]

    return {"inserted": total_inserted, "skipped": total_skipped}


def run_cleaning():
    """Run the cleaning pipeline for new transactions."""
    from src.cleaning.processor import process_all
    from src.cleaning.matcher import match_all

    print("  Cleaning raw merchants...")
    process_all()
    print("  Matching to canonical merchants...")
    match_all()


def run_dedup():
    """Run the dedup pipeline."""
    from src.dedup.matcher import find_duplicates

    conn = psycopg2.connect(settings.dsn)
    try:
        result = find_duplicates(conn)
        print(f"  Superseded: {result['source_superseded']}, "
              f"Cross-source: {result['cross_source_groups']}, "
              f"Extended: {result['cross_source_extended']}, "
              f"iBank internal: {result['ibank_internal_groups']}, "
              f"Skipped: {result['skipped']}")
    finally:
        conn.close()


def main():
    hc_monzo = os.environ.get("HEALTHCHECK_MONZO_URL")
    hc_wise = os.environ.get("HEALTHCHECK_WISE_URL")

    print(f"=== Daily Sync — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")

    # Step 1: Wise
    wise_ok = False
    print("Step 1: Sync Wise...")
    try:
        result = sync_wise()
        print(f"  Result: {result['inserted']} new, {result['skipped']} dupes, "
              f"{result.get('fx_events', 0)} FX events")
        wise_ok = True
    except Exception as e:
        print(f"  ERROR syncing Wise: {e}")
        traceback.print_exc()

    # Step 2: Monzo
    monzo_ok = False
    print("\nStep 2: Sync Monzo...")
    try:
        result = sync_monzo()
        print(f"  Result: {result['inserted']} new, {result['skipped']} dupes")
        monzo_ok = True
    except Exception as e:
        print(f"  ERROR syncing Monzo: {e}")
        traceback.print_exc()

    # Step 3: Cleaning
    print("\nStep 3: Cleaning pipeline...")
    try:
        run_cleaning()
        print("  Done.")
    except Exception as e:
        print(f"  ERROR in cleaning: {e}")
        traceback.print_exc()

    # Step 4: Dedup
    print("\nStep 4: Dedup pipeline...")
    try:
        run_dedup()
        print("  Done.")
    except Exception as e:
        print(f"  ERROR in dedup: {e}")
        traceback.print_exc()

    # Step 5: Healthcheck pings (only on source-level success)
    print("\nStep 5: Healthcheck pings...")
    if wise_ok:
        ping_healthcheck(hc_wise, "Wise")
    else:
        print("  [Wise] Skipped (sync failed)")

    if monzo_ok:
        ping_healthcheck(hc_monzo, "Monzo")
    else:
        print("  [Monzo] Skipped (sync failed)")

    print(f"\n=== Done ===")

    # Exit with error if either source failed
    if not (wise_ok and monzo_ok):
        sys.exit(1)


if __name__ == "__main__":
    main()
