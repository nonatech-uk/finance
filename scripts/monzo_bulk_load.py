#!/usr/bin/env python3
"""Monzo bulk transaction loader.

Authenticates via OAuth, fetches all transactions, and writes to raw_transaction.
Must be run from the project root (finance/).

Usage:
    python scripts/monzo_bulk_load.py
    python scripts/monzo_bulk_load.py --account-type uk_retail
    python scripts/monzo_bulk_load.py --since 2023-01-01
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.monzo import authenticate, list_accounts, fetch_transactions
from src.ingestion.writer import write_monzo_transactions


def main():
    parser = argparse.ArgumentParser(description="Bulk load Monzo transactions")
    parser.add_argument("--account-type", help="Filter: uk_retail or uk_retail_joint")
    parser.add_argument("--since", help="Fetch from this date (YYYY-MM-DD), default: all history")
    args = parser.parse_args()

    since = None
    if args.since:
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # Authenticate
    print("=== Monzo Bulk Loader ===\n")
    access_token = authenticate()
    auth_time = time.time()

    # List accounts
    accounts = list_accounts(access_token, args.account_type)
    if not accounts:
        print("No accounts found.")
        return

    print(f"\nFound {len(accounts)} account(s):")
    for i, acc in enumerate(accounts):
        closed = " (CLOSED)" if acc.get("closed") else ""
        print(f"  [{i}] {acc['id']} — {acc.get('description', 'N/A')}{closed}")

    # Fetch and write for each account
    total_inserted = 0
    total_skipped = 0

    for acc in accounts:
        if acc.get("closed"):
            print(f"\nSkipping closed account {acc['id']}")
            continue

        acc_id = acc["id"]
        print(f"\nFetching transactions for {acc_id}...")

        txns = fetch_transactions(access_token, acc_id, since=since, auth_time=auth_time)
        print(f"  Fetched {len(txns)} transactions.")

        if txns:
            result = write_monzo_transactions(txns, acc_id)
            print(f"  Written: {result['inserted']} new, {result['skipped']} duplicates.")
            total_inserted += result["inserted"]
            total_skipped += result["skipped"]

    print(f"\n=== Done ===")
    print(f"Total: {total_inserted} new transactions, {total_skipped} duplicates skipped.")


if __name__ == "__main__":
    main()
