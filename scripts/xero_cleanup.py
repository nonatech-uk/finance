#!/usr/bin/env python3
"""Delete finance-system-pushed transactions from Xero and clear sync log.

The Xero bank feed was already importing transactions, so our pushed
transactions created duplicates. This script:
1. Reads all xero_sync_log entries (our pushed transaction IDs)
2. Deletes them from Xero (sets Status=DELETED)
3. Removes the xero_sync_log entries so future syncs start clean

Usage:
    python scripts/xero_cleanup.py              # dry run (default)
    python scripts/xero_cleanup.py --execute    # actually delete
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings
from src.ingestion.xero import (
    authenticate, delete_bank_transactions, AuthRequiredError,
)

BATCH_SIZE = 50


def main():
    parser = argparse.ArgumentParser(description="Clean up duplicated Xero transactions")
    parser.add_argument("--execute", action="store_true", help="Actually delete (default is dry run)")
    args = parser.parse_args()

    print("=== Xero Cleanup ===\n")

    # Authenticate
    try:
        access_token = authenticate(headless=False)
    except AuthRequiredError as e:
        print(f"Auth required: {e}")
        sys.exit(1)

    conn = psycopg2.connect(settings.dsn)
    try:
        cur = conn.cursor()

        # Get all synced transaction IDs (skip placeholder entries)
        cur.execute("""
            SELECT id, raw_transaction_id, xero_transaction_id, synced_at
            FROM xero_sync_log
            WHERE xero_transaction_id NOT LIKE 'skipped-%'
            ORDER BY synced_at
        """)
        rows = cur.fetchall()

        if not rows:
            print("No synced transactions found in xero_sync_log.")
            return

        print(f"Found {len(rows)} transactions pushed to Xero.\n")

        # Also get skipped entries count
        cur.execute("SELECT COUNT(*) FROM xero_sync_log WHERE xero_transaction_id LIKE 'skipped-%'")
        skipped_count = cur.fetchone()[0]
        if skipped_count:
            print(f"(Also {skipped_count} skipped-zero-amount entries to clear.)\n")

        xero_ids = [row[2] for row in rows]

        if not args.execute:
            print("[DRY RUN] Would delete these Xero transactions:")
            for row in rows[:20]:
                print(f"  {row[3].strftime('%Y-%m-%d %H:%M')}  xero={row[2][:12]}...  raw={str(row[1])[:12]}...")
            if len(rows) > 20:
                print(f"  ... and {len(rows) - 20} more")
            print(f"\nRun with --execute to delete from Xero and clear sync log.")
            return

        # Delete from Xero in batches
        deleted = 0
        errors = []
        for batch_start in range(0, len(xero_ids), BATCH_SIZE):
            batch = xero_ids[batch_start:batch_start + BATCH_SIZE]
            try:
                result = delete_bank_transactions(access_token, batch)
                txns = result.get("BankTransactions", [])
                batch_ok = sum(1 for t in txns if t.get("Status") == "DELETED")
                batch_err = sum(1 for t in txns if t.get("HasValidationErrors"))
                deleted += batch_ok
                if batch_err:
                    for t in txns:
                        if t.get("HasValidationErrors"):
                            errs = t.get("ValidationErrors", [])
                            msg = "; ".join(e.get("Message", "") for e in errs)
                            errors.append(f"  {t.get('BankTransactionID', '?')}: {msg}")
                print(f"  Batch {batch_start // BATCH_SIZE + 1}: {batch_ok}/{len(batch)} deleted")
            except Exception as e:
                errors.append(f"  Batch {batch_start // BATCH_SIZE + 1} failed: {e}")

        print(f"\nDeleted {deleted}/{len(xero_ids)} transactions from Xero.")

        if errors:
            print("\nErrors:")
            for err in errors:
                print(err)

        # Clear sync log
        cur.execute("DELETE FROM xero_sync_log")
        cleared = cur.rowcount
        conn.commit()
        print(f"Cleared {cleared} entries from xero_sync_log.")
        print("\nDone. Future xero_sync.py runs will re-push transactions.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
