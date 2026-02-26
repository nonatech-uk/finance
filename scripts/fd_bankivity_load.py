#!/usr/bin/env python3
"""First Direct transaction loader from Bankivity (Salt Edge).

Reads the Bankivity .bank8 SQLite database and extracts First Direct
transactions that were downloaded via Salt Edge. Uses the Salt Edge
transaction ID as transaction_ref for idempotent inserts.

Usage:
    python scripts/fd_bankivity_load.py ~/path/to/NonaFinance.bank8
    python scripts/fd_bankivity_load.py --dry-run ~/path/to/NonaFinance.bank8
"""

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings

# Core Data epoch offset: seconds between Unix epoch (1970) and Core Data epoch (2001-01-01)
CORE_DATA_EPOCH_OFFSET = 978307200

# Map Bankivity account names to our account_ref convention
ACCOUNT_MAP = {
    "40478790245682": "fd_5682",
    "XXXX XXXX XXXX 8897": "fd_8897",
}


def extract_transactions(db_path: str) -> list[dict]:
    """Extract Salt Edge-sourced FD transactions from Bankivity SQLite DB."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT
            a.ZPNAME as account_name,
            t.ZPDATE as txn_date_cd,
            t.ZPTITLE as title,
            li.ZPTRANSACTIONAMOUNT as amount,
            li.ZPRUNNINGBALANCE as running_balance,
            li.ZPMEMO as memo,
            ls.ZPSOURCEIDENTIFIER as saltedge_txn_id,
            ls.ZPDETAILS as source_details,
            ls.ZPAMOUNT as source_amount
        FROM ZTRANSACTION t
        JOIN ZLINEITEM li ON li.ZPTRANSACTION = t.Z_PK
        JOIN ZACCOUNT a ON a.Z_PK = li.ZPACCOUNT
        JOIN ZLINEITEMSOURCE ls ON ls.ZPLINEITEM = li.Z_PK
            AND ls.ZPSOURCETYPE = 'saltedge'
        WHERE a.ZPNAME IN ({placeholders})
        ORDER BY t.ZPDATE DESC
    """.format(placeholders=",".join("?" for _ in ACCOUNT_MAP)),
        list(ACCOUNT_MAP.keys()),
    )

    txns = []
    for row in cur:
        account_name = row["account_name"]
        account_ref = ACCOUNT_MAP.get(account_name)
        if not account_ref:
            continue

        # Convert Core Data timestamp to date string
        cd_timestamp = row["txn_date_cd"]
        unix_ts = cd_timestamp + CORE_DATA_EPOCH_OFFSET
        posted_at = datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d")

        amount = Decimal(str(row["amount"]))
        saltedge_txn_id = row["saltedge_txn_id"]

        raw_data = {
            "saltedge_txn_id": saltedge_txn_id,
            "description": row["title"],
            "amount": str(amount),
            "date": posted_at,
        }
        if row["running_balance"] is not None:
            raw_data["running_balance"] = str(Decimal(str(row["running_balance"])))
        if row["memo"]:
            raw_data["memo"] = row["memo"]
        if row["source_details"] and row["source_details"] != row["title"]:
            raw_data["source_details"] = row["source_details"]

        txns.append({
            "account_ref": account_ref,
            "transaction_ref": f"se_{saltedge_txn_id}",
            "posted_at": posted_at,
            "amount": amount,
            "raw_merchant": row["title"],
            "raw_memo": row["memo"] or None,
            "raw_data": raw_data,
        })

    conn.close()
    return txns


def write_transactions(txns: list[dict], conn) -> dict[str, int]:
    """Write transactions to raw_transaction. Idempotent via ON CONFLICT."""
    cur = conn.cursor()
    inserted = 0

    for txn in txns:
        cur.execute("""
            INSERT INTO raw_transaction (
                source, institution, account_ref, transaction_ref,
                posted_at, amount, currency,
                raw_merchant, raw_memo, is_dirty, raw_data
            ) VALUES (
                'first_direct_bankivity', 'first_direct', %s, %s,
                %s, %s, 'GBP',
                %s, %s, false, %s
            )
            ON CONFLICT (institution, account_ref, transaction_ref)
                WHERE transaction_ref IS NOT NULL
            DO NOTHING
            RETURNING id
        """, (
            txn["account_ref"],
            txn["transaction_ref"],
            txn["posted_at"],
            txn["amount"],
            txn["raw_merchant"],
            txn["raw_memo"],
            json.dumps(txn["raw_data"]),
        ))

        result = cur.fetchone()
        if result:
            inserted += 1

    conn.commit()
    skipped = len(txns) - inserted
    return {"inserted": inserted, "skipped": skipped}


def main():
    parser = argparse.ArgumentParser(
        description="Load First Direct transactions from Bankivity (Salt Edge)")
    parser.add_argument("bank8", help="Path to .bank8 directory")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only")
    args = parser.parse_args()

    bank8_path = Path(args.bank8)
    db_path = bank8_path / "StoreContent" / "core.sql"

    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        sys.exit(1)

    print("=== First Direct Bankivity (Salt Edge) Loader ===\n")
    print(f"  Database: {db_path}\n")

    txns = extract_transactions(str(db_path))

    # Group by account for reporting
    by_account: dict[str, list[dict]] = {}
    for txn in txns:
        by_account.setdefault(txn["account_ref"], []).append(txn)

    print(f"  Extracted {len(txns)} Salt Edge transactions:")
    for acct, acct_txns in sorted(by_account.items()):
        dates = [t["posted_at"] for t in acct_txns]
        print(f"    {acct}: {len(acct_txns)} ({min(dates)} to {max(dates)})")

    if args.dry_run:
        print("\n  [DRY RUN] No data written.")
        return

    conn = psycopg2.connect(settings.dsn)
    try:
        result = write_transactions(txns, conn)
        print(f"\n  Result: {result['inserted']} new, {result['skipped']} duplicates.")
    finally:
        conn.close()

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
