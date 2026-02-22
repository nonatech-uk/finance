#!/usr/bin/env python3
"""Goldman Sachs Marcus CSV transaction loader.

Loads Marcus savings CSV exports into raw_transaction.

CSV format:
  "TransactionDate","Description","Value","AccountBalance","AccountName","AccountNumber"
  "20260212","Withdrawal to 90245682","-2500.00","11661.80"," Mees Savings","90310601"

Date format: YYYYMMDD
Account ref derived from AccountNumber (last 4 digits).

Usage:
    python scripts/marcus_csv_load.py ~/Downloads/Marcus.csv
    python scripts/marcus_csv_load.py --dry-run ~/Downloads/Marcus.csv
"""

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings


def make_hash_ref(date_str: str, amount: str, description: str, row_idx: int) -> str:
    """Generate a stable transaction ref from date + amount + description + row index.

    Row index is needed because Marcus can have multiple identical transactions
    on the same date (e.g. two transfers on the same day for the same amount).
    We use the balance as a disambiguator instead when available.
    """
    key = f"{date_str}|{amount}|{description}|{row_idx}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def make_balance_ref(date_str: str, amount: str, balance: str) -> str:
    """Generate a stable transaction ref using balance as disambiguator.

    Since balance is a running total, date + amount + resulting balance
    is unique for each transaction.
    """
    key = f"{date_str}|{amount}|{balance}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def parse_marcus_csv(filepath: str) -> list[dict]:
    """Parse a Marcus CSV file.

    Returns list of transaction dicts.
    """
    txns = []

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            date_str = row["TransactionDate"].strip().strip('"')
            description = row["Description"].strip().strip('"')
            value_str = row["Value"].strip().strip('"')
            balance_str = row["AccountBalance"].strip().strip('"')
            account_name = row["AccountName"].strip().strip('"')
            account_number = row["AccountNumber"].strip().strip('"')

            if not date_str or not value_str:
                continue

            # Parse date YYYYMMDD -> YYYY-MM-DD
            try:
                posted_at = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                print(f"  WARNING: Skipping row with bad date: {date_str}")
                continue

            amount = Decimal(value_str)

            # Use balance as disambiguator for transaction ref
            transaction_ref = make_balance_ref(date_str, value_str, balance_str)

            # Derive account_ref from account number
            account_ref = f"marcus"

            raw_data = {
                "date": date_str,
                "description": description,
                "value": value_str,
                "balance": balance_str,
                "account_name": account_name,
                "account_number": account_number,
            }

            txns.append({
                "transaction_ref": transaction_ref,
                "posted_at": posted_at,
                "amount": amount,
                "raw_merchant": description,
                "raw_data": raw_data,
                "account_ref": account_ref,
            })

    return txns


def write_transactions(txns: list[dict], conn) -> dict:
    """Write parsed Marcus transactions to raw_transaction. Idempotent."""
    cur = conn.cursor()
    inserted = 0

    for txn in txns:
        cur.execute("""
            INSERT INTO raw_transaction (
                source, institution, account_ref, transaction_ref,
                posted_at, amount, currency,
                raw_merchant, raw_memo, is_dirty, raw_data
            ) VALUES (
                'marcus_csv', 'goldman_sachs', %s, %s,
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
            None,
            json.dumps(txn["raw_data"]),
        ))

        result = cur.fetchone()
        if result:
            inserted += 1

    conn.commit()
    skipped = len(txns) - inserted
    return {"inserted": inserted, "skipped": skipped}


def main():
    parser = argparse.ArgumentParser(description="Load Goldman Sachs Marcus CSV exports")
    parser.add_argument("files", nargs="+", help="Path(s) to Marcus CSV files")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only")
    args = parser.parse_args()

    print("=== Marcus CSV Loader ===\n")

    for f in args.files:
        if not Path(f).exists():
            print(f"ERROR: File not found: {f}")
            sys.exit(1)

    all_txns = []
    for fp in args.files:
        txns = parse_marcus_csv(fp)
        print(f"  {Path(fp).name}: {len(txns)} rows")
        all_txns.extend(txns)

    if not all_txns:
        print("\n  No transactions found.")
        return

    dates = [t["posted_at"] for t in all_txns]
    amounts = [t["amount"] for t in all_txns]
    print(f"\n  Total: {len(all_txns)} transactions")
    print(f"  Date range: {min(dates)} to {max(dates)}")
    print(f"  Sum: {sum(amounts):.2f}")

    # Verify against final balance in CSV (first row = most recent)
    print(f"  Latest balance in CSV: {all_txns[0]['raw_data']['balance']}")

    if args.dry_run:
        print("\n  [DRY RUN] No data written.")
        return

    conn = psycopg2.connect(settings.dsn)
    try:
        result = write_transactions(all_txns, conn)
        print(f"\n  marcus: {result['inserted']} new, {result['skipped']} duplicates.")
        print(f"\n=== Done ===")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
