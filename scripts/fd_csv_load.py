#!/usr/bin/env python3
"""First Direct CSV transaction loader.

Loads First Direct CSV exports into raw_transaction.
Auto-detects two CSV formats:
  Format A (5682): Date,Description,Amount,Balance
  Format B (8897): Date,Description,Amount,Reference

Usage:
    python scripts/fd_csv_load.py ~/Downloads/20022026_5682*.csv
    python scripts/fd_csv_load.py ~/Downloads/20022026_8897.csv
    python scripts/fd_csv_load.py --dry-run ~/Downloads/*.csv
    python scripts/fd_csv_load.py --account 1234 ~/Downloads/file.csv
"""

import argparse
import csv
import hashlib
import json
import re
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings

# Filename pattern: *_{NNNN}*.csv or *_{NNNN}.csv
ACCOUNT_RE = re.compile(r"_(\d{4})(?:-\d+)?\.csv$")


def detect_format(headers: List[str]) -> str:
    """Detect CSV format from header row."""
    normalised = [h.strip().lower() for h in headers]
    if "reference" in normalised:
        return "B"  # Date,Description,Amount,Reference
    if "balance" in normalised:
        return "A"  # Date,Description,Amount,Balance
    raise ValueError(f"Unknown CSV format. Headers: {headers}")


def extract_account_from_filename(filepath: str) -> Optional[str]:
    """Try to extract 4-digit account number from filename."""
    match = ACCOUNT_RE.search(Path(filepath).name)
    if match:
        return match.group(1)
    return None


def make_hash_ref(date_str: str, amount: str, description: str) -> str:
    """Generate a stable transaction ref from date + amount + description."""
    key = f"{date_str}|{amount}|{description}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def parse_fd_csv(filepath: str) -> Tuple[List[dict], str, Optional[str]]:
    """Parse a First Direct CSV file.

    Returns (transactions, format_type, account_number).
    """
    account_num = extract_account_from_filename(filepath)

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        fmt = detect_format(headers)

        txns = []
        for row in reader:
            if not row or len(row) < 3:
                continue

            date_str = row[0].strip()
            description = row[1].strip()
            amount_str = row[2].strip()

            if not date_str or not amount_str:
                continue

            # Parse date DD/MM/YYYY -> YYYY-MM-DD
            try:
                posted_at = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                continue

            amount = Decimal(amount_str)

            if fmt == "B":
                ref = row[3].strip() if len(row) > 3 and row[3].strip() else None
                # Use date + reference as transaction_ref for uniqueness
                # (reference alone may not be unique across dates)
                if ref:
                    transaction_ref = f"{date_str}_{ref}"
                else:
                    transaction_ref = make_hash_ref(date_str, amount_str, description)
            else:
                # Format A: no reference, use hash
                transaction_ref = make_hash_ref(date_str, amount_str, description)

            raw_data = {
                "date": date_str,
                "description": description,
                "amount": amount_str,
            }
            if fmt == "B" and len(row) > 3:
                raw_data["reference"] = row[3].strip()
            if fmt == "A" and len(row) > 3:
                raw_data["balance"] = row[3].strip()

            txns.append({
                "transaction_ref": transaction_ref,
                "posted_at": posted_at,
                "amount": amount,
                "raw_merchant": description,
                "raw_data": raw_data,
            })

    return txns, fmt, account_num


def load_csv_files(filepaths: List[str], account_override: Optional[str]) -> Dict[str, List[dict]]:
    """Load and group transactions by account from multiple CSV files.

    Returns {account_ref: [txns]}.
    """
    by_account: Dict[str, List[dict]] = {}
    seen: Set[Tuple[str, str]] = set()

    for fp in filepaths:
        txns, fmt, account_num = parse_fd_csv(fp)
        acct = account_override or account_num
        if not acct:
            print(f"  WARNING: Cannot determine account for {Path(fp).name}. "
                  "Use --account to specify.")
            continue

        account_ref = f"fd_{acct}"

        if account_ref not in by_account:
            by_account[account_ref] = []

        added = 0
        for txn in txns:
            key = (account_ref, txn["transaction_ref"])
            if key in seen:
                continue
            seen.add(key)
            txn["account_ref"] = account_ref
            by_account[account_ref].append(txn)
            added += 1

        print(f"  {Path(fp).name} (format {fmt}, account {acct}): "
              f"{len(txns)} rows, {added} unique")

    return by_account


def write_transactions(txns: List[dict], conn) -> Dict[str, int]:
    """Write parsed FD transactions to raw_transaction. Idempotent."""
    cur = conn.cursor()
    inserted = 0

    for txn in txns:
        cur.execute("""
            INSERT INTO raw_transaction (
                source, institution, account_ref, transaction_ref,
                posted_at, amount, currency,
                raw_merchant, raw_memo, is_dirty, raw_data
            ) VALUES (
                'first_direct_csv', 'first_direct', %s, %s,
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
    parser = argparse.ArgumentParser(description="Load First Direct CSV exports")
    parser.add_argument("files", nargs="+", help="Path(s) to First Direct CSV files")
    parser.add_argument("--account", help="Override account number (e.g. 5682)")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only")
    args = parser.parse_args()

    print("=== First Direct CSV Loader ===\n")

    # Validate files
    for f in args.files:
        if not Path(f).exists():
            print(f"ERROR: File not found: {f}")
            sys.exit(1)

    # Parse
    by_account = load_csv_files(args.files, args.account)

    total = sum(len(txns) for txns in by_account.values())
    print(f"\n  Total unique transactions: {total}")
    for acct, txns in sorted(by_account.items()):
        dates = [t["posted_at"] for t in txns]
        print(f"    {acct}: {len(txns)} ({min(dates)} to {max(dates)})")

    if args.dry_run:
        print("\n  [DRY RUN] No data written.")
        return

    # Write
    conn = psycopg2.connect(settings.dsn)
    try:
        total_inserted = 0
        total_skipped = 0
        for acct, txns in sorted(by_account.items()):
            result = write_transactions(txns, conn)
            print(f"\n  {acct}: {result['inserted']} new, {result['skipped']} duplicates.")
            total_inserted += result["inserted"]
            total_skipped += result["skipped"]

        print(f"\n=== Done ===")
        print(f"Total: {total_inserted} new, {total_skipped} duplicates.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
