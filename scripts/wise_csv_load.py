#!/usr/bin/env python3
"""Wise CSV transaction loader.

Loads Wise transaction-history CSV exports into raw_transaction.
Handles cross-currency transactions, deduplication, and FX events.

Usage:
    python scripts/wise_csv_load.py /path/to/transaction-history.csv [more.csv ...]
    python scripts/wise_csv_load.py --no-fx /path/to/*.csv
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings


def parse_wise_csv(filepath: str) -> List[dict]:
    """Parse a Wise transaction-history CSV into normalised dicts."""
    rows = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def determine_account_currency(row: dict) -> str:
    """Work out which Wise balance this CSV row belongs to.

    For same-currency txns: source currency = account currency.
    For cross-currency txns (BALANCE_TRANSACTION / rate != 1.0):
      the 'source currency' is the balance it was debited from.
    """
    return row["Source currency"]


def build_raw_transactions(row: dict) -> List[dict]:
    """Convert a CSV row into one or two raw_transaction-shaped dicts.

    For same-currency transactions: returns one record.
    For cross-currency transactions: returns two records — a debit on the
    source currency balance and a credit on the target currency balance.

    Returns empty list for rows we should skip.
    """
    txn_id = row["ID"]
    status = row["Status"]
    direction = row["Direction"]

    if status != "COMPLETED":
        return []

    source_currency = row["Source currency"]
    target_currency = row["Target currency"]
    source_amount = Decimal(row["Source amount (after fees)"])
    target_amount = Decimal(row["Target amount (after fees)"])
    fee_amount = Decimal(row["Source fee amount"]) if row["Source fee amount"] else Decimal("0")
    rate = Decimal(row["Exchange rate"]) if row["Exchange rate"] else None

    # Determine the amount from the perspective of the source balance
    if direction == "OUT":
        amount = -source_amount
    elif direction == "IN":
        amount = source_amount
    elif direction == "NEUTRAL":
        # Balance conversions: source side is debited
        amount = -source_amount
    else:
        amount = source_amount

    # Merchant / counterparty
    target_name = row.get("Target name", "").strip()
    source_name = row.get("Source name", "").strip()
    if direction == "OUT" or direction == "NEUTRAL":
        merchant = target_name
    else:
        merchant = source_name

    # Posted date
    finished = row.get("Finished on", "") or row.get("Created on", "")
    posted_at = finished[:10] if finished else None

    # Is this a cross-currency transaction?
    is_fx = source_currency != target_currency

    # Build the raw_data JSON blob with all CSV fields
    raw_data = dict(row)
    raw_data["_csv_source"] = True

    results = []

    # Source-side record (always created)
    results.append({
        "transaction_ref": txn_id,
        "account_ref": f"wise_{source_currency}",
        "currency": source_currency,
        "amount": amount,
        "fee_amount": fee_amount,
        "posted_at": posted_at,
        "raw_merchant": merchant,
        "raw_memo": row.get("Reference") or None,
        "raw_data": raw_data,
        "is_fx": is_fx,
        "source_currency": source_currency,
        "target_currency": target_currency,
        "source_amount": source_amount,
        "target_amount": target_amount,
        "rate": rate,
        "direction": direction,
        "category": row.get("Category", ""),
        "note": row.get("Note", ""),
    })

    # Target-side credit record for balance conversions only.
    # NEUTRAL = explicit balance conversion (GBP→CHF in your account)
    # Cross-currency OUT = spending from one balance at a merchant in another
    #   currency — the target currency doesn't enter your balance.
    if is_fx and direction == "NEUTRAL":
        target_raw_data = dict(raw_data)
        target_raw_data["_fx_target_leg"] = True

        results.append({
            "transaction_ref": f"{txn_id}_target",
            "account_ref": f"wise_{target_currency}",
            "currency": target_currency,
            "amount": target_amount,
            "fee_amount": Decimal("0"),
            "posted_at": posted_at,
            "raw_merchant": f"Balance conversion from {source_currency}",
            "raw_memo": row.get("Reference") or None,
            "raw_data": target_raw_data,
            "is_fx": is_fx,
            "source_currency": source_currency,
            "target_currency": target_currency,
            "source_amount": source_amount,
            "target_amount": target_amount,
            "rate": rate,
            "direction": direction,
            "category": row.get("Category", ""),
            "note": row.get("Note", ""),
        })

    return results


def load_csv_files(filepaths: List[str]) -> List[dict]:
    """Load and deduplicate transactions from multiple CSV files.

    The same transaction ID can appear in multiple files (e.g. a card payment
    in EUR that was funded from CHF appears in both the EUR and CHF exports).
    We keep one record per (txn_id, source_currency) pair.
    """
    all_txns = []
    seen: Set[Tuple[str, str]] = set()

    for fp in filepaths:
        rows = parse_wise_csv(fp)
        print(f"  {Path(fp).name}: {len(rows)} rows")

        for row in rows:
            parsed_list = build_raw_transactions(row)
            for parsed in parsed_list:
                key = (parsed["transaction_ref"], parsed["currency"])
                if key in seen:
                    continue
                seen.add(key)
                all_txns.append(parsed)

    return all_txns


def write_transactions(txns: List[dict], conn) -> Dict[str, int]:
    """Write parsed Wise transactions to raw_transaction. Idempotent."""
    cur = conn.cursor()
    inserted = 0

    for txn in txns:
        cur.execute("""
            INSERT INTO raw_transaction (
                source, institution, account_ref, transaction_ref,
                posted_at, amount, currency,
                raw_merchant, raw_memo, is_dirty, raw_data
            ) VALUES (
                'wise_csv', 'wise', %s, %s,
                %s, %s, %s,
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
            txn["currency"],
            txn["raw_merchant"],
            txn["raw_memo"],
            json.dumps(txn["raw_data"]),
        ))

        result = cur.fetchone()
        if result:
            txn["_raw_transaction_id"] = result[0]
            inserted += 1
        else:
            # Already exists — fetch ID for FX processing
            cur.execute("""
                SELECT id FROM raw_transaction
                WHERE institution = 'wise' AND account_ref = %s AND transaction_ref = %s
            """, (txn["account_ref"], txn["transaction_ref"]))
            existing = cur.fetchone()
            if existing:
                txn["_raw_transaction_id"] = existing[0]

    conn.commit()
    skipped = len(txns) - inserted
    return {"inserted": inserted, "skipped": skipped}


def build_fx_events(txns: List[dict], conn) -> Dict[str, int]:
    """Create economic_event + fx_event records for cross-currency transactions."""
    cur = conn.cursor()
    stats = {"fx_events": 0, "transfer_events": 0, "skipped": 0}

    for txn in txns:
        if not txn.get("is_fx"):
            continue

        raw_txn_id = txn.get("_raw_transaction_id")
        if not raw_txn_id:
            stats["skipped"] += 1
            continue

        # Check if we already created an event for this raw_transaction
        cur.execute("""
            SELECT ee.id FROM economic_event ee
            JOIN economic_event_leg eel ON eel.economic_event_id = ee.id
            WHERE eel.raw_transaction_id = %s
        """, (raw_txn_id,))
        if cur.fetchone():
            stats["skipped"] += 1
            continue

        source_amount = txn["source_amount"]
        target_amount = txn["target_amount"]
        source_currency = txn["source_currency"]
        target_currency = txn["target_currency"]
        rate = txn["rate"]
        fee_amount = txn["fee_amount"]

        description = txn["raw_merchant"] or ""
        if source_currency != target_currency:
            description = f"{source_amount} {source_currency} → {target_amount} {target_currency}"

        event_type = "fx_conversion"

        # Create economic event
        cur.execute("""
            INSERT INTO economic_event (event_type, initiated_at, description, match_status)
            VALUES (%s, %s, %s, 'auto_matched')
            RETURNING id
        """, (event_type, txn["posted_at"], description))
        event_id = cur.fetchone()[0]

        # Create leg for the source side
        cur.execute("""
            INSERT INTO economic_event_leg
                (economic_event_id, raw_transaction_id, leg_type, amount, currency)
            VALUES (%s, %s, %s, %s, %s)
        """, (event_id, raw_txn_id, "source", -abs(source_amount), source_currency))

        # Create FX event
        cur.execute("""
            INSERT INTO fx_event
                (economic_event_id, source_amount, source_currency,
                 target_amount, target_currency, achieved_rate,
                 fee_amount, fee_currency, provider)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'wise')
        """, (
            event_id,
            abs(source_amount),
            source_currency,
            abs(target_amount),
            target_currency,
            rate,
            fee_amount if fee_amount else None,
            source_currency if fee_amount else None,
        ))
        stats["fx_events"] += 1

    conn.commit()
    return stats


def main():
    parser = argparse.ArgumentParser(description="Load Wise CSV transaction exports")
    parser.add_argument("files", nargs="+", help="Path(s) to Wise CSV export files")
    parser.add_argument("--no-fx", action="store_true", help="Skip FX event creation")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report, don't write")
    args = parser.parse_args()

    print("=== Wise CSV Loader ===\n")

    # Validate files exist
    for f in args.files:
        if not Path(f).exists():
            print(f"ERROR: File not found: {f}")
            sys.exit(1)

    # Parse and deduplicate
    txns = load_csv_files(args.files)
    print(f"\n  Total unique transactions: {len(txns)}")

    # Summary by currency
    from collections import Counter
    by_currency = Counter(t["currency"] for t in txns)
    for cur, count in sorted(by_currency.items()):
        print(f"    {cur}: {count}")

    fx_txns = [t for t in txns if t["is_fx"]]
    print(f"    Cross-currency (FX): {len(fx_txns)}")

    if args.dry_run:
        print("\n  [DRY RUN] No data written.")
        return

    # Connect and write
    conn = psycopg2.connect(settings.dsn)
    try:
        result = write_transactions(txns, conn)
        print(f"\n  Written: {result['inserted']} new, {result['skipped']} duplicates.")

        if not args.no_fx and fx_txns:
            fx_stats = build_fx_events(txns, conn)
            print(f"  FX events: {fx_stats['fx_events']}, skipped: {fx_stats['skipped']}")

        print("\n=== Done ===")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
