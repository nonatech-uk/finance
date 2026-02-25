#!/usr/bin/env python3
"""Amazon order history loader and bank transaction matcher.

Loads Amazon order CSV exports (from browser extension) into amazon_order_item,
then attempts to match orders to bank transactions by date + amount.

Usage:
    python scripts/amazon_load.py ~/Downloads/amazon_order_history*.csv
    python scripts/amazon_load.py --match-only          # skip load, just re-match
    python scripts/amazon_load.py --dry-run ~/Downloads/amazon_order_history.csv
"""

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings


def parse_price(price_str: str) -> Optional[Decimal]:
    """Parse a price string like '£23.36' or '$5.99' into a Decimal."""
    if not price_str:
        return None
    cleaned = re.sub(r"[£$€,\s]", "", price_str.strip())
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def load_amazon_csv(filepath: str) -> List[dict]:
    """Parse an Amazon order history CSV into normalised dicts."""
    items = []
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            order_id = row.get("order id", "").strip()
            order_date = row.get("order date", "").strip()

            if not order_id or not order_date:
                continue

            # Validate date
            try:
                parsed_date = datetime.strptime(order_date, "%Y-%m-%d").date()
            except ValueError:
                continue

            description = row.get("description", "").strip()
            if not description:
                continue

            quantity_str = row.get("quantity", "1").strip()
            try:
                quantity = int(quantity_str)
            except ValueError:
                quantity = 1

            unit_price = parse_price(row.get("price", ""))
            asin = row.get("ASIN", "").strip() or None
            category = row.get("category", "").strip() or None
            is_sub = row.get("subscribe & save", "0").strip() == "1"
            order_url = row.get("order url", "").strip() or None
            item_url = row.get("item url", "").strip() or None

            raw_data = {k: v for k, v in row.items() if v}

            items.append({
                "order_id": order_id,
                "order_date": parsed_date,
                "asin": asin,
                "description": description,
                "quantity": quantity,
                "unit_price": unit_price,
                "category": category,
                "is_subscription": is_sub,
                "order_url": order_url,
                "item_url": item_url,
                "raw_data": raw_data,
            })

    return items


def write_items(items: List[dict], conn) -> Dict[str, int]:
    """Write Amazon order items to amazon_order_item. Idempotent."""
    cur = conn.cursor()
    inserted = 0

    for item in items:
        cur.execute("""
            INSERT INTO amazon_order_item (
                order_id, order_date, asin, description,
                quantity, unit_price, currency, category,
                order_url, item_url, is_subscription, raw_data
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, 'GBP', %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (order_id, asin, description) DO NOTHING
            RETURNING id
        """, (
            item["order_id"],
            item["order_date"],
            item["asin"],
            item["description"],
            item["quantity"],
            item["unit_price"],
            item["category"],
            item["order_url"],
            item["item_url"],
            item["is_subscription"],
            json.dumps(item["raw_data"]),
        ))
        if cur.fetchone():
            inserted += 1

    conn.commit()
    return {"inserted": inserted, "skipped": len(items) - inserted}


def build_order_totals(conn) -> Dict[str, dict]:
    """Group amazon_order_item by order_id, compute total per order."""
    cur = conn.cursor()
    cur.execute("""
        SELECT order_id, order_date,
               SUM(unit_price * quantity) as total,
               COUNT(*) as item_count,
               array_agg(DISTINCT category) FILTER (WHERE category IS NOT NULL) as categories
        FROM amazon_order_item
        WHERE unit_price IS NOT NULL
        GROUP BY order_id, order_date
        ORDER BY order_date DESC
    """)

    orders = {}
    for row in cur.fetchall():
        orders[row[0]] = {
            "order_id": row[0],
            "order_date": row[1],
            "total": row[2],
            "item_count": row[3],
            "categories": row[4] or [],
        }

    return orders


def find_amazon_transactions(conn) -> List[dict]:
    """Find all active bank transactions that look like Amazon charges."""
    cur = conn.cursor()
    # Match on raw_merchant containing Amazon-like patterns
    # Use active_transaction to only match visible (non-suppressed) transactions
    cur.execute("""
        SELECT id, posted_at, amount, currency, raw_merchant, institution
        FROM active_transaction
        WHERE (
            raw_merchant ILIKE '%%AMZN%%'
            OR raw_merchant ILIKE '%%AMAZON%%'
            OR raw_merchant ILIKE '%%AMZ %%'
            OR raw_merchant ILIKE '%%Amazon.co%%'
        )
        AND amount < 0
        ORDER BY posted_at DESC
    """)

    txns = []
    for row in cur.fetchall():
        txns.append({
            "id": row[0],
            "posted_at": row[1],
            "amount": abs(row[2]),  # make positive for matching
            "currency": row[3],
            "raw_merchant": row[4],
            "institution": row[5],
        })

    return txns


def match_orders_to_transactions(
    orders: Dict[str, dict],
    txns: List[dict],
    conn,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Match Amazon orders to bank transactions by date + amount.

    Strategy:
    1. Exact match: order total == transaction amount, within ±5 days
    2. Close match: order total within 5% of transaction amount, within ±5 days
       (accounts for shipping, discounts, etc.)
    3. For unmatched transactions, try combinations of orders that sum to the amount

    Not 1:1 — one transaction can match multiple orders (combined shipment),
    one order can match multiple transactions (split payment).
    """
    cur = conn.cursor()
    stats = {"exact": 0, "close": 0, "skipped_existing": 0}
    date_window = timedelta(days=5)

    # Index orders by date for efficient lookup
    orders_by_date = defaultdict(list)
    for order in orders.values():
        orders_by_date[order["order_date"]].append(order)

    matched_order_ids = set()

    for txn in txns:
        txn_date = txn["posted_at"]
        txn_amount = txn["amount"]

        # Check existing matches for this transaction
        cur.execute("""
            SELECT order_id FROM amazon_order_match
            WHERE raw_transaction_id = %s
        """, (txn["id"],))
        existing = {r[0] for r in cur.fetchall()}

        # Collect candidate orders within date window
        candidates = []
        check_date = txn_date - date_window
        while check_date <= txn_date + date_window:
            for order in orders_by_date.get(check_date, []):
                if order["order_id"] not in existing:
                    candidates.append(order)
            check_date += timedelta(days=1)

        if not candidates:
            continue

        # Try exact match first (within 1p tolerance)
        for order in candidates:
            if order["total"] is None:
                continue
            diff = abs(order["total"] - txn_amount)
            if diff <= Decimal("0.01"):
                if not dry_run:
                    _insert_match(cur, order["order_id"], txn["id"],
                                  Decimal("0.95"), "date_amount_exact",
                                  f"Exact: order {order['total']} == txn {txn_amount}")
                matched_order_ids.add(order["order_id"])
                stats["exact"] += 1

        # Try close match (within 5% — shipping, rounding)
        for order in candidates:
            if order["total"] is None or order["order_id"] in matched_order_ids:
                continue
            diff = abs(order["total"] - txn_amount)
            if diff <= txn_amount * Decimal("0.05") and diff > Decimal("0.01"):
                confidence = Decimal("0.70") - (diff / txn_amount)
                confidence = max(Decimal("0.50"), min(Decimal("0.85"), confidence))
                if not dry_run:
                    _insert_match(cur, order["order_id"], txn["id"],
                                  confidence, "date_amount_close",
                                  f"Close: order {order['total']} vs txn {txn_amount} (diff {diff})")
                matched_order_ids.add(order["order_id"])
                stats["close"] += 1

    if not dry_run:
        conn.commit()

    return stats


def _insert_match(cur, order_id: str, txn_id, confidence, method: str, notes: str):
    """Insert a match row, ignoring duplicates."""
    cur.execute("""
        INSERT INTO amazon_order_match (order_id, raw_transaction_id,
                                        match_confidence, match_method, notes)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (order_id, raw_transaction_id) DO NOTHING
    """, (order_id, txn_id, confidence, method, notes))


def fixup_suppressed_matches(conn, dry_run: bool = False) -> Dict[str, int]:
    """Re-point matches from suppressed transactions to their preferred counterparts.

    When matches were created against raw_transaction, some may point to
    non-preferred dedup group members. This finds those and re-points them
    to the preferred member via dedup_group_member.
    """
    cur = conn.cursor()

    # Find matches pointing to non-active transactions
    cur.execute("""
        SELECT am.order_id, am.raw_transaction_id, am.match_confidence, am.match_method
        FROM amazon_order_match am
        WHERE NOT EXISTS (
            SELECT 1 FROM active_transaction at WHERE at.id = am.raw_transaction_id
        )
    """)
    stale = cur.fetchall()

    if not stale:
        return {"fixed": 0, "orphaned": 0}

    fixed = 0
    orphaned = 0

    for order_id, old_txn_id, confidence, method in stale:
        # Find the preferred member in the same dedup group
        cur.execute("""
            SELECT dgm2.raw_transaction_id
            FROM dedup_group_member dgm1
            JOIN dedup_group_member dgm2 ON dgm2.dedup_group_id = dgm1.dedup_group_id
            WHERE dgm1.raw_transaction_id = %s
              AND dgm2.is_preferred = true
              AND dgm2.raw_transaction_id != %s
        """, (str(old_txn_id), str(old_txn_id)))
        row = cur.fetchone()

        if row:
            new_txn_id = row[0]
            if not dry_run:
                # Insert new match (ON CONFLICT handles duplicates)
                cur.execute("""
                    INSERT INTO amazon_order_match
                        (order_id, raw_transaction_id, match_confidence, match_method, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (order_id, raw_transaction_id) DO NOTHING
                """, (order_id, str(new_txn_id), confidence, method,
                      f"Fixup: re-pointed from suppressed {old_txn_id}"))
                # Delete old match
                cur.execute("""
                    DELETE FROM amazon_order_match
                    WHERE order_id = %s AND raw_transaction_id = %s
                """, (order_id, str(old_txn_id)))
            fixed += 1
        else:
            orphaned += 1

    if not dry_run:
        conn.commit()

    return {"fixed": fixed, "orphaned": orphaned}


def main():
    parser = argparse.ArgumentParser(description="Load Amazon order history and match to transactions")
    parser.add_argument("files", nargs="*", help="Path(s) to Amazon order history CSV files")
    parser.add_argument("--match-only", action="store_true", help="Skip loading, just run matching")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only")
    args = parser.parse_args()

    print("=== Amazon Order Loader ===\n")

    conn = psycopg2.connect(settings.dsn)

    try:
        # Step 1: Load CSVs
        if not args.match_only:
            if not args.files:
                print("ERROR: No files specified (use --match-only to skip loading)")
                sys.exit(1)

            all_items = []
            seen = set()
            for f in args.files:
                if not Path(f).exists():
                    print(f"  WARNING: File not found: {f}")
                    continue
                items = load_amazon_csv(f)
                for item in items:
                    key = (item["order_id"], item["asin"], item["description"])
                    if key not in seen:
                        seen.add(key)
                        all_items.append(item)
                print(f"  {Path(f).name}: {len(items)} items")

            print(f"\n  Total unique items: {len(all_items)}")

            if args.dry_run:
                # Show summary
                from collections import Counter
                years = Counter(i["order_date"].year for i in all_items)
                orders = set(i["order_id"] for i in all_items)
                print(f"  Unique orders: {len(orders)}")
                for y, c in sorted(years.items()):
                    print(f"    {y}: {c} items")
                print("\n  [DRY RUN] No data written.")
                return

            result = write_items(all_items, conn)
            print(f"  Written: {result['inserted']} new, {result['skipped']} duplicates.")

        # Step 2: Match orders to bank transactions
        print("\nStep 2: Matching orders to bank transactions...")

        orders = build_order_totals(conn)
        print(f"  Orders in DB: {len(orders)}")

        txns = find_amazon_transactions(conn)
        print(f"  Amazon bank transactions: {len(txns)}")

        if txns and orders:
            match_stats = match_orders_to_transactions(orders, txns, conn, args.dry_run)
            print(f"  Matches: {match_stats['exact']} exact, {match_stats['close']} close")

            if not args.dry_run:
                # Fix up any matches pointing to suppressed transactions
                print("\nStep 3: Fixing up suppressed matches...")
                fixup_stats = fixup_suppressed_matches(conn)
                print(f"  Fixed: {fixup_stats['fixed']}, Orphaned: {fixup_stats['orphaned']}")

                # Summary
                cur = conn.cursor()
                cur.execute("SELECT count(DISTINCT order_id) FROM amazon_order_match")
                matched_orders = cur.fetchone()[0]
                cur.execute("""
                    SELECT count(DISTINCT am.raw_transaction_id)
                    FROM amazon_order_match am
                    JOIN active_transaction at ON at.id = am.raw_transaction_id
                """)
                matched_txns = cur.fetchone()[0]
                print(f"\n  Total matched: {matched_orders} orders ↔ {matched_txns} active transactions")

                cur.execute("""
                    SELECT count(*) FROM active_transaction
                    WHERE (raw_merchant ILIKE '%%AMZN%%' OR raw_merchant ILIKE '%%AMAZON%%'
                           OR raw_merchant ILIKE '%%AMZ %%' OR raw_merchant ILIKE '%%Amazon.co%%')
                    AND amount < 0
                """)
                total_amazon = cur.fetchone()[0]
                print(f"  Coverage: {matched_txns}/{total_amazon} Amazon transactions matched "
                      f"({100*matched_txns/total_amazon:.0f}%)" if total_amazon > 0 else "")

        print("\n=== Done ===")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
