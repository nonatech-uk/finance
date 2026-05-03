#!/usr/bin/env python3
"""Tag eBay-related transactions in the finance database.

Finds transactions where raw_merchant or raw_memo contains 'ebay' and tags
them with the 'ebay' tag. Also normalises any existing 'Ebay' tags to 'ebay'.

Usage:
    python scripts/ebay_tag.py
    python scripts/ebay_tag.py --dry-run
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings

TAG = "ebay"
SOURCE = "ebay_match"


def normalise_existing_tags(conn, dry_run: bool) -> int:
    """Rename 'Ebay' (ibank_import) tags to 'ebay' for consistency."""
    cur = conn.cursor()
    cur.execute("""
        SELECT tt.id, tt.raw_transaction_id
        FROM transaction_tag tt
        WHERE tt.tag = 'Ebay'
        AND NOT EXISTS (
            SELECT 1 FROM transaction_tag tt2
            WHERE tt2.raw_transaction_id = tt.raw_transaction_id AND tt2.tag = %s
        )
    """, (TAG,))
    rows = cur.fetchall()

    if not dry_run:
        for tag_id, _ in rows:
            cur.execute("UPDATE transaction_tag SET tag = %s WHERE id = %s", (TAG, tag_id))

    return len(rows)


def tag_by_merchant(conn, dry_run: bool) -> int:
    """Tag active transactions where merchant or memo contains 'ebay'."""
    cur = conn.cursor()

    cur.execute("""
        SELECT rt.id, rt.raw_merchant, rt.raw_memo, rt.amount, rt.posted_at
        FROM active_transaction rt
        WHERE (rt.raw_merchant ILIKE '%%ebay%%' OR rt.raw_memo ILIKE '%%ebay%%')
        AND NOT EXISTS (
            SELECT 1 FROM transaction_tag tt
            WHERE tt.raw_transaction_id = rt.id AND tt.tag = %s
        )
        ORDER BY rt.posted_at DESC
    """, (TAG,))

    rows = cur.fetchall()
    if not rows:
        return 0

    for txn_id, merchant, memo, amount, date in rows:
        desc = memo or merchant or "?"
        if dry_run:
            print(f"  {date} {amount:>10} {desc:.60s}")
        else:
            cur.execute("""
                INSERT INTO transaction_tag (raw_transaction_id, tag, source)
                VALUES (%s, %s, %s)
                ON CONFLICT (raw_transaction_id, tag) DO NOTHING
            """, (txn_id, TAG, SOURCE))

    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Tag eBay transactions")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = psycopg2.connect(settings.dsn)
    conn.autocommit = False

    try:
        print("Step 1: Normalising existing 'Ebay' tags to 'ebay'...")
        n0 = normalise_existing_tags(conn, args.dry_run)
        print(f"  {n0} tags normalised")

        print("\nStep 2: Tagging by merchant/memo match...")
        n1 = tag_by_merchant(conn, args.dry_run)
        print(f"  {n1} new transactions tagged")

        if not args.dry_run:
            conn.commit()

        # Show totals
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM transaction_tag
            WHERE tag = %s OR tag = 'Ebay'
        """, (TAG,))
        total = cur.fetchone()[0]
        print(f"\nTotal transactions tagged '{TAG}': {total}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
