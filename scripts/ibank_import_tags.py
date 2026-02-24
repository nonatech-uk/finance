#!/usr/bin/env python3
"""Import tags from iBank SQLite into transaction_tag table.

Two-phase approach:
  Phase 1: Direct import — match iBank line item UIDs to raw_transaction.transaction_ref
           for iBank-sourced transactions. Insert with source='ibank_import'.
  Phase 2: Propagation — for suppressed iBank transactions, find the active
           API/CSV counterpart (same account/date/amount/currency) and copy tags.

Idempotent: uses ON CONFLICT DO NOTHING on (raw_transaction_id, tag).

Usage:
    python scripts/ibank_import_tags.py              # run import
    python scripts/ibank_import_tags.py --dry-run     # preview
"""

import argparse
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import settings

IBANK_DB = "/Users/stu/Documents/01 Filing/01 Finance/11 iBank/iBank-Mac.bank8/StoreContent/core.sql"


def extract_tags_from_ibank():
    """Extract (line_item_uid, tag_name) pairs from iBank SQLite.

    Tags sit on line items (ZLINEITEM) via the Z_19PTAGS junction table.
    Most tags are on category-side line items (account class 6000/7000),
    so we join through ZPTRANSACTION to reach the bank-side line item.
    """
    ib = sqlite3.connect(IBANK_DB)
    cur = ib.cursor()

    cur.execute("""
        -- Tags via category line items -> bank line items
        SELECT DISTINCT bank_li.ZPUNIQUEID, tag.ZPNAME
        FROM Z_19PTAGS jt
        JOIN ZLINEITEM cat_li ON cat_li.Z_PK = jt.Z_19PLINEITEMS
        JOIN ZACCOUNT cat_a ON cat_a.Z_PK = cat_li.ZPACCOUNT
        JOIN ZLINEITEM bank_li ON bank_li.ZPTRANSACTION = cat_li.ZPTRANSACTION
        JOIN ZACCOUNT bank_a ON bank_a.Z_PK = bank_li.ZPACCOUNT
        JOIN ZTAG tag ON tag.Z_PK = jt.Z_47PTAGS
        WHERE cat_a.ZPACCOUNTCLASS IN (6000, 7000)
          AND bank_a.ZPACCOUNTCLASS NOT IN (6000, 7000)

        UNION

        -- Tags directly on bank line items
        SELECT DISTINCT li.ZPUNIQUEID, tag.ZPNAME
        FROM Z_19PTAGS jt
        JOIN ZLINEITEM li ON li.Z_PK = jt.Z_19PLINEITEMS
        JOIN ZACCOUNT a ON a.Z_PK = li.ZPACCOUNT
        JOIN ZTAG tag ON tag.Z_PK = jt.Z_47PTAGS
        WHERE a.ZPACCOUNTCLASS NOT IN (6000, 7000)
    """)
    rows = cur.fetchall()
    ib.close()
    return rows  # list of (line_item_uid, tag_name)


def phase1_direct_import(pg_conn, ibank_tags, dry_run=False):
    """Map iBank line item UIDs to raw_transaction.transaction_ref and insert tags."""
    cur = pg_conn.cursor()

    # Build lookup: transaction_ref -> raw_transaction.id for iBank source
    cur.execute("""
        SELECT transaction_ref, id
        FROM raw_transaction
        WHERE source = 'ibank' AND transaction_ref IS NOT NULL
    """)
    ref_to_id = {r[0]: r[1] for r in cur.fetchall()}

    # Group tags by raw_transaction_id
    tags_by_txn = defaultdict(set)
    unmatched = 0
    for li_uid, tag_name in ibank_tags:
        rt_id = ref_to_id.get(li_uid)
        if rt_id:
            tags_by_txn[rt_id].add(tag_name)
        else:
            unmatched += 1

    total_associations = sum(len(tags) for tags in tags_by_txn.values())
    print(f"  Matched: {total_associations} tag associations across {len(tags_by_txn)} transactions")
    if unmatched:
        print(f"  Unmatched line item UIDs: {unmatched}")

    if dry_run:
        return tags_by_txn

    # Insert tags
    inserted = 0
    for rt_id, tags in tags_by_txn.items():
        for tag in tags:
            cur.execute("""
                INSERT INTO transaction_tag (raw_transaction_id, tag, source)
                VALUES (%s, %s, 'ibank_import')
                ON CONFLICT (raw_transaction_id, tag) DO NOTHING
            """, (str(rt_id), tag))
            inserted += cur.rowcount

    pg_conn.commit()
    print(f"  Inserted: {inserted} (skipped {total_associations - inserted} existing)")
    return tags_by_txn


def phase2_propagate(pg_conn, tags_by_ibank_id, dry_run=False):
    """Propagate tags from suppressed iBank transactions to active API/CSV counterparts.

    Uses the same matching approach as ibank_enrich.py:
    match by (institution, account_key, posted_at, amount, currency).
    """
    cur = pg_conn.cursor()

    # Get suppressed iBank transaction IDs that have tags
    tagged_ids = list(tags_by_ibank_id.keys())
    if not tagged_ids:
        print("  No tags to propagate")
        return

    # Find which tagged iBank transactions are suppressed (not active)
    cur.execute("""
        SELECT rt.id, rt.institution,
               COALESCE(aa.canonical_ref, rt.account_ref) AS account_key,
               rt.posted_at, rt.amount, rt.currency
        FROM raw_transaction rt
        LEFT JOIN account_alias aa
            ON aa.institution = rt.institution AND aa.account_ref = rt.account_ref
        WHERE rt.source = 'ibank'
          AND rt.id = ANY(%s::uuid[])
          AND EXISTS (
              SELECT 1 FROM dedup_group_member dgm
              WHERE dgm.raw_transaction_id = rt.id AND NOT dgm.is_preferred
          )
    """, ([str(i) for i in tagged_ids],))
    suppressed = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
    print(f"  Suppressed iBank transactions with tags: {len(suppressed)}")

    if not suppressed:
        return

    # Get all active non-iBank transactions for matching
    cur.execute("""
        SELECT rt.id, rt.institution,
               COALESCE(aa.canonical_ref, rt.account_ref) AS account_key,
               rt.posted_at, rt.amount, rt.currency
        FROM raw_transaction rt
        LEFT JOIN account_alias aa
            ON aa.institution = rt.institution AND aa.account_ref = rt.account_ref
        WHERE rt.source <> 'ibank'
          AND NOT EXISTS (
              SELECT 1 FROM dedup_group_member dgm
              WHERE dgm.raw_transaction_id = rt.id AND NOT dgm.is_preferred
          )
    """)
    active_rows = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # Index active by match key
    active_by_key = defaultdict(list)
    for r in active_rows:
        key = (r['institution'], r['account_key'], r['posted_at'], r['amount'], r['currency'])
        active_by_key[key].append(r)

    # Match and propagate
    propagated = 0
    matched_txns = 0
    unmatched_txns = 0

    for ib_row in suppressed:
        key = (ib_row['institution'], ib_row['account_key'],
               ib_row['posted_at'], ib_row['amount'], ib_row['currency'])
        candidates = active_by_key.get(key, [])

        if len(candidates) == 1:
            # 1:1 match — propagate tags
            active_id = candidates[0]['id']
            tags = tags_by_ibank_id.get(ib_row['id'], set())
            matched_txns += 1

            if not dry_run:
                for tag in tags:
                    cur.execute("""
                        INSERT INTO transaction_tag (raw_transaction_id, tag, source)
                        VALUES (%s, %s, 'ibank_import')
                        ON CONFLICT (raw_transaction_id, tag) DO NOTHING
                    """, (str(active_id), tag))
                    propagated += cur.rowcount
            else:
                propagated += len(tags)
        else:
            unmatched_txns += 1

    if not dry_run:
        pg_conn.commit()

    print(f"  Propagated: {propagated} tags across {matched_txns} matched transactions")
    if unmatched_txns:
        print(f"  Unmatched (ambiguous or no counterpart): {unmatched_txns}")


def main():
    parser = argparse.ArgumentParser(description="Import tags from iBank SQLite")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no writes")
    args = parser.parse_args()

    print("=== iBank Tag Import ===\n")

    # Extract from iBank
    print("Extracting tags from iBank SQLite...")
    ibank_tags = extract_tags_from_ibank()
    print(f"  Total associations: {len(ibank_tags)}")
    print(f"  Unique tags: {len(set(r[1] for r in ibank_tags))}\n")

    pg = psycopg2.connect(settings.dsn)
    try:
        # Phase 1: Direct import to iBank raw_transactions
        print("Phase 1: Direct import (iBank transaction_ref match)")
        tags_by_ibank_id = phase1_direct_import(pg, ibank_tags, dry_run=args.dry_run)

        # Phase 2: Propagate to active API/CSV counterparts
        print("\nPhase 2: Propagate to active transactions")
        phase2_propagate(pg, tags_by_ibank_id, dry_run=args.dry_run)

        if args.dry_run:
            print("\n(dry run — no changes made)")

        # Summary
        if not args.dry_run:
            cur = pg.cursor()
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT raw_transaction_id) FROM transaction_tag")
            total, unique = cur.fetchone()
            cur.execute("SELECT COUNT(DISTINCT tag) FROM transaction_tag")
            tags = cur.fetchone()[0]
            print(f"\n=== Summary ===")
            print(f"  Total tag associations: {total}")
            print(f"  Transactions with tags: {unique}")
            print(f"  Distinct tags: {tags}")
    finally:
        pg.close()


if __name__ == "__main__":
    main()
