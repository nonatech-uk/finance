#!/usr/bin/env python3
"""Load iBank category taxonomy, merchant mappings, and accounts into Postgres.

Extracts from the iBank SQLite database:
1. Category hierarchy → category table
2. Merchant→category mappings → canonical_merchant + merchant_raw_mapping
3. Bank accounts → account table

Usage:
    python scripts/load_ibank_categories.py
    python scripts/load_ibank_categories.py --db /path/to/iBank-Mac.bank8
"""

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
import psycopg2.extras

from config.settings import settings

DEFAULT_IBANK_PATH = (
    "/Users/stu/Documents/01 Filing/01 Finance/11 iBank/iBank-Mac.bank8"
    "/StoreContent/core.sql"
)

# iBank account class → our account_type mapping
ACCOUNT_CLASS_MAP = {
    1000: "cash",
    1001: "current",
    1002: "savings",
    1006: "credit_card",
    2000: "investment",
    2001: "pension",
    4000: "mortgage",
}

# iBank account class for categories
INCOME_CLASS = 6000
EXPENSE_CLASS = 7000


def load_categories(ibank_cur, pg_conn) -> Dict[str, str]:
    """Load iBank category hierarchy into Postgres category table.

    Returns dict mapping full_path → category UUID.
    """
    pg_cur = pg_conn.cursor()

    # Extract categories from iBank (account classes 6000=income, 7000=expense)
    ibank_cur.execute("""
        SELECT a.Z_PK, a.ZPACCOUNTCLASS, a.ZPNAME, a.ZPFULLNAME,
               a.ZPPARENTACCOUNT, a.ZPHIDDEN
        FROM ZACCOUNT a
        WHERE a.ZPACCOUNTCLASS IN (6000, 7000)
        ORDER BY a.ZPFULLNAME
    """)
    rows = ibank_cur.fetchall()

    # Build parent PK → full_path lookup
    pk_to_path = {r[0]: r[3] for r in rows}

    # Insert categories, tracking path → UUID
    path_to_uuid = {}
    inserted = 0

    for pk, acc_class, name, full_path, parent_pk, hidden in rows:
        cat_type = "income" if acc_class == INCOME_CLASS else "expense"

        # Find parent UUID if exists
        parent_uuid = None
        if parent_pk and parent_pk in pk_to_path:
            parent_path = pk_to_path[parent_pk]
            parent_uuid = path_to_uuid.get(parent_path)

        is_active = not bool(hidden)

        pg_cur.execute("""
            INSERT INTO category (full_path, name, parent_id, category_type, is_active)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (full_path) DO UPDATE SET
                parent_id = EXCLUDED.parent_id,
                is_active = EXCLUDED.is_active
            RETURNING id
        """, (full_path, name, parent_uuid, cat_type, is_active))

        cat_uuid = pg_cur.fetchone()[0]
        path_to_uuid[full_path] = str(cat_uuid)
        inserted += 1

    pg_conn.commit()
    print(f"  Categories: {inserted} loaded")
    return path_to_uuid


def load_merchant_mappings(ibank_cur, pg_conn, cat_path_to_uuid: Dict[str, str]):
    """Load merchant→category mappings as canonical merchants + raw mappings.

    For merchants with multiple categories, prefers the most recent
    non-archive category. Falls back to archive only if no alternative exists.
    """
    pg_cur = pg_conn.cursor()

    # Get merchant→category with counts and recency
    ibank_cur.execute("""
        SELECT t.ZPTITLE as merchant,
               cat.ZPFULLNAME as category,
               count(*) as txn_count,
               max(t.ZPDATE) as last_used
        FROM ZTRANSACTION t
        JOIN ZLINEITEM li ON li.ZPTRANSACTION = t.Z_PK
        JOIN ZACCOUNT cat ON li.ZPACCOUNT = cat.Z_PK
        WHERE cat.ZPACCOUNTCLASS IN (6000, 7000)
          AND t.ZPTITLE IS NOT NULL AND t.ZPTITLE != ''
        GROUP BY t.ZPTITLE, cat.ZPFULLNAME
        ORDER BY t.ZPTITLE, txn_count DESC
    """)

    # Group by merchant, pick best category
    merchant_cats = defaultdict(list)
    for merchant, category, count, last_used in ibank_cur.fetchall():
        merchant_cats[merchant].append((category, count, last_used))

    canonical_inserted = 0
    mapping_inserted = 0

    for merchant, cat_counts in merchant_cats.items():
        # Separate archive vs non-archive categories
        non_archive = [(c, n, t) for c, n, t in cat_counts if not c.startswith("ZZZ-Archive")]
        archive = [(c, n, t) for c, n, t in cat_counts if c.startswith("ZZZ-Archive")]

        if non_archive:
            # Prefer most frequent non-archive category
            dominant_cat, dominant_count, _ = max(non_archive, key=lambda x: x[1])
            total_count = sum(n for _, n, _ in non_archive)
        else:
            # All archive — use most frequent archive category
            dominant_cat, dominant_count, _ = max(archive, key=lambda x: x[1])
            total_count = sum(n for _, n, _ in archive)

        confidence = Decimal(dominant_count) / Decimal(total_count)

        cat_uuid = cat_path_to_uuid.get(dominant_cat)
        if not cat_uuid:
            continue

        # Create canonical merchant
        pg_cur.execute("""
            INSERT INTO canonical_merchant (name, category_hint)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE SET category_hint = EXCLUDED.category_hint
            RETURNING id
        """, (merchant, dominant_cat))
        cm_id = pg_cur.fetchone()[0]
        canonical_inserted += 1

        # Create raw mapping (merchant string → canonical merchant)
        pg_cur.execute("""
            INSERT INTO merchant_raw_mapping
                (cleaned_merchant, canonical_merchant_id, match_type, confidence, mapped_by)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (cleaned_merchant) DO UPDATE SET
                canonical_merchant_id = EXCLUDED.canonical_merchant_id,
                confidence = EXCLUDED.confidence
        """, (merchant, cm_id, "ibank_history", float(confidence), "ibank_migration"))
        mapping_inserted += 1

    pg_conn.commit()
    print(f"  Canonical merchants: {canonical_inserted} loaded")
    print(f"  Merchant mappings: {mapping_inserted} loaded")


def load_accounts(ibank_cur, pg_conn):
    """Load iBank bank accounts into Postgres account table."""
    pg_cur = pg_conn.cursor()

    ibank_cur.execute("""
        SELECT a.Z_PK, a.ZPACCOUNTCLASS, a.ZPNAME, a.ZPFULLNAME,
               a.ZPHIDDEN,
               c.ZPCODE as currency_code
        FROM ZACCOUNT a
        LEFT JOIN ZCURRENCY c ON a.ZCURRENCY = c.Z_PK
        WHERE a.ZPACCOUNTCLASS IN (1000, 1001, 1002, 1006, 2000, 2001, 4000)
        ORDER BY a.ZPACCOUNTCLASS, a.ZPNAME
    """)

    inserted = 0
    for pk, acc_class, name, full_name, hidden, currency in ibank_cur.fetchall():
        account_type = ACCOUNT_CLASS_MAP.get(acc_class, "other")
        is_active = not bool(hidden)

        # Derive institution from name
        institution = _guess_institution(name)

        pg_cur.execute("""
            INSERT INTO account (institution, name, currency, account_type, is_active)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (institution, name, currency or "GBP", account_type, is_active))

        if pg_cur.fetchone():
            inserted += 1

    pg_conn.commit()
    print(f"  Accounts: {inserted} loaded")


def _guess_institution(name: str) -> str:
    """Best-effort institution from account name."""
    lower = name.lower()
    if "monzo" in lower:
        return "monzo"
    if "wise" in lower:
        return "wise"
    if "fidelity" in lower:
        return "fidelity"
    if "aegon" in lower:
        return "aegon"
    if "marcus" in lower or "goldman" in lower:
        return "goldman_sachs"
    if "national savings" in lower:
        return "ns_and_i"
    if "computershare" in lower:
        return "computershare"
    if "standard life" in lower:
        return "standard_life"
    if "scottish widows" in lower or "trp" in lower.split():
        return "scottish_widows"
    if "swiss bank" in lower:
        return "swiss_bank"
    if "citi" in lower:
        return "citi"
    if "octopus" in lower:
        return "octopus"
    if "puma" in lower:
        return "puma_vct"
    if "credit card" in lower:
        return "unknown"
    if "sole account" in lower or "regular saver" in lower or "cash isa" in lower or "bonus savings" in lower or "e-savings" in lower:
        return "first_direct"
    if "mortgage" in lower:
        return "first_direct"
    if "cash (" in lower:
        return "cash"
    if "fund & share" in lower:
        return "hl"
    return "unknown"


def main():
    parser = argparse.ArgumentParser(description="Load iBank data into Postgres")
    parser.add_argument("--db", default=DEFAULT_IBANK_PATH, help="Path to iBank core.sql")
    args = parser.parse_args()

    print("=== iBank Data Loader ===\n")

    # Connect to iBank SQLite
    ibank_conn = sqlite3.connect(args.db)
    ibank_cur = ibank_conn.cursor()

    # Connect to Postgres
    pg_conn = psycopg2.connect(settings.dsn)

    try:
        # Check for unique constraint on category.full_path
        pg_cur = pg_conn.cursor()
        pg_cur.execute("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'category' AND indexdef LIKE '%full_path%'
        """)
        if not pg_cur.fetchall():
            print("  Creating unique index on category.full_path...")
            pg_cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS category_full_path_idx ON category (full_path)")
            pg_conn.commit()

        # Check for unique constraint on canonical_merchant.name
        pg_cur.execute("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'canonical_merchant' AND indexdef LIKE '%name%'
        """)
        if not pg_cur.fetchall():
            print("  Creating unique index on canonical_merchant.name...")
            pg_cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS canonical_merchant_name_idx ON canonical_merchant (name)")
            pg_conn.commit()

        # Check for unique constraint on merchant_raw_mapping.cleaned_merchant
        pg_cur.execute("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'merchant_raw_mapping' AND indexdef LIKE '%cleaned_merchant%'
        """)
        existing = pg_cur.fetchall()
        has_unique = False
        for idx in existing:
            pg_cur.execute("SELECT indexdef FROM pg_indexes WHERE indexname = %s", (idx[0],))
            defn = pg_cur.fetchone()[0]
            if "UNIQUE" in defn.upper():
                has_unique = True
                break
        if not has_unique:
            print("  Creating unique index on merchant_raw_mapping.cleaned_merchant...")
            pg_cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS merchant_raw_mapping_cleaned_idx ON merchant_raw_mapping (cleaned_merchant)")
            pg_conn.commit()

        # 1. Categories
        print("Loading categories...")
        cat_map = load_categories(ibank_cur, pg_conn)

        # 2. Merchant mappings
        print("Loading merchant mappings...")
        load_merchant_mappings(ibank_cur, pg_conn, cat_map)

        # 3. Accounts
        print("Loading accounts...")
        load_accounts(ibank_cur, pg_conn)

        print("\nDone!")

    finally:
        pg_conn.close()
        ibank_conn.close()


if __name__ == "__main__":
    main()
