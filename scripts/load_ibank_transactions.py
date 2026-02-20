#!/usr/bin/env python3
"""iBank (Bankivity) transaction loader.

Extracts transactions from iBank's SQLite database and loads them into
raw_transaction. Each iBank transaction has a double-entry structure:
one line item for the bank account, one for the category (income/expense).

This gives us:
  - Historical transactions not available from bank APIs/CSVs
  - Category assignments for training the auto-categoriser
  - Split transactions (multiple category legs)
  - Account-to-account transfers

Idempotent: uses iBank's ZPUNIQUEID as transaction_ref.

Usage:
    python scripts/load_ibank_transactions.py
    python scripts/load_ibank_transactions.py --dry-run
    python scripts/load_ibank_transactions.py --account "Sole Account (5682)"
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
import sqlite3

from config.settings import settings

IBANK_PATH = "/Users/stu/Documents/01 Filing/01 Finance/11 iBank/iBank-Mac.bank8/StoreContent/core.sql"
COREDATA_EPOCH = datetime(2001, 1, 1)

# Map iBank account names to (institution, account_ref)
ACCOUNT_MAP = {
    "Sole Account (5682)":                  ("first_direct", "fd_5682"),
    "Credit Card":                          ("first_direct", "fd_8897"),
    "Monzo":                                ("monzo", "monzo_current"),
    "Wise (CHF)":                           ("wise", "wise_CHF"),
    "Wise (EUR)":                           ("wise", "wise_EUR"),
    "Wise (USD)":                           ("wise", "wise_USD"),
    "Wise (GBP)":                           ("wise", "wise_GBP"),
    "Wise (PLN)":                           ("wise", "wise_PLN"),
    "zz - VS Account (1517)":              ("first_direct", "fd_1517"),
    "Cash ISA (0489)":                      ("first_direct", "fd_0489"),
    "Cash ISA (1814)":                      ("first_direct", "fd_1814"),
    "Cash ISA (6676)":                      ("first_direct", "fd_6676"),
    "e-Savings Account (2439)":             ("first_direct", "fd_2439"),
    "Bonus Savings (3883)":                 ("first_direct", "fd_3883"),
    "Regular Saver (4795)":                 ("first_direct", "fd_4795"),
    "Offset Mortgage (9088)":               ("first_direct", "fd_9088"),
    "Marcus (Goldman sachs - UK)":          ("goldman_sachs", "marcus"),
    "Citi Savings (US)":                    ("citi", "citi_savings"),
    "Citi Pension":                         ("citi", "citi_pension"),
    "Fund & Share Account":                 ("hl", "hl_fund_share"),
    "AEGON ISA":                            ("aegon", "aegon_isa"),
    "AEGON Savings":                        ("aegon", "aegon_savings"),
    "Aegon Pension":                        ("aegon", "aegon_pension"),
    "Mees Pot":                             ("monzo", "monzo_mees_pot"),
    "Fidelity GS":                          ("fidelity", "fidelity_gs"),
    "DB Pension (Standard Life)":           ("standard_life", "db_pension"),
    "Dresdner Pension (Standard Life)":     ("standard_life", "dresdner_pension"),
    "Goldman Sachs Pension":                ("goldman_sachs", "gs_pension"),
    "Merrill Pension":                      ("fidelity", "merrill_pension"),
    "BT Pension":                           ("standard_life", "bt_pension"),
    "Swiss Bank Pension":                   ("swiss_bank", "swiss_pension"),
    "National Savings (Premium Bonds) Savings": ("ns_and_i", "premium_bonds"),
    "National Savings - Bond":              ("ns_and_i", "ns_bond"),
    "TRP (Scottish Widows)":                ("scottish_widows", "trp"),
    "TRP Brokerage":                        ("scottish_widows", "trp_brokerage"),
    "Computershare (Citi)":                 ("computershare", "computershare_citi"),
    "Octopus Titan VCT":                    ("octopus", "octopus_titan"),
    "Puma VCT":                             ("puma_vct", "puma_vct"),
    "Cash (GBP)":                           ("cash", "cash_gbp"),
    "Cash (CHF)":                           ("cash", "cash_chf"),
    "Cash (EUR)":                           ("cash", "cash_eur"),
    "Fran Savings":                         ("first_direct", "fd_fran"),
    "Hanielle Loan":                        ("other", "hanielle_loan"),
    "Lehman Pension (Fidelity)":            ("fidelity", "lehman_pension"),
    "Credit Suisse Pension  (Fidelity)":    ("fidelity", "cs_pension"),
    # Property / assets (class 2, 3)
    "Middle Farm":                          ("property", "middle_farm"),
    "Vincent Square":                       ("property", "vincent_square"),
    "The Beast":                            ("vehicle", "the_beast"),
}


def coredata_to_date(timestamp: float) -> Optional[str]:
    """Convert CoreData timestamp to YYYY-MM-DD."""
    if timestamp is None:
        return None
    dt = COREDATA_EPOCH + timedelta(seconds=timestamp)
    return dt.strftime("%Y-%m-%d")


def extract_transactions(ibank_conn, account_filter: Optional[str] = None) -> List[dict]:
    """Extract transactions from iBank SQLite with category info.

    For each transaction:
    - Find the bank account line item (non-category)
    - Find the category line item(s) (class 6000/7000)
    - If both legs are bank accounts, it's a transfer
    """
    cur = ibank_conn.cursor()

    # Get all transactions with their line items
    where_clause = ""
    params = ()
    if account_filter:
        where_clause = "AND a.ZPNAME = ?"
        params = (account_filter,)

    cur.execute(f"""
        SELECT t.Z_PK, t.ZPTITLE, t.ZPDATE, t.ZPUNIQUEID, t.ZPNOTE,
               t.ZPCLEARED, t.ZPVOID,
               li.ZPTRANSACTIONAMOUNT, li.ZPMEMO, li.ZPUNIQUEID as li_uid,
               a.ZPNAME as acct_name, a.ZPACCOUNTCLASS as acct_class,
               a.Z_PK as acct_pk
        FROM ZTRANSACTION t
        JOIN ZLINEITEM li ON li.ZPTRANSACTION = t.Z_PK
        JOIN ZACCOUNT a ON li.ZPACCOUNT = a.Z_PK
        WHERE a.ZPACCOUNTCLASS NOT IN (6000, 7000)
        {where_clause}
        ORDER BY t.ZPDATE DESC
    """, params)

    # Group line items by transaction
    txn_bank_legs = defaultdict(list)
    txn_meta = {}

    for row in cur.fetchall():
        txn_pk = row[0]
        if txn_pk not in txn_meta:
            txn_meta[txn_pk] = {
                "title": row[1],
                "date": row[2],
                "unique_id": row[3],
                "note": row[4],
                "cleared": row[5],
                "void": row[6],
            }
        txn_bank_legs[txn_pk].append({
            "amount": row[7],
            "memo": row[8],
            "li_uid": row[9],
            "acct_name": row[10],
            "acct_class": row[11],
            "acct_pk": row[12],
        })

    # Now get category legs for each transaction
    cur.execute("""
        SELECT li.ZPTRANSACTION, a.ZPNAME, a.ZPFULLNAME,
               li.ZPTRANSACTIONAMOUNT
        FROM ZLINEITEM li
        JOIN ZACCOUNT a ON li.ZPACCOUNT = a.Z_PK
        WHERE a.ZPACCOUNTCLASS IN (6000, 7000)
    """)

    txn_categories = defaultdict(list)
    for row in cur.fetchall():
        txn_categories[row[0]].append({
            "category_name": row[1],
            "category_full": row[2],
            "amount": row[3],
        })

    # Build output
    results = []
    for txn_pk, bank_legs in txn_bank_legs.items():
        meta = txn_meta[txn_pk]

        if meta["void"] == 1:
            continue

        posted_at = coredata_to_date(meta["date"])
        if not posted_at:
            continue

        categories = txn_categories.get(txn_pk, [])

        # Build category string from full name path
        cat_parts = []
        for cat in categories:
            full = cat.get("category_full") or cat.get("category_name") or ""
            if full:
                cat_parts.append(full)
        ibank_category = " | ".join(cat_parts) if cat_parts else None

        # Is this a transfer? (multiple bank legs, no category legs)
        is_transfer = len(bank_legs) > 1 and not categories

        for leg in bank_legs:
            acct_name = leg["acct_name"]
            mapping = ACCOUNT_MAP.get(acct_name)
            if not mapping:
                continue

            institution, account_ref = mapping

            # Build transaction_ref from iBank unique IDs
            transaction_ref = leg["li_uid"] or meta["unique_id"]
            if not transaction_ref:
                continue

            amount = Decimal(str(leg["amount"])) if leg["amount"] is not None else None
            if amount is None:
                continue

            # Determine currency from account
            currency = "GBP"
            if "CHF" in acct_name:
                currency = "CHF"
            elif "EUR" in acct_name:
                currency = "EUR"
            elif "USD" in acct_name or acct_name in ("Citi Savings (US)", "Fidelity GS",
                                                      "TRP Brokerage", "Computershare (Citi)"):
                currency = "USD"
            elif "PLN" in acct_name:
                currency = "PLN"

            raw_merchant = meta["title"] or ""

            raw_data = {
                "ibank_txn_pk": txn_pk,
                "ibank_title": meta["title"],
                "ibank_note": meta["note"],
                "ibank_cleared": meta["cleared"],
                "ibank_memo": leg["memo"],
                "ibank_account": acct_name,
                "ibank_category": ibank_category,
                "ibank_is_transfer": is_transfer,
            }

            # For transfers, note the other account
            if is_transfer:
                other_legs = [l for l in bank_legs if l["acct_pk"] != leg["acct_pk"]]
                if other_legs:
                    raw_data["ibank_transfer_to"] = other_legs[0]["acct_name"]

            results.append({
                "institution": institution,
                "account_ref": account_ref,
                "transaction_ref": transaction_ref,
                "posted_at": posted_at,
                "amount": amount,
                "currency": currency,
                "raw_merchant": raw_merchant,
                "raw_memo": leg["memo"] or meta["note"] or None,
                "raw_data": raw_data,
                "ibank_category": ibank_category,
            })

    return results


def write_transactions(txns: List[dict], pg_conn) -> Dict[str, int]:
    """Write iBank transactions to raw_transaction. Idempotent."""
    cur = pg_conn.cursor()
    inserted = 0

    for txn in txns:
        cur.execute("""
            INSERT INTO raw_transaction (
                source, institution, account_ref, transaction_ref,
                posted_at, amount, currency,
                raw_merchant, raw_memo, is_dirty, raw_data
            ) VALUES (
                'ibank', %s, %s, %s,
                %s, %s, %s,
                %s, %s, false, %s
            )
            ON CONFLICT (institution, account_ref, transaction_ref)
                WHERE transaction_ref IS NOT NULL
            DO NOTHING
            RETURNING id
        """, (
            txn["institution"],
            txn["account_ref"],
            txn["transaction_ref"],
            txn["posted_at"],
            txn["amount"],
            txn["currency"],
            txn["raw_merchant"],
            txn["raw_memo"],
            json.dumps(txn["raw_data"]),
        ))

        if cur.fetchone():
            inserted += 1

    pg_conn.commit()
    return {"inserted": inserted, "skipped": len(txns) - inserted}


def main():
    parser = argparse.ArgumentParser(description="Load iBank transactions")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only")
    parser.add_argument("--account", help="Only load specific iBank account name")
    args = parser.parse_args()

    print("=== iBank Transaction Loader ===\n")

    ibank_conn = sqlite3.connect(IBANK_PATH)
    txns = extract_transactions(ibank_conn, args.account)
    ibank_conn.close()

    print(f"  Extracted: {len(txns)} transaction legs\n")

    # Summary by institution/account
    from collections import Counter
    by_account = Counter((t["institution"], t["account_ref"]) for t in txns)
    for (inst, ref), count in sorted(by_account.items()):
        print(f"    {inst}/{ref}: {count}")

    # Category coverage
    with_cat = sum(1 for t in txns if t["ibank_category"])
    print(f"\n  With category: {with_cat} ({100*with_cat/len(txns):.0f}%)")

    if args.dry_run:
        print("\n  [DRY RUN] No data written.")
        return

    pg_conn = psycopg2.connect(settings.dsn)
    try:
        result = write_transactions(txns, pg_conn)
        print(f"\n  Written: {result['inserted']} new, {result['skipped']} duplicates/overlaps.")
        print("\n=== Done ===")
    finally:
        pg_conn.close()


if __name__ == "__main__":
    main()
