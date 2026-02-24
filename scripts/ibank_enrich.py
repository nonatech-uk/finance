#!/usr/bin/env python3
"""Enrich canonical merchants using iBank category and display name data.

Matches iBank transactions to active API/CSV transactions by
(institution, account, date, amount, currency). Where there is exactly
one of each (1:1 match), transfers:
  - iBank category → canonical_merchant.category_hint
  - iBank raw_merchant → canonical_merchant.display_name

Only updates canonical_merchant. Does not modify raw_transaction,
dedup groups, merchant_raw_mapping, or cleaned_transaction.

Idempotent: skips merchants that already have a category_hint.

Usage:
    python scripts/ibank_enrich.py                     # run enrichment
    python scripts/ibank_enrich.py --dry-run            # preview without writing
    python scripts/ibank_enrich.py --institution monzo  # just one institution
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings


def find_all_transactions(conn, *, institution=None):
    """Load all iBank and active API/CSV transactions with resolved account keys.

    Returns (ibank_rows, api_rows) where each row is a dict.
    """
    cur = conn.cursor()

    inst_filter = "AND rt.institution = %(inst)s" if institution else ""

    # iBank transactions
    cur.execute(f"""
        SELECT
            rt.id, rt.source, rt.institution,
            COALESCE(aa.canonical_ref, rt.account_ref) AS account_key,
            rt.posted_at, rt.amount, rt.currency,
            rt.raw_merchant,
            rt.raw_data->>'ibank_category' AS ibank_category,
            rt.raw_data->>'ibank_note' AS ibank_note
        FROM raw_transaction rt
        LEFT JOIN account_alias aa
            ON aa.institution = rt.institution
            AND aa.account_ref = rt.account_ref
        WHERE rt.source = 'ibank'
        {inst_filter}
    """, {"inst": institution} if institution else {})
    ibank_cols = [desc[0] for desc in cur.description]
    ibank_rows = [dict(zip(ibank_cols, row)) for row in cur.fetchall()]

    # Active API/CSV transactions with canonical merchant linkage
    cur.execute(f"""
        SELECT
            rt.id, rt.source, rt.institution,
            COALESCE(aa.canonical_ref, rt.account_ref) AS account_key,
            rt.posted_at, rt.amount, rt.currency,
            rt.raw_merchant,
            mrm.canonical_merchant_id,
            cm.name AS cm_name,
            cm.category_hint AS cm_current_category,
            cm.display_name AS cm_current_display
        FROM raw_transaction rt
        LEFT JOIN account_alias aa
            ON aa.institution = rt.institution
            AND aa.account_ref = rt.account_ref
        JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
        JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        WHERE rt.source <> 'ibank'
          AND rt.id IN (SELECT id FROM active_transaction)
          AND cm.merged_into_id IS NULL
        {inst_filter}
    """, {"inst": institution} if institution else {})
    api_cols = [desc[0] for desc in cur.description]
    api_rows = [dict(zip(api_cols, row)) for row in cur.fetchall()]

    return ibank_rows, api_rows


def match_transactions(ibank_rows, api_rows):
    """Match iBank to API/CSV transactions by (account, date, amount, currency).

    Strategy:
      1. Exact date: progressive 1:1 matching on (account, date, amount, currency)
      2. Fuzzy date ±1 day: handles transaction vs posted date offsets (FD, Wise)
      3. Fuzzy date ±3 days: handles wider offsets (Monzo created vs settled)

    Each stage runs progressive passes — match, remove, repeat until stable.

    Returns (matches, unmatched_ibank, unmatched_api) where:
      - matches: list of (ibank_row, api_row) tuples
      - unmatched_ibank: list of ibank rows with no API/CSV match
      - unmatched_api: list of api rows with no iBank match
    """
    matched_ibank_ids = set()
    matched_api_ids = set()
    all_matches = []

    def _run_exact_passes():
        """Progressive 1:1 matching on exact (account, date, amount, currency)."""
        def _make_key(row):
            return (row['institution'], row['account_key'], row['posted_at'],
                    row['amount'], row['currency'])

        total = 0
        pass_num = 0
        while True:
            pass_num += 1

            ibank_by_key = defaultdict(list)
            for r in ibank_rows:
                if r['id'] not in matched_ibank_ids:
                    ibank_by_key[_make_key(r)].append(r)

            api_by_key = defaultdict(list)
            for r in api_rows:
                if r['id'] not in matched_api_ids:
                    api_by_key[_make_key(r)].append(r)

            new_matches = 0
            for key, ib_list in ibank_by_key.items():
                ap_list = api_by_key.get(key, [])
                if len(ib_list) == 1 and len(ap_list) == 1:
                    all_matches.append((ib_list[0], ap_list[0]))
                    matched_ibank_ids.add(ib_list[0]['id'])
                    matched_api_ids.add(ap_list[0]['id'])
                    new_matches += 1

            total += new_matches
            if new_matches == 0:
                break

        return total

    def _run_fuzzy_date_passes(day_tolerance=1):
        """Progressive 1:1 matching with +/- day_tolerance on date.

        For each unmatched API/CSV transaction, look for iBank transactions
        with same (account, amount, currency) within +/- day_tolerance days.
        Only match if exactly one candidate on each side.
        """
        from datetime import timedelta

        total = 0
        pass_num = 0
        while True:
            pass_num += 1

            # Index unmatched iBank by (account, amount, currency) -> list of (date, row)
            ibank_by_amt = defaultdict(list)
            for r in ibank_rows:
                if r['id'] not in matched_ibank_ids:
                    key = (r['institution'], r['account_key'], r['amount'], r['currency'])
                    ibank_by_amt[key].append(r)

            new_matches = 0
            # For each unmatched API txn, find iBank candidates within date window
            for ap in api_rows:
                if ap['id'] in matched_api_ids:
                    continue
                key = (ap['institution'], ap['account_key'], ap['amount'], ap['currency'])
                ib_candidates = ibank_by_amt.get(key, [])
                if not ib_candidates:
                    continue

                # Filter to those within date tolerance
                nearby = [
                    ib for ib in ib_candidates
                    if abs((ib['posted_at'] - ap['posted_at']).days) <= day_tolerance
                    and ib['id'] not in matched_ibank_ids
                ]
                if len(nearby) == 1:
                    # Also check: is this the only unmatched API txn that would
                    # match this iBank txn? (1:1 requirement from the iBank side)
                    ib = nearby[0]
                    api_candidates_for_ib = [
                        a for a in api_rows
                        if a['id'] not in matched_api_ids
                        and a['institution'] == ib['institution']
                        and a['account_key'] == ib['account_key']
                        and a['amount'] == ib['amount']
                        and a['currency'] == ib['currency']
                        and abs((a['posted_at'] - ib['posted_at']).days) <= day_tolerance
                    ]
                    if len(api_candidates_for_ib) == 1:
                        all_matches.append((ib, ap))
                        matched_ibank_ids.add(ib['id'])
                        matched_api_ids.add(ap['id'])
                        new_matches += 1

            total += new_matches
            if new_matches == 0:
                break

        return total

    # Stage 1: Exact date matching
    exact_count = _run_exact_passes()
    print(f"    Exact date: {exact_count} matches")

    # Stage 2: Fuzzy date matching (+/- 1 day) on remainder
    fuzzy1_count = _run_fuzzy_date_passes(day_tolerance=1)
    print(f"    Fuzzy date (±1 day): {fuzzy1_count} matches")

    # Stage 3: Wider fuzzy date matching (+/- 3 days) on remainder
    fuzzy3_count = _run_fuzzy_date_passes(day_tolerance=3)
    print(f"    Fuzzy date (±3 days): {fuzzy3_count} matches")

    print(f"    Total: {exact_count + fuzzy1_count + fuzzy3_count} matches")

    unmatched_ibank = [r for r in ibank_rows if r['id'] not in matched_ibank_ids]
    unmatched_api = [r for r in api_rows if r['id'] not in matched_api_ids]

    return all_matches, unmatched_ibank, unmatched_api


def _is_better_display_name(ibank_name, current_name, current_display):
    """Check if iBank name is a better display name."""
    if not ibank_name or not ibank_name.strip():
        return False

    existing = current_display or current_name
    ibank_stripped = ibank_name.strip()

    if ibank_stripped.lower() == existing.lower():
        return False

    # Reject iBank names that look like internal/system strings
    lower = ibank_stripped.lower()
    if any(s in lower for s in [
        'internal transfer', 'interest from', 'interest to',
        'monzo-', 'funds from employer', 'received money from',
        'card transaction of', 'xxxxxx', 'to monzo', 'bevan',
    ]):
        return False

    # Reject if iBank name is longer (we want simpler names)
    if len(ibank_stripped) > len(existing):
        return False

    return True


def run_enrichment(conn, *, dry_run=False, institution=None):
    """Main enrichment logic."""
    cur = conn.cursor()

    # Load source_category_mapping for iBank
    cur.execute("""
        SELECT source_category, category_id, confidence
        FROM source_category_mapping
        WHERE source = 'ibank'
    """)
    scm = {}
    for source_cat, cat_id, conf in cur.fetchall():
        scm[source_cat] = (str(cat_id), float(conf))

    # Load category paths for display
    cur.execute("SELECT id, full_path FROM category")
    cat_paths = {str(r[0]): r[1] for r in cur.fetchall()}

    # Load and match transactions
    print("  Loading transactions...")
    ibank_rows, api_rows = find_all_transactions(conn, institution=institution)
    print(f"  iBank: {len(ibank_rows)}, API/CSV: {len(api_rows)}")

    print("  Matching...")
    matches, unmatched_ibank, unmatched_api = match_transactions(ibank_rows, api_rows)
    print(f"  Matched: {len(matches)}, Unmatched iBank: {len(unmatched_ibank)}, Unmatched API/CSV: {len(unmatched_api)}")

    # --- Reconciliation summary by account ---
    print("\n  Reconciliation by account:")
    acct_stats = defaultdict(lambda: {"matched": 0, "unmatched_ibank": 0, "unmatched_api": 0})
    for ib, ap in matches:
        key = f"{ib['institution']}/{ib['account_key']}"
        acct_stats[key]["matched"] += 1
    for r in unmatched_ibank:
        key = f"{r['institution']}/{r['account_key']}"
        acct_stats[key]["unmatched_ibank"] += 1
    for r in unmatched_api:
        key = f"{r['institution']}/{r['account_key']}"
        acct_stats[key]["unmatched_api"] += 1

    print(f"    {'Account':<40} {'Matched':>8} {'Unm.iBank':>10} {'Unm.API':>10}")
    print(f"    {'-'*40} {'-'*8} {'-'*10} {'-'*10}")
    for acct in sorted(acct_stats):
        s = acct_stats[acct]
        print(f"    {acct:<40} {s['matched']:>8} {s['unmatched_ibank']:>10} {s['unmatched_api']:>10}")

    # --- Merchant enrichment ---
    print("\n  Enriching canonical merchants...")

    # Group matches by canonical_merchant_id
    by_merchant = defaultdict(list)
    for ib, ap in matches:
        cm_id = str(ap['canonical_merchant_id'])
        by_merchant[cm_id].append({
            **ib,
            'cm_name': ap['cm_name'],
            'cm_current_category': ap['cm_current_category'],
            'cm_current_display': ap['cm_current_display'],
        })

    print(f"  Covering {len(by_merchant)} distinct canonical merchants")

    categories_set = 0
    display_names_set = 0
    skipped_has_category = 0
    skipped_no_ibank_cat = 0
    skipped_unmapped_cat = 0

    for cm_id, match_list in sorted(by_merchant.items(), key=lambda x: x[1][0]['cm_name']):
        first = match_list[0]
        cm_name = first['cm_name']
        has_category = first['cm_current_category'] is not None
        has_display = first['cm_current_display'] is not None

        # --- Category enrichment ---
        if has_category:
            skipped_has_category += 1
        else:
            # Find the best iBank category from all matches for this merchant
            best_cat_path = None
            best_cat_id = None
            for m in match_list:
                ibank_cat = m['ibank_category']
                if not ibank_cat:
                    continue
                primary_cat = ibank_cat.split(' | ')[0].strip()
                if primary_cat in scm:
                    cat_id, _ = scm[primary_cat]
                    best_cat_id = cat_id
                    best_cat_path = cat_paths.get(cat_id, primary_cat)
                    break  # take first valid match

            if best_cat_id:
                if dry_run:
                    print(f"    [category] {cm_name:<45} -> {best_cat_path}")
                else:
                    cur.execute("""
                        UPDATE canonical_merchant
                        SET category_hint = %s,
                            category_method = 'ibank_enrichment',
                            category_confidence = 0.90,
                            category_set_at = now()
                        WHERE id = %s AND category_hint IS NULL
                    """, (best_cat_path, cm_id))
                    categories_set += cur.rowcount
            elif any(m['ibank_category'] for m in match_list):
                skipped_unmapped_cat += 1
            else:
                skipped_no_ibank_cat += 1

        # --- Display name enrichment ---
        if not has_display:
            for m in match_list:
                if _is_better_display_name(m['raw_merchant'], cm_name, first['cm_current_display']):
                    if dry_run:
                        print(f"    [display]  {cm_name:<45} -> {m['raw_merchant']}")
                    else:
                        cur.execute(
                            "UPDATE canonical_merchant SET display_name = %s WHERE id = %s AND display_name IS NULL",
                            (m['raw_merchant'], cm_id),
                        )
                        display_names_set += cur.rowcount
                    break

    # --- Note transfer ---
    print("\n  Transferring iBank notes...")

    # Find which API/CSV transactions already have notes
    cur.execute("SELECT raw_transaction_id FROM transaction_note")
    existing_notes = {str(r[0]) for r in cur.fetchall()}

    notes_set = 0
    notes_skipped_exists = 0
    notes_skipped_empty = 0
    notes_skipped_echo = 0

    for ib, ap in matches:
        api_id = str(ap['id'])
        ibank_note = (ib.get('ibank_note') or '').strip()

        if not ibank_note:
            notes_skipped_empty += 1
            continue

        if api_id in existing_notes:
            notes_skipped_exists += 1
            continue

        # Skip notes that are just the merchant name echoed
        ibank_merchant = (ib.get('raw_merchant') or '').strip()
        if ibank_note.lower() == ibank_merchant.lower():
            notes_skipped_echo += 1
            continue

        if dry_run:
            if notes_set < 20:
                print(f"    [note] {ib['posted_at']} {ib['amount']:>10} {ibank_note[:60]}")
        else:
            cur.execute("""
                INSERT INTO transaction_note (raw_transaction_id, note, source)
                VALUES (%s, %s, 'ibank_import')
                ON CONFLICT (raw_transaction_id) DO NOTHING
            """, (api_id, ibank_note))
            notes_set += cur.rowcount

        if dry_run:
            notes_set += 1

    if not dry_run:
        conn.commit()

    print(f"\n  Enrichment results:")
    print(f"    Categories set:           {categories_set}")
    print(f"    Display names set:        {display_names_set}")
    print(f"    Skipped (has category):   {skipped_has_category}")
    print(f"    Skipped (no iBank cat):   {skipped_no_ibank_cat}")
    print(f"    Skipped (unmapped cat):   {skipped_unmapped_cat}")
    print(f"    Notes transferred:        {notes_set}")
    print(f"    Notes skipped (exists):   {notes_skipped_exists}")
    print(f"    Notes skipped (empty):    {notes_skipped_empty}")
    print(f"    Notes skipped (echo):     {notes_skipped_echo}")

    return {
        "matches": len(matches),
        "unmatched_ibank": len(unmatched_ibank),
        "unmatched_api": len(unmatched_api),
        "merchants": len(by_merchant),
        "categories_set": categories_set,
        "display_names_set": display_names_set,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Enrich canonical merchants from iBank data via amount/date matching"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing to DB")
    parser.add_argument("--institution",
                        help="Only process this institution")
    args = parser.parse_args()

    print("=== iBank Enrichment ===\n")

    conn = psycopg2.connect(settings.dsn)
    try:
        run_enrichment(conn, dry_run=args.dry_run, institution=args.institution)
    finally:
        conn.close()

    print("\nDone!")


if __name__ == "__main__":
    main()
