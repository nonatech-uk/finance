#!/usr/bin/env python3
"""Card statement PDF transaction loader.

Loads parsed card transactions (from parse_amex_statements.py CSV output)
into raw_transaction. Covers Amex (corporate + personal), Citibank commercial.

Usage:
    python scripts/card_statement_load.py /Users/stu/tmp/card_transactions.csv
    python scripts/card_statement_load.py --dry-run /Users/stu/tmp/card_transactions.csv
"""

import argparse
import csv
import hashlib
import json
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings

# Map card numbers to (institution, account_ref) pairs
CARD_ACCOUNT_MAP = {
    # GS Business Account (corporate Amex via Goldman Sachs)
    '3742-924216-61001': ('amex', 'gs_corp_61001'),
    '3742-924216-62009': ('amex', 'gs_corp_62009'),
    '3742-924216-63007': ('amex', 'gs_corp_63007'),
    # Credit Suisse / CSFB corporate
    '3742-956770-12001': ('amex', 'cs_corp_12001'),
    '3785-052496-81000': ('amex', 'cs_corp_81000'),
    # Deutsche Bank corporate
    'XXXX-XXXXXX-41006': ('amex', 'db_corp_41006'),
    # Personal UK Platinum
    '3742-893067-13014': ('amex', 'uk_plat_13014'),
    '3742-893067-15001': ('amex', 'uk_plat_15001'),
    '3742-893067-17007': ('amex', 'uk_plat_17007'),
    # Personal US Platinum
    'XXXX-XXXXX7-71000': ('amex', 'us_plat_71000'),
    # Citibank commercial
    'XXXX-XXXX-XX30-3908': ('citibank', 'citi_corp_3908'),
}


def make_transaction_ref(row):
    """Generate a stable, unique transaction ref from row data."""
    # Combine filename + transaction_date + amount + description for uniqueness
    key = f"{row['filename']}|{row['transaction_date']}|{row['amount']}|{row['description']}"
    return f"pdf_{hashlib.sha256(key.encode('utf-8')).hexdigest()[:16]}"


def parse_csv(filepath):
    """Parse the card_transactions.csv and return grouped transactions."""
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    transactions = []
    skipped = 0

    for row in rows:
        card = row.get('card_number', '')
        mapping = CARD_ACCOUNT_MAP.get(card)
        if not mapping:
            skipped += 1
            continue

        institution, account_ref = mapping

        # Amount: negative for debits, positive for credits (finance DB convention)
        amount_str = row.get('amount', '')
        if not amount_str:
            skipped += 1
            continue

        amount = Decimal(amount_str)
        if row.get('is_credit') == 'True':
            # Credits are positive (payments received)
            pass
        else:
            # Debits (charges) are negative
            amount = -amount

        posted_at = row.get('transaction_date')
        if not posted_at:
            skipped += 1
            continue

        currency = row.get('currency', 'GBP')

        # Build raw_data with all original fields
        raw_data = {k: v for k, v in row.items() if v}

        raw_merchant = row.get('description', '')
        raw_memo = row.get('category', '') or None

        transactions.append({
            'institution': institution,
            'account_ref': account_ref,
            'transaction_ref': make_transaction_ref(row),
            'posted_at': posted_at,
            'amount': amount,
            'currency': currency,
            'raw_merchant': raw_merchant,
            'raw_memo': raw_memo,
            'raw_data': raw_data,
        })

    return transactions, skipped


def write_transactions(txns, conn):
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
                'card_statement_pdf', %s, %s, %s,
                %s, %s, %s,
                %s, %s, false, %s
            )
            ON CONFLICT (institution, account_ref, transaction_ref)
                WHERE transaction_ref IS NOT NULL
            DO NOTHING
            RETURNING id
        """, (
            txn['institution'],
            txn['account_ref'],
            txn['transaction_ref'],
            txn['posted_at'],
            txn['amount'],
            txn['currency'],
            txn['raw_merchant'],
            txn['raw_memo'],
            json.dumps(txn['raw_data']),
        ))

        result = cur.fetchone()
        if result:
            inserted += 1

    conn.commit()
    return {'inserted': inserted, 'skipped': len(txns) - inserted}


def main():
    parser = argparse.ArgumentParser(description='Load card statement PDF transactions')
    parser.add_argument('file', help='Path to card_transactions.csv')
    parser.add_argument('--dry-run', action='store_true', help='Parse and report only')
    args = parser.parse_args()

    print('=== Card Statement PDF Loader ===\n')

    filepath = Path(args.file)
    if not filepath.exists():
        print(f'ERROR: File not found: {filepath}')
        sys.exit(1)

    # Parse
    transactions, parse_skipped = parse_csv(str(filepath))

    # Group by account
    by_account = {}
    for txn in transactions:
        key = f"{txn['institution']}/{txn['account_ref']}"
        by_account.setdefault(key, []).append(txn)

    print(f'  Parsed: {len(transactions)} transactions ({parse_skipped} skipped)\n')

    for acct, txns in sorted(by_account.items()):
        dates = [t['posted_at'] for t in txns]
        currencies = set(t['currency'] for t in txns)
        print(f'  {acct}: {len(txns)} txns ({min(dates)} to {max(dates)}) {",".join(currencies)}')

    if args.dry_run:
        print('\n  [DRY RUN] No data written.')
        return

    # Write
    conn = psycopg2.connect(settings.dsn)
    try:
        total_inserted = 0
        total_skipped = 0

        for acct, txns in sorted(by_account.items()):
            result = write_transactions(txns, conn)
            print(f'\n  {acct}: {result["inserted"]} new, {result["skipped"]} duplicates.')
            total_inserted += result['inserted']
            total_skipped += result['skipped']

        print(f'\n=== Done ===')
        print(f'Total: {total_inserted} new, {total_skipped} duplicates.')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
