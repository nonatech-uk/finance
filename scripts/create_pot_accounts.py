#!/usr/bin/env python3
"""Create synthetic pot accounts from Monzo pot transfer transactions.

Monzo records pot transfers as debits/credits on the main current account.
This script creates mirrored transactions on separate pot account_refs so
each pot appears as its own account with a visible balance.

The transfers on the main account remain untouched — they correctly reduce
the current account balance. The synthetic pot transactions are the other
side of the ledger.

Idempotent: uses ON CONFLICT with transaction_ref = 'pot_mirror_{original_id}'.
"""

import json
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import settings

# Map pot IDs to human-readable account_refs
POT_ACCOUNTS = {
    "pot_0000A8w7XzKo0b1fGyC7iT": "monzo_pot_general",      # currently 0 balance
    "pot_0000AtRxDkJdUC25WBXQyg": "monzo_pot_savings",      # active savings pot
}

# Known actual balances — used to insert a reconciliation adjustment
# (covers interest earned in pot, which isn't in the transfer feed)
POT_ACTUAL_BALANCE = {
    "monzo_pot_general": 0,
    "monzo_pot_savings": 30353.15,
}


def create_pot_accounts(dry_run: bool = False) -> dict:
    conn = psycopg2.connect(settings.dsn)
    try:
        cur = conn.cursor()

        # Find all pot transfers on the main Monzo account
        cur.execute("""
            SELECT id, posted_at, amount, currency, raw_merchant, raw_memo, raw_data
            FROM active_transaction
            WHERE institution = 'monzo'
              AND account_ref = 'acc_00009cSZpPQxiG2CFWlPjF'
              AND raw_merchant LIKE 'pot_%%'
            ORDER BY posted_at
        """)
        rows = cur.fetchall()

        inserted = 0
        skipped = 0

        for row in rows:
            txn_id, posted_at, amount, currency, raw_merchant, raw_memo, raw_data = row

            # Determine which pot this belongs to
            pot_id = raw_merchant  # raw_merchant IS the pot ID for pot transfers
            account_ref = POT_ACCOUNTS.get(pot_id)
            if not account_ref:
                print(f"  WARNING: Unknown pot {pot_id}, skipping")
                skipped += 1
                continue

            # Mirror the amount: main account debit (-) becomes pot credit (+)
            mirror_amount = -amount
            mirror_ref = f"pot_mirror_{txn_id}"

            if dry_run:
                print(f"  [DRY RUN] {posted_at} {mirror_amount:>10.2f} {currency} -> {account_ref}")
                inserted += 1
                continue

            cur.execute("""
                INSERT INTO raw_transaction (
                    source, institution, account_ref, transaction_ref,
                    posted_at, amount, currency,
                    raw_merchant, raw_memo, is_dirty, raw_data
                ) VALUES (
                    'synthetic', 'monzo', %s, %s,
                    %s, %s, %s,
                    %s, %s, false, %s
                )
                ON CONFLICT (institution, account_ref, transaction_ref)
                    WHERE transaction_ref IS NOT NULL
                DO NOTHING
            """, (
                account_ref,
                mirror_ref,
                posted_at,
                mirror_amount,
                currency.strip(),
                'Pot Transfer',
                raw_memo,
                json.dumps({"mirror_of": str(txn_id), "pot_id": pot_id}),
            ))
            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1

        # Insert reconciliation adjustments for interest / untracked movements
        adjustments = 0
        for account_ref, actual_balance in POT_ACTUAL_BALANCE.items():
            cur.execute("""
                SELECT COALESCE(SUM(amount), 0)
                FROM raw_transaction
                WHERE institution = 'monzo'
                  AND account_ref = %s
                  AND source = 'synthetic'
            """, (account_ref,))
            current_sum = float(cur.fetchone()[0])
            diff = actual_balance - current_sum
            if abs(diff) < 0.01:
                continue

            adj_ref = f"pot_adjustment_{account_ref}"
            if dry_run:
                print(f"  [DRY RUN] Adjustment for {account_ref}: {diff:+.2f} (interest/reconciliation)")
                adjustments += 1
                continue

            # Use latest pot transfer date for the adjustment
            cur.execute("""
                SELECT MAX(posted_at) FROM raw_transaction
                WHERE institution = 'monzo' AND account_ref = %s AND source = 'synthetic'
            """, (account_ref,))
            latest = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO raw_transaction (
                    source, institution, account_ref, transaction_ref,
                    posted_at, amount, currency,
                    raw_merchant, raw_memo, is_dirty, raw_data
                ) VALUES (
                    'synthetic', 'monzo', %s, %s,
                    %s, %s, 'GBP',
                    %s, %s, false, %s
                )
                ON CONFLICT (institution, account_ref, transaction_ref)
                    WHERE transaction_ref IS NOT NULL
                DO UPDATE SET amount = EXCLUDED.amount, posted_at = EXCLUDED.posted_at
            """, (
                account_ref,
                adj_ref,
                latest,
                diff,
                'Interest / Reconciliation',
                f'Adjustment to match actual pot balance of {actual_balance:.2f}',
                json.dumps({"type": "reconciliation", "actual_balance": actual_balance}),
            ))
            adjustments += 1

        if not dry_run:
            conn.commit()

        return {"inserted": inserted, "skipped": skipped, "total": len(rows), "adjustments": adjustments}
    finally:
        conn.close()


def main():
    dry_run = "--dry-run" in sys.argv

    print("Creating synthetic pot accounts from Monzo transfers...")
    if dry_run:
        print("(DRY RUN — no changes will be made)\n")

    result = create_pot_accounts(dry_run=dry_run)

    print(f"\nResults:")
    print(f"  Total pot transfers found: {result['total']}")
    print(f"  Inserted: {result['inserted']}")
    print(f"  Skipped (already exist or unknown pot): {result['skipped']}")
    print(f"  Reconciliation adjustments: {result['adjustments']}")

    if not dry_run:
        # Show resulting balances
        conn = psycopg2.connect(settings.dsn)
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT account_ref, COUNT(*) as txns, SUM(amount) as balance
                FROM active_transaction
                WHERE institution = 'monzo' AND account_ref LIKE 'monzo_pot_%%'
                GROUP BY account_ref
                ORDER BY account_ref
            """)
            rows = cur.fetchall()
            if rows:
                print(f"\nPot account balances:")
                for ref, txns, balance in rows:
                    print(f"  {ref}: {txns} txns, balance = {balance:.2f}")
            else:
                print("\n  (no pot accounts visible yet — run cleaning + dedup)")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
