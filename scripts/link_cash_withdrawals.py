#!/usr/bin/env python3
"""Link ATM cash withdrawals to synthetic cash account transactions.

Detects ATM withdrawals from First Direct, Monzo, and Citi, then creates
matching credit transactions on the appropriate cash account (cash_gbp,
cash_eur, cash_chf, etc.). Links both sides as an economic event.

Detection rules:
  - First Direct: raw_merchant LIKE 'CASH %' AND amount < 0
      (excluding FEE, DEPOSIT, ADVANCE FEE, Etsy)
  - Monzo: raw_data->>'category' = 'cash' AND amount < 0
  - Citi: raw_merchant = 'Cash Withdrawal' AND amount < 0
  - Goldman Sachs: EXCLUDED (withdrawals are bank transfers)
  - iBank: SKIPPED (cash accounts already have iBank credit entries)

Foreign currency handling:
  - Monzo: raw_data->>'local_amount' (pence, /100) + raw_data->>'local_currency'
  - First Direct: regex parse "CHF 500.00 @ 1.1346" from merchant string
  - Domestic (no FX): abs(amount) in source currency

Idempotent: uses transaction_ref = 'cash_mirror_{source_txn_id}'.

Usage:
    python scripts/link_cash_withdrawals.py              # live run
    python scripts/link_cash_withdrawals.py --dry-run    # preview only
"""

import argparse
import json
import re
import sys
from decimal import Decimal
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import settings


# Regex for FD foreign currency in merchant string:
# e.g. "CASH  1495827291  RB Zermatt        Zermatt           CHF 500.00         @ 1.134"
FD_FX_REGEX = re.compile(r'([A-Z]{3})\s+([\d,]+\.?\d*)\s+@')


# ── Detection queries ────────────────────────────────────────────────


def find_fd_withdrawals(cur):
    """Find First Direct ATM withdrawals not yet mirrored."""
    cur.execute("""
        SELECT at.id, at.posted_at, at.amount, at.currency,
               at.raw_merchant, at.account_ref
        FROM active_transaction at
        WHERE at.institution = 'first_direct'
          AND at.raw_merchant LIKE 'CASH %%'
          AND at.amount < 0
          AND at.raw_merchant NOT LIKE '%%FEE%%'
          AND at.raw_merchant NOT LIKE '%%DEPOSIT%%'
          AND at.raw_merchant NOT LIKE '%%ADVANCE FEE%%'
          AND at.raw_merchant NOT LIKE 'Etsy%%'
          AND NOT EXISTS (
              SELECT 1 FROM raw_transaction mirror
              WHERE mirror.transaction_ref = 'cash_mirror_' || at.id::text
          )
        ORDER BY at.posted_at
    """)
    rows = cur.fetchall()
    results = []
    for txn_id, posted_at, amount, currency, raw_merchant, account_ref in rows:
        # Try to extract foreign currency from merchant string
        local_amount, local_currency = _parse_fd_fx(raw_merchant, amount, currency)
        results.append({
            "id": txn_id,
            "posted_at": posted_at,
            "amount": amount,
            "currency": currency.strip(),
            "raw_merchant": raw_merchant,
            "account_ref": account_ref,
            "institution": "first_direct",
            "local_amount": local_amount,
            "local_currency": local_currency,
        })
    return results


def _parse_fd_fx(raw_merchant: str, amount, currency: str):
    """Extract foreign currency amount from FD merchant string.

    Returns (local_amount, local_currency). Falls back to (abs(amount), currency)
    for domestic withdrawals.
    """
    match = FD_FX_REGEX.search(raw_merchant)
    if match:
        fx_ccy = match.group(1)
        fx_amount = Decimal(match.group(2).replace(",", ""))
        return fx_amount, fx_ccy
    # Domestic withdrawal
    return abs(amount), currency.strip()


def find_monzo_withdrawals(cur):
    """Find Monzo ATM withdrawals not yet mirrored."""
    cur.execute("""
        SELECT at.id, at.posted_at, at.amount, at.currency,
               at.raw_merchant, at.account_ref,
               at.raw_data->>'local_amount' AS local_amount,
               at.raw_data->>'local_currency' AS local_currency
        FROM active_transaction at
        WHERE at.institution = 'monzo'
          AND at.raw_data->>'category' = 'cash'
          AND at.amount < 0
          AND NOT EXISTS (
              SELECT 1 FROM raw_transaction mirror
              WHERE mirror.transaction_ref = 'cash_mirror_' || at.id::text
          )
        ORDER BY at.posted_at
    """)
    rows = cur.fetchall()
    results = []
    for txn_id, posted_at, amount, currency, raw_merchant, account_ref, local_amt_raw, local_ccy in rows:
        # Monzo local_amount is in pence (minor units), divide by 100
        if local_amt_raw and local_ccy:
            local_amount = abs(Decimal(local_amt_raw)) / 100
            local_currency = local_ccy.strip()
        else:
            # Fallback to source amount
            local_amount = abs(amount)
            local_currency = currency.strip()
        results.append({
            "id": txn_id,
            "posted_at": posted_at,
            "amount": amount,
            "currency": currency.strip(),
            "raw_merchant": raw_merchant,
            "account_ref": account_ref,
            "institution": "monzo",
            "local_amount": local_amount,
            "local_currency": local_currency,
        })
    return results


def find_citi_withdrawals(cur):
    """Find Citi ATM withdrawals not yet mirrored."""
    cur.execute("""
        SELECT at.id, at.posted_at, at.amount, at.currency,
               at.raw_merchant, at.account_ref
        FROM active_transaction at
        WHERE at.institution = 'citi'
          AND at.raw_merchant = 'Cash Withdrawal'
          AND at.amount < 0
          AND NOT EXISTS (
              SELECT 1 FROM raw_transaction mirror
              WHERE mirror.transaction_ref = 'cash_mirror_' || at.id::text
          )
        ORDER BY at.posted_at
    """)
    rows = cur.fetchall()
    results = []
    for txn_id, posted_at, amount, currency, raw_merchant, account_ref in rows:
        results.append({
            "id": txn_id,
            "posted_at": posted_at,
            "amount": amount,
            "currency": currency.strip(),
            "raw_merchant": raw_merchant,
            "account_ref": account_ref,
            "institution": "citi",
            "local_amount": abs(amount),
            "local_currency": currency.strip(),
        })
    return results


# ── Shared helpers ────────────────────────────────────────────────────


def _ensure_cash_account(cur, currency: str):
    """Ensure a cash account exists for the given currency, creating if needed.

    Returns the account_ref (e.g. 'cash_gbp').
    """
    ccy = currency.upper().strip()
    account_ref = f"cash_{ccy.lower()}"

    cur.execute("""
        INSERT INTO account (institution, account_ref, name, currency, account_type, scope)
        VALUES ('cash', %s, %s, %s, 'cash', 'personal')
        ON CONFLICT (institution, account_ref) WHERE account_ref IS NOT NULL
        DO NOTHING
    """, (account_ref, f"Cash ({ccy})", ccy))

    return account_ref


def _set_transfer_category(cur, txn_ids):
    """Set +Transfer category override on transaction IDs."""
    for tid in txn_ids:
        cur.execute("""
            INSERT INTO transaction_category_override (raw_transaction_id, category_path, source)
            VALUES (%s, '+Transfer', 'system')
            ON CONFLICT (raw_transaction_id)
            DO UPDATE SET category_path = '+Transfer', source = 'system', updated_at = now()
        """, (str(tid),))


def _find_already_linked(cur):
    """Get set of transaction IDs already linked to economic events."""
    cur.execute("SELECT raw_transaction_id FROM economic_event_leg")
    return {row[0] for row in cur.fetchall()}


# ── Core linking logic ────────────────────────────────────────────────


def link_cash_withdrawals(dry_run: bool = False) -> dict:
    """Find and link ATM cash withdrawals across all supported institutions."""
    conn = psycopg2.connect(settings.dsn)
    try:
        cur = conn.cursor()

        # Gather all unmirrored withdrawals
        fd_rows = find_fd_withdrawals(cur)
        monzo_rows = find_monzo_withdrawals(cur)
        citi_rows = find_citi_withdrawals(cur)

        all_withdrawals = fd_rows + monzo_rows + citi_rows
        all_withdrawals.sort(key=lambda r: r["posted_at"])

        print(f"  Found: {len(fd_rows)} FD, {len(monzo_rows)} Monzo, "
              f"{len(citi_rows)} Citi ({len(all_withdrawals)} total)")

        if not all_withdrawals:
            return {"inserted": 0, "linked": 0, "accounts_created": 0}

        already_linked = _find_already_linked(cur)
        inserted = 0
        linked = 0
        accounts_created = set()

        for w in all_withdrawals:
            source_id = w["id"]
            local_amount = w["local_amount"]
            local_currency = w["local_currency"]

            # Ensure cash account exists
            cash_account_ref = _ensure_cash_account(cur, local_currency)
            if cash_account_ref not in accounts_created:
                # Track newly referenced accounts (may or may not be new rows)
                accounts_created.add(cash_account_ref)

            mirror_ref = f"cash_mirror_{source_id}"

            if dry_run:
                fx_note = ""
                if local_currency != w["currency"]:
                    fx_note = f" (FX: {w['amount']} {w['currency']} -> {local_amount} {local_currency})"
                print(f"  [DRY RUN] {w['posted_at']} | {w['institution']:14s} | "
                      f"+{local_amount:>10} {local_currency} -> {cash_account_ref}{fx_note}")
                inserted += 1
                linked += 1
                continue

            # Insert synthetic credit on cash account
            cur.execute("""
                INSERT INTO raw_transaction (
                    source, institution, account_ref, transaction_ref,
                    posted_at, amount, currency,
                    raw_merchant, raw_memo, is_dirty, raw_data
                ) VALUES (
                    'synthetic', 'cash', %s, %s,
                    %s, %s, %s,
                    'ATM Withdrawal', %s, false, %s
                )
                ON CONFLICT (institution, account_ref, transaction_ref)
                    WHERE transaction_ref IS NOT NULL
                DO NOTHING
                RETURNING id
            """, (
                cash_account_ref,
                mirror_ref,
                w["posted_at"],
                local_amount,  # positive credit
                local_currency,
                f"ATM withdrawal from {w['institution']}/{w['account_ref']}",
                json.dumps({
                    "mirror_of": str(source_id),
                    "source_institution": w["institution"],
                    "source_account_ref": w["account_ref"],
                    "type": "atm_cash_credit",
                }),
            ))

            if cur.rowcount == 0:
                # Already existed (idempotent)
                continue

            mirror_id = cur.fetchone()[0]
            inserted += 1

            # Always set +Transfer on the cash mirror
            _set_transfer_category(cur, [mirror_id])

            # Create economic event linking source debit + cash credit
            if source_id not in already_linked:
                description = (
                    f"{abs(w['amount'])} {w['currency']} | "
                    f"{w['institution']}/{w['account_ref']} -> cash/{cash_account_ref}"
                )
                if local_currency != w["currency"]:
                    description = (
                        f"{local_amount} {local_currency} | "
                        f"{w['institution']}/{w['account_ref']} -> cash/{cash_account_ref}"
                    )

                cur.execute("""
                    INSERT INTO economic_event (event_type, initiated_at, description, match_status)
                    VALUES ('atm_withdrawal', %s, %s, 'auto_matched')
                    RETURNING id
                """, (w["posted_at"], description))
                event_id = cur.fetchone()[0]

                # Source leg (debit on bank account)
                cur.execute("""
                    INSERT INTO economic_event_leg
                        (economic_event_id, raw_transaction_id, leg_type, amount, currency)
                    VALUES (%s, %s, 'source', %s, %s)
                """, (event_id, str(source_id), w["amount"], w["currency"]))

                # Target leg (credit on cash account)
                cur.execute("""
                    INSERT INTO economic_event_leg
                        (economic_event_id, raw_transaction_id, leg_type, amount, currency)
                    VALUES (%s, %s, 'target', %s, %s)
                """, (event_id, str(mirror_id), local_amount, local_currency))

                # Set +Transfer on the source side too
                _set_transfer_category(cur, [source_id])
                already_linked.update([source_id, mirror_id])
                linked += 1

        if not dry_run:
            conn.commit()

        return {
            "inserted": inserted,
            "linked": linked,
            "accounts_created": len(accounts_created),
        }
    finally:
        conn.close()


def suppress_ibank_cash_duplicates(dry_run: bool = False) -> int:
    """Suppress iBank 'Cash Withdrawal' entries that duplicate an ATM mirror.

    When link_cash_withdrawals creates a synthetic mirror on a cash account,
    any pre-existing iBank credit for the same withdrawal is a duplicate.
    Match by (account_ref, amount ±0.01, posted_at ±1 day).

    Returns count of iBank entries suppressed.
    """
    conn = psycopg2.connect(settings.dsn)
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT ib.id
            FROM raw_transaction ib
            JOIN raw_transaction mir
                ON mir.institution = 'cash'
                AND mir.account_ref = ib.account_ref
                AND mir.source = 'synthetic'
                AND mir.transaction_ref LIKE 'cash_mirror_%%'
                AND ABS(ib.amount - mir.amount) < 0.01
                AND ABS(ib.posted_at - mir.posted_at) <= 1
            WHERE ib.institution = 'cash'
              AND ib.source = 'ibank'
              AND ib.raw_merchant = 'Cash Withdrawal'
              AND ib.amount > 0
              AND NOT EXISTS (
                  SELECT 1 FROM dedup_group_member dgm
                  WHERE dgm.raw_transaction_id = ib.id
                    AND NOT dgm.is_preferred
              )
        """)
        ids = [row[0] for row in cur.fetchall()]

        if not ids or dry_run:
            return len(ids)

        # Create single-member dedup groups marking iBank entries as non-preferred
        cur.execute("""
            WITH new_groups AS (
                INSERT INTO dedup_group (canonical_id, match_rule, confidence)
                SELECT id, 'ibank_cash_superseded', 1.0
                FROM raw_transaction
                WHERE id = ANY(%(ids)s::uuid[])
                RETURNING id AS group_id, canonical_id AS txn_id
            )
            INSERT INTO dedup_group_member (dedup_group_id, raw_transaction_id, is_preferred)
            SELECT group_id, txn_id, false
            FROM new_groups
        """, {"ids": [str(i) for i in ids]})

        conn.commit()
        return len(ids)
    finally:
        conn.close()


def recalculate_balance_resets(dry_run: bool = False) -> dict:
    """Recalculate existing balance reset adjustments to maintain target balances.

    After new transactions are added (e.g. ATM mirrors), existing reset amounts
    may be stale. This reads the target_balance from each reset's raw_data and
    recalculates the adjustment so the account balance stays at the target.

    Returns dict with counts of resets checked and updated.
    """
    conn = psycopg2.connect(settings.dsn)
    try:
        cur = conn.cursor()

        # Find all balance reset transactions, newest first per account
        cur.execute("""
            SELECT id, account_ref, transaction_ref, amount,
                   raw_data->>'target_balance' AS target_balance
            FROM raw_transaction
            WHERE institution = 'cash'
              AND transaction_ref LIKE 'cash_reset_%%'
              AND raw_data->>'target_balance' IS NOT NULL
            ORDER BY account_ref, posted_at DESC
        """)
        resets = cur.fetchall()

        if not resets:
            return {"checked": 0, "updated": 0}

        # Group by account — only recalculate the NEWEST reset per account
        # (older resets are already baked into the balance)
        seen_accounts = set()
        checked = 0
        updated = 0

        for reset_id, account_ref, txn_ref, current_amount, target_str in resets:
            if account_ref in seen_accounts:
                continue
            seen_accounts.add(account_ref)
            checked += 1

            target_balance = Decimal(target_str)

            # Current balance excluding this reset
            cur.execute("""
                SELECT COALESCE(SUM(amount), 0)
                FROM active_transaction
                WHERE institution = 'cash' AND account_ref = %s
                  AND transaction_ref IS DISTINCT FROM %s
            """, (account_ref, txn_ref))
            current_balance = cur.fetchone()[0]

            new_adjustment = target_balance - current_balance

            if abs(new_adjustment - current_amount) < Decimal("0.01"):
                continue

            if dry_run:
                print(f"  [DRY RUN] {account_ref}: {current_amount} -> {new_adjustment} "
                      f"(target={target_balance}, balance_excl_reset={current_balance})")
                updated += 1
                continue

            cur.execute("""
                UPDATE raw_transaction
                SET amount = %s
                WHERE id = %s
            """, (new_adjustment, reset_id))
            print(f"  {account_ref}: adjusted {current_amount} -> {new_adjustment} "
                  f"(target={target_balance})")
            updated += 1

        if not dry_run and updated > 0:
            conn.commit()

        return {"checked": checked, "updated": updated}
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Link ATM cash withdrawals to synthetic cash account transactions"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no writes")
    args = parser.parse_args()

    print("=== ATM Cash Withdrawal Linker ===\n")

    if args.dry_run:
        print("(DRY RUN -- no changes will be made)\n")

    result = link_cash_withdrawals(dry_run=args.dry_run)

    print(f"\nResults:")
    print(f"  Synthetic transactions inserted: {result['inserted']}")
    print(f"  Economic events linked: {result['linked']}")
    print(f"  Cash accounts referenced: {result['accounts_created']}")

    if not args.dry_run and result["inserted"] > 0:
        # Show resulting cash balances
        conn = psycopg2.connect(settings.dsn)
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT account_ref, COUNT(*) as txns, SUM(amount) as balance
                FROM active_transaction
                WHERE institution = 'cash'
                GROUP BY account_ref
                ORDER BY account_ref
            """)
            rows = cur.fetchall()
            if rows:
                print(f"\nCash account balances:")
                for ref, txns, balance in rows:
                    print(f"  {ref}: {txns} txns, balance = {balance:.2f}")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
