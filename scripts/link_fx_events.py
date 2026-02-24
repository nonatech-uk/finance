"""Link inter-account transfers and FX conversions as economic events.

Two matching strategies:
1. FX conversions: Wise CSV pairs sharing the same batch ID with different currencies
2. Same-currency transfers: Same date, same absolute amount, same currency,
   opposite signs, different accounts. Only unambiguous 1:1 pairs are auto-linked.

Usage:
    python scripts/link_fx_events.py              # live run
    python scripts/link_fx_events.py --dry-run    # preview only
"""

import argparse
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import settings


# ── Shared helpers ──────────────────────────────────────────────────


def find_already_linked(cur):
    """Get set of transaction IDs already linked to economic events."""
    cur.execute("SELECT raw_transaction_id FROM economic_event_leg")
    return {row[0] for row in cur.fetchall()}


def _set_transfer_category(cur, txn_ids):
    """Set +Transfer category override on transaction IDs."""
    for tid in txn_ids:
        cur.execute("""
            INSERT INTO transaction_category_override (raw_transaction_id, category_path, source)
            VALUES (%s, '+Transfer', 'system')
            ON CONFLICT (raw_transaction_id)
            DO UPDATE SET category_path = '+Transfer', source = 'system', updated_at = now()
        """, (str(tid),))


# ── FX conversion pairs (Wise CSV batch ID) ────────────────────────


def find_fx_pairs(cur):
    """Find Wise CSV FX conversion pairs by batch ID."""
    cur.execute("""
        SELECT
            id, amount, currency, account_ref, posted_at,
            raw_data->>'ID' AS batch_id,
            raw_data->>'Exchange rate' AS exchange_rate,
            raw_data->>'Source amount (after fees)' AS source_amount,
            raw_data->>'Source currency' AS source_currency,
            raw_data->>'Target amount (after fees)' AS target_amount,
            raw_data->>'Target currency' AS target_currency,
            raw_data->>'Source fee amount' AS fee_amount,
            raw_data->>'Source fee currency' AS fee_currency
        FROM raw_transaction
        WHERE institution = 'wise'
          AND source = 'wise_csv'
          AND raw_data ? 'Source currency'
          AND raw_data->>'Direction' = 'NEUTRAL'
          AND raw_data->>'Source currency' != raw_data->>'Target currency'
        ORDER BY posted_at, raw_data->>'ID', amount
    """)
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    groups = defaultdict(list)
    for row in rows:
        groups[row["batch_id"]].append(row)

    pairs = []
    for batch_id, members in groups.items():
        if len(members) == 2 and members[0]["currency"] != members[1]["currency"]:
            pairs.append({
                "batch_id": batch_id,
                "txn_ids": [m["id"] for m in members],
                "amounts": [m["amount"] for m in members],
                "currencies": [m["currency"] for m in members],
                "accounts": [m["account_ref"] for m in members],
                "posted_at": members[0]["posted_at"],
                "exchange_rate": members[0]["exchange_rate"],
                "source_amount": members[0]["source_amount"],
                "source_currency": members[0]["source_currency"],
                "target_amount": members[0]["target_amount"],
                "target_currency": members[0]["target_currency"],
                "fee_amount": members[0]["fee_amount"],
                "fee_currency": members[0]["fee_currency"],
            })

    pairs.sort(key=lambda p: p["posted_at"])
    return pairs


def link_fx_pairs(cur, pairs, already_linked, dry_run=False):
    """Create economic events for FX conversion pairs."""
    created = 0
    skipped_linked = 0

    for pair in pairs:
        txn_ids = pair["txn_ids"]
        if txn_ids[0] in already_linked or txn_ids[1] in already_linked:
            skipped_linked += 1
            continue

        amounts = pair["amounts"]
        currencies = pair["currencies"]
        source_ccy = pair["source_currency"]
        target_ccy = pair["target_currency"]
        source_amt = pair["source_amount"]
        target_amt = pair["target_amount"]
        rate = pair["exchange_rate"]
        fee_amt = pair["fee_amount"]
        fee_ccy = pair["fee_currency"]

        if amounts[0] < 0:
            source_txn_id, target_txn_id = txn_ids[0], txn_ids[1]
            source_txn_amount, target_txn_amount = amounts[0], amounts[1]
            source_txn_ccy, target_txn_ccy = currencies[0], currencies[1]
        else:
            source_txn_id, target_txn_id = txn_ids[1], txn_ids[0]
            source_txn_amount, target_txn_amount = amounts[1], amounts[0]
            source_txn_ccy, target_txn_ccy = currencies[1], currencies[0]

        description = f"{source_amt} {source_ccy} -> {target_amt} {target_ccy}"

        if dry_run:
            print(f"  FX  {pair['posted_at']} | {description} | {pair['batch_id']}")
            created += 1
            continue

        cur.execute("""
            INSERT INTO economic_event (event_type, initiated_at, description, match_status)
            VALUES ('fx_conversion', %s, %s, 'auto_matched')
            RETURNING id
        """, (pair["posted_at"], description))
        event_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO economic_event_leg
                (economic_event_id, raw_transaction_id, leg_type, amount, currency)
            VALUES (%s, %s, 'source', %s, %s)
        """, (event_id, str(source_txn_id), source_txn_amount, source_txn_ccy))

        cur.execute("""
            INSERT INTO economic_event_leg
                (economic_event_id, raw_transaction_id, leg_type, amount, currency)
            VALUES (%s, %s, 'target', %s, %s)
        """, (event_id, str(target_txn_id), target_txn_amount, target_txn_ccy))

        if source_amt and target_amt and rate:
            cur.execute("""
                INSERT INTO fx_event
                    (economic_event_id, source_amount, source_currency,
                     target_amount, target_currency, achieved_rate,
                     fee_amount, fee_currency, provider)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'wise')
            """, (
                event_id,
                Decimal(source_amt),
                source_ccy,
                Decimal(target_amt),
                target_ccy,
                Decimal(rate),
                Decimal(fee_amt) if fee_amt else None,
                fee_ccy if fee_amt else None,
            ))

        _set_transfer_category(cur, [source_txn_id, target_txn_id])
        already_linked.update(txn_ids)
        created += 1

    return created, skipped_linked


# ── Same-currency transfer pairs ───────────────────────────────────


def find_same_ccy_pairs(cur):
    """Find unambiguous same-currency inter-account transfer pairs.

    Matches debits and credits across different accounts on the same date
    with the same absolute amount and currency. Only returns pairs where
    exactly one debit matches exactly one credit for that (date, amount, currency)
    combination — ambiguous many-to-many cases are skipped.
    """
    cur.execute("""
        WITH active AS (
            SELECT rt.id, rt.posted_at, rt.amount, rt.currency,
                   rt.institution, rt.account_ref
            FROM raw_transaction rt
            WHERE NOT EXISTS (
                SELECT 1 FROM dedup_group_member dgm
                WHERE dgm.raw_transaction_id = rt.id AND NOT dgm.is_preferred
            )
        ),
        debits AS (
            SELECT id, posted_at::date AS dt, amount, currency,
                   institution || '/' || account_ref AS acct
            FROM active WHERE amount < 0
        ),
        credits AS (
            SELECT id, posted_at::date AS dt, amount, currency,
                   institution || '/' || account_ref AS acct
            FROM active WHERE amount > 0
        ),
        candidates AS (
            SELECT d.id AS debit_id, c.id AS credit_id,
                   d.dt, d.amount AS debit_amount, c.amount AS credit_amount,
                   d.currency, d.acct AS debit_acct, c.acct AS credit_acct
            FROM debits d
            JOIN credits c ON d.dt = c.dt
                AND d.currency = c.currency
                AND ABS(d.amount) = c.amount
                AND d.acct != c.acct
        )
        SELECT debit_id, credit_id, dt, debit_amount, credit_amount,
               currency, debit_acct, credit_acct
        FROM candidates c
        WHERE
            -- Unambiguous: this debit only matches one credit
            (SELECT COUNT(*) FROM candidates x WHERE x.debit_id = c.debit_id) = 1
            -- And this credit only matches one debit
            AND (SELECT COUNT(*) FROM candidates x WHERE x.credit_id = c.credit_id) = 1
        ORDER BY dt, debit_acct
    """)
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def link_same_ccy_pairs(cur, pairs, already_linked, dry_run=False):
    """Create economic events for same-currency transfer pairs."""
    created = 0
    skipped_linked = 0

    for pair in pairs:
        debit_id = pair["debit_id"]
        credit_id = pair["credit_id"]

        if debit_id in already_linked or credit_id in already_linked:
            skipped_linked += 1
            continue

        amt = abs(pair["debit_amount"])
        ccy = pair["currency"]
        description = f"{amt} {ccy} | {pair['debit_acct']} -> {pair['credit_acct']}"

        if dry_run:
            print(f"  XFR {pair['dt']} | {description}")
            created += 1
            continue

        cur.execute("""
            INSERT INTO economic_event (event_type, initiated_at, description, match_status)
            VALUES ('transfer', %s, %s, 'auto_matched')
            RETURNING id
        """, (pair["dt"], description))
        event_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO economic_event_leg
                (economic_event_id, raw_transaction_id, leg_type, amount, currency)
            VALUES (%s, %s, 'source', %s, %s)
        """, (event_id, str(debit_id), pair["debit_amount"], ccy))

        cur.execute("""
            INSERT INTO economic_event_leg
                (economic_event_id, raw_transaction_id, leg_type, amount, currency)
            VALUES (%s, %s, 'target', %s, %s)
        """, (event_id, str(credit_id), pair["credit_amount"], ccy))

        _set_transfer_category(cur, [debit_id, credit_id])
        already_linked.update([debit_id, credit_id])
        created += 1

    return created, skipped_linked


# ── FD Joint → Visa payment pairs (fuzzy date) ─────────────────────


def find_visa_payment_pairs(cur):
    """Find FD Joint→Visa payment pairs with up to 5-day date tolerance.

    The Joint account debits show "FIRST DIRECT VISA" and the Visa account
    credits show "PAYMENT RECEIVED - THANK YOU". Amounts match exactly but
    the credit can post up to 5 days after the debit.

    Only unambiguous 1:1 matches are returned.
    """
    cur.execute("""
        WITH active AS (
            SELECT rt.id, rt.posted_at, rt.amount, rt.currency,
                   rt.institution, rt.account_ref, rt.raw_merchant
            FROM raw_transaction rt
            WHERE NOT EXISTS (
                SELECT 1 FROM dedup_group_member dgm
                WHERE dgm.raw_transaction_id = rt.id AND NOT dgm.is_preferred
            )
        ),
        debits AS (
            SELECT id, posted_at::date AS dt, amount, currency
            FROM active
            WHERE institution = 'first_direct' AND account_ref = 'fd_5682'
              AND amount < 0
              AND UPPER(COALESCE(raw_merchant, '')) LIKE '%%FIRST DIRECT VISA%%'
        ),
        credits AS (
            SELECT id, posted_at::date AS dt, amount, currency
            FROM active
            WHERE institution = 'first_direct' AND account_ref = 'fd_8897'
              AND amount > 0
              AND UPPER(COALESCE(raw_merchant, '')) LIKE '%%PAYMENT RECEIVED%%'
        ),
        candidates AS (
            SELECT d.id AS debit_id, c.id AS credit_id,
                   d.dt AS debit_date, c.dt AS credit_date,
                   c.dt - d.dt AS day_gap,
                   d.amount AS debit_amount, c.amount AS credit_amount,
                   d.currency
            FROM debits d
            JOIN credits c ON d.currency = c.currency
                AND ABS(d.amount) = c.amount
                AND c.dt >= d.dt
                AND c.dt <= d.dt + 5
        )
        SELECT debit_id, credit_id, debit_date, credit_date, day_gap,
               debit_amount, credit_amount, currency
        FROM candidates c
        WHERE
            (SELECT COUNT(*) FROM candidates x WHERE x.debit_id = c.debit_id) = 1
            AND (SELECT COUNT(*) FROM candidates x WHERE x.credit_id = c.credit_id) = 1
        ORDER BY debit_date
    """)
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def link_visa_payment_pairs(cur, pairs, already_linked, dry_run=False):
    """Create economic events for FD Joint→Visa payment pairs."""
    created = 0
    skipped_linked = 0

    for pair in pairs:
        debit_id = pair["debit_id"]
        credit_id = pair["credit_id"]

        if debit_id in already_linked or credit_id in already_linked:
            skipped_linked += 1
            continue

        amt = abs(pair["debit_amount"])
        ccy = pair["currency"]
        gap = pair["day_gap"]
        description = f"{amt} {ccy} | first_direct/fd_5682 -> first_direct/fd_8897"

        if dry_run:
            print(f"  VISA {pair['debit_date']} -> {pair['credit_date']} ({gap}d) | {amt} {ccy}")
            created += 1
            continue

        cur.execute("""
            INSERT INTO economic_event (event_type, initiated_at, description, match_status)
            VALUES ('transfer', %s, %s, 'auto_matched')
            RETURNING id
        """, (pair["debit_date"], description))
        event_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO economic_event_leg
                (economic_event_id, raw_transaction_id, leg_type, amount, currency)
            VALUES (%s, %s, 'source', %s, %s)
        """, (event_id, str(debit_id), pair["debit_amount"], ccy))

        cur.execute("""
            INSERT INTO economic_event_leg
                (economic_event_id, raw_transaction_id, leg_type, amount, currency)
            VALUES (%s, %s, 'target', %s, %s)
        """, (event_id, str(credit_id), pair["credit_amount"], ccy))

        _set_transfer_category(cur, [debit_id, credit_id])
        already_linked.update([debit_id, credit_id])
        created += 1

    return created, skipped_linked


# ── Entrypoint ─────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Link inter-account transfers and FX conversions")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no writes")
    args = parser.parse_args()

    conn = psycopg2.connect(settings.dsn)
    try:
        cur = conn.cursor()

        print("=== Transfer & FX Event Linker ===\n")

        already_linked = find_already_linked(cur)
        print(f"Already linked transactions: {len(already_linked)}\n")

        # Step 1: FX conversion pairs (Wise CSV batch ID)
        print("Step 1: FX conversion pairs (Wise CSV batch ID)")
        fx_pairs = find_fx_pairs(cur)
        print(f"  Found: {len(fx_pairs)} pairs")
        fx_created, fx_skipped = link_fx_pairs(cur, fx_pairs, already_linked, dry_run=args.dry_run)
        print(f"  Created: {fx_created}, Skipped: {fx_skipped}\n")

        if not args.dry_run:
            conn.commit()

        # Step 2: Same-currency transfer pairs
        print("Step 2: Same-currency transfer pairs (unambiguous only)")
        xfr_pairs = find_same_ccy_pairs(cur)
        print(f"  Found: {len(xfr_pairs)} unambiguous pairs")
        xfr_created, xfr_skipped = link_same_ccy_pairs(cur, xfr_pairs, already_linked, dry_run=args.dry_run)
        print(f"  Created: {xfr_created}, Skipped: {xfr_skipped}\n")

        if not args.dry_run:
            conn.commit()

        # Step 3: FD Joint → Visa payment pairs (fuzzy date, up to 5 days)
        print("Step 3: FD Joint -> Visa payments (fuzzy date)")
        visa_pairs = find_visa_payment_pairs(cur)
        print(f"  Found: {len(visa_pairs)} pairs")
        visa_created, visa_skipped = link_visa_payment_pairs(cur, visa_pairs, already_linked, dry_run=args.dry_run)
        print(f"  Created: {visa_created}, Skipped: {visa_skipped}\n")

        if not args.dry_run:
            conn.commit()

        print(f"=== Summary ===")
        print(f"  FX events:       {fx_created} created, {fx_skipped} skipped")
        print(f"  Transfer events: {xfr_created} created, {xfr_skipped} skipped")
        print(f"  Visa payments:   {visa_created} created, {visa_skipped} skipped")
        print(f"  Total:           {fx_created + xfr_created + visa_created} created")
        if args.dry_run:
            print("  (dry run — no changes made)")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
