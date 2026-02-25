#!/usr/bin/env python3
"""Apply merchant split rules to create per-transaction merchant overrides.

Reads merchant_split_rule table, finds matching transactions (by cleaned_merchant
pattern + amount), and writes to transaction_merchant_override.

Idempotent: only creates overrides for transactions that don't already have one.
Safe to re-run.

Usage:
    python scripts/apply_split_rules.py              # apply all rules
    python scripts/apply_split_rules.py --dry-run    # preview without writing
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings


def apply_split_rules(dry_run: bool = False) -> dict:
    """Apply all merchant split rules, creating transaction_merchant_override rows.

    Returns dict with stats: {total_rules, overrides_created}.
    """
    conn = psycopg2.connect(settings.dsn)
    try:
        cur = conn.cursor()

        # Load rules ordered by priority
        cur.execute("""
            SELECT id, merchant_pattern, amount_exact, amount_min, amount_max,
                   target_merchant_id, description
            FROM merchant_split_rule
            ORDER BY priority, id
        """)
        rules = cur.fetchall()
        print(f"  Split rules loaded: {len(rules)}")

        if not rules:
            return {"total_rules": 0, "overrides_created": 0}

        total_created = 0

        for rule_id, pattern, amt_exact, amt_min, amt_max, target_id, desc in rules:
            # Build amount condition
            amount_conditions = []
            params: dict = {"pattern": pattern, "target_id": str(target_id), "rule_id": rule_id}

            if amt_exact is not None:
                amount_conditions.append("rt.amount = %(amt_exact)s")
                params["amt_exact"] = amt_exact
            if amt_min is not None:
                amount_conditions.append("rt.amount >= %(amt_min)s")
                params["amt_min"] = amt_min
            if amt_max is not None:
                amount_conditions.append("rt.amount <= %(amt_max)s")
                params["amt_max"] = amt_max

            amount_where = (" AND " + " AND ".join(amount_conditions)) if amount_conditions else ""

            if dry_run:
                # Count matching transactions without existing overrides
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM cleaned_transaction ct
                    JOIN active_transaction rt ON rt.id = ct.raw_transaction_id
                    WHERE ct.cleaned_merchant LIKE %(pattern)s
                      {amount_where}
                      AND NOT EXISTS (
                          SELECT 1 FROM transaction_merchant_override tmo
                          WHERE tmo.raw_transaction_id = rt.id
                      )
                """, params)
                count = cur.fetchone()[0]
                print(f"    Rule {rule_id}: {desc or pattern} -> {count} transactions would be overridden")
                total_created += count
            else:
                cur.execute(f"""
                    INSERT INTO transaction_merchant_override
                        (raw_transaction_id, canonical_merchant_id, split_rule_id)
                    SELECT rt.id, %(target_id)s::uuid, %(rule_id)s
                    FROM cleaned_transaction ct
                    JOIN active_transaction rt ON rt.id = ct.raw_transaction_id
                    WHERE ct.cleaned_merchant LIKE %(pattern)s
                      {amount_where}
                      AND NOT EXISTS (
                          SELECT 1 FROM transaction_merchant_override tmo
                          WHERE tmo.raw_transaction_id = rt.id
                      )
                    ON CONFLICT (raw_transaction_id) DO NOTHING
                """, params)
                created = cur.rowcount
                total_created += created
                if created:
                    print(f"    Rule {rule_id}: {desc or pattern} -> {created} overrides created")

        if not dry_run:
            conn.commit()

        print(f"  Total overrides created: {total_created}")
        return {"total_rules": len(rules), "overrides_created": total_created}

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Apply merchant split rules")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing to DB")
    args = parser.parse_args()

    print("=== Apply Merchant Split Rules ===\n")
    result = apply_split_rules(dry_run=args.dry_run)
    print(f"\nDone! Rules: {result['total_rules']}, Overrides: {result['overrides_created']}")


if __name__ == "__main__":
    main()
