"""Idempotent writer for raw_transaction table."""

import json
from datetime import date
from decimal import Decimal

import psycopg2
import psycopg2.extras

from config.settings import settings


def write_monzo_transactions(transactions: list[dict], account_ref: str) -> dict:
    """
    Write Monzo transactions to raw_transaction. Idempotent via ON CONFLICT.

    Returns {"inserted": n, "skipped": n}.
    """
    if not transactions:
        return {"inserted": 0, "skipped": 0}

    conn = psycopg2.connect(settings.dsn)
    try:
        cur = conn.cursor()
        inserted = 0

        for txn in transactions:
            amount = Decimal(txn["amount"]) / 100  # pence → pounds
            posted_at = txn.get("settled") or txn.get("created")
            if posted_at:
                posted_at = posted_at[:10]  # just the date portion

            cur.execute("""
                INSERT INTO raw_transaction (
                    source, institution, account_ref, transaction_ref,
                    posted_at, amount, currency,
                    raw_merchant, raw_memo, is_dirty, raw_data
                ) VALUES (
                    'monzo_api', 'monzo', %s, %s,
                    %s, %s, %s,
                    %s, %s, false, %s
                )
                ON CONFLICT (institution, account_ref, transaction_ref)
                    WHERE transaction_ref IS NOT NULL
                DO NOTHING
            """, (
                account_ref,
                txn["id"],
                posted_at,
                amount,
                txn.get("currency", "GBP"),
                txn.get("description"),
                txn.get("notes") or None,
                json.dumps(txn),
            ))
            if cur.rowcount > 0:
                inserted += 1

        conn.commit()
        skipped = len(transactions) - inserted
        return {"inserted": inserted, "skipped": skipped}
    finally:
        conn.close()
