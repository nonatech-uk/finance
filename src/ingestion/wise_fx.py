"""FX event builder for Wise transactions.

Creates economic_event, economic_event_leg, and fx_event records
for Wise transactions that involve currency conversion.
"""

import json
from decimal import Decimal
from typing import Dict, List, Optional

import psycopg2

from config.settings import settings


def build_fx_events(transactions: List[dict], conn) -> Dict[str, int]:
    """Create economic events and FX records for Wise transactions with exchange details.

    Args:
        transactions: List of Wise statement transactions (with raw_transaction IDs attached).
        conn: Postgres connection.

    Returns stats dict.
    """
    cur = conn.cursor()
    stats = {"fx_events": 0, "transfer_events": 0, "skipped": 0}

    for txn in transactions:
        raw_txn_id = txn.get("_raw_transaction_id")
        if not raw_txn_id:
            stats["skipped"] += 1
            continue

        exchange = txn.get("exchangeDetails")
        if not exchange:
            stats["skipped"] += 1
            continue

        from_amount = exchange.get("fromAmount", {})
        to_amount = exchange.get("toAmount", {})
        rate = exchange.get("rate")

        if not from_amount.get("value") or not to_amount.get("value"):
            stats["skipped"] += 1
            continue

        # Determine event type
        details_type = txn.get("details", {}).get("type", "")
        if from_amount.get("currency") != to_amount.get("currency"):
            event_type = "fx_conversion"
        else:
            event_type = "transfer"

        description = txn.get("details", {}).get("description", "")
        txn_date = txn.get("date", "")[:10] if txn.get("date") else None

        # Check if we already created an event for this raw_transaction
        cur.execute("""
            SELECT ee.id FROM economic_event ee
            JOIN economic_event_leg eel ON eel.economic_event_id = ee.id
            WHERE eel.raw_transaction_id = %s
        """, (raw_txn_id,))
        if cur.fetchone():
            stats["skipped"] += 1
            continue

        # Create economic event
        cur.execute("""
            INSERT INTO economic_event (event_type, initiated_at, description, match_status)
            VALUES (%s, %s, %s, 'auto_matched')
            RETURNING id
        """, (event_type, txn_date, description))
        event_id = cur.fetchone()[0]

        # Create leg for the source side (the raw transaction itself)
        amount_val = txn.get("amount", {}).get("value", 0)
        amount_cur = txn.get("amount", {}).get("currency", "")
        cur.execute("""
            INSERT INTO economic_event_leg
                (economic_event_id, raw_transaction_id, leg_type, amount, currency)
            VALUES (%s, %s, %s, %s, %s)
        """, (event_id, raw_txn_id, "source", Decimal(str(amount_val)), amount_cur))

        # Create FX event if cross-currency
        if event_type == "fx_conversion":
            fee = txn.get("totalFees", {})
            fee_amount = Decimal(str(fee.get("value", 0))) if fee.get("value") else None
            fee_currency = fee.get("currency")

            cur.execute("""
                INSERT INTO fx_event
                    (economic_event_id, source_amount, source_currency,
                     target_amount, target_currency, achieved_rate,
                     fee_amount, fee_currency, provider)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'wise')
            """, (
                event_id,
                abs(Decimal(str(from_amount["value"]))),
                from_amount["currency"],
                abs(Decimal(str(to_amount["value"]))),
                to_amount["currency"],
                Decimal(str(rate)) if rate else None,
                fee_amount,
                fee_currency,
            ))
            stats["fx_events"] += 1
        else:
            stats["transfer_events"] += 1

    conn.commit()
    return stats
