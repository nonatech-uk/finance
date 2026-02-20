#!/usr/bin/env python3
"""Wise API transaction loader (activities-based).

Primary data source for Wise transactions. Uses the activities endpoint
with monthly windowing, then fetches rich detail per card-transaction
or transfer (MCC codes, merchant location, fee breakdown).

For balance reconciliation via CSV, use wise_csv_load.py separately.

Usage:
    python scripts/wise_bulk_load.py
    python scripts/wise_bulk_load.py --since 2024-01-01
    python scripts/wise_bulk_load.py --currency GBP
    python scripts/wise_bulk_load.py --no-fx --skip-detail
    python scripts/wise_bulk_load.py --dry-run
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings
from src.ingestion.wise import (
    get_profiles,
    get_balances,
    fetch_activities,
    enrich_activities,
)


def _parse_amount_string(amount_str: str) -> Tuple[Optional[Decimal], Optional[str]]:
    """Parse '26.70 CHF' or '<positive>+ 1,400 GBP</positive>' into (Decimal, currency).

    Returns (None, None) for empty strings.
    """
    if not amount_str or not amount_str.strip():
        return None, None

    # Strip HTML tags and leading +/- signs
    cleaned = _strip_html(amount_str).lstrip("+ ")
    parts = cleaned.strip().split()
    if len(parts) < 2:
        return None, None

    try:
        # Handle commas in numbers like "1,234.56"
        num_str = parts[0].replace(",", "")
        return Decimal(num_str), parts[-1]
    except InvalidOperation:
        return None, None


def _strip_html(text: str) -> str:
    """Strip HTML tags from Wise activity titles."""
    return re.sub(r"<[^>]+>", "", text).strip()


def _is_positive_amount(amount_str: str) -> bool:
    """Check if the amount string has Wise's <positive> tag, indicating incoming money."""
    return "<positive>" in amount_str


def parse_activity(activity: dict) -> Optional[dict]:
    """Convert a Wise activity into a raw_transaction-shaped dict.

    Actual API activity structure:
    {
      "id": "TU9ORVRBUllfQUNUSVZJVFk6Ojg2NTUyMTU6OkNBUkRfVFJBTlNBQ1RJT046OjM0MDQxNTQ3ODA=",
      "type": "CARD_PAYMENT",
      "resource": {"type": "CARD_TRANSACTION", "id": "3404154780"},
      "title": "<strong>Migrol</strong>",
      "description": "",
      "primaryAmount": "26.70 CHF",
      "secondaryAmount": "",
      "status": "COMPLETED",
      "createdOn": "2026-01-31T08:39:16.608Z",
      "updatedOn": "2026-02-01T01:46:43.083Z",
      "_detail": { ... rich detail from card-transaction/transfer endpoint }
    }
    """
    status = activity.get("status", "")
    if status not in ("COMPLETED", "OUTGOING_PAYMENT_SENT", "FUNDS_CONVERTED"):
        return None

    activity_id = activity.get("id", "")
    if not activity_id:
        return None

    # Parse "26.70 CHF" -> (Decimal("26.70"), "CHF")
    primary_str = activity.get("primaryAmount", "")
    amount, currency = _parse_amount_string(primary_str)
    if amount is None or not currency:
        return None

    account_ref = f"wise_{currency}"

    activity_type = activity.get("type", "")

    # Date — use createdOn or updatedOn
    created_on = activity.get("createdOn", "")
    updated_on = activity.get("updatedOn", "")
    date_str = updated_on or created_on
    posted_at = date_str[:10] if date_str else None

    # Merchant from title (strip HTML tags)
    title_raw = activity.get("title", "")
    title = _strip_html(title_raw)
    raw_merchant = title

    detail = activity.get("_detail", {}) or {}
    resource = activity.get("resource", {}) or {}
    resource_type = resource.get("type", "")
    resource_id = resource.get("id")

    # Extract richer merchant info from card transaction detail
    if resource_type == "CARD_TRANSACTION" and detail:
        merchant_info = detail.get("merchant", {})
        if merchant_info and merchant_info.get("name"):
            raw_merchant = merchant_info["name"]

    # Extract memo/description
    desc = activity.get("description", "")
    raw_memo = desc if desc and desc != title else None

    # Parse secondary amount for FX detection
    secondary_str = activity.get("secondaryAmount", "")
    secondary_amount, secondary_currency = _parse_amount_string(secondary_str)

    # Build raw_data blob preserving full API response
    raw_data = {
        "activity_id": activity_id,
        "activity_type": activity.get("type", ""),
        "resource_type": resource_type,
        "resource_id": resource_id,
        "status": status,
        "title": title,
        "title_raw": title_raw,
        "description": desc,
        "primary_amount_str": primary_str,
        "secondary_amount_str": secondary_str,
        "created_on": created_on,
        "updated_on": updated_on,
        "_api_source": True,
    }

    # Merge card transaction detail if available
    if resource_type == "CARD_TRANSACTION" and detail:
        merchant = detail.get("merchant", {}) or {}
        raw_data["card_detail"] = {
            "merchant": merchant,
            "mcc_code": merchant.get("mcc"),
            "auth_method": (detail.get("providerDetails") or {}).get("paymentMethod"),
            "fees": detail.get("fees", {}),
            "exchange_rate": (detail.get("exchangeDetails") or {}).get("rate"),
            "scheme": detail.get("cardScheme"),
        }
        if merchant.get("name"):
            raw_data["merchant_name"] = merchant["name"]
            raw_data["merchant_city"] = merchant.get("city")
            raw_data["merchant_country"] = merchant.get("country")
            raw_data["mcc_code"] = merchant.get("mcc")

    # Merge transfer detail if available
    if resource_type == "TRANSFER" and detail:
        raw_data["transfer_detail"] = {
            "source_amount": detail.get("sourceAmount"),
            "source_currency": detail.get("sourceCurrency"),
            "target_amount": detail.get("targetAmount"),
            "target_currency": detail.get("targetCurrency"),
            "rate": detail.get("rate"),
            "fee": detail.get("fee"),
            "target_name": (detail.get("targetAccount") or {}).get("name"),
        }

    # Build FX info
    is_fx = False
    fx_info = {}
    if secondary_currency and secondary_currency != currency:
        is_fx = True
        # For INTERBALANCE: primary is target (received), secondary is source (sent)
        # For others: primary is source (spent), secondary is in other currency
        if activity_type == "INTERBALANCE":
            fx_info = {
                "source_currency": secondary_currency,
                "target_currency": currency,
                "source_amount": abs(secondary_amount) if secondary_amount else Decimal("0"),
                "target_amount": abs(amount),
            }
        else:
            fx_info = {
                "source_currency": currency,
                "target_currency": secondary_currency,
                "source_amount": abs(amount),
                "target_amount": abs(secondary_amount) if secondary_amount else Decimal("0"),
            }
        # Get rate from detail
        if resource_type == "CARD_TRANSACTION" and detail:
            exchange = detail.get("exchangeDetails") or {}
            if exchange.get("rate"):
                try:
                    fx_info["rate"] = Decimal(str(exchange["rate"]))
                except InvalidOperation:
                    pass
            fees = detail.get("fees")
            if fees and isinstance(fees, list):
                try:
                    total_fee = sum(
                        Decimal(str(f.get("amount", {}).get("value", 0)))
                        for f in fees if isinstance(f, dict)
                    )
                    fx_info["fee_amount"] = total_fee
                except (InvalidOperation, TypeError):
                    pass
        elif resource_type == "TRANSFER" and detail:
            if detail.get("rate"):
                try:
                    fx_info["rate"] = Decimal(str(detail["rate"]))
                except InvalidOperation:
                    pass
            fee = detail.get("fee")
            if fee:
                try:
                    fx_info["fee_amount"] = Decimal(str(fee))
                except InvalidOperation:
                    pass

    # Determine sign: Wise API returns unsigned amounts.
    # Incoming money has <positive> tag in primaryAmount string.
    # INTERBALANCE "Moved" is incoming to the primary currency balance.
    # Everything else (card payments, sent transfers, withdrawals) is outgoing.
    primary_str_raw = activity.get("primaryAmount", "")
    is_incoming = _is_positive_amount(primary_str_raw)

    # INTERBALANCE is always a credit to the primary currency balance
    if activity_type == "INTERBALANCE":
        is_incoming = True

    if not is_incoming and amount > 0:
        amount = -amount

    return {
        "transaction_ref": activity_id,
        "account_ref": account_ref,
        "currency": currency,
        "amount": amount,
        "posted_at": posted_at,
        "raw_merchant": raw_merchant,
        "raw_memo": raw_memo,
        "raw_data": raw_data,
        "is_fx": is_fx,
        "fx_info": fx_info,
    }


def write_transactions(txns: List[dict], conn) -> Dict[str, int]:
    """Write parsed Wise activities to raw_transaction. Idempotent.

    Also attaches _raw_transaction_id to each txn for FX processing.
    """
    cur = conn.cursor()
    inserted = 0

    for txn in txns:
        cur.execute("""
            INSERT INTO raw_transaction (
                source, institution, account_ref, transaction_ref,
                posted_at, amount, currency,
                raw_merchant, raw_memo, is_dirty, raw_data
            ) VALUES (
                'wise_api', 'wise', %s, %s,
                %s, %s, %s,
                %s, %s, false, %s
            )
            ON CONFLICT (institution, account_ref, transaction_ref)
                WHERE transaction_ref IS NOT NULL
            DO NOTHING
            RETURNING id
        """, (
            txn["account_ref"],
            txn["transaction_ref"],
            txn["posted_at"],
            txn["amount"],
            txn["currency"],
            txn["raw_merchant"],
            txn["raw_memo"],
            json.dumps(txn["raw_data"], default=str),
        ))

        result = cur.fetchone()
        if result:
            txn["_raw_transaction_id"] = result[0]
            inserted += 1
        else:
            # Already exists — fetch ID for FX processing
            cur.execute("""
                SELECT id FROM raw_transaction
                WHERE institution = 'wise' AND account_ref = %s AND transaction_ref = %s
            """, (txn["account_ref"], txn["transaction_ref"]))
            existing = cur.fetchone()
            if existing:
                txn["_raw_transaction_id"] = existing[0]

    conn.commit()
    skipped = len(txns) - inserted
    return {"inserted": inserted, "skipped": skipped}


def build_api_fx_events(txns: List[dict], conn) -> Dict[str, int]:
    """Create economic_event + fx_event for activities with cross-currency amounts."""
    cur = conn.cursor()
    stats = {"fx_events": 0, "skipped": 0}

    for txn in txns:
        if not txn.get("is_fx"):
            continue

        raw_txn_id = txn.get("_raw_transaction_id")
        if not raw_txn_id:
            stats["skipped"] += 1
            continue

        fx = txn.get("fx_info", {})
        if not fx:
            stats["skipped"] += 1
            continue

        # Check if event already exists
        cur.execute("""
            SELECT ee.id FROM economic_event ee
            JOIN economic_event_leg eel ON eel.economic_event_id = ee.id
            WHERE eel.raw_transaction_id = %s
        """, (raw_txn_id,))
        if cur.fetchone():
            stats["skipped"] += 1
            continue

        source_cur = fx.get("source_currency", "")
        target_cur = fx.get("target_currency", "")
        source_amt = fx.get("source_amount", Decimal("0"))
        target_amt = fx.get("target_amount", Decimal("0"))
        rate = fx.get("rate")
        fee_amount = fx.get("fee_amount")

        description = f"{source_amt} {source_cur} -> {target_amt} {target_cur}"

        # Create economic event
        cur.execute("""
            INSERT INTO economic_event (event_type, initiated_at, description, match_status)
            VALUES ('fx_conversion', %s, %s, 'auto_matched')
            RETURNING id
        """, (txn["posted_at"], description))
        event_id = cur.fetchone()[0]

        # Create leg
        cur.execute("""
            INSERT INTO economic_event_leg
                (economic_event_id, raw_transaction_id, leg_type, amount, currency)
            VALUES (%s, %s, 'source', %s, %s)
        """, (event_id, raw_txn_id, -abs(source_amt), source_cur))

        # Create FX event
        cur.execute("""
            INSERT INTO fx_event
                (economic_event_id, source_amount, source_currency,
                 target_amount, target_currency, achieved_rate,
                 fee_amount, fee_currency, provider)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'wise')
        """, (
            event_id,
            abs(source_amt),
            source_cur,
            abs(target_amt),
            target_cur,
            rate,
            fee_amount if fee_amount else None,
            source_cur if fee_amount else None,
        ))
        stats["fx_events"] += 1

    conn.commit()
    return stats


def main():
    parser = argparse.ArgumentParser(description="Load Wise transactions via API (activities)")
    parser.add_argument("--currency", help="Only load this currency (e.g. GBP)")
    parser.add_argument("--since", help="Fetch from this date (YYYY-MM-DD)")
    parser.add_argument("--no-fx", action="store_true", help="Skip FX event creation")
    parser.add_argument("--skip-detail", action="store_true",
                        help="Skip fetching card-transaction/transfer detail")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only")
    args = parser.parse_args()

    since = None
    if args.since:
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    print("=== Wise API Loader (Activities) ===\n")

    # Get profile
    profiles = get_profiles()
    profile = next(p for p in profiles if p["type"] == "PERSONAL")
    profile_id = profile["id"]
    print(f"Profile: {profile_id} ({profile['type']})")

    # Get balances for info
    balances = get_balances(profile_id)
    print(f"Balances: {', '.join(b['currency'] for b in balances)}\n")

    # Fetch all activities with monthly windowing
    print("Step 1: Fetching activities...")
    activities = fetch_activities(profile_id, since=since)
    print(f"\n  Total activities: {len(activities)}")

    # Enrich with detail endpoints
    if not args.skip_detail:
        print("\nStep 2: Enriching with detail data...")
        activities = enrich_activities(profile_id, activities)
    else:
        print("\nStep 2: Skipping detail enrichment (--skip-detail)")

    # Parse activities into transaction dicts
    print("\nStep 3: Parsing activities...")
    txns = []
    skipped_status = 0
    skipped_currency = 0
    skipped_parse = 0

    for activity in activities:
        parsed = parse_activity(activity)
        if parsed is None:
            skipped_status += 1
            continue

        if args.currency and parsed["currency"] != args.currency.upper():
            skipped_currency += 1
            continue

        txns.append(parsed)

    print(f"  Parsed: {len(txns)} transactions")
    print(f"  Skipped: {skipped_status} (non-completed/unparseable), "
          f"{skipped_currency} (currency filter)")

    # Summary by currency
    from collections import Counter
    by_currency = Counter(t["currency"] for t in txns)
    for cur, count in sorted(by_currency.items()):
        print(f"    {cur}: {count}")

    fx_txns = [t for t in txns if t["is_fx"]]
    print(f"  Cross-currency (FX): {len(fx_txns)}")

    # Enrichment stats
    with_detail = sum(1 for t in txns
                      if t["raw_data"].get("card_detail") or t["raw_data"].get("transfer_detail"))
    print(f"  With rich detail: {with_detail}")

    if args.dry_run:
        print("\n  [DRY RUN] No data written.")
        # Show sample transactions
        for t in txns[:5]:
            fx_marker = " [FX]" if t["is_fx"] else ""
            detail_marker = ""
            if t["raw_data"].get("card_detail"):
                mcc = t["raw_data"].get("mcc_code", "")
                detail_marker = f" MCC:{mcc}" if mcc else " +card_detail"
            elif t["raw_data"].get("transfer_detail"):
                detail_marker = " +transfer_detail"
            print(f"    {t['posted_at']} {t['amount']:>10} {t['currency']} "
                  f"'{t['raw_merchant']}'{fx_marker}{detail_marker}")
        return

    # Write to DB
    print("\nStep 4: Writing to raw_transaction...")
    conn = psycopg2.connect(settings.dsn)
    try:
        result = write_transactions(txns, conn)
        print(f"  Written: {result['inserted']} new, {result['skipped']} duplicates.")

        # FX events
        if not args.no_fx and fx_txns:
            print("\nStep 5: Creating FX events...")
            fx_stats = build_api_fx_events(txns, conn)
            print(f"  FX events: {fx_stats['fx_events']}, skipped: {fx_stats['skipped']}")

        # Summary
        cur = conn.cursor()
        cur.execute("""
            SELECT source, count(*), min(posted_at), max(posted_at)
            FROM raw_transaction
            WHERE institution = 'wise'
            GROUP BY source
        """)
        print("\n  Wise transactions in DB:")
        for row in cur.fetchall():
            print(f"    {row[0]}: {row[1]} ({row[2]} to {row[3]})")

        print("\n=== Done ===")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
