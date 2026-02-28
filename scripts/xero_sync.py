#!/usr/bin/env python3
"""Push business transactions to Xero.

Queries unsynced business-scope transactions from the finance system,
maps categories to Xero account codes, and creates BankTransactions
via the Xero API.

Usage:
    python scripts/xero_sync.py                  # interactive auth
    python scripts/xero_sync.py --headless        # token refresh only (for daily sync)
    python scripts/xero_sync.py --dry-run         # show what would be pushed
    python scripts/xero_sync.py --since 2025-06-01
"""

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings
from src.ingestion.xero import (
    authenticate, build_bank_transaction, create_bank_transactions,
    get_bank_transactions, AuthRequiredError,
)

BATCH_SIZE = 50


def fetch_unsynced_transactions(conn, since: date | None = None) -> list[dict]:
    """Fetch business transactions not yet synced to Xero."""
    cur = conn.cursor()

    since_clause = ""
    params: dict = {}
    if since:
        since_clause = "AND rt.posted_at >= %(since)s"
        params["since"] = since

    cur.execute(f"""
        SELECT
            rt.id,
            rt.posted_at,
            rt.amount,
            rt.currency,
            rt.raw_merchant,
            rt.raw_memo,
            ct.cleaned_merchant,
            COALESCE(cm_override.display_name, cm_override.name,
                     cm.display_name, cm.name) AS merchant_name,
            COALESCE(tcat.full_path, cat_override.full_path, cat.full_path) AS category_path
        FROM active_transaction rt
        JOIN account a
            ON a.institution = rt.institution AND a.account_ref = rt.account_ref
        LEFT JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
        LEFT JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        LEFT JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        LEFT JOIN transaction_merchant_override tmo ON tmo.raw_transaction_id = rt.id
        LEFT JOIN canonical_merchant cm_override ON cm_override.id = tmo.canonical_merchant_id
        LEFT JOIN category cat ON cat.full_path = cm.category_hint
        LEFT JOIN category cat_override ON cat_override.full_path = cm_override.category_hint
        LEFT JOIN transaction_category_override tco ON tco.raw_transaction_id = rt.id
        LEFT JOIN category tcat ON tcat.full_path = tco.category_path
        LEFT JOIN xero_sync_log xsl ON xsl.raw_transaction_id = rt.id
        WHERE a.scope = 'business'
          AND a.is_archived IS NOT TRUE
          AND xsl.id IS NULL
          {since_clause}
        ORDER BY rt.posted_at, rt.id
    """, params)

    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def load_account_mappings(conn) -> dict[str, str]:
    """Load category_path -> xero_account_code mappings."""
    cur = conn.cursor()
    cur.execute("SELECT category_path, xero_account_code FROM xero_account_mapping")
    return {row[0]: row[1] for row in cur.fetchall()}


def resolve_account_code(category_path: str | None, mappings: dict[str, str]) -> str:
    """Find the best matching Xero account code for a category path.

    Tries exact match first, then walks up the hierarchy.
    Falls back to the configured default account code.
    """
    if category_path and category_path in mappings:
        return mappings[category_path]

    # Walk up the hierarchy: "Expenses:Office:Supplies" -> "Expenses:Office" -> "Expenses"
    if category_path:
        parts = category_path.split(":")
        for i in range(len(parts) - 1, 0, -1):
            parent = ":".join(parts[:i])
            if parent in mappings:
                return mappings[parent]

    return settings.xero_default_account_code


def record_sync(conn, raw_transaction_id: str, xero_transaction_id: str):
    """Record a successful sync in xero_sync_log."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO xero_sync_log (raw_transaction_id, xero_transaction_id)
        VALUES (%s, %s)
        ON CONFLICT (raw_transaction_id) DO NOTHING
    """, (str(raw_transaction_id), xero_transaction_id))


def sync_to_xero(headless: bool = False, since: date | None = None, dry_run: bool = False) -> dict:
    """Main sync entry point.

    Returns {"pushed": n, "skipped": n, "failed": n, "errors": [...]}.
    """
    access_token = authenticate(headless=headless)

    bank_account_id = settings.xero_bank_account_id
    if not bank_account_id:
        raise RuntimeError(
            "XERO_BANK_ACCOUNT_ID not configured. Run: python scripts/xero_setup.py"
        )

    conn = psycopg2.connect(settings.dsn)
    try:
        txns = fetch_unsynced_transactions(conn, since=since)
        if not txns:
            print("  No unsynced business transactions found.")
            return {"pushed": 0, "skipped": 0, "failed": 0, "errors": []}

        print(f"  Found {len(txns)} unsynced transactions.")
        mappings = load_account_mappings(conn)

        # Filter out zero-amount transactions (pre-auths, holds)
        skipped_zero = [t for t in txns if float(t["amount"]) == 0]
        txns = [t for t in txns if float(t["amount"]) != 0]
        if skipped_zero:
            print(f"  Skipped {len(skipped_zero)} zero-amount transactions (pre-auths).")
            # Mark them as synced so they don't come back
            for t in skipped_zero:
                record_sync(conn, t["id"], "skipped-zero-amount")
            conn.commit()

        if not txns:
            print("  No non-zero transactions to push.")
            return {"pushed": 0, "skipped": len(skipped_zero), "failed": 0, "errors": []}

        # Dedup: fetch existing Xero transactions and build lookup
        # so we don't re-push transactions already present (e.g. from bank feed)
        print("  Fetching existing Xero transactions for dedup...")
        existing = get_bank_transactions(access_token, bank_account_id)
        print(f"  Found {len(existing)} existing transactions in Xero.")

        # Build lookup: (date_str, type, amount_str) -> list of xero IDs
        # Xero dates come as /Date(epoch)/ — parse to YYYY-MM-DD
        from datetime import datetime as _dt, timezone as _tz
        import re as _re

        def _parse_xero_date(d: str) -> str:
            m = _re.search(r'/Date\((\d+)', d)
            if m:
                return _dt.fromtimestamp(int(m.group(1)) / 1000, tz=_tz.utc).strftime('%Y-%m-%d')
            return d[:10]

        existing_lookup: dict[tuple, list[str]] = {}
        for xt in existing:
            key = (
                _parse_xero_date(xt.get("Date", "")),
                xt.get("Type", ""),
                f"{float(xt.get('Total', 0)):.2f}",
            )
            existing_lookup.setdefault(key, []).append(xt["BankTransactionID"])

        # Build Xero transactions, dedup against existing
        xero_txns = []
        txn_map = {}  # index -> our transaction id
        matched = 0
        dedup_used: dict[tuple, int] = {}  # track how many times we've matched each key

        for txn in txns:
            merchant = txn["merchant_name"] or txn["cleaned_merchant"] or txn["raw_merchant"] or "Unknown"
            amount = float(txn["amount"])
            txn_type = "SPEND" if amount < 0 else "RECEIVE"
            account_code = resolve_account_code(txn["category_path"], mappings)
            description = txn["raw_memo"] or ""

            # Check if this transaction already exists in Xero
            dedup_key = (txn["posted_at"].strftime('%Y-%m-%d'), txn_type, f"{abs(amount):.2f}")
            xero_ids = existing_lookup.get(dedup_key, [])
            used_count = dedup_used.get(dedup_key, 0)

            if used_count < len(xero_ids):
                # Match found — record in sync log, don't push
                xero_id = xero_ids[used_count]
                dedup_used[dedup_key] = used_count + 1
                record_sync(conn, txn["id"], f"matched-existing-{xero_id}")
                matched += 1
                continue

            i = len(xero_txns)
            xero_txn = build_bank_transaction(
                txn_type=txn_type,
                merchant=merchant,
                bank_account_id=bank_account_id,
                date=txn["posted_at"].isoformat(),
                amount=abs(amount),
                account_code=account_code,
                reference=str(txn["id"]),
                description=description,
            )
            xero_txns.append(xero_txn)
            txn_map[i] = txn["id"]

        if matched:
            conn.commit()
            print(f"  Dedup: {matched} transactions matched existing Xero entries (skipped).")

        if not xero_txns:
            print("  All transactions matched existing Xero entries. Nothing to push.")
            return {"pushed": 0, "skipped": matched + len(skipped_zero), "failed": 0, "errors": []}

        if dry_run:
            print(f"\n  [DRY RUN] Would push {len(xero_txns)} transactions:")
            for i, xt in enumerate(xero_txns):
                print(f"    {xt['Date']}  {xt['Type']:7s}  {xt['LineItems'][0]['LineAmount']:>10.2f}  "
                      f"{xt['Contact']['Name'][:30]:<30s}  → {xt['LineItems'][0]['AccountCode']}")
            return {"pushed": 0, "skipped": len(xero_txns) + matched, "failed": 0, "errors": []}

        # Push in batches
        pushed = 0
        failed = 0
        errors = []

        for batch_start in range(0, len(xero_txns), BATCH_SIZE):
            batch = xero_txns[batch_start:batch_start + BATCH_SIZE]
            batch_indices = list(range(batch_start, batch_start + len(batch)))

            try:
                result = create_bank_transactions(access_token, batch)
                # Xero returns "BankTransactions" on success, "Elements" on 400
                created = result.get("BankTransactions") or result.get("Elements") or []

                for j, created_txn in enumerate(created):
                    idx = batch_indices[j]
                    if created_txn.get("HasValidationErrors") or created_txn.get("ValidationErrors"):
                        errs = created_txn.get("ValidationErrors", [])
                        msg = "; ".join(e.get("Message", "") for e in errs)
                        merchant = txns[idx].get('merchant_name') or txns[idx].get('raw_merchant') or '?'
                        errors.append(f"{txns[idx]['posted_at']} {merchant}: {msg}")
                        failed += 1
                    else:
                        xero_id = created_txn.get("BankTransactionID", "unknown")
                        record_sync(conn, txn_map[idx], xero_id)
                        pushed += 1

                conn.commit()
                print(f"  Batch {batch_start // BATCH_SIZE + 1}: "
                      f"{sum(1 for c in created if not c.get('HasValidationErrors'))}/{len(batch)} OK")

            except Exception as e:
                errors.append(f"  Batch {batch_start // BATCH_SIZE + 1} failed: {e}")
                failed += len(batch)

        if errors:
            print("\n  Errors:")
            for err in errors:
                print(f"    {err}")

        return {"pushed": pushed, "skipped": 0, "failed": failed, "errors": errors}

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Sync business transactions to Xero")
    parser.add_argument("--headless", action="store_true", help="Token refresh only (no browser)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be pushed")
    parser.add_argument("--since", type=str, help="Only sync transactions from this date (YYYY-MM-DD)")
    args = parser.parse_args()

    since = date.fromisoformat(args.since) if args.since else None

    print("=== Xero Sync ===\n")
    try:
        result = sync_to_xero(headless=args.headless, since=since, dry_run=args.dry_run)
        print(f"\nResult: pushed={result['pushed']}, failed={result['failed']}")
    except AuthRequiredError as e:
        print(f"\nAuth required: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
