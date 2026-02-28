#!/usr/bin/env python3
"""Map finance categories to Xero account codes.

Shows all categories used by business transactions and lets you
assign Xero account codes to each. Stores mappings in the
xero_account_mapping table.

Usage:
    python scripts/xero_map_accounts.py              # interactive mapping
    python scripts/xero_map_accounts.py --show        # show current mappings
    python scripts/xero_map_accounts.py --unmapped    # show only unmapped categories
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings
from src.ingestion.xero import authenticate, get_accounts, AuthRequiredError


def get_business_categories(conn) -> list[dict]:
    """Fetch all categories used by business transactions, with transaction counts."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COALESCE(tcat.full_path, cat_override.full_path, cat.full_path) AS category_path,
            COUNT(*) AS txn_count,
            SUM(ABS(rt.amount)) AS total_amount
        FROM active_transaction rt
        JOIN account a ON a.institution = rt.institution AND a.account_ref = rt.account_ref
        LEFT JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
        LEFT JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        LEFT JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        LEFT JOIN transaction_merchant_override tmo ON tmo.raw_transaction_id = rt.id
        LEFT JOIN canonical_merchant cm_override ON cm_override.id = tmo.canonical_merchant_id
        LEFT JOIN category cat ON cat.full_path = cm.category_hint
        LEFT JOIN category cat_override ON cat_override.full_path = cm_override.category_hint
        LEFT JOIN transaction_category_override tco ON tco.raw_transaction_id = rt.id
        LEFT JOIN category tcat ON tcat.full_path = tco.category_path
        WHERE a.scope = 'business'
          AND a.is_archived IS NOT TRUE
        GROUP BY 1
        ORDER BY txn_count DESC
    """)
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def get_existing_mappings(conn) -> dict[str, tuple[str, str]]:
    """Fetch existing mappings as {category_path: (account_code, account_name)}."""
    cur = conn.cursor()
    cur.execute("SELECT category_path, xero_account_code, xero_account_name FROM xero_account_mapping")
    return {row[0]: (row[1], row[2]) for row in cur.fetchall()}


def save_mapping(conn, category_path: str, account_code: str, account_name: str | None):
    """Upsert a category mapping."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO xero_account_mapping (category_path, xero_account_code, xero_account_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (category_path) DO UPDATE
        SET xero_account_code = EXCLUDED.xero_account_code,
            xero_account_name = EXCLUDED.xero_account_name
    """, (category_path, account_code, account_name))
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Map categories to Xero account codes")
    parser.add_argument("--show", action="store_true", help="Show current mappings")
    parser.add_argument("--unmapped", action="store_true", help="Show only unmapped categories")
    args = parser.parse_args()

    conn = psycopg2.connect(settings.dsn)

    if args.show:
        mappings = get_existing_mappings(conn)
        if not mappings:
            print("No mappings configured yet.")
        else:
            print(f"\n{'Category':<50s}  {'Code':<8s}  {'Xero Account'}")
            print("-" * 90)
            for cat, (code, name) in sorted(mappings.items()):
                print(f"{cat:<50s}  {code:<8s}  {name or ''}")
        conn.close()
        return

    # Fetch Xero accounts for reference
    try:
        access_token = authenticate(headless=True)
        xero_accounts = get_accounts(access_token)
        expense_accounts = {
            a["Code"]: a["Name"]
            for a in xero_accounts
            if a.get("Class") in ("EXPENSE", "REVENUE") and a.get("Status") == "ACTIVE"
        }
    except (AuthRequiredError, Exception) as e:
        print(f"Could not fetch Xero accounts ({e}). Proceeding with manual codes.\n")
        expense_accounts = {}

    categories = get_business_categories(conn)
    mappings = get_existing_mappings(conn)

    if args.unmapped:
        categories = [c for c in categories if c["category_path"] not in mappings]

    if not categories:
        print("All categories are mapped (or no business transactions found).")
        conn.close()
        return

    # Show Xero accounts for reference
    if expense_accounts:
        print("\nXero expense/revenue accounts:")
        for code in sorted(expense_accounts):
            print(f"  {code:<8s}  {expense_accounts[code]}")
        print()

    print(f"{'#':<4s}  {'Category':<50s}  {'Txns':<6s}  {'Amount':>10s}  {'Current Mapping'}")
    print("-" * 100)

    for i, cat in enumerate(categories):
        path = cat["category_path"] or "(uncategorised)"
        existing = mappings.get(cat["category_path"])
        mapping_str = f"{existing[0]} ({existing[1]})" if existing else "-"
        print(f"{i + 1:<4d}  {path:<50s}  {cat['txn_count']:<6d}  "
              f"{float(cat['total_amount']):>10.2f}  {mapping_str}")

    print(f"\nEnter mappings as: <number> <account_code>")
    print(f"Example: 1 429  (maps category #1 to Xero account 429)")
    print(f"Type 'done' to finish.\n")

    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if line.lower() in ("done", "quit", "exit", "q"):
            break

        parts = line.split()
        if len(parts) != 2:
            print("  Usage: <number> <account_code>")
            continue

        try:
            idx = int(parts[0]) - 1
            code = parts[1]
        except ValueError:
            print("  Invalid input.")
            continue

        if idx < 0 or idx >= len(categories):
            print(f"  Number must be 1-{len(categories)}.")
            continue

        cat_path = categories[idx]["category_path"]
        if not cat_path:
            print("  Cannot map uncategorised transactions.")
            continue

        account_name = expense_accounts.get(code)
        save_mapping(conn, cat_path, code, account_name)
        print(f"  Mapped: {cat_path} -> {code}" + (f" ({account_name})" if account_name else ""))

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
