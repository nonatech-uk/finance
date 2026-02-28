#!/usr/bin/env python3
"""Interactive Xero first-time setup.

1. Runs OAuth flow (opens browser)
2. Shows connected organisation
3. Lists bank accounts — pick the Monzo business one
4. Lists chart of accounts
5. Prints env vars to add to config/.env

Usage:
    python scripts/xero_setup.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.xero import (
    authenticate, get_organisation, get_bank_accounts, get_accounts,
    get_tenant_id,
)


def main():
    print("=== Xero Setup ===\n")

    # Step 1: Authenticate
    print("Step 1: Authenticate with Xero")
    access_token = authenticate(headless=False)
    tenant_id = get_tenant_id()
    print()

    # Step 2: Organisation info
    print("Step 2: Connected organisation")
    org = get_organisation(access_token)
    print(f"  Name:     {org.get('Name')}")
    print(f"  Tenant:   {tenant_id}")
    print(f"  Country:  {org.get('CountryCode')}")
    print(f"  Tax:      {org.get('OrganisationType')}")
    print()

    # Step 3: Bank accounts
    print("Step 3: Bank accounts")
    bank_accounts = get_bank_accounts(access_token)
    if not bank_accounts:
        print("  No bank accounts found. Create one in Xero first.")
    else:
        for i, ba in enumerate(bank_accounts):
            print(f"  [{i + 1}] {ba.get('Name', '?'):<30s}  "
                  f"Code: {ba.get('Code', '?'):<10s}  "
                  f"ID: {ba.get('AccountID', '?')}")

        if len(bank_accounts) == 1:
            chosen = bank_accounts[0]
            print(f"\n  Auto-selected: {chosen['Name']}")
        else:
            while True:
                try:
                    choice = int(input(f"\n  Select bank account [1-{len(bank_accounts)}]: "))
                    if 1 <= choice <= len(bank_accounts):
                        chosen = bank_accounts[choice - 1]
                        break
                except (ValueError, EOFError):
                    pass
                print("  Invalid choice, try again.")

        bank_account_id = chosen["AccountID"]
        print(f"  Selected: {chosen['Name']} ({bank_account_id})")
    print()

    # Step 4: Chart of accounts (expense/revenue accounts)
    print("Step 4: Chart of accounts (expense & revenue)")
    all_accounts = get_accounts(access_token)
    expense_accounts = [a for a in all_accounts if a.get("Class") in ("EXPENSE", "REVENUE") and a.get("Status") == "ACTIVE"]
    expense_accounts.sort(key=lambda a: a.get("Code", ""))

    for acc in expense_accounts:
        print(f"  {acc.get('Code', '?'):<8s}  {acc.get('Name', '?'):<40s}  {acc.get('Type', '?')}")

    print(f"\n  Total: {len(expense_accounts)} active expense/revenue accounts")
    print()

    # Step 5: Print env vars
    print("=" * 50)
    print("Add these to config/.env:\n")
    print(f"XERO_TENANT_ID={tenant_id}")
    if bank_accounts:
        print(f"XERO_BANK_ACCOUNT_ID={bank_account_id}")
    print()
    print("Next steps:")
    print("  1. Add the env vars above to config/.env")
    print("  2. Run: python scripts/xero_map_accounts.py  (map categories to Xero accounts)")
    print("  3. Run: python scripts/xero_sync.py --dry-run  (preview what would sync)")
    print("  4. Run: python scripts/xero_sync.py  (sync for real)")


if __name__ == "__main__":
    main()
