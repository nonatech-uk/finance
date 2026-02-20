#!/usr/bin/env python3
"""Run the cleaning pipeline: clean raw merchants and match to canonical names.

Usage:
    python scripts/run_cleaning.py                     # process new transactions
    python scripts/run_cleaning.py --reprocess         # reprocess everything
    python scripts/run_cleaning.py --dry-run            # preview without writing
    python scripts/run_cleaning.py --institution monzo  # just one institution
    python scripts/run_cleaning.py --match-only         # skip cleaning, just match
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.cleaning.processor import process_all
from src.cleaning.matcher import match_all


def main():
    parser = argparse.ArgumentParser(description="Run cleaning pipeline")
    parser.add_argument("--reprocess", action="store_true",
                        help="Delete and reprocess all cleaned transactions")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing to DB")
    parser.add_argument("--institution", help="Only process this institution")
    parser.add_argument("--match-only", action="store_true",
                        help="Skip cleaning, only run matcher")
    args = parser.parse_args()

    print("=== Cleaning Pipeline ===\n")

    if not args.match_only:
        print("Step 1: Cleaning raw merchant strings...")
        process_all(
            reprocess=args.reprocess,
            dry_run=args.dry_run,
            institution=args.institution,
        )

    print("\nStep 2: Matching to canonical merchants...")
    match_all(dry_run=args.dry_run)

    print("\nDone!")


if __name__ == "__main__":
    main()
