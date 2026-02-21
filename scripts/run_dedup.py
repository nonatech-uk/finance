#!/usr/bin/env python3
"""Run the deduplication pipeline.

Identifies cross-source duplicates and iBank internal duplicates,
creating dedup_group records without modifying raw_transaction.

Usage:
    python scripts/run_dedup.py                          # find all dupes
    python scripts/run_dedup.py --dry-run                # preview only
    python scripts/run_dedup.py --institution wise        # one institution
    python scripts/run_dedup.py --stats                  # show current state
    python scripts/run_dedup.py --reset                  # clear all groups
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings
from src.dedup.matcher import find_duplicates, show_stats, reset_groups


def main():
    parser = argparse.ArgumentParser(description="Run deduplication pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Preview matches only")
    parser.add_argument("--institution", help="Only process this institution")
    parser.add_argument("--stats", action="store_true", help="Show dedup stats")
    parser.add_argument("--reset", action="store_true", help="Clear all dedup groups")
    args = parser.parse_args()

    conn = psycopg2.connect(settings.dsn)

    try:
        if args.reset:
            print("=== Resetting dedup groups ===\n")
            count = reset_groups(conn)
            print(f"  Deleted {count} groups.\n")
            return

        if args.stats:
            print("=== Dedup Statistics ===\n")
            show_stats(conn)
            return

        mode = "[DRY RUN] " if args.dry_run else ""
        print(f"=== {mode}Dedup Pipeline ===\n")

        print("Step 1: Cross-source matching...")
        result = find_duplicates(
            conn,
            institution=args.institution,
            dry_run=args.dry_run,
        )

        print(f"\n=== Results ===")
        print(f"  Source superseded:            {result['source_superseded']}")
        print(f"  Cross-source groups created:  {result['cross_source_groups']}")
        print(f"  Cross-source groups extended:  {result['cross_source_extended']}")
        print(f"  iBank internal groups:         {result['ibank_internal_groups']}")
        print(f"  Skipped (already grouped):     {result['skipped']}")

        if not args.dry_run:
            print(f"\n=== Post-dedup state ===")
            show_stats(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
