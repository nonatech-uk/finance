#!/usr/bin/env python3
"""Run the auto-categorisation pipeline.

Sets display names from iBank, extracts source category hints,
and optionally runs LLM categorisation for remaining merchants.

Usage:
    python scripts/run_categorisation.py                          # names + source hints
    python scripts/run_categorisation.py --names-only             # display names only
    python scripts/run_categorisation.py --include-llm            # also run LLM
    python scripts/run_categorisation.py --dry-run                # preview only
    python scripts/run_categorisation.py --stats                  # show current state
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2

from config.settings import settings
from src.categorisation.engine import run_naming, run_source_hints, run_llm


def show_stats(conn):
    """Show current categorisation state."""
    cur = conn.cursor()

    cur.execute("""
        SELECT
            count(*) as total,
            count(*) FILTER (WHERE category_hint IS NOT NULL) as categorised,
            count(*) FILTER (WHERE category_hint IS NULL) as uncategorised,
            count(*) FILTER (WHERE display_name IS NOT NULL) as with_display_name,
            count(*) FILTER (WHERE merged_into_id IS NOT NULL) as merged
        FROM canonical_merchant
    """)
    total, categorised, uncategorised, with_name, merged = cur.fetchone()
    print(f"  Total merchants:     {total}")
    print(f"  Categorised:         {categorised} ({categorised*100//total}%)")
    print(f"  Uncategorised:       {uncategorised} ({uncategorised*100//total}%)")
    print(f"  With display name:   {with_name}")
    print(f"  Merged:              {merged}")

    cur.execute("""
        SELECT category_method, count(*)
        FROM canonical_merchant
        WHERE category_method IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """)
    print("\n  By method:")
    for method, cnt in cur.fetchall():
        print(f"    {method:<20} {cnt}")

    cur.execute("""
        SELECT status, count(*)
        FROM category_suggestion
        GROUP BY 1 ORDER BY 1
    """)
    rows = cur.fetchall()
    if rows:
        print("\n  Suggestions:")
        for status, cnt in rows:
            print(f"    {status:<20} {cnt}")
    else:
        print("\n  No suggestions in queue.")


def main():
    parser = argparse.ArgumentParser(description="Run auto-categorisation pipeline")
    parser.add_argument("--names-only", action="store_true", help="Only set display names from iBank")
    parser.add_argument("--include-llm", action="store_true", help="Also run LLM categorisation")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, don't write to DB")
    parser.add_argument("--stats", action="store_true", help="Show categorisation stats")
    args = parser.parse_args()

    conn = psycopg2.connect(settings.dsn)

    try:
        if args.stats:
            print("=== Categorisation Statistics ===\n")
            show_stats(conn)
            return

        mode = "[DRY RUN] " if args.dry_run else ""
        print(f"=== {mode}Categorisation Pipeline ===\n")

        # Phase 1: Display names
        print("Step 1: Setting display names from iBank...")
        naming_result = run_naming(conn, dry_run=args.dry_run)

        if args.names_only:
            print(f"\n=== Results ===")
            print(f"  Display name candidates: {naming_result['candidates']}")
            print(f"  Display names set:       {naming_result['display_names_set']}")
            return

        # Phase 2: Source hints
        print("\nStep 2: Extracting source category hints...")
        hints_result = run_source_hints(conn, dry_run=args.dry_run)

        # Phase 3: LLM (optional)
        llm_result = {}
        if args.include_llm:
            print("\nStep 3: Running LLM categorisation...")
            llm_result = run_llm(conn, dry_run=args.dry_run)

        print(f"\n=== Results ===")
        print(f"  Display names set:       {naming_result['display_names_set']}")
        print(f"  Source hint suggestions:  {hints_result['total_suggestions']}")
        print(f"  Auto-accepted:           {hints_result['auto_accepted']}")
        print(f"  Queued for review:       {hints_result['queued']}")
        if llm_result:
            print(f"  LLM queued:              {llm_result.get('llm_queued', 0)}")

        if not args.dry_run:
            print(f"\n=== Post-categorisation state ===")
            show_stats(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
