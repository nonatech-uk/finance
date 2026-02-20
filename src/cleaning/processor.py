"""Batch cleaning processor.

Processes raw_transaction rows through the cleaning rules and writes
to cleaned_transaction. Idempotent — skips already-processed rows.
"""

from typing import Optional

import psycopg2

from config.settings import settings
from src.cleaning.rules import clean_merchant, CLEANING_VERSION


def process_all(reprocess: bool = False, dry_run: bool = False, institution: Optional[str] = None):
    """Clean all unprocessed raw transactions.

    Args:
        reprocess: If True, delete existing cleaned entries and reprocess everything.
        dry_run: If True, don't write to DB, just print what would happen.
        institution: If set, only process this institution.
    """
    conn = psycopg2.connect(settings.dsn)
    try:
        cur = conn.cursor()

        # Ensure unique index exists
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS cleaned_transaction_raw_id_idx
            ON cleaned_transaction (raw_transaction_id)
        """)
        conn.commit()

        if reprocess and not dry_run:
            if institution:
                cur.execute("""
                    DELETE FROM cleaned_transaction WHERE raw_transaction_id IN (
                        SELECT id FROM raw_transaction WHERE institution = %s
                    )
                """, (institution,))
            else:
                cur.execute("DELETE FROM cleaned_transaction")
            deleted = cur.rowcount
            conn.commit()
            print(f"  Deleted {deleted} existing cleaned entries.")

        # Fetch unprocessed raw transactions
        query = """
            SELECT rt.id, rt.raw_merchant, rt.institution
            FROM raw_transaction rt
            LEFT JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
            WHERE ct.id IS NULL
        """
        params = []
        if institution:
            query += " AND rt.institution = %s"
            params.append(institution)

        cur.execute(query, params)
        rows = cur.fetchall()
        print(f"  Found {len(rows)} unprocessed transactions.")

        if not rows:
            return {"processed": 0, "skipped": 0}

        inserted = 0
        sample_shown = 0

        for raw_id, raw_merchant, inst in rows:
            cleaned, rules_applied = clean_merchant(raw_merchant or "", inst)

            if dry_run:
                if sample_shown < 20 and raw_merchant and cleaned != raw_merchant:
                    print(f"    [{inst}] {raw_merchant}")
                    print(f"         -> {cleaned}  (rules: {', '.join(rules_applied)})")
                    sample_shown += 1
                continue

            cur.execute("""
                INSERT INTO cleaned_transaction
                    (raw_transaction_id, cleaning_version, cleaning_rules, cleaned_merchant)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (raw_transaction_id) DO NOTHING
            """, (raw_id, CLEANING_VERSION, rules_applied, cleaned or None))

            if cur.rowcount > 0:
                inserted += 1

        if not dry_run:
            conn.commit()

        print(f"  Cleaned: {inserted} transactions.")
        return {"processed": inserted, "skipped": len(rows) - inserted}

    finally:
        conn.close()
