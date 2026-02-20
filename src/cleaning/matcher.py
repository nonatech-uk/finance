"""Canonical merchant matcher.

Matches cleaned merchant strings to canonical merchants using:
1. Exact match (case-insensitive)
2. Prefix match (canonical name is prefix of cleaned)
3. Fuzzy match (Levenshtein distance)
4. Unmatched → create new canonical merchant, flag for review
"""

from typing import Optional

import psycopg2
from rapidfuzz import fuzz, process

from config.settings import settings

FUZZY_THRESHOLD = 85  # minimum score to accept a fuzzy match


def match_all(dry_run: bool = False):
    """Match all cleaned merchants that don't yet have a mapping.

    Loads canonical merchants into memory, then matches each distinct
    cleaned_merchant that isn't already in merchant_raw_mapping.
    """
    conn = psycopg2.connect(settings.dsn)
    try:
        cur = conn.cursor()

        # Load existing canonical merchants
        cur.execute("SELECT id, name FROM canonical_merchant")
        canonicals = {row[1]: str(row[0]) for row in cur.fetchall()}
        # Case-insensitive lookup
        canon_lower = {name.lower(): name for name in canonicals}
        canon_names = list(canonicals.keys())

        # Find distinct cleaned merchants without a mapping
        cur.execute("""
            SELECT DISTINCT ct.cleaned_merchant
            FROM cleaned_transaction ct
            WHERE ct.cleaned_merchant IS NOT NULL
              AND ct.cleaned_merchant != ''
              AND NOT EXISTS (
                  SELECT 1 FROM merchant_raw_mapping mrm
                  WHERE mrm.cleaned_merchant = ct.cleaned_merchant
              )
        """)
        unmatched = [row[0] for row in cur.fetchall()]
        print(f"  Unmatched cleaned merchants: {len(unmatched)}")

        if not unmatched:
            return {"exact": 0, "prefix": 0, "fuzzy": 0, "new": 0}

        stats = {"exact": 0, "prefix": 0, "fuzzy": 0, "new": 0}

        for cleaned in unmatched:
            match_type, canonical_name, confidence = _find_match(
                cleaned, canonicals, canon_lower, canon_names
            )

            if dry_run:
                if match_type != "new" or stats["new"] < 30:
                    print(f"    [{match_type:>6}] {cleaned:<40} -> {canonical_name} ({confidence:.0%})")
                stats[match_type] += 1
                continue

            # Ensure canonical merchant exists
            if match_type == "new":
                cur.execute("""
                    INSERT INTO canonical_merchant (name)
                    VALUES (%s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING id
                """, (canonical_name,))
                result = cur.fetchone()
                if result:
                    cm_id = str(result[0])
                else:
                    cur.execute("SELECT id FROM canonical_merchant WHERE name = %s", (canonical_name,))
                    cm_id = str(cur.fetchone()[0])
                canonicals[canonical_name] = cm_id
                canon_lower[canonical_name.lower()] = canonical_name
                canon_names.append(canonical_name)
            else:
                cm_id = canonicals[canonical_name]

            # Create mapping
            cur.execute("""
                INSERT INTO merchant_raw_mapping
                    (cleaned_merchant, canonical_merchant_id, match_type, confidence, mapped_by)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cleaned_merchant) DO NOTHING
            """, (cleaned, cm_id, match_type, confidence, "auto_matcher"))

            stats[match_type] += 1

        if not dry_run:
            conn.commit()

        print(f"  Matched: {stats['exact']} exact, {stats['prefix']} prefix, "
              f"{stats['fuzzy']} fuzzy, {stats['new']} new")
        return stats

    finally:
        conn.close()


def _find_match(cleaned: str, canonicals: dict, canon_lower: dict, canon_names: list):
    """Find the best canonical merchant match for a cleaned string.

    Returns (match_type, canonical_name, confidence).
    """
    # 1. Exact match (case-insensitive)
    lower = cleaned.lower()
    if lower in canon_lower:
        return ("exact", canon_lower[lower], 1.0)

    # 2. Prefix match: canonical name starts with cleaned or vice versa
    for canon_name in canon_names:
        cl = canon_name.lower()
        if cl.startswith(lower) or lower.startswith(cl):
            # Require at least 4 chars overlap and 60% length match
            overlap = min(len(cl), len(lower))
            max_len = max(len(cl), len(lower))
            if overlap >= 4 and overlap / max_len >= 0.6:
                confidence = overlap / max_len
                return ("prefix", canon_name, confidence)

    # 3. Fuzzy match
    if canon_names:
        result = process.extractOne(
            cleaned, canon_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=FUZZY_THRESHOLD,
        )
        if result:
            match_name, score, _ = result
            return ("fuzzy", match_name, score / 100.0)

    # 4. No match — create new canonical merchant using cleaned string
    return ("new", cleaned, 0.0)
