"""Fuzzy merchant matching — match uncategorised merchants against categorised ones.

Phase 4 in the categorisation pipeline. Uses rapidfuzz to find near-duplicate
merchants that should be merged, inheriting the category from the match.

Matching is done on canonical name only (not display_name, which can be misleading
from iBank imports). This avoids false merges where unrelated merchants share a
display name.

Thresholds:
- Score >= 95: auto-merge (uncategorised becomes alias of matched merchant)
- Score 80-94: queue as suggestion for manual review
"""

from rapidfuzz import fuzz, process

from src.categorisation.llm_categoriser import _is_amazon
from src.categorisation.merger import merge


AUTO_MERGE_THRESHOLD = 95
SUGGEST_MERGE_THRESHOLD = 80


def find_fuzzy_matches(conn, *, dry_run: bool = False) -> dict:
    """Match uncategorised merchants against categorised ones using fuzzy matching.

    Returns stats dict with counts of auto-merged and suggested merges.
    """
    cur = conn.cursor()

    # Load uncategorised, unmerged merchants (no pending suggestion either)
    cur.execute("""
        SELECT cm.id, cm.name, cm.display_name
        FROM canonical_merchant cm
        WHERE cm.category_hint IS NULL
          AND cm.merged_into_id IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM category_suggestion cs
              WHERE cs.canonical_merchant_id = cm.id
          )
        ORDER BY cm.name
    """)
    uncategorised = [(str(r[0]), r[1], r[2]) for r in cur.fetchall()]

    # Filter out Amazon merchants
    uncategorised = [(mid, name, dname) for mid, name, dname in uncategorised
                     if not _is_amazon(name)]

    if not uncategorised:
        print("  No uncategorised merchants for fuzzy matching")
        return {"fuzzy_auto_merged": 0, "fuzzy_suggested": 0}

    # Load categorised, unmerged merchants as reference set
    cur.execute("""
        SELECT cm.id, cm.name, cm.display_name
        FROM canonical_merchant cm
        WHERE cm.category_hint IS NOT NULL
          AND cm.merged_into_id IS NULL
        ORDER BY cm.name
    """)
    categorised = [(str(r[0]), r[1], r[2]) for r in cur.fetchall()]

    if not categorised:
        print("  No categorised merchants to match against")
        return {"fuzzy_auto_merged": 0, "fuzzy_suggested": 0}

    # Build reference using canonical NAME only (not display_name)
    # Display names can be misleading (e.g. iBank might assign wrong display names)
    ref_names = {}  # name -> (merchant_id, display_name)
    ref_list = []
    for mid, name, dname in categorised:
        ref_names[name] = (mid, dname)
        ref_list.append(name)

    print(f"  {len(uncategorised)} uncategorised merchants, {len(ref_list)} reference merchants")

    auto_merged = 0
    suggested = 0

    for mid, name, dname in uncategorised:
        # Match on canonical name only
        if len(name) < 4:
            continue

        result = process.extractOne(
            name, ref_list,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=SUGGEST_MERGE_THRESHOLD,
        )

        if not result:
            continue

        match_name, score, _ = result
        match_id, match_dname = ref_names[match_name]

        if score >= AUTO_MERGE_THRESHOLD:
            if dry_run:
                print(f"    [auto-merge] {name:<40} -> {match_name} ({score:.0f}%)")
            else:
                try:
                    merge(conn, secondary_id=mid, surviving_id=match_id)
                    auto_merged += 1
                except ValueError as e:
                    print(f"    Merge error for {name}: {e}")
        else:
            if dry_run:
                print(f"    [suggest]    {name:<40} -> {match_name} ({score:.0f}%)")
            else:
                # Queue as a fuzzy_merge suggestion
                cur.execute("""
                    SELECT c.id FROM category c
                    JOIN canonical_merchant cm ON cm.category_hint = c.full_path
                    WHERE cm.id = %s
                """, (match_id,))
                cat_row = cur.fetchone()
                if cat_row:
                    cur.execute("""
                        INSERT INTO category_suggestion
                            (canonical_merchant_id, suggested_category_id, method, confidence, reasoning)
                        VALUES (%s, %s, 'fuzzy_merge', %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (mid, str(cat_row[0]), score / 100.0,
                          f"Fuzzy match ({score:.0f}%) to {match_name} [merge_target:{match_id}]"))
                    suggested += cur.rowcount

    if not dry_run:
        conn.commit()

    print(f"  Fuzzy matching: {auto_merged} auto-merged, {suggested} suggested")
    return {"fuzzy_auto_merged": auto_merged, "fuzzy_suggested": suggested}
