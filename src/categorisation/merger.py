"""Canonical merchant merger.

Merges duplicate canonical merchants by reassigning all merchant_raw_mapping
rows from the secondary merchant to the surviving merchant, and setting
merged_into_id on the secondary.
"""

import psycopg2


def merge(conn, secondary_id: str, surviving_id: str, *, dry_run: bool = False) -> dict:
    """Merge secondary merchant into surviving merchant.

    - Reassigns all merchant_raw_mapping rows to surviving
    - Transfers category_hint if surviving has none
    - Sets merged_into_id on secondary

    Returns dict with counts.
    """
    cur = conn.cursor()

    # Verify both exist
    cur.execute("SELECT id, name, category_hint, display_name FROM canonical_merchant WHERE id = %s", (surviving_id,))
    surviving = cur.fetchone()
    if not surviving:
        raise ValueError(f"Surviving merchant {surviving_id} not found")

    cur.execute("SELECT id, name, category_hint, display_name FROM canonical_merchant WHERE id = %s", (secondary_id,))
    secondary = cur.fetchone()
    if not secondary:
        raise ValueError(f"Secondary merchant {secondary_id} not found")

    if secondary_id == surviving_id:
        raise ValueError("Cannot merge a merchant into itself")

    surviving_name, surviving_cat, surviving_display = surviving[1], surviving[2], surviving[3]
    secondary_name, secondary_cat, secondary_display = secondary[1], secondary[2], secondary[3]

    # Count mappings that will move
    cur.execute(
        "SELECT count(*) FROM merchant_raw_mapping WHERE canonical_merchant_id = %s",
        (secondary_id,),
    )
    mapping_count = cur.fetchone()[0]

    if dry_run:
        print(f"  Would merge '{secondary_name}' ({mapping_count} mappings) -> '{surviving_name}'")
        if secondary_cat and not surviving_cat:
            print(f"  Would transfer category_hint: {secondary_cat}")
        if secondary_display and not surviving_display:
            print(f"  Would transfer display_name: {secondary_display}")
        return {"mappings_moved": mapping_count, "category_transferred": bool(secondary_cat and not surviving_cat)}

    # Reassign mappings
    cur.execute(
        "UPDATE merchant_raw_mapping SET canonical_merchant_id = %s WHERE canonical_merchant_id = %s",
        (surviving_id, secondary_id),
    )

    # Transfer category if surviving has none
    category_transferred = False
    if secondary_cat and not surviving_cat:
        cur.execute(
            "UPDATE canonical_merchant SET category_hint = %s, category_method = 'merge_transfer', category_confidence = 0.85, category_set_at = now() WHERE id = %s",
            (secondary_cat, surviving_id),
        )
        category_transferred = True

    # Transfer display_name if surviving has none
    if secondary_display and not surviving_display:
        cur.execute(
            "UPDATE canonical_merchant SET display_name = %s WHERE id = %s",
            (secondary_display, surviving_id),
        )

    # Mark secondary as merged
    cur.execute(
        "UPDATE canonical_merchant SET merged_into_id = %s WHERE id = %s",
        (surviving_id, secondary_id),
    )

    return {"mappings_moved": mapping_count, "category_transferred": category_transferred}
