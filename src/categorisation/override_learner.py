"""Override learning — detect patterns in user overrides and apply to new transactions.

Phase 6 in the categorisation pipeline.

Level A: Merchant-level correction
  If all user overrides for a merchant point to the same category (and it differs
  from the merchant's default), update the merchant's category_hint to match.

Level B: Contextual exceptions (amount-based and seasonal patterns)
  For merchants where user overrides vary, look for patterns:
  - Amount: recurring charges at a specific amount → specific category
  - Month: seasonal overrides (e.g. summer vs winter category)
  If a pattern is detected with ≥3 data points, apply to future matching transactions.
"""

from collections import defaultdict
from decimal import Decimal


MIN_OVERRIDES_FOR_MERCHANT_CORRECTION = 2
MIN_OVERRIDES_FOR_PATTERN = 3
AMOUNT_TOLERANCE = 0.05  # 5% tolerance for recurring amount detection


def learn_from_overrides(conn, *, dry_run: bool = False) -> dict:
    """Detect patterns in user overrides and apply learned rules.

    Returns stats dict with counts of corrections and patterns applied.
    """
    cur = conn.cursor()

    stats = {
        "merchant_corrections": 0,
        "amount_patterns_applied": 0,
        "seasonal_patterns_applied": 0,
    }

    # ── Level A: Merchant-level correction ────────────────────────────
    stats["merchant_corrections"] = _learn_merchant_corrections(cur, conn, dry_run=dry_run)

    # ── Level B: Contextual exceptions ────────────────────────────────
    amount, seasonal = _learn_contextual_patterns(cur, conn, dry_run=dry_run)
    stats["amount_patterns_applied"] = amount
    stats["seasonal_patterns_applied"] = seasonal

    if not dry_run:
        conn.commit()

    total = sum(stats.values())
    print(f"  Override learning: {stats['merchant_corrections']} merchant corrections, "
          f"{stats['amount_patterns_applied']} amount patterns, "
          f"{stats['seasonal_patterns_applied']} seasonal patterns")
    return stats


def _learn_merchant_corrections(cur, conn, *, dry_run: bool = False) -> int:
    """Level A: If all user overrides for a merchant agree, update merchant default."""

    # Find merchants where user consistently overrides to the same category
    cur.execute("""
        SELECT cm.id, cm.name, cm.category_hint,
               tco.category_path AS override_cat,
               count(*) AS override_count
        FROM transaction_category_override tco
        JOIN active_transaction at ON at.id = tco.raw_transaction_id
        JOIN cleaned_transaction ct ON ct.raw_transaction_id = at.id
        JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        WHERE tco.source = 'user'
          AND tco.category_path <> '+Transfer'
          AND cm.merged_into_id IS NULL
        GROUP BY cm.id, cm.name, cm.category_hint, tco.category_path
        HAVING count(*) >= %s
    """, (MIN_OVERRIDES_FOR_MERCHANT_CORRECTION,))

    candidates = cur.fetchall()
    if not candidates:
        print("  No merchant-level corrections found")
        return 0

    # Group by merchant to check if ALL overrides point to same category
    merchant_overrides = defaultdict(list)
    for mid, mname, current_cat, override_cat, count in candidates:
        merchant_overrides[str(mid)].append({
            'name': mname,
            'current_cat': current_cat,
            'override_cat': override_cat,
            'count': count,
        })

    corrections = 0
    for mid, overrides in merchant_overrides.items():
        # Only correct if all overrides agree on the same category
        if len(overrides) != 1:
            continue

        override = overrides[0]
        # Skip if already matches
        if override['current_cat'] == override['override_cat']:
            continue

        if dry_run:
            print(f"    [correct] {override['name']:<35} "
                  f"{override['current_cat'] or '(none)'} -> {override['override_cat']} "
                  f"({override['count']} overrides)")
        else:
            cur.execute("""
                UPDATE canonical_merchant
                SET category_hint = %s,
                    category_method = 'learned',
                    category_confidence = 0.95,
                    category_set_at = now()
                WHERE id = %s
            """, (override['override_cat'], mid))
            corrections += cur.rowcount

    return corrections


def _learn_contextual_patterns(cur, conn, *, dry_run: bool = False) -> tuple[int, int]:
    """Level B: Detect amount-based and seasonal patterns in user overrides.

    Returns (amount_patterns_applied, seasonal_patterns_applied).
    """
    # Get all user overrides with context (amount, month, merchant)
    cur.execute("""
        SELECT cm.id AS merchant_id, cm.name, cm.category_hint,
               tco.category_path, at.amount, extract(month from at.posted_at)::int AS month,
               at.id AS txn_id
        FROM transaction_category_override tco
        JOIN active_transaction at ON at.id = tco.raw_transaction_id
        JOIN cleaned_transaction ct ON ct.raw_transaction_id = at.id
        JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        WHERE tco.source = 'user'
          AND tco.category_path <> '+Transfer'
          AND cm.merged_into_id IS NULL
        ORDER BY cm.id, at.posted_at
    """)

    rows = cur.fetchall()
    if not rows:
        return 0, 0

    # Group by merchant
    merchant_data = defaultdict(list)
    for mid, mname, mcat, override_cat, amount, month, txn_id in rows:
        merchant_data[str(mid)].append({
            'name': mname,
            'merchant_cat': mcat,
            'override_cat': override_cat,
            'amount': float(amount),
            'month': month,
        })

    amount_applied = 0
    seasonal_applied = 0

    for mid, entries in merchant_data.items():
        # Only look at merchants with multiple different override categories
        override_cats = set(e['override_cat'] for e in entries)
        if len(override_cats) <= 1:
            continue  # Level A handles this case

        # ── Amount patterns ──
        # Group by (amount within tolerance, category)
        amount_groups = defaultdict(list)
        for e in entries:
            amount_groups[(round(e['amount'], 2), e['override_cat'])].append(e)

        for (amount, cat), group in amount_groups.items():
            if len(group) >= MIN_OVERRIDES_FOR_PATTERN:
                # Found a recurring amount pattern
                applied = _apply_amount_pattern(
                    cur, mid, amount, cat, entries[0]['name'], dry_run=dry_run
                )
                amount_applied += applied

        # ── Seasonal patterns ──
        # Group by (month range, category)
        month_cats = defaultdict(list)
        for e in entries:
            month_cats[e['override_cat']].append(e['month'])

        for cat, months in month_cats.items():
            if len(months) >= MIN_OVERRIDES_FOR_PATTERN:
                # Check if months cluster (e.g. all summer or all winter)
                month_set = set(months)
                if _is_seasonal_cluster(month_set):
                    applied = _apply_seasonal_pattern(
                        cur, mid, month_set, cat, entries[0]['name'], dry_run=dry_run
                    )
                    seasonal_applied += applied

    return amount_applied, seasonal_applied


def _apply_amount_pattern(cur, merchant_id, amount, category_path, merchant_name,
                          *, dry_run: bool = False) -> int:
    """Apply an amount-based override pattern to matching un-overridden transactions."""
    tolerance = abs(amount * AMOUNT_TOLERANCE)
    low = amount - tolerance
    high = amount + tolerance

    if dry_run:
        # Count matching transactions
        cur.execute("""
            SELECT count(*)
            FROM active_transaction at
            JOIN cleaned_transaction ct ON ct.raw_transaction_id = at.id
            JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
            WHERE mrm.canonical_merchant_id = %s
              AND at.amount BETWEEN %s AND %s
              AND NOT EXISTS (
                  SELECT 1 FROM transaction_category_override tco
                  WHERE tco.raw_transaction_id = at.id
              )
        """, (merchant_id, low, high))
        count = cur.fetchone()[0]
        if count:
            print(f"    [amount] {merchant_name} at £{abs(amount):.2f} -> {category_path} "
                  f"({count} transactions)")
        return 0

    # Apply to matching transactions without an override
    cur.execute("""
        INSERT INTO transaction_category_override (raw_transaction_id, category_path, source)
        SELECT at.id, %s, 'enrichment'
        FROM active_transaction at
        JOIN cleaned_transaction ct ON ct.raw_transaction_id = at.id
        JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        WHERE mrm.canonical_merchant_id = %s
          AND at.amount BETWEEN %s AND %s
          AND NOT EXISTS (
              SELECT 1 FROM transaction_category_override tco
              WHERE tco.raw_transaction_id = at.id
          )
        ON CONFLICT (raw_transaction_id) DO NOTHING
    """, (category_path, merchant_id, low, high))
    return cur.rowcount


def _apply_seasonal_pattern(cur, merchant_id, months, category_path, merchant_name,
                            *, dry_run: bool = False) -> int:
    """Apply a seasonal override pattern to matching un-overridden transactions."""
    months_list = list(months)

    if dry_run:
        cur.execute("""
            SELECT count(*)
            FROM active_transaction at
            JOIN cleaned_transaction ct ON ct.raw_transaction_id = at.id
            JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
            WHERE mrm.canonical_merchant_id = %s
              AND extract(month from at.posted_at)::int = ANY(%s)
              AND NOT EXISTS (
                  SELECT 1 FROM transaction_category_override tco
                  WHERE tco.raw_transaction_id = at.id
              )
        """, (merchant_id, months_list))
        count = cur.fetchone()[0]
        if count:
            month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                           7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
            month_str = ', '.join(month_names.get(m, str(m)) for m in sorted(months))
            print(f"    [seasonal] {merchant_name} in {month_str} -> {category_path} "
                  f"({count} transactions)")
        return 0

    cur.execute("""
        INSERT INTO transaction_category_override (raw_transaction_id, category_path, source)
        SELECT at.id, %s, 'enrichment'
        FROM active_transaction at
        JOIN cleaned_transaction ct ON ct.raw_transaction_id = at.id
        JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        WHERE mrm.canonical_merchant_id = %s
          AND extract(month from at.posted_at)::int = ANY(%s)
          AND NOT EXISTS (
              SELECT 1 FROM transaction_category_override tco
              WHERE tco.raw_transaction_id = at.id
          )
        ON CONFLICT (raw_transaction_id) DO NOTHING
    """, (category_path, merchant_id, months_list))
    return cur.rowcount


def _is_seasonal_cluster(months: set[int]) -> bool:
    """Check if a set of months forms a plausible seasonal cluster.

    Returns True if all months fall within a 6-month window (allowing wrap-around).
    """
    if len(months) > 6:
        return False

    months_sorted = sorted(months)
    # Check contiguous span (allowing December→January wrap)
    for start in months_sorted:
        window = set()
        for offset in range(6):
            window.add((start + offset - 1) % 12 + 1)
        if months.issubset(window):
            return True

    return False
