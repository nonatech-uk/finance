"""Source hint extraction for auto-categorisation.

Extracts category hints from raw_data JSONB fields:
- iBank: raw_data->>'ibank_category' (PRIMARY — human-assigned, largely correct)
- Monzo API: raw_data->>'category'
- Wise CSV: raw_data->>'Category'

Also extracts iBank merchant names as display_name candidates.

All queries are bulk (no per-merchant queries) for performance.
"""

from collections import defaultdict


def extract_hints(conn, *, skip_amazon: bool = True) -> list[dict]:
    """Extract source category hints for all unmerged canonical merchants.

    Uses a single bulk query to collect all source hints, then processes in memory.
    """
    cur = conn.cursor()

    # Load source_category_mapping into memory
    cur.execute("SELECT source, source_category, category_id, confidence FROM source_category_mapping")
    scm = {}
    for source, source_cat, cat_id, conf in cur.fetchall():
        scm[(source, source_cat)] = (str(cat_id), float(conf))

    # Load category lookup for path display
    cur.execute("SELECT id, full_path FROM category")
    cat_paths = {str(r[0]): r[1] for r in cur.fetchall()}

    # Bulk query: get all source hints for all uncategorised, unmerged merchants
    cur.execute("""
        SELECT cm.id, cm.name, rt.source,
            rt.raw_data->>'ibank_category' as ibank_cat,
            rt.raw_data->>'category' as monzo_cat,
            rt.raw_data->>'Category' as wise_cat
        FROM canonical_merchant cm
        JOIN merchant_raw_mapping mrm ON mrm.canonical_merchant_id = cm.id
        JOIN cleaned_transaction ct ON ct.cleaned_merchant = mrm.cleaned_merchant
        JOIN raw_transaction rt ON rt.id = ct.raw_transaction_id
        WHERE cm.category_hint IS NULL
          AND cm.merged_into_id IS NULL
    """)

    # Group hints by merchant
    merchant_hints = defaultdict(list)  # cm_id -> [(cat_id, conf, source)]
    merchant_names = {}  # cm_id -> name

    for cm_id, cm_name, source, ibank_cat, monzo_cat, wise_cat in cur.fetchall():
        cm_id = str(cm_id)
        merchant_names[cm_id] = cm_name

        if skip_amazon and _is_amazon(cm_name):
            continue

        if source == 'ibank' and ibank_cat:
            primary_cat = ibank_cat.split(' | ')[0].strip()
            key = ('ibank', primary_cat)
            if key in scm:
                cat_id, conf = scm[key]
                merchant_hints[cm_id].append((cat_id, conf, 'ibank'))
        elif source == 'monzo_api' and monzo_cat:
            key = ('monzo_api', monzo_cat)
            if key in scm:
                cat_id, conf = scm[key]
                merchant_hints[cm_id].append((cat_id, conf, 'monzo_api'))
        elif source in ('wise_csv', 'wise_api') and wise_cat:
            key = ('wise_csv', wise_cat)
            if key in scm:
                cat_id, conf = scm[key]
                merchant_hints[cm_id].append((cat_id, conf, 'wise_csv'))

    # Also bulk-fetch iBank display names
    ibank_names = _bulk_get_ibank_display_names(cur)

    # Build suggestions
    suggestions = []
    for cm_id, hints in merchant_hints.items():
        if not hints:
            continue

        best = _pick_best_hint(hints)
        if best:
            cat_id, confidence, reasoning = best
            suggestions.append({
                'canonical_merchant_id': cm_id,
                'merchant_name': merchant_names[cm_id],
                'suggested_category_id': cat_id,
                'suggested_category_path': cat_paths.get(cat_id, '?'),
                'confidence': confidence,
                'method': 'source_hint',
                'reasoning': reasoning,
                'display_name_candidate': ibank_names.get(cm_id),
            })

    return suggestions


def extract_display_names(conn) -> list[dict]:
    """Extract iBank merchant names as display_name candidates for all unmerged merchants.

    Uses bulk queries for performance.
    """
    cur = conn.cursor()

    # Get all unmerged merchants without display_name
    cur.execute("""
        SELECT cm.id, cm.name
        FROM canonical_merchant cm
        WHERE cm.display_name IS NULL
          AND cm.merged_into_id IS NULL
    """)
    merchants = {str(r[0]): r[1] for r in cur.fetchall()}

    if not merchants:
        return []

    # Bulk get iBank names
    ibank_names = _bulk_get_ibank_display_names(cur)

    results = []
    for cm_id, cm_name in merchants.items():
        display = ibank_names.get(cm_id)
        if display and display != cm_name and _is_better_name(display, cm_name):
            results.append({
                'canonical_merchant_id': cm_id,
                'current_name': cm_name,
                'display_name': display,
            })

    return results


def _bulk_get_ibank_display_names(cur) -> dict:
    """Bulk fetch iBank raw_merchant names for all canonical merchants.

    Returns {canonical_merchant_id: ibank_raw_merchant}.
    """
    cur.execute("""
        SELECT DISTINCT ON (mrm.canonical_merchant_id)
            mrm.canonical_merchant_id, rt.raw_merchant
        FROM merchant_raw_mapping mrm
        JOIN cleaned_transaction ct ON ct.cleaned_merchant = mrm.cleaned_merchant
        JOIN raw_transaction rt ON rt.id = ct.raw_transaction_id
        WHERE rt.source = 'ibank'
          AND rt.raw_merchant IS NOT NULL
          AND rt.raw_merchant != ''
        ORDER BY mrm.canonical_merchant_id, rt.posted_at DESC
    """)
    return {str(r[0]): r[1] for r in cur.fetchall()}


def _pick_best_hint(hints: list[tuple]) -> tuple | None:
    """Pick the best category suggestion from available hints.

    Confidence tiers:
    - iBank alone → 0.90
    - iBank + Monzo/Wise agree → 0.95
    - Multiple non-iBank sources agree → 0.85
    - Monzo/Wise alone → their base confidence (0.75-0.90)
    """
    if not hints:
        return None

    # Deduplicate hints (same cat_id + source)
    seen = set()
    unique_hints = []
    for cat_id, conf, source in hints:
        key = (cat_id, source)
        if key not in seen:
            seen.add(key)
            unique_hints.append((cat_id, conf, source))

    # Group by category_id
    by_category = defaultdict(list)
    for cat_id, conf, source in unique_hints:
        by_category[cat_id].append((conf, source))

    best_cat_id = None
    best_confidence = 0.0
    best_reasoning = ""

    for cat_id, entries in by_category.items():
        sources = {e[1] for e in entries}
        has_ibank = 'ibank' in sources
        has_other = sources - {'ibank'}

        if has_ibank and has_other:
            confidence = 0.95
            reasoning = f"iBank + {', '.join(sorted(has_other))} agree"
        elif has_ibank:
            confidence = 0.90
            reasoning = "iBank (human-assigned)"
        elif len(sources) > 1:
            confidence = 0.85
            reasoning = f"Multiple sources agree: {', '.join(sorted(sources))}"
        else:
            confidence = entries[0][0]
            reasoning = f"Single source: {entries[0][1]}"

        if confidence > best_confidence:
            best_confidence = confidence
            best_cat_id = cat_id
            best_reasoning = reasoning

    if best_cat_id:
        return (best_cat_id, best_confidence, best_reasoning)
    return None


def _is_better_name(ibank_name: str, canonical_name: str) -> bool:
    """Check if the iBank name is a better display name than the canonical name."""
    ibank_stripped = ibank_name.strip()
    canon_stripped = canonical_name.strip()

    if ibank_stripped.lower() == canon_stripped.lower():
        return False

    # Reject iBank names that look like internal/system strings
    lower = ibank_stripped.lower()
    if any(s in lower for s in [
        'internal transfer', 'interest from', 'interest to',
        'monzo-', 'funds from employer', 'received money from',
        'card transaction of', 'xxxxxx',
    ]):
        return False

    # Reject if iBank name is longer (we want simpler names)
    if len(ibank_stripped) > len(canon_stripped):
        return False

    # Prefer mixed case over ALL CAPS
    if canon_stripped == canon_stripped.upper() and ibank_stripped != ibank_stripped.upper():
        return True

    # iBank names are human-curated, prefer if shorter or same length
    return True


def _is_amazon(name: str) -> bool:
    """Check if a merchant name is Amazon-related."""
    lower = name.lower()
    return 'amazon' in lower or 'amzn' in lower or 'amz ' in lower
