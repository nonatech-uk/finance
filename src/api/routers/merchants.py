"""Merchant endpoints."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.deps import get_conn
from src.api.models import (
    CategorySuggestionItem,
    CategorySuggestionList,
    DisplayRuleCreate,
    DisplayRuleItem,
    DisplayRuleList,
    MerchantDetail,
    MerchantItem,
    MerchantList,
    MerchantMappingUpdate,
    MerchantMergeRequest,
    MerchantNameUpdate,
    MerchantTransaction,
    SuggestionReview,
)

router = APIRouter()


@router.get("/merchants", response_model=MerchantList)
def list_merchants(
    search: str | None = Query(None, description="Search merchant name"),
    unmapped: bool = Query(False, description="Only show merchants without category"),
    has_suggestions: bool = Query(False, description="Only show merchants with pending suggestions"),
    cursor: str | None = Query(None, description="Cursor for pagination (merchant name)"),
    limit: int = Query(50, ge=1, le=200),
    conn=Depends(get_conn),
):
    """List canonical merchants with mapping counts."""
    cur = conn.cursor()

    conditions = ["cm.merged_into_id IS NULL"]
    params: dict = {"limit": limit + 1}

    if search:
        conditions.append("(cm.name ILIKE %(search)s OR cm.display_name ILIKE %(search)s)")
        params["search"] = f"%{search}%"
    if unmapped:
        conditions.append("cm.category_hint IS NULL")
    if has_suggestions:
        conditions.append("""
            EXISTS (SELECT 1 FROM category_suggestion cs
                    WHERE cs.canonical_merchant_id = cm.id AND cs.status = 'pending')
        """)
    if cursor:
        conditions.append("cm.name > %(cursor)s")
        params["cursor"] = cursor

    where = "WHERE " + " AND ".join(conditions)

    cur.execute(f"""
        SELECT
            cm.id,
            cm.name,
            cm.display_name,
            cm.category_hint,
            cm.category_method,
            cm.category_confidence,
            COUNT(mrm.cleaned_merchant) AS mapping_count
        FROM canonical_merchant cm
        LEFT JOIN merchant_raw_mapping mrm ON mrm.canonical_merchant_id = cm.id
        {where}
        GROUP BY cm.id, cm.name, cm.display_name, cm.category_hint,
                 cm.category_method, cm.category_confidence
        ORDER BY cm.name
        LIMIT %(limit)s
    """, params)

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    items = [MerchantItem(**dict(zip(columns, row))) for row in rows]

    next_cursor = None
    if has_more and items:
        next_cursor = items[-1].name

    return MerchantList(items=items, next_cursor=next_cursor, has_more=has_more)


@router.get("/merchants/suggestions", response_model=CategorySuggestionList)
def list_suggestions(
    status: str = Query("pending", description="Filter by status"),
    limit: int = Query(50, ge=1, le=200),
    conn=Depends(get_conn),
):
    """List category suggestions for review."""
    cur = conn.cursor()

    cur.execute("""
        SELECT cs.id, cs.canonical_merchant_id, cm.name as merchant_name,
               cs.suggested_category_id, cat.full_path as suggested_category_path,
               cs.method, cs.confidence, cs.reasoning, cs.status, cs.created_at
        FROM category_suggestion cs
        JOIN canonical_merchant cm ON cm.id = cs.canonical_merchant_id
        JOIN category cat ON cat.id = cs.suggested_category_id
        WHERE cs.status = %(status)s
        ORDER BY cs.confidence DESC, cm.name
        LIMIT %(limit)s
    """, {"status": status, "limit": limit})

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    items = [CategorySuggestionItem(**dict(zip(columns, row))) for row in rows]

    # Get total count
    cur.execute(
        "SELECT count(*) FROM category_suggestion WHERE status = %(status)s",
        {"status": status},
    )
    total = cur.fetchone()[0]

    return CategorySuggestionList(items=items, total=total)


@router.put("/merchants/suggestions/{suggestion_id}")
def review_suggestion(
    suggestion_id: int,
    body: SuggestionReview,
    conn=Depends(get_conn),
):
    """Accept or reject a category suggestion."""
    if body.status not in ('accepted', 'rejected'):
        raise HTTPException(400, "Status must be 'accepted' or 'rejected'")

    cur = conn.cursor()

    cur.execute("""
        SELECT cs.id, cs.canonical_merchant_id, cs.suggested_category_id, cs.confidence, cs.method
        FROM category_suggestion cs
        WHERE cs.id = %s AND cs.status = 'pending'
    """, (suggestion_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Suggestion not found or already reviewed")

    _, cm_id, cat_id, confidence, method = row

    # Update suggestion status
    cur.execute(
        "UPDATE category_suggestion SET status = %s, reviewed_at = now() WHERE id = %s",
        (body.status, suggestion_id),
    )

    # If accepted, apply the category to the merchant
    if body.status == 'accepted':
        cur.execute("""
            UPDATE canonical_merchant
            SET category_hint = (SELECT full_path FROM category WHERE id = %s),
                category_method = %s,
                category_confidence = %s,
                category_set_at = now()
            WHERE id = %s
        """, (str(cat_id), method, float(confidence), str(cm_id)))

    conn.commit()

    return {"id": suggestion_id, "status": body.status, "applied": body.status == 'accepted'}


# ── Display Rules ────────────────────────────────────────────────────────────
# These must be defined BEFORE /merchants/{merchant_id} to avoid
# FastAPI matching "rules" as a UUID parameter.


@router.get("/merchants/rules", response_model=DisplayRuleList)
def list_rules(conn=Depends(get_conn)):
    """List all merchant display rules."""
    cur = conn.cursor()
    cur.execute("SELECT id, pattern, display_name, merge_group, category_hint, priority FROM merchant_display_rule ORDER BY priority, id")
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    items = [DisplayRuleItem(**dict(zip(columns, row))) for row in rows]
    return DisplayRuleList(items=items)


@router.post("/merchants/rules", response_model=DisplayRuleItem)
def create_rule(body: DisplayRuleCreate, conn=Depends(get_conn)):
    """Create a new merchant display rule."""
    import re
    try:
        re.compile(body.pattern)
    except re.error as e:
        raise HTTPException(400, f"Invalid regex: {e}")

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO merchant_display_rule (pattern, display_name, merge_group, category_hint, priority)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (body.pattern, body.display_name, body.merge_group, body.category_hint, body.priority))
    rule_id = cur.fetchone()[0]
    conn.commit()

    return DisplayRuleItem(id=rule_id, **body.model_dump())


@router.delete("/merchants/rules/{rule_id}")
def delete_rule(rule_id: int, conn=Depends(get_conn)):
    """Delete a merchant display rule."""
    cur = conn.cursor()
    cur.execute("DELETE FROM merchant_display_rule WHERE id = %s", (rule_id,))
    if cur.rowcount == 0:
        raise HTTPException(404, "Rule not found")
    conn.commit()
    return {"id": rule_id, "deleted": True}


# ── Merchant Detail ──────────────────────────────────────────────────────────


@router.get("/merchants/{merchant_id}", response_model=MerchantDetail)
def get_merchant(
    merchant_id: UUID,
    conn=Depends(get_conn),
):
    """Get full merchant detail including aliases."""
    cur = conn.cursor()

    cur.execute("""
        SELECT cm.id, cm.name, cm.display_name, cm.category_hint,
               cm.category_method, cm.category_confidence
        FROM canonical_merchant cm
        WHERE cm.id = %s
    """, (str(merchant_id),))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Merchant not found")

    cm_id, name, display_name, cat_hint, cat_method, cat_conf = row

    # Get aliases (all cleaned merchant strings mapped to this canonical)
    cur.execute("""
        SELECT mrm.cleaned_merchant
        FROM merchant_raw_mapping mrm
        WHERE mrm.canonical_merchant_id = %s
        ORDER BY mrm.cleaned_merchant
    """, (str(merchant_id),))
    aliases = [r[0] for r in cur.fetchall()]

    # Get recent transactions for this merchant
    cur.execute("""
        SELECT rt.id, rt.posted_at, rt.amount, rt.currency, rt.raw_merchant,
               rt.source, rt.institution, rt.account_ref
        FROM merchant_raw_mapping mrm
        JOIN cleaned_transaction ct ON ct.cleaned_merchant = mrm.cleaned_merchant
        JOIN raw_transaction rt ON rt.id = ct.raw_transaction_id
        WHERE mrm.canonical_merchant_id = %s
        ORDER BY rt.posted_at DESC
        LIMIT 20
    """, (str(merchant_id),))
    txn_columns = [desc[0] for desc in cur.description]
    txn_rows = cur.fetchall()
    transactions = [MerchantTransaction(**dict(zip(txn_columns, row))) for row in txn_rows]

    return MerchantDetail(
        id=cm_id,
        name=name,
        display_name=display_name,
        category_hint=cat_hint,
        category_method=cat_method,
        category_confidence=cat_conf,
        mapping_count=len(aliases),
        aliases=aliases,
        recent_transactions=transactions,
    )


@router.put("/merchants/{merchant_id}/name")
def update_merchant_name(
    merchant_id: UUID,
    body: MerchantNameUpdate,
    conn=Depends(get_conn),
):
    """Update a canonical merchant's display name."""
    cur = conn.cursor()

    cur.execute("SELECT id FROM canonical_merchant WHERE id = %s", (str(merchant_id),))
    if not cur.fetchone():
        raise HTTPException(404, "Merchant not found")

    cur.execute(
        "UPDATE canonical_merchant SET display_name = %s WHERE id = %s",
        (body.display_name, str(merchant_id)),
    )
    conn.commit()

    return {"id": str(merchant_id), "display_name": body.display_name, "updated": True}


@router.put("/merchants/{merchant_id}/mapping")
def update_merchant_mapping(
    merchant_id: UUID,
    body: MerchantMappingUpdate,
    conn=Depends(get_conn),
):
    """Update a canonical merchant's category hint."""
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM canonical_merchant WHERE id = %s", (str(merchant_id),))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Merchant not found")

    cur.execute(
        """UPDATE canonical_merchant
           SET category_hint = %s, category_method = 'human', category_confidence = 1.00,
               category_set_at = now()
           WHERE id = %s""",
        (body.category_hint, str(merchant_id)),
    )
    conn.commit()

    return {
        "id": str(merchant_id),
        "name": row[1],
        "category_hint": body.category_hint,
        "updated": True,
    }


@router.post("/merchants/{merchant_id}/merge")
def merge_merchant(
    merchant_id: UUID,
    body: MerchantMergeRequest,
    conn=Depends(get_conn),
):
    """Merge another merchant into this one (this one survives)."""
    from src.categorisation.merger import merge

    try:
        result = merge(conn, secondary_id=str(body.merge_from_id), surviving_id=str(merchant_id))
        conn.commit()
    except ValueError as e:
        raise HTTPException(400, str(e))

    return {
        "surviving_id": str(merchant_id),
        "merged_from_id": str(body.merge_from_id),
        **result,
    }


@router.post("/categorisation/run")
def run_categorisation(
    include_llm: bool = Query(False, description="Also run LLM categorisation"),
    conn=Depends(get_conn),
):
    """Trigger the categorisation engine."""
    from src.categorisation.engine import run_naming, run_source_hints, run_llm

    naming_result = run_naming(conn)
    hints_result = run_source_hints(conn)

    llm_result = {}
    if include_llm:
        llm_result = run_llm(conn)

    return {
        "display_names_set": naming_result["display_names_set"],
        "rules_merchants_merged": naming_result.get("rules_merchants_merged", 0),
        "rules_merchants_renamed": naming_result.get("rules_merchants_renamed", 0),
        "source_hint_suggestions": hints_result["total_suggestions"],
        "auto_accepted": hints_result["auto_accepted"],
        "queued_for_review": hints_result["queued"],
        "llm_queued": llm_result.get("llm_queued", 0),
    }
