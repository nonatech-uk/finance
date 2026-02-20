"""Merchant endpoints."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.deps import get_conn
from src.api.models import MerchantItem, MerchantList, MerchantMappingUpdate

router = APIRouter()


@router.get("/merchants", response_model=MerchantList)
def list_merchants(
    search: str | None = Query(None, description="Search merchant name"),
    unmapped: bool = Query(False, description="Only show merchants without category"),
    cursor: str | None = Query(None, description="Cursor for pagination (merchant name)"),
    limit: int = Query(50, ge=1, le=200),
    conn=Depends(get_conn),
):
    """List canonical merchants with mapping counts."""
    cur = conn.cursor()

    conditions = []
    params: dict = {"limit": limit + 1}

    if search:
        conditions.append("cm.name ILIKE %(search)s")
        params["search"] = f"%{search}%"
    if unmapped:
        conditions.append("cm.category_hint IS NULL")
    if cursor:
        conditions.append("cm.name > %(cursor)s")
        params["cursor"] = cursor

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    cur.execute(f"""
        SELECT
            cm.id,
            cm.name,
            cm.category_hint,
            COUNT(mrm.cleaned_merchant) AS mapping_count
        FROM canonical_merchant cm
        LEFT JOIN merchant_raw_mapping mrm ON mrm.canonical_merchant_id = cm.id
        {where}
        GROUP BY cm.id, cm.name, cm.category_hint
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
        "UPDATE canonical_merchant SET category_hint = %s WHERE id = %s",
        (body.category_hint, str(merchant_id)),
    )
    conn.commit()

    return {
        "id": str(merchant_id),
        "name": row[1],
        "category_hint": body.category_hint,
        "updated": True,
    }
