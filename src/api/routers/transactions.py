"""Transaction endpoints."""

from datetime import date
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.deps import get_conn
from src.api.models import (
    DedupGroupInfo,
    DedupMember,
    EconomicEventInfo,
    EconomicEventLeg,
    TransactionDetail,
    TransactionItem,
    TransactionList,
)

router = APIRouter()


@router.get("/transactions", response_model=TransactionList)
def list_transactions(
    cursor: str | None = Query(None, description="Cursor for pagination (posted_at,id)"),
    limit: int = Query(50, ge=1, le=200),
    institution: str | None = None,
    account_ref: str | None = None,
    source: str | None = None,
    category: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    amount_min: Decimal | None = None,
    amount_max: Decimal | None = None,
    currency: str | None = None,
    search: str | None = Query(None, description="Search merchant name"),
    conn=Depends(get_conn),
):
    """List deduplicated transactions with full merchant/category chain."""
    cur = conn.cursor()

    # Build WHERE clauses
    conditions = []
    params: dict = {"limit": limit + 1}  # fetch one extra to detect has_more

    # Keyset pagination: cursor is "posted_at,id"
    if cursor:
        try:
            cursor_date_str, cursor_id = cursor.split(",", 1)
            params["cursor_date"] = cursor_date_str
            params["cursor_id"] = cursor_id
            conditions.append(
                "(rt.posted_at, rt.id) < (%(cursor_date)s::date, %(cursor_id)s::uuid)"
            )
        except ValueError:
            raise HTTPException(400, "Invalid cursor format, expected 'date,uuid'")

    if institution:
        conditions.append("rt.institution = %(institution)s")
        params["institution"] = institution
    if account_ref:
        conditions.append("rt.account_ref = %(account_ref)s")
        params["account_ref"] = account_ref
    if source:
        conditions.append("rt.source = %(source)s")
        params["source"] = source
    if date_from:
        conditions.append("rt.posted_at >= %(date_from)s")
        params["date_from"] = date_from
    if date_to:
        conditions.append("rt.posted_at <= %(date_to)s")
        params["date_to"] = date_to
    if amount_min is not None:
        conditions.append("rt.amount >= %(amount_min)s")
        params["amount_min"] = amount_min
    if amount_max is not None:
        conditions.append("rt.amount <= %(amount_max)s")
        params["amount_max"] = amount_max
    if currency:
        conditions.append("rt.currency = %(currency)s")
        params["currency"] = currency
    if search:
        conditions.append(
            "(cm.name ILIKE %(search)s OR ct.cleaned_merchant ILIKE %(search)s "
            "OR rt.raw_merchant ILIKE %(search)s)"
        )
        params["search"] = f"%{search}%"
    if category:
        conditions.append("cat.full_path LIKE %(category)s")
        params["category"] = f"{category}%"

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT
            rt.id, rt.source, rt.institution, rt.account_ref,
            rt.posted_at, rt.amount, rt.currency,
            rt.raw_merchant, rt.raw_memo,
            ct.cleaned_merchant,
            cm.id AS canonical_merchant_id,
            cm.name AS canonical_merchant_name,
            mrm.match_type AS merchant_match_type,
            cat.full_path AS category_path,
            cat.name AS category_name,
            cat.category_type
        FROM active_transaction rt
        LEFT JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
        LEFT JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        LEFT JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        LEFT JOIN category cat ON cat.full_path = cm.category_hint
        {where}
        ORDER BY rt.posted_at DESC, rt.id DESC
        LIMIT %(limit)s
    """

    cur.execute(sql, params)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    items = [TransactionItem(**dict(zip(columns, row))) for row in rows]

    next_cursor = None
    if has_more and items:
        last = items[-1]
        next_cursor = f"{last.posted_at},{last.id}"

    return TransactionList(items=items, next_cursor=next_cursor, has_more=has_more)


@router.get("/transactions/{transaction_id}", response_model=TransactionDetail)
def get_transaction(
    transaction_id: UUID,
    conn=Depends(get_conn),
):
    """Get full transaction detail including dedup group and economic event."""
    cur = conn.cursor()

    # Base transaction with merchant/category chain
    cur.execute("""
        SELECT
            rt.id, rt.source, rt.institution, rt.account_ref,
            rt.posted_at, rt.amount, rt.currency,
            rt.raw_merchant, rt.raw_memo, rt.raw_data,
            ct.cleaned_merchant,
            cm.id AS canonical_merchant_id,
            cm.name AS canonical_merchant_name,
            mrm.match_type AS merchant_match_type,
            cat.full_path AS category_path,
            cat.name AS category_name,
            cat.category_type
        FROM raw_transaction rt
        LEFT JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
        LEFT JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        LEFT JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        LEFT JOIN category cat ON cat.full_path = cm.category_hint
        WHERE rt.id = %s
    """, (str(transaction_id),))

    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Transaction not found")

    columns = [desc[0] for desc in cur.description]
    txn_data = dict(zip(columns, row))

    # Dedup group info
    dedup_group = None
    cur.execute("""
        SELECT dg.id, dg.match_rule, dg.confidence,
               dgm2.raw_transaction_id, rt2.source, dgm2.is_preferred
        FROM dedup_group_member dgm
        JOIN dedup_group dg ON dg.id = dgm.dedup_group_id
        JOIN dedup_group_member dgm2 ON dgm2.dedup_group_id = dg.id
        JOIN raw_transaction rt2 ON rt2.id = dgm2.raw_transaction_id
        WHERE dgm.raw_transaction_id = %s
    """, (str(transaction_id),))

    dedup_rows = cur.fetchall()
    if dedup_rows:
        members = [
            DedupMember(
                raw_transaction_id=r[3],
                source=r[4],
                is_preferred=r[5],
            )
            for r in dedup_rows
        ]
        dedup_group = DedupGroupInfo(
            group_id=dedup_rows[0][0],
            match_rule=dedup_rows[0][1],
            confidence=dedup_rows[0][2],
            members=members,
        )

    # Economic event info
    economic_event = None
    cur.execute("""
        SELECT ee.id, ee.event_type, ee.initiated_at, ee.description,
               eel2.raw_transaction_id, eel2.leg_type, eel2.amount, eel2.currency
        FROM economic_event_leg eel
        JOIN economic_event ee ON ee.id = eel.economic_event_id
        JOIN economic_event_leg eel2 ON eel2.economic_event_id = ee.id
        WHERE eel.raw_transaction_id = %s
    """, (str(transaction_id),))

    event_rows = cur.fetchall()
    if event_rows:
        legs = [
            EconomicEventLeg(
                raw_transaction_id=r[4],
                leg_type=r[5],
                amount=r[6],
                currency=r[7],
            )
            for r in event_rows
        ]
        economic_event = EconomicEventInfo(
            event_id=event_rows[0][0],
            event_type=event_rows[0][1],
            initiated_at=event_rows[0][2],
            description=event_rows[0][3],
            legs=legs,
        )

    return TransactionDetail(
        **txn_data,
        dedup_group=dedup_group,
        economic_event=economic_event,
    )
