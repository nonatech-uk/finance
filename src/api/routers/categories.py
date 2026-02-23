"""Category endpoints."""

from collections import defaultdict
from datetime import date
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from src.api.deps import get_conn
from src.api.models import (
    CategoryItem,
    CategoryTree,
    SpendingByCategory,
    SpendingReport,
)

router = APIRouter()


@router.get("/categories", response_model=CategoryTree)
def list_categories(
    conn=Depends(get_conn),
):
    """Get the full category tree."""
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, full_path, category_type, is_active, parent_id
        FROM category
        ORDER BY full_path
    """)

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    # Build flat list and index by id
    all_cats: dict[UUID, dict] = {}
    for row in rows:
        cat = dict(zip(columns, row))
        cat["children"] = []
        all_cats[cat["id"]] = cat

    # Build tree
    roots = []
    for cat in all_cats.values():
        parent_id = cat["parent_id"]
        if parent_id and parent_id in all_cats:
            all_cats[parent_id]["children"].append(cat)
        else:
            roots.append(cat)

    def _to_model(cat_dict: dict) -> CategoryItem:
        return CategoryItem(
            id=cat_dict["id"],
            name=cat_dict["name"],
            full_path=cat_dict["full_path"],
            category_type=cat_dict["category_type"],
            is_active=cat_dict["is_active"],
            parent_id=cat_dict["parent_id"],
            children=[_to_model(c) for c in cat_dict["children"]],
        )

    items = [_to_model(r) for r in roots]
    return CategoryTree(items=items)


@router.get("/categories/spending", response_model=SpendingReport)
def spending_by_category(
    date_from: date = Query(..., description="Start date (inclusive)"),
    date_to: date = Query(..., description="End date (inclusive)"),
    institution: str | None = None,
    account_ref: str | None = None,
    currency: str = Query("GBP", description="Currency to report in"),
    conn=Depends(get_conn),
):
    """Get spending aggregated by category for a date range."""
    cur = conn.cursor()

    conditions = [
        "rt.posted_at >= %(date_from)s",
        "rt.posted_at <= %(date_to)s",
        "rt.currency = %(currency)s",
        "(acct.exclude_from_reports IS NOT TRUE)",
    ]
    params: dict = {
        "date_from": date_from,
        "date_to": date_to,
        "currency": currency,
    }

    if institution:
        conditions.append("rt.institution = %(institution)s")
        params["institution"] = institution
    if account_ref:
        conditions.append("rt.account_ref = %(account_ref)s")
        params["account_ref"] = account_ref

    where = " AND ".join(conditions)

    cur.execute(f"""
        SELECT
            COALESCE(cat.full_path, 'Uncategorised') AS category_path,
            COALESCE(cat.name, 'Uncategorised') AS category_name,
            cat.category_type,
            SUM(rt.amount) AS total,
            COUNT(*) AS transaction_count
        FROM active_transaction rt
        LEFT JOIN account acct
            ON acct.institution = rt.institution
            AND acct.account_ref = rt.account_ref
        LEFT JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
        LEFT JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        LEFT JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        LEFT JOIN category cat ON cat.full_path = cm.category_hint
        WHERE {where}
        GROUP BY cat.full_path, cat.name, cat.category_type
        ORDER BY total ASC
    """, params)

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    items = [SpendingByCategory(**dict(zip(columns, row))) for row in rows]

    total_income = sum(i.total for i in items if i.total > 0)
    total_expense = sum(i.total for i in items if i.total < 0)

    return SpendingReport(
        items=items,
        date_from=date_from,
        date_to=date_to,
        total_income=total_income,
        total_expense=total_expense,
    )
