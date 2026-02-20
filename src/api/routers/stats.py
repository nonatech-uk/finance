"""Statistics endpoints."""

from decimal import Decimal

from fastapi import APIRouter, Depends, Query

from src.api.deps import get_conn
from src.api.models import MonthlyReport, MonthlyTotal, OverviewStats

router = APIRouter()


@router.get("/stats/monthly", response_model=MonthlyReport)
def monthly_totals(
    months: int = Query(12, ge=1, le=120, description="Number of months"),
    institution: str | None = None,
    account_ref: str | None = None,
    currency: str = Query("GBP", description="Currency"),
    conn=Depends(get_conn),
):
    """Monthly income/expense totals."""
    cur = conn.cursor()

    conditions = [
        "rt.currency = %(currency)s",
        "rt.posted_at >= (CURRENT_DATE - %(months)s * INTERVAL '1 month')",
    ]
    params: dict = {"currency": currency, "months": months}

    if institution:
        conditions.append("rt.institution = %(institution)s")
        params["institution"] = institution
    if account_ref:
        conditions.append("rt.account_ref = %(account_ref)s")
        params["account_ref"] = account_ref

    where = " AND ".join(conditions)

    cur.execute(f"""
        SELECT
            TO_CHAR(rt.posted_at, 'YYYY-MM') AS month,
            SUM(CASE WHEN rt.amount > 0 THEN rt.amount ELSE 0 END) AS income,
            SUM(CASE WHEN rt.amount < 0 THEN rt.amount ELSE 0 END) AS expense,
            SUM(rt.amount) AS net,
            COUNT(*) AS transaction_count
        FROM active_transaction rt
        WHERE {where}
        GROUP BY TO_CHAR(rt.posted_at, 'YYYY-MM')
        ORDER BY month DESC
    """, params)

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    items = [MonthlyTotal(**dict(zip(columns, row))) for row in rows]

    return MonthlyReport(items=items, currency=currency)


@router.get("/stats/overview", response_model=OverviewStats)
def overview(
    conn=Depends(get_conn),
):
    """Dashboard overview statistics."""
    cur = conn.cursor()

    # Accounts
    cur.execute("SELECT count(*) FROM account")
    total_accounts = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM account WHERE is_active")
    active_accounts = cur.fetchone()[0]

    # Transactions
    cur.execute("SELECT count(*) FROM raw_transaction")
    total_raw = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM active_transaction")
    total_active = cur.fetchone()[0]

    # Dedup
    cur.execute("SELECT count(*) FROM dedup_group")
    dedup_groups = cur.fetchone()[0]

    # Category coverage: active transactions that have a category via the JOIN chain
    cur.execute("""
        SELECT count(*) FROM active_transaction rt
        JOIN cleaned_transaction ct ON ct.raw_transaction_id = rt.id
        JOIN merchant_raw_mapping mrm ON mrm.cleaned_merchant = ct.cleaned_merchant
        JOIN canonical_merchant cm ON cm.id = mrm.canonical_merchant_id
        WHERE cm.category_hint IS NOT NULL
    """)
    categorised = cur.fetchone()[0]
    coverage = Decimal(categorised * 100) / Decimal(total_active) if total_active > 0 else Decimal(0)

    # Institutions
    cur.execute("SELECT DISTINCT institution FROM active_transaction ORDER BY institution")
    institutions = [r[0] for r in cur.fetchall()]

    # Date range
    cur.execute("SELECT min(posted_at), max(posted_at) FROM active_transaction")
    date_row = cur.fetchone()

    return OverviewStats(
        total_accounts=total_accounts,
        active_accounts=active_accounts,
        total_raw_transactions=total_raw,
        active_transactions=total_active,
        dedup_groups=dedup_groups,
        removed_by_dedup=total_raw - total_active,
        category_coverage_pct=round(coverage, 1),
        institutions=institutions,
        date_range_from=date_row[0] if date_row else None,
        date_range_to=date_row[1] if date_row else None,
    )
