"""Account endpoints."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.deps import get_conn
from src.api.models import TransactionItem

router = APIRouter()


@router.get("/accounts")
def list_accounts(
    conn=Depends(get_conn),
):
    """List all accounts derived from transaction data, with summaries.

    Returns distinct institution/account_ref/currency combos from active
    transactions, enriched with account table metadata where available.
    """
    cur = conn.cursor()

    cur.execute("""
        SELECT
            rt.institution,
            rt.account_ref,
            rt.currency,
            count(*) AS transaction_count,
            min(rt.posted_at) AS earliest_date,
            max(rt.posted_at) AS latest_date,
            sum(rt.amount) AS balance,
            a.id AS account_id,
            a.name AS account_name,
            a.account_type,
            a.is_active
        FROM active_transaction rt
        LEFT JOIN account a
            ON a.institution = rt.institution
            AND a.currency = rt.currency
            AND a.name LIKE '%%' || rt.account_ref || '%%'
        GROUP BY rt.institution, rt.account_ref, rt.currency,
                 a.id, a.name, a.account_type, a.is_active
        ORDER BY rt.institution, rt.account_ref
    """)

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    items = [dict(zip(columns, row)) for row in rows]

    # Convert Decimal/date to JSON-friendly types
    for item in items:
        if item["balance"] is not None:
            item["balance"] = str(item["balance"])
        if item["earliest_date"] is not None:
            item["earliest_date"] = str(item["earliest_date"])
        if item["latest_date"] is not None:
            item["latest_date"] = str(item["latest_date"])
        if item["account_id"] is not None:
            item["account_id"] = str(item["account_id"])

    return {"items": items}


@router.get("/accounts/{institution}/{account_ref}")
def get_account_detail(
    institution: str,
    account_ref: str,
    limit: int = Query(20, ge=1, le=100),
    conn=Depends(get_conn),
):
    """Get account detail with summary and recent transactions."""
    cur = conn.cursor()

    # Summary
    cur.execute("""
        SELECT
            count(*) AS transaction_count,
            min(posted_at) AS earliest_date,
            max(posted_at) AS latest_date,
            sum(amount) AS balance,
            currency
        FROM active_transaction
        WHERE institution = %s AND account_ref = %s
        GROUP BY currency
    """, (institution, account_ref))

    summary_row = cur.fetchone()
    if not summary_row:
        raise HTTPException(404, "Account not found")

    summary_cols = [desc[0] for desc in cur.description]
    summary = dict(zip(summary_cols, summary_row))

    # Recent transactions
    cur.execute("""
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
        WHERE rt.institution = %s AND rt.account_ref = %s
        ORDER BY rt.posted_at DESC, rt.id DESC
        LIMIT %s
    """, (institution, account_ref, limit))

    txn_columns = [desc[0] for desc in cur.description]
    txn_rows = cur.fetchall()
    transactions = [TransactionItem(**dict(zip(txn_columns, r))) for r in txn_rows]

    return {
        "institution": institution,
        "account_ref": account_ref,
        "summary": summary,
        "recent_transactions": [t.model_dump(mode="json") for t in transactions],
    }
