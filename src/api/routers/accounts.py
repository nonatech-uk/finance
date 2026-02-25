"""Account endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.deps import get_conn
from src.api.models import AccountUpdate, TransactionItem

router = APIRouter()


@router.get("/accounts")
def list_accounts(
    include_archived: bool = Query(False, description="Include archived accounts"),
    scope: str | None = Query("personal", description="Filter by scope (personal/business/null for all)"),
    conn=Depends(get_conn),
):
    """List all accounts derived from transaction data, with summaries.

    Returns distinct institution/account_ref/currency combos from active
    transactions, enriched with account table metadata.
    """
    cur = conn.cursor()

    extra_filters = ""
    if not include_archived:
        extra_filters += " AND (a.is_archived IS NOT TRUE)"
    if scope and scope in ("personal", "business"):
        extra_filters += f" AND (a.scope = '{scope}')"

    cur.execute(f"""
        SELECT
            rt.institution,
            rt.account_ref,
            rt.currency,
            count(*) AS transaction_count,
            min(rt.posted_at) AS earliest_date,
            max(rt.posted_at) AS latest_date,
            sum(rt.amount) AS balance,
            a.name AS account_name,
            a.display_name,
            a.account_type,
            a.is_active,
            a.is_archived,
            a.exclude_from_reports,
            a.scope
        FROM active_transaction rt
        LEFT JOIN account a
            ON a.institution = rt.institution
            AND a.account_ref = rt.account_ref
        WHERE 1=1 {extra_filters}
        GROUP BY rt.institution, rt.account_ref, rt.currency,
                 a.name, a.display_name, a.account_type, a.is_active,
                 a.is_archived, a.exclude_from_reports, a.scope
        ORDER BY rt.institution, rt.account_ref
    """)

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    items = []
    for row in rows:
        item = dict(zip(columns, row))
        # Rename account_name -> name for model
        item["name"] = item.pop("account_name", None)
        # Convert types for JSON
        if item["balance"] is not None:
            item["balance"] = str(item["balance"])
        if item["earliest_date"] is not None:
            item["earliest_date"] = str(item["earliest_date"])
        if item["latest_date"] is not None:
            item["latest_date"] = str(item["latest_date"])
        items.append(item)

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

    # Summary with account metadata
    cur.execute("""
        SELECT
            count(*) AS transaction_count,
            min(rt.posted_at) AS earliest_date,
            max(rt.posted_at) AS latest_date,
            sum(rt.amount) AS balance,
            rt.currency,
            a.name AS account_name,
            a.display_name,
            a.account_type,
            a.is_active,
            a.is_archived,
            a.exclude_from_reports,
            a.scope
        FROM active_transaction rt
        LEFT JOIN account a
            ON a.institution = rt.institution
            AND a.account_ref = rt.account_ref
        WHERE rt.institution = %s AND rt.account_ref = %s
        GROUP BY rt.currency, a.name, a.display_name, a.account_type,
                 a.is_active, a.is_archived, a.exclude_from_reports, a.scope
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


@router.put("/accounts/{institution}/{account_ref}")
def update_account(
    institution: str,
    account_ref: str,
    body: AccountUpdate,
    conn=Depends(get_conn),
):
    """Update account metadata (display_name, is_archived, exclude_from_reports).

    Upserts: creates the account row if it doesn't exist.
    """
    cur = conn.cursor()

    # Build SET clause from non-None fields
    updates = {}
    if body.display_name is not None:
        updates["display_name"] = body.display_name
    if body.is_archived is not None:
        updates["is_archived"] = body.is_archived
    if body.exclude_from_reports is not None:
        updates["exclude_from_reports"] = body.exclude_from_reports
    if body.scope is not None:
        if body.scope not in ("personal", "business"):
            raise HTTPException(400, "scope must be 'personal' or 'business'")
        updates["scope"] = body.scope

    if not updates:
        raise HTTPException(400, "No fields to update")

    # Check if row exists
    cur.execute(
        "SELECT id FROM account WHERE institution = %s AND account_ref = %s",
        (institution, account_ref),
    )
    existing = cur.fetchone()

    if existing:
        set_clause = ", ".join(f"{k} = %({k})s" for k in updates)
        updates["institution"] = institution
        updates["account_ref"] = account_ref
        cur.execute(
            f"UPDATE account SET {set_clause} "
            "WHERE institution = %(institution)s AND account_ref = %(account_ref)s",
            updates,
        )
    else:
        # Determine currency from transactions
        cur.execute(
            "SELECT currency FROM active_transaction "
            "WHERE institution = %s AND account_ref = %s LIMIT 1",
            (institution, account_ref),
        )
        ccy_row = cur.fetchone()
        if not ccy_row:
            raise HTTPException(404, "Account not found in transactions")

        updates["institution"] = institution
        updates["account_ref"] = account_ref
        updates["name"] = account_ref
        updates["currency"] = ccy_row[0]
        updates["account_type"] = "current"

        cols = ", ".join(updates.keys())
        vals = ", ".join(f"%({k})s" for k in updates)
        cur.execute(f"INSERT INTO account ({cols}) VALUES ({vals})", updates)

    conn.commit()

    # Return updated row
    cur.execute("""
        SELECT institution, account_ref, name, display_name, currency,
               account_type, is_active, is_archived, exclude_from_reports, scope
        FROM account
        WHERE institution = %s AND account_ref = %s
    """, (institution, account_ref))
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))
