"""Cash account API — manual transactions and balance resets."""

import json
import uuid

from fastapi import APIRouter, Depends, HTTPException

from src.api.deps import CurrentUser, get_conn, require_admin
from src.api.models import CashBalanceReset, CashTransactionCreate

router = APIRouter()


@router.post("/cash/transactions")
def create_cash_transaction(
    body: CashTransactionCreate,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Create a manual cash transaction (spending, income, etc.)."""
    cur = conn.cursor()

    # Validate account exists and is a cash account
    cur.execute("""
        SELECT currency FROM account
        WHERE institution = 'cash' AND account_ref = %s
    """, (body.account_ref,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Cash account '{body.account_ref}' not found")
    currency = row[0].strip()

    txn_ref = f"cash_manual_{uuid.uuid4()}"

    cur.execute("""
        INSERT INTO raw_transaction (
            source, institution, account_ref, transaction_ref,
            posted_at, amount, currency,
            raw_merchant, raw_memo, is_dirty, raw_data
        ) VALUES (
            'manual', 'cash', %s, %s,
            %s, %s, %s,
            %s, NULL, false, %s
        )
        RETURNING id
    """, (
        body.account_ref,
        txn_ref,
        body.posted_at,
        body.amount,
        currency,
        body.description,
        json.dumps({"type": "manual_cash_entry"}),
    ))
    txn_id = cur.fetchone()[0]

    # Optional category override
    if body.category_path:
        cur.execute("""
            INSERT INTO transaction_category_override (raw_transaction_id, category_path, source)
            VALUES (%s, %s, 'manual')
            ON CONFLICT (raw_transaction_id) DO UPDATE
                SET category_path = EXCLUDED.category_path, updated_at = now()
        """, (txn_id, body.category_path))

    # Optional tags
    for tag in body.tags:
        cur.execute("""
            INSERT INTO transaction_tag (raw_transaction_id, tag, source)
            VALUES (%s, %s, 'manual')
            ON CONFLICT (raw_transaction_id, tag) DO NOTHING
        """, (txn_id, tag))

    # Optional note
    if body.note:
        cur.execute("""
            INSERT INTO transaction_note (raw_transaction_id, note, source)
            VALUES (%s, %s, 'manual')
            ON CONFLICT (raw_transaction_id) DO UPDATE
                SET note = EXCLUDED.note, updated_at = now()
        """, (txn_id, body.note))

    conn.commit()

    return {"ok": True, "transaction_id": str(txn_id)}


@router.post("/cash/{account_ref}/reset-balance")
def reset_cash_balance(
    account_ref: str,
    body: CashBalanceReset,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Reset a cash account balance by inserting a synthetic adjustment."""
    cur = conn.cursor()

    # Validate account exists and is a cash account
    cur.execute("""
        SELECT currency FROM account
        WHERE institution = 'cash' AND account_ref = %s
    """, (account_ref,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Cash account '{account_ref}' not found")
    currency = row[0].strip()

    txn_ref = f"cash_reset_{account_ref}_{body.posted_at}"

    # Current balance EXCLUDING any existing same-date reset, so the
    # adjustment is correct even when upserting over a prior reset.
    cur.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM active_transaction
        WHERE institution = 'cash' AND account_ref = %s
          AND transaction_ref IS DISTINCT FROM %s
    """, (account_ref, txn_ref))
    current_balance = cur.fetchone()[0]

    adjustment = body.target_balance - current_balance
    if abs(adjustment) < 0.01:
        return {
            "ok": True,
            "adjustment": "0.00",
            "new_balance": str(current_balance),
        }

    # Upsert: same-date resets update rather than duplicate
    cur.execute("""
        INSERT INTO raw_transaction (
            source, institution, account_ref, transaction_ref,
            posted_at, amount, currency,
            raw_merchant, raw_memo, is_dirty, raw_data
        ) VALUES (
            'synthetic', 'cash', %s, %s,
            %s, %s, %s,
            'Balance Adjustment', %s, false, %s
        )
        ON CONFLICT (institution, account_ref, transaction_ref)
            WHERE transaction_ref IS NOT NULL
        DO UPDATE SET amount = EXCLUDED.amount, posted_at = EXCLUDED.posted_at
        RETURNING id
    """, (
        account_ref,
        txn_ref,
        body.posted_at,
        adjustment,
        currency,
        f"Reset balance to {body.target_balance}",
        json.dumps({
            "type": "balance_reset",
            "target_balance": str(body.target_balance),
            "previous_balance": str(current_balance),
        }),
    ))
    txn_id = cur.fetchone()[0]

    # Set +Ignore category on balance adjustments
    cur.execute("""
        INSERT INTO transaction_category_override (raw_transaction_id, category_path, source)
        VALUES (%s, '+Ignore', 'system')
        ON CONFLICT (raw_transaction_id) DO UPDATE
            SET category_path = '+Ignore', updated_at = now()
    """, (txn_id,))

    conn.commit()

    new_balance = current_balance + adjustment

    return {
        "ok": True,
        "adjustment": str(adjustment),
        "new_balance": str(new_balance),
    }
