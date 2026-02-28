"""Other Assets endpoints — manual valuations for non-stock assets."""

from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException

from src.api.deps import CurrentUser, get_conn, get_current_user, require_admin
from src.api.models import (
    AssetHoldingCreate,
    AssetHoldingUpdate,
    AssetValuationCreate,
)

router = APIRouter()


# ── Helpers ──────────────────────────────────────────────────────────────────


def _holding_with_latest(cur, row: dict) -> dict:
    """Enrich a holding dict with latest valuation data."""
    cur.execute("""
        SELECT valuation_date, gross_value, tax_payable
        FROM asset_valuation
        WHERE holding_id = %s
        ORDER BY valuation_date DESC, created_at DESC
        LIMIT 1
    """, (str(row["id"]),))
    val = cur.fetchone()
    if val:
        gross = val[1]
        tax = val[2]
        row["valuation_date"] = val[0]
        row["latest_gross_value"] = str(gross)
        row["latest_tax_payable"] = str(tax)
        row["latest_net_value"] = str(gross - tax)
    else:
        row["valuation_date"] = None
        row["latest_gross_value"] = None
        row["latest_tax_payable"] = None
        row["latest_net_value"] = None
    return row


# ── Summary ──────────────────────────────────────────────────────────────────


@router.get("/assets/summary")
def get_summary(
    conn=Depends(get_conn),
    user: CurrentUser = Depends(get_current_user),
):
    """Total values + all holdings with latest valuation."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, asset_type, currency, scope, is_active, notes,
               created_at, updated_at
        FROM asset_holding
        WHERE is_active
        ORDER BY name
    """)
    columns = [d[0] for d in cur.description]
    holdings = []
    total_gross = Decimal("0")
    total_tax = Decimal("0")

    for row_tuple in cur.fetchall():
        row = dict(zip(columns, row_tuple))
        row["id"] = str(row["id"])
        row = _holding_with_latest(cur, row)
        if row["latest_gross_value"]:
            total_gross += Decimal(row["latest_gross_value"])
            total_tax += Decimal(row["latest_tax_payable"])
        holdings.append(row)

    return {
        "total_gross_value": str(total_gross),
        "total_tax_payable": str(total_tax),
        "total_net_value": str(total_gross - total_tax),
        "holdings": holdings,
    }


# ── Holdings CRUD ────────────────────────────────────────────────────────────


@router.get("/assets/holdings")
def list_holdings(
    conn=Depends(get_conn),
    user: CurrentUser = Depends(get_current_user),
):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, asset_type, currency, scope, is_active, notes,
               created_at, updated_at
        FROM asset_holding
        ORDER BY name
    """)
    columns = [d[0] for d in cur.description]
    items = []
    for row_tuple in cur.fetchall():
        row = dict(zip(columns, row_tuple))
        row["id"] = str(row["id"])
        row = _holding_with_latest(cur, row)
        items.append(row)
    return {"items": items}


@router.post("/assets/holdings")
def create_holding(
    body: AssetHoldingCreate,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO asset_holding (name, asset_type, currency, scope, notes)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id, name, asset_type, currency, scope, is_active, notes,
                  created_at, updated_at
    """, (body.name, body.asset_type, body.currency, body.scope, body.notes))
    columns = [d[0] for d in cur.description]
    row = dict(zip(columns, cur.fetchone()))
    row["id"] = str(row["id"])
    conn.commit()
    return _holding_with_latest(cur, row)


@router.put("/assets/holdings/{holding_id}")
def update_holding(
    holding_id: str,
    body: AssetHoldingUpdate,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    cur = conn.cursor()
    updates = []
    values = []
    for field in ("name", "asset_type", "is_active", "notes"):
        val = getattr(body, field)
        if val is not None:
            updates.append(f"{field} = %s")
            values.append(val)
    if not updates:
        raise HTTPException(400, "No fields to update")
    updates.append("updated_at = now()")
    values.append(holding_id)
    cur.execute(f"""
        UPDATE asset_holding SET {', '.join(updates)}
        WHERE id = %s
        RETURNING id, name, asset_type, currency, scope, is_active, notes,
                  created_at, updated_at
    """, values)
    row_tuple = cur.fetchone()
    if not row_tuple:
        raise HTTPException(404, "Holding not found")
    columns = [d[0] for d in cur.description]
    row = dict(zip(columns, row_tuple))
    row["id"] = str(row["id"])
    conn.commit()
    return _holding_with_latest(cur, row)


@router.delete("/assets/holdings/{holding_id}")
def delete_holding(
    holding_id: str,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    cur = conn.cursor()
    # Delete valuations first (FK constraint)
    cur.execute("DELETE FROM asset_valuation WHERE holding_id = %s", (holding_id,))
    cur.execute("DELETE FROM asset_holding WHERE id = %s RETURNING id", (holding_id,))
    if not cur.fetchone():
        raise HTTPException(404, "Holding not found")
    conn.commit()
    return {"ok": True}


@router.get("/assets/holdings/{holding_id}")
def get_holding(
    holding_id: str,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(get_current_user),
):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, asset_type, currency, scope, is_active, notes,
               created_at, updated_at
        FROM asset_holding WHERE id = %s
    """, (holding_id,))
    row_tuple = cur.fetchone()
    if not row_tuple:
        raise HTTPException(404, "Holding not found")
    columns = [d[0] for d in cur.description]
    row = dict(zip(columns, row_tuple))
    row["id"] = str(row["id"])
    row = _holding_with_latest(cur, row)

    # Valuation history
    cur.execute("""
        SELECT id, holding_id, valuation_date, gross_value, tax_payable, notes, created_at
        FROM asset_valuation
        WHERE holding_id = %s
        ORDER BY valuation_date DESC, created_at DESC
    """, (holding_id,))
    val_columns = [d[0] for d in cur.description]
    valuations = []
    for vr in cur.fetchall():
        v = dict(zip(val_columns, vr))
        v["id"] = str(v["id"])
        v["holding_id"] = str(v["holding_id"])
        v["gross_value"] = str(v["gross_value"])
        v["tax_payable"] = str(v["tax_payable"])
        v["net_value"] = str(v["gross_value"] if isinstance(v["gross_value"], Decimal) else Decimal(v["gross_value"]) - (v["tax_payable"] if isinstance(v["tax_payable"], Decimal) else Decimal(v["tax_payable"])))
        valuations.append(v)

    row["valuations"] = valuations
    return row


# ── Valuations ───────────────────────────────────────────────────────────────


@router.post("/assets/holdings/{holding_id}/valuations")
def add_valuation(
    holding_id: str,
    body: AssetValuationCreate,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    cur = conn.cursor()
    # Verify holding exists
    cur.execute("SELECT id FROM asset_holding WHERE id = %s", (holding_id,))
    if not cur.fetchone():
        raise HTTPException(404, "Holding not found")

    cur.execute("""
        INSERT INTO asset_valuation (holding_id, valuation_date, gross_value, tax_payable, notes)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id, holding_id, valuation_date, gross_value, tax_payable, notes, created_at
    """, (holding_id, body.valuation_date, body.gross_value, body.tax_payable, body.notes))
    columns = [d[0] for d in cur.description]
    row = dict(zip(columns, cur.fetchone()))
    row["id"] = str(row["id"])
    row["holding_id"] = str(row["holding_id"])
    gross = row["gross_value"]
    tax = row["tax_payable"]
    row["gross_value"] = str(gross)
    row["tax_payable"] = str(tax)
    row["net_value"] = str(gross - tax)
    conn.commit()
    return row
