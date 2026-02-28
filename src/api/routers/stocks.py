"""Stock portfolio endpoints."""

from decimal import Decimal
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.deps import CurrentUser, get_conn, get_current_user, require_admin
from src.api.models import (
    CgtSummary,
    DisposalItem,
    PortfolioSummary,
    StockHoldingCreate,
    StockHoldingItem,
    StockHoldingUpdate,
    StockTradeCreate,
    StockTradeItem,
    TaxYearIncomeItem,
    TaxYearIncomeUpdate,
)

router = APIRouter()


# ── Helpers ──────────────────────────────────────────────────────────────────


def _holding_with_stats(cur, holding_row: dict, prices: dict | None = None) -> dict:
    """Enrich a holding dict with computed shares, cost, and P&L."""
    hid = str(holding_row["id"])

    # Compute current shares and total cost from trades
    cur.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN trade_type = 'buy' THEN quantity ELSE -quantity END), 0) AS current_shares,
            COALESCE(SUM(CASE WHEN trade_type = 'buy' THEN total_cost ELSE 0 END), 0) AS buy_cost,
            COALESCE(SUM(CASE WHEN trade_type = 'sell' THEN quantity ELSE 0 END), 0) AS sold_shares,
            COALESCE(SUM(CASE WHEN trade_type = 'sell' THEN total_cost ELSE 0 END), 0) AS sell_proceeds
        FROM stock_trade WHERE holding_id = %s
    """, (hid,))
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    stats = dict(zip(cols, row))

    current_shares = stats["current_shares"]
    buy_cost = stats["buy_cost"]
    sold_shares = stats["sold_shares"]

    # Average cost per share (from buys only)
    total_bought = current_shares + sold_shares
    avg_cost = (buy_cost / total_bought) if total_bought > 0 else Decimal("0")

    # Cost basis for currently held shares
    total_cost = avg_cost * current_shares

    holding_row["current_shares"] = str(current_shares)
    holding_row["average_cost"] = str(avg_cost.quantize(Decimal("0.01")))
    holding_row["total_cost"] = str(total_cost.quantize(Decimal("0.01")))

    # Price data
    price_info = (prices or {}).get(hid)
    if price_info and current_shares > 0:
        price = price_info["close_price"]
        value = price * current_shares
        pnl = value - total_cost
        pnl_pct = (pnl / total_cost * 100) if total_cost > 0 else Decimal("0")
        holding_row["current_price"] = str(price)
        holding_row["current_value"] = str(value.quantize(Decimal("0.01")))
        holding_row["unrealised_pnl"] = str(pnl.quantize(Decimal("0.01")))
        holding_row["unrealised_pnl_pct"] = str(pnl_pct.quantize(Decimal("0.01")))
        holding_row["price_date"] = str(price_info["price_date"])
    else:
        holding_row["current_price"] = None
        holding_row["current_value"] = None
        holding_row["unrealised_pnl"] = None
        holding_row["unrealised_pnl_pct"] = None
        holding_row["price_date"] = None

    return holding_row


# ── Holdings ─────────────────────────────────────────────────────────────────


@router.get("/stocks/holdings")
def list_holdings(
    scope: str | None = Query("personal"),
    conn=Depends(get_conn),
    user: CurrentUser = Depends(get_current_user),
):
    """List all holdings with computed stats."""
    cur = conn.cursor()

    # Get latest prices
    from src.stocks.prices import get_latest_prices
    prices = get_latest_prices(conn)

    conditions = ["is_active"]
    params: dict = {}
    if scope and scope != "all":
        conditions.append("scope = %(scope)s")
        params["scope"] = scope

    where = "WHERE " + " AND ".join(conditions)
    cur.execute(f"SELECT * FROM stock_holding {where} ORDER BY symbol", params)
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()

    items = []
    for row in rows:
        item = dict(zip(columns, row))
        item["id"] = str(item["id"])
        item = _holding_with_stats(cur, item, prices)
        items.append(item)

    return {"items": items}


@router.get("/stocks/holdings/{holding_id}")
def get_holding_detail(
    holding_id: str,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(get_current_user),
):
    """Get holding detail with trade history."""
    cur = conn.cursor()

    cur.execute("SELECT * FROM stock_holding WHERE id = %s", (holding_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Holding not found")

    columns = [d[0] for d in cur.description]
    holding = dict(zip(columns, row))
    holding["id"] = str(holding["id"])

    from src.stocks.prices import get_latest_prices
    prices = get_latest_prices(conn)
    holding = _holding_with_stats(cur, holding, prices)

    # Trade history
    cur.execute("""
        SELECT * FROM stock_trade
        WHERE holding_id = %s
        ORDER BY trade_date DESC, created_at DESC
    """, (holding_id,))
    trade_cols = [d[0] for d in cur.description]
    trades = []
    for r in cur.fetchall():
        t = dict(zip(trade_cols, r))
        t["id"] = str(t["id"])
        t["holding_id"] = str(t["holding_id"])
        t["quantity"] = str(t["quantity"])
        t["price_per_share"] = str(t["price_per_share"])
        t["total_cost"] = str(t["total_cost"])
        t["fees"] = str(t["fees"])
        t["trade_date"] = str(t["trade_date"])
        t["created_at"] = t["created_at"].isoformat()
        trades.append(t)

    holding["trades"] = trades
    return holding


@router.post("/stocks/holdings")
def create_holding(
    body: StockHoldingCreate,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Create a new stock holding."""
    cur = conn.cursor()

    cur.execute(
        "SELECT id FROM stock_holding WHERE symbol = %s",
        (body.symbol.upper(),),
    )
    if cur.fetchone():
        raise HTTPException(409, f"Holding for {body.symbol.upper()} already exists")

    cur.execute("""
        INSERT INTO stock_holding (symbol, name, country, currency, scope, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING *
    """, (body.symbol.upper(), body.name, body.country, body.currency, body.scope, body.notes))
    columns = [d[0] for d in cur.description]
    row = cur.fetchone()
    conn.commit()

    item = dict(zip(columns, row))
    item["id"] = str(item["id"])
    return item


@router.put("/stocks/holdings/{holding_id}")
def update_holding(
    holding_id: str,
    body: StockHoldingUpdate,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Update holding metadata."""
    cur = conn.cursor()

    updates = {}
    if body.name is not None:
        updates["name"] = body.name
    if body.country is not None:
        updates["country"] = body.country
    if body.is_active is not None:
        updates["is_active"] = body.is_active
    if body.notes is not None:
        updates["notes"] = body.notes

    if not updates:
        raise HTTPException(400, "No fields to update")

    set_clause = ", ".join(f"{k} = %({k})s" for k in updates)
    updates["id"] = holding_id
    cur.execute(f"UPDATE stock_holding SET {set_clause} WHERE id = %(id)s RETURNING *", updates)
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Holding not found")

    columns = [d[0] for d in cur.description]
    conn.commit()
    item = dict(zip(columns, row))
    item["id"] = str(item["id"])
    return item


# ── Trades ───────────────────────────────────────────────────────────────────


@router.post("/stocks/holdings/{holding_id}/trades")
def create_trade(
    holding_id: str,
    body: StockTradeCreate,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Add a trade to a holding."""
    cur = conn.cursor()

    # Verify holding exists
    cur.execute("SELECT currency FROM stock_holding WHERE id = %s", (holding_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Holding not found")
    currency = row[0]

    if body.trade_type not in ("buy", "sell"):
        raise HTTPException(400, "trade_type must be 'buy' or 'sell'")

    # Compute total_cost: quantity * price + fees for buy, quantity * price - fees for sell
    total = body.quantity * body.price_per_share
    if body.trade_type == "buy":
        total_cost = total + body.fees
    else:
        total_cost = total - body.fees

    cur.execute("""
        INSERT INTO stock_trade (holding_id, trade_type, trade_date, quantity,
                                  price_per_share, total_cost, fees, currency, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """, (
        holding_id, body.trade_type, body.trade_date, body.quantity,
        body.price_per_share, total_cost, body.fees, currency, body.notes,
    ))
    columns = [d[0] for d in cur.description]
    row = cur.fetchone()
    conn.commit()

    trade = dict(zip(columns, row))
    trade["id"] = str(trade["id"])
    trade["holding_id"] = str(trade["holding_id"])
    trade["quantity"] = str(trade["quantity"])
    trade["price_per_share"] = str(trade["price_per_share"])
    trade["total_cost"] = str(trade["total_cost"])
    trade["fees"] = str(trade["fees"])
    trade["trade_date"] = str(trade["trade_date"])
    trade["created_at"] = trade["created_at"].isoformat()
    return trade


@router.delete("/stocks/trades/{trade_id}")
def delete_trade(
    trade_id: str,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Delete a trade."""
    cur = conn.cursor()
    cur.execute("DELETE FROM stock_trade WHERE id = %s RETURNING id", (trade_id,))
    if not cur.fetchone():
        raise HTTPException(404, "Trade not found")
    conn.commit()
    return {"ok": True}


# ── Portfolio Summary ────────────────────────────────────────────────────────


@router.get("/stocks/portfolio")
def get_portfolio(
    scope: str | None = Query("personal"),
    conn=Depends(get_conn),
    user: CurrentUser = Depends(get_current_user),
):
    """Portfolio summary: total value, cost, unrealised P&L."""
    cur = conn.cursor()

    from src.stocks.prices import get_latest_prices
    prices = get_latest_prices(conn)

    conditions = ["is_active"]
    params: dict = {}
    if scope and scope != "all":
        conditions.append("scope = %(scope)s")
        params["scope"] = scope

    where = "WHERE " + " AND ".join(conditions)
    cur.execute(f"SELECT * FROM stock_holding {where} ORDER BY symbol", params)
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()

    holdings = []
    total_value = Decimal("0")
    total_cost = Decimal("0")
    latest_price_date = None

    for row in rows:
        item = dict(zip(columns, row))
        item["id"] = str(item["id"])
        item = _holding_with_stats(cur, item, prices)
        holdings.append(item)

        if item["current_value"]:
            total_value += Decimal(item["current_value"])
        if item["total_cost"]:
            total_cost += Decimal(item["total_cost"])
        if item["price_date"]:
            pd = item["price_date"]
            if isinstance(pd, str):
                pd = date.fromisoformat(pd)
            if latest_price_date is None or pd > latest_price_date:
                latest_price_date = pd

    pnl = total_value - total_cost
    pnl_pct = (pnl / total_cost * 100) if total_cost > 0 else Decimal("0")

    return {
        "total_value": str(total_value.quantize(Decimal("0.01"))),
        "total_cost": str(total_cost.quantize(Decimal("0.01"))),
        "unrealised_pnl": str(pnl.quantize(Decimal("0.01"))),
        "unrealised_pnl_pct": str(pnl_pct.quantize(Decimal("0.01"))),
        "holdings": holdings,
        "price_date": str(latest_price_date) if latest_price_date else None,
    }


# ── CGT ──────────────────────────────────────────────────────────────────────


@router.get("/stocks/cgt")
def get_cgt(
    tax_year: str | None = Query(None, description="e.g. '2025/26'"),
    conn=Depends(get_conn),
    user: CurrentUser = Depends(get_current_user),
):
    """CGT summary — all tax years or a specific one."""
    cur = conn.cursor()

    # Load all trades with holding info
    cur.execute("""
        SELECT st.*, sh.symbol
        FROM stock_trade st
        JOIN stock_holding sh ON sh.id = st.holding_id
        ORDER BY st.trade_date, st.created_at
    """)
    columns = [d[0] for d in cur.description]
    trades = [dict(zip(columns, r)) for r in cur.fetchall()]

    # Load income data
    cur.execute("SELECT tax_year, gross_income FROM tax_year_income")
    income_by_year = {r[0]: r[1] for r in cur.fetchall()}

    from src.stocks.cgt import compute_cgt
    summaries = compute_cgt(trades, income_by_year)

    if tax_year:
        if tax_year in summaries:
            s = summaries[tax_year]
            return _summary_to_dict(s)
        else:
            # Return empty summary for requested year
            return {
                "tax_year": tax_year,
                "disposals": [],
                "total_gains": "0.00",
                "total_losses": "0.00",
                "net_gains": "0.00",
                "exempt_amount": "3000.00",
                "taxable_gains": "0.00",
                "gross_income": str(income_by_year.get(tax_year)) if tax_year in income_by_year else None,
                "basic_rate_amount": "0.00",
                "higher_rate_amount": "0.00",
                "basic_rate_tax": "0.00",
                "higher_rate_tax": "0.00",
                "total_tax": "0.00",
            }

    # Return all years, sorted
    items = [_summary_to_dict(s) for s in sorted(summaries.values(), key=lambda s: s.tax_year)]
    return {"items": items}


def _summary_to_dict(s) -> dict:
    return {
        "tax_year": s.tax_year,
        "disposals": [
            {
                "trade_id": str(d.trade_id),
                "holding_id": str(d.holding_id),
                "symbol": d.symbol,
                "trade_date": str(d.trade_date),
                "quantity": str(d.quantity),
                "proceeds": str(d.proceeds),
                "cost_basis": str(d.cost_basis),
                "gain_loss": str(d.gain_loss),
                "match_type": d.match_type,
            }
            for d in s.disposals
        ],
        "total_gains": str(s.total_gains),
        "total_losses": str(s.total_losses),
        "net_gains": str(s.net_gains),
        "exempt_amount": str(s.exempt_amount),
        "taxable_gains": str(s.taxable_gains),
        "gross_income": str(s.gross_income) if s.gross_income is not None else None,
        "basic_rate_amount": str(s.basic_rate_amount),
        "higher_rate_amount": str(s.higher_rate_amount),
        "basic_rate_tax": str(s.basic_rate_tax),
        "higher_rate_tax": str(s.higher_rate_tax),
        "total_tax": str(s.total_tax),
    }


# ── Tax Year Income ──────────────────────────────────────────────────────────


@router.get("/stocks/tax-years")
def list_tax_years(
    conn=Depends(get_conn),
    user: CurrentUser = Depends(get_current_user),
):
    """List all tax year income entries."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM tax_year_income ORDER BY tax_year DESC")
    columns = [d[0] for d in cur.description]
    items = []
    for row in cur.fetchall():
        item = dict(zip(columns, row))
        item["id"] = str(item["id"])
        item["gross_income"] = str(item["gross_income"])
        item["personal_allowance"] = str(item["personal_allowance"])
        items.append(item)
    return {"items": items}


@router.put("/stocks/tax-years/{tax_year:path}")
def upsert_tax_year(
    tax_year: str,
    body: TaxYearIncomeUpdate,
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Create or update income for a tax year."""
    cur = conn.cursor()

    cur.execute("SELECT id FROM tax_year_income WHERE tax_year = %s", (tax_year,))
    existing = cur.fetchone()

    if existing:
        cur.execute("""
            UPDATE tax_year_income
            SET gross_income = %s, personal_allowance = %s, notes = %s, updated_at = now()
            WHERE tax_year = %s
            RETURNING *
        """, (body.gross_income, body.personal_allowance, body.notes, tax_year))
    else:
        cur.execute("""
            INSERT INTO tax_year_income (tax_year, gross_income, personal_allowance, notes)
            VALUES (%s, %s, %s, %s)
            RETURNING *
        """, (tax_year, body.gross_income, body.personal_allowance, body.notes))

    columns = [d[0] for d in cur.description]
    row = cur.fetchone()
    conn.commit()

    item = dict(zip(columns, row))
    item["id"] = str(item["id"])
    item["gross_income"] = str(item["gross_income"])
    item["personal_allowance"] = str(item["personal_allowance"])
    return item


# ── Price Refresh ────────────────────────────────────────────────────────────


@router.post("/stocks/prices/refresh")
def refresh_prices(
    conn=Depends(get_conn),
    user: CurrentUser = Depends(require_admin),
):
    """Trigger manual price refresh from Yahoo Finance."""
    from src.stocks.prices import fetch_current_prices
    result = fetch_current_prices(conn)
    return result
