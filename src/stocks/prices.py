"""Stock price fetching and caching via Yahoo Finance."""

from datetime import date
from decimal import Decimal

import yfinance as yf


def fetch_current_prices(conn) -> dict:
    """Fetch current prices for all active holdings and upsert into stock_price.

    Returns {"updated": int, "errors": list[dict]}.
    """
    cur = conn.cursor()
    cur.execute("SELECT id, symbol, currency FROM stock_holding WHERE is_active")
    holdings = cur.fetchall()

    if not holdings:
        return {"updated": 0, "errors": []}

    symbols = [h[1] for h in holdings]
    symbol_map = {h[1]: {"id": h[0], "currency": h[2]} for h in holdings}

    # Batch download — single HTTP call for all tickers
    tickers = yf.Tickers(" ".join(symbols))
    today = date.today()
    updated = 0
    errors = []

    for symbol in symbols:
        try:
            ticker = tickers.tickers[symbol]
            price = Decimal(str(ticker.fast_info.last_price))
            holding = symbol_map[symbol]

            cur.execute("""
                INSERT INTO stock_price (holding_id, price_date, close_price, currency, source)
                VALUES (%s, %s, %s, %s, 'yahoo')
                ON CONFLICT (holding_id, price_date)
                DO UPDATE SET close_price = EXCLUDED.close_price,
                              fetched_at = now()
            """, (str(holding["id"]), today, price, holding["currency"]))
            updated += 1
        except Exception as e:
            errors.append({"symbol": symbol, "error": str(e)})

    conn.commit()
    return {"updated": updated, "errors": errors}


def get_latest_prices(conn) -> dict:
    """Get the most recent cached price for each active holding.

    Returns {holding_id_str: {"close_price": Decimal, "price_date": date, ...}}.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (sp.holding_id)
            sp.holding_id, sh.symbol, sp.close_price, sp.currency,
            sp.price_date, sp.fetched_at
        FROM stock_price sp
        JOIN stock_holding sh ON sh.id = sp.holding_id
        WHERE sh.is_active
        ORDER BY sp.holding_id, sp.price_date DESC
    """)
    columns = [desc[0] for desc in cur.description]
    result = {}
    for row in cur.fetchall():
        d = dict(zip(columns, row))
        result[str(d["holding_id"])] = d
    return result
