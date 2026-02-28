"""Stock price fetching and caching.

Uses Yahoo Finance v8 API directly (no yfinance dependency).
"""

from datetime import date
from decimal import Decimal

import requests


YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"


def _fetch_price(symbol: str) -> Decimal | None:
    """Fetch the current price for a single symbol from Yahoo Finance."""
    resp = requests.get(
        YAHOO_QUOTE_URL.format(symbol=symbol),
        params={"range": "1d", "interval": "1d"},
        headers={"User-Agent": USER_AGENT},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
    price = meta.get("regularMarketPrice")
    if price is None:
        raise ValueError(f"No price in response for {symbol}")
    return Decimal(str(price))


def fetch_current_prices(conn) -> dict:
    """Fetch current prices for all active holdings and upsert into stock_price.

    Returns {"updated": int, "errors": list[dict]}.
    """
    cur = conn.cursor()
    cur.execute("SELECT id, symbol, currency FROM stock_holding WHERE is_active")
    holdings = cur.fetchall()

    if not holdings:
        return {"updated": 0, "errors": []}

    today = date.today()
    updated = 0
    errors = []

    for holding_id, symbol, currency in holdings:
        try:
            price = _fetch_price(symbol)

            cur.execute("""
                INSERT INTO stock_price (holding_id, price_date, close_price, currency, source)
                VALUES (%s, %s, %s, %s, 'yahoo')
                ON CONFLICT (holding_id, price_date)
                DO UPDATE SET close_price = EXCLUDED.close_price,
                              fetched_at = now()
            """, (str(holding_id), today, price, currency))
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
