"""UK Capital Gains Tax calculation for shares.

Implements:
- Same-day rule: shares bought and sold on the same day matched first
- Section 104 pool: weighted average cost basis for all remaining shares
- Annual exempt amount: £3,000 (2024/25 onwards)
- Tax rates: 18% basic, 24% higher
- UK tax year: April 6 to April 5

NOTE: Calculations are currently in the holding's native currency (typically USD).
FX conversion to GBP for proper UK CGT reporting will be added when the UK tax
module is built — that module will consume the Disposal/TaxYearSummary outputs
from this engine and apply GBP conversion at the disposal-date exchange rate.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

CGT_EXEMPT_AMOUNT = Decimal("3000")
BASIC_RATE = Decimal("0.18")
HIGHER_RATE = Decimal("0.24")
BASIC_RATE_LIMIT = Decimal("37700")  # basic rate band width (2024/25 onwards)


@dataclass
class Disposal:
    """A single disposal event with CGT calculation."""

    trade_id: str
    holding_id: str
    symbol: str
    trade_date: date
    quantity: Decimal
    proceeds: Decimal
    cost_basis: Decimal
    gain_loss: Decimal
    match_type: str  # 'same_day' or 'section_104'


@dataclass
class Section104Pool:
    """Section 104 pool for a single holding — weighted average cost."""

    holding_id: str
    symbol: str
    total_shares: Decimal = Decimal("0")
    total_cost: Decimal = Decimal("0")

    @property
    def average_cost(self) -> Decimal:
        if self.total_shares <= 0:
            return Decimal("0")
        return self.total_cost / self.total_shares

    def add_shares(self, quantity: Decimal, cost: Decimal):
        self.total_shares += quantity
        self.total_cost += cost

    def remove_shares(self, quantity: Decimal) -> Decimal:
        """Remove shares and return the cost basis removed."""
        if self.total_shares <= 0:
            return Decimal("0")
        if quantity >= self.total_shares:
            cost = self.total_cost
            self.total_shares = Decimal("0")
            self.total_cost = Decimal("0")
            return cost
        cost_basis = (self.total_cost / self.total_shares) * quantity
        self.total_shares -= quantity
        self.total_cost -= cost_basis
        return cost_basis


@dataclass
class TaxYearSummary:
    """CGT summary for a single UK tax year."""

    tax_year: str
    disposals: list[Disposal] = field(default_factory=list)
    total_gains: Decimal = Decimal("0")
    total_losses: Decimal = Decimal("0")
    net_gains: Decimal = Decimal("0")
    exempt_amount: Decimal = CGT_EXEMPT_AMOUNT
    taxable_gains: Decimal = Decimal("0")
    gross_income: Decimal | None = None
    basic_rate_amount: Decimal = Decimal("0")
    higher_rate_amount: Decimal = Decimal("0")
    basic_rate_tax: Decimal = Decimal("0")
    higher_rate_tax: Decimal = Decimal("0")
    total_tax: Decimal = Decimal("0")


def get_tax_year(d: date) -> str:
    """Return UK tax year string for a date. Apr 6 starts new year.

    2025-06-15 -> '2025/26'
    2026-03-01 -> '2025/26'
    2026-04-06 -> '2026/27'
    """
    if d.month > 4 or (d.month == 4 and d.day >= 6):
        start = d.year
    else:
        start = d.year - 1
    return f"{start}/{str(start + 1)[-2:]}"


def _round2(v: Decimal) -> Decimal:
    return v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def compute_cgt(
    trades: list[dict],
    income_by_year: dict[str, Decimal] | None = None,
    hypothetical_prices: dict[str, Decimal] | None = None,
    quantity_overrides: dict[str, Decimal] | None = None,
) -> dict[str, TaxYearSummary]:
    """Compute CGT from a chronological list of trades.

    Args:
        trades: list of dicts with keys: id, holding_id, symbol, trade_type,
                trade_date, quantity, price_per_share, total_cost, fees
        income_by_year: mapping of tax_year string -> gross_income Decimal
        hypothetical_prices: mapping of holding_id -> current price per share.
            If provided, any remaining shares in each pool will be treated as
            a hypothetical liquidation at today's price for planning purposes.
        quantity_overrides: mapping of holding_id -> number of shares to
            hypothetically sell (defaults to full position if not specified).

    Returns:
        dict mapping tax_year -> TaxYearSummary
    """
    if income_by_year is None:
        income_by_year = {}
    if hypothetical_prices is None:
        hypothetical_prices = {}
    if quantity_overrides is None:
        quantity_overrides = {}

    if not trades and not hypothetical_prices:
        return {}

    # Sort: by date, then buys before sells on same day
    trades = sorted(
        trades,
        key=lambda t: (t["trade_date"], 0 if t["trade_type"] == "buy" else 1),
    )

    pools: dict[str, Section104Pool] = {}
    summaries: dict[str, TaxYearSummary] = {}

    # Group by (date, holding_id)
    buys_by_key: dict[tuple, list] = defaultdict(list)
    sells_by_key: dict[tuple, list] = defaultdict(list)

    for t in trades:
        key = (t["trade_date"], str(t["holding_id"]))
        if t["trade_type"] == "buy":
            buys_by_key[key].append(t)
        else:
            sells_by_key[key].append(t)

    # All unique (date, holding) keys, sorted
    all_keys = sorted(set(list(buys_by_key.keys()) + list(sells_by_key.keys())))

    for d, h_id in all_keys:
        day_buys = buys_by_key.get((d, h_id), [])
        day_sells = sells_by_key.get((d, h_id), [])

        # Ensure pool exists
        if h_id not in pools:
            sample = (day_buys + day_sells)[0]
            pools[h_id] = Section104Pool(holding_id=h_id, symbol=sample["symbol"])

        pool = pools[h_id]

        # Totals for same-day matching
        buy_qty = sum(Decimal(str(t["quantity"])) for t in day_buys)
        buy_cost = sum(Decimal(str(t["total_cost"])) for t in day_buys)
        sell_qty = sum(Decimal(str(t["quantity"])) for t in day_sells)

        if day_sells and buy_qty > 0:
            # Same-day rule: match up to min(buy_qty, sell_qty)
            matched_qty = min(buy_qty, sell_qty)
            matched_buy_cost = (buy_cost / buy_qty) * matched_qty if buy_qty > 0 else Decimal("0")

            # Record same-day disposals, distributing across individual sell trades
            remaining_match = matched_qty
            for sell in day_sells:
                s_qty = Decimal(str(sell["quantity"]))
                sd_qty = min(s_qty, remaining_match)
                if sd_qty > 0:
                    proceeds = sd_qty * Decimal(str(sell["price_per_share"]))
                    cost = (buy_cost / buy_qty) * sd_qty
                    disposal = Disposal(
                        trade_id=str(sell["id"]),
                        holding_id=h_id,
                        symbol=sell["symbol"],
                        trade_date=d,
                        quantity=sd_qty,
                        proceeds=_round2(proceeds),
                        cost_basis=_round2(cost),
                        gain_loss=_round2(proceeds - cost),
                        match_type="same_day",
                    )
                    ty = get_tax_year(d)
                    if ty not in summaries:
                        summaries[ty] = TaxYearSummary(tax_year=ty)
                    summaries[ty].disposals.append(disposal)
                    remaining_match -= sd_qty

            # Remaining buys (not matched same-day) go to pool
            remaining_buy_qty = buy_qty - matched_qty
            if remaining_buy_qty > 0:
                remaining_buy_cost = (buy_cost / buy_qty) * remaining_buy_qty
                pool.add_shares(remaining_buy_qty, remaining_buy_cost)

            # Remaining sells (not matched same-day) take from pool
            remaining_sell_qty = sell_qty - matched_qty
            if remaining_sell_qty > 0:
                # Distribute across sells that weren't fully matched
                leftover = remaining_sell_qty
                for sell in day_sells:
                    s_qty = Decimal(str(sell["quantity"]))
                    already_matched = min(s_qty, matched_qty)
                    pool_qty = s_qty - already_matched
                    pool_qty = min(pool_qty, leftover)
                    if pool_qty > 0:
                        proceeds = pool_qty * Decimal(str(sell["price_per_share"]))
                        cost = pool.remove_shares(pool_qty)
                        disposal = Disposal(
                            trade_id=str(sell["id"]),
                            holding_id=h_id,
                            symbol=sell["symbol"],
                            trade_date=d,
                            quantity=pool_qty,
                            proceeds=_round2(proceeds),
                            cost_basis=_round2(cost),
                            gain_loss=_round2(proceeds - cost),
                            match_type="section_104",
                        )
                        ty = get_tax_year(d)
                        if ty not in summaries:
                            summaries[ty] = TaxYearSummary(tax_year=ty)
                        summaries[ty].disposals.append(disposal)
                        leftover -= pool_qty
        else:
            # No same-day matching needed
            # Add all buys to pool
            for buy in day_buys:
                pool.add_shares(
                    Decimal(str(buy["quantity"])),
                    Decimal(str(buy["total_cost"])),
                )

            # All sells matched from pool
            for sell in day_sells:
                s_qty = Decimal(str(sell["quantity"]))
                proceeds = s_qty * Decimal(str(sell["price_per_share"]))
                cost = pool.remove_shares(s_qty)
                disposal = Disposal(
                    trade_id=str(sell["id"]),
                    holding_id=h_id,
                    symbol=sell["symbol"],
                    trade_date=d,
                    quantity=s_qty,
                    proceeds=_round2(proceeds),
                    cost_basis=_round2(cost),
                    gain_loss=_round2(proceeds - cost),
                    match_type="section_104",
                )
                ty = get_tax_year(d)
                if ty not in summaries:
                    summaries[ty] = TaxYearSummary(tax_year=ty)
                summaries[ty].disposals.append(disposal)

    # Hypothetical liquidation: sell remaining pool at current price
    if hypothetical_prices:
        today = date.today()
        ty = get_tax_year(today)
        if ty not in summaries:
            summaries[ty] = TaxYearSummary(tax_year=ty)

        for h_id, price in hypothetical_prices.items():
            pool = pools.get(h_id)
            if not pool or pool.total_shares <= 0:
                continue
            # Allow partial liquidation via quantity_overrides
            available = pool.total_shares
            qty = min(quantity_overrides.get(h_id, available), available)
            if qty <= 0:
                continue
            proceeds = _round2(qty * price)
            cost = pool.remove_shares(qty)
            disposal = Disposal(
                trade_id="hypothetical",
                holding_id=h_id,
                symbol=pool.symbol,
                trade_date=today,
                quantity=qty,
                proceeds=proceeds,
                cost_basis=_round2(cost),
                gain_loss=_round2(proceeds - cost),
                match_type="hypothetical",
            )
            summaries[ty].disposals.append(disposal)

    # Aggregate and calculate tax per year
    for ty, summary in summaries.items():
        for d in summary.disposals:
            if d.gain_loss > 0:
                summary.total_gains += d.gain_loss
            else:
                summary.total_losses += abs(d.gain_loss)

        summary.total_gains = _round2(summary.total_gains)
        summary.total_losses = _round2(summary.total_losses)
        summary.net_gains = _round2(summary.total_gains - summary.total_losses)
        summary.taxable_gains = _round2(max(Decimal("0"), summary.net_gains - summary.exempt_amount))

        # Tax bands based on income
        income = income_by_year.get(ty)
        summary.gross_income = income

        if income and income > 0:
            taxable_income = max(Decimal("0"), income - Decimal("12570"))
            remaining_basic = max(Decimal("0"), BASIC_RATE_LIMIT - taxable_income)
        else:
            remaining_basic = BASIC_RATE_LIMIT

        summary.basic_rate_amount = _round2(min(summary.taxable_gains, remaining_basic))
        summary.higher_rate_amount = _round2(max(Decimal("0"), summary.taxable_gains - remaining_basic))
        summary.basic_rate_tax = _round2(summary.basic_rate_amount * BASIC_RATE)
        summary.higher_rate_tax = _round2(summary.higher_rate_amount * HIGHER_RATE)
        summary.total_tax = _round2(summary.basic_rate_tax + summary.higher_rate_tax)

    return summaries
