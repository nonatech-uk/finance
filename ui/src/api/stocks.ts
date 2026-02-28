import { apiFetch } from './client'
import type {
  StockHoldingItem,
  StockHoldingList,
  StockTradeItem,
  PortfolioSummary,
  CgtSummary,
  TaxYearIncomeItem,
} from './types'

export function fetchPortfolio(scope?: string) {
  const params = new URLSearchParams()
  if (scope) params.set('scope', scope)
  const qs = params.toString()
  return apiFetch<PortfolioSummary>(`/stocks/portfolio${qs ? '?' + qs : ''}`)
}

export function fetchHoldings(scope?: string) {
  const params = new URLSearchParams()
  if (scope) params.set('scope', scope)
  const qs = params.toString()
  return apiFetch<StockHoldingList>(`/stocks/holdings${qs ? '?' + qs : ''}`)
}

export function fetchHoldingDetail(id: string) {
  return apiFetch<StockHoldingItem & { trades: StockTradeItem[] }>(
    `/stocks/holdings/${id}`,
  )
}

export function createHolding(data: {
  symbol: string
  name: string
  country?: string
  currency?: string
}) {
  return apiFetch<StockHoldingItem>('/stocks/holdings', {
    method: 'POST',
    body: JSON.stringify(data),
  })
}

export function updateHolding(id: string, data: {
  name?: string
  country?: string
  is_active?: boolean
  notes?: string
}) {
  return apiFetch<StockHoldingItem>(`/stocks/holdings/${id}`, {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}

export function createTrade(
  holdingId: string,
  data: {
    trade_type: string
    trade_date: string
    quantity: string
    price_per_share: string
    fees?: string
    notes?: string
  },
) {
  return apiFetch<StockTradeItem>(`/stocks/holdings/${holdingId}/trades`, {
    method: 'POST',
    body: JSON.stringify(data),
  })
}

export function deleteTrade(tradeId: string) {
  return apiFetch<{ ok: boolean }>(`/stocks/trades/${tradeId}`, {
    method: 'DELETE',
  })
}

export function fetchCgt(taxYear?: string, qtyOverrides?: Record<string, string>) {
  const params = new URLSearchParams()
  if (taxYear) params.set('tax_year', taxYear)
  if (qtyOverrides) {
    for (const [holdingId, qty] of Object.entries(qtyOverrides)) {
      params.set(`qty_${holdingId}`, qty)
    }
  }
  const qs = params.toString()
  return apiFetch<CgtSummary | { items: CgtSummary[] }>(`/stocks/cgt${qs ? '?' + qs : ''}`)
}

export function fetchTaxYears() {
  return apiFetch<{ items: TaxYearIncomeItem[] }>('/stocks/tax-years')
}

export function updateTaxYear(
  taxYear: string,
  data: { gross_income: string; personal_allowance?: string; notes?: string },
) {
  return apiFetch<TaxYearIncomeItem>(`/stocks/tax-years/${taxYear}`, {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}

export function refreshPrices() {
  return apiFetch<{ updated: number; errors: unknown[] }>(
    '/stocks/prices/refresh',
    { method: 'POST' },
  )
}
