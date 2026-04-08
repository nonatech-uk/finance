import { useState, useEffect, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '../api/client'
import CurrencyAmount from '../components/common/CurrencyAmount'
import LoadingSpinner from '../components/common/LoadingSpinner'
import Badge from '../components/common/Badge'

interface PayPalListItem {
  id: string
  paypal_transaction_id: string
  description: string
  amount: number | null
  fee: number | null
  net_amount: number | null
  currency: string
  counterparty: string | null
  counterparty_email: string | null
  transaction_date: string | null
  status: string | null
  match_id: string | null
  matched_transaction_id: string | null
  matched_merchant: string | null
  matched_date: string | null
}

interface PayPalListResponse {
  items: PayPalListItem[]
  total: number
}

function fetchPayPalList(params: { matched: string; q?: string; limit: number; offset: number }) {
  const qs = new URLSearchParams()
  qs.set('matched', params.matched)
  if (params.q) qs.set('q', params.q)
  qs.set('limit', String(params.limit))
  qs.set('offset', String(params.offset))
  return apiFetch<PayPalListResponse>(`/paypal/list?${qs}`)
}

export default function PayPal() {
  const [filter, setFilter] = useState<'unmatched' | 'matched' | 'all'>('unmatched')
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [offset, setOffset] = useState(0)
  const limit = 100

  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 300)
    return () => clearTimeout(t)
  }, [search])

  useEffect(() => { setOffset(0) }, [filter, debouncedSearch])

  const params = useMemo(() => ({
    matched: filter,
    q: debouncedSearch || undefined,
    limit,
    offset,
  }), [filter, debouncedSearch, offset])

  const { data, isLoading } = useQuery({
    queryKey: ['paypal-list', params],
    queryFn: () => fetchPayPalList(params),
  })

  const fmt = (v: number | null, currency: string) =>
    v != null ? new Intl.NumberFormat('en-GB', { style: 'currency', currency }).format(Math.abs(v)) : ''

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-semibold">PayPal</h2>
        {data && (
          <span className="text-sm text-text-secondary">{data.total} transactions</span>
        )}
      </div>

      <div className="flex gap-3 mb-4 items-center flex-wrap">
        {(['unmatched', 'matched', 'all'] as const).map(f => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-3 py-1.5 text-sm rounded-md transition-colors ${
              filter === f
                ? 'bg-accent/15 text-accent font-medium'
                : 'text-text-secondary hover:text-text-primary hover:bg-bg-hover'
            }`}
          >
            {f.charAt(0).toUpperCase() + f.slice(1)}
          </button>
        ))}
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Search..."
          className="ml-auto bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary placeholder:text-text-secondary focus:outline-none focus:border-accent w-64"
        />
      </div>

      {isLoading && <LoadingSpinner />}

      {data && (
        <>
          <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="border-b border-border bg-bg-primary">
                <tr>
                  <th className="text-left px-4 py-2 font-medium text-text-secondary">Date</th>
                  <th className="text-left px-4 py-2 font-medium text-text-secondary">Description</th>
                  <th className="text-left px-4 py-2 font-medium text-text-secondary">Counterparty</th>
                  <th className="text-right px-4 py-2 font-medium text-text-secondary">Amount</th>
                  <th className="text-right px-4 py-2 font-medium text-text-secondary">Fee</th>
                  <th className="text-left px-4 py-2 font-medium text-text-secondary">Status</th>
                  {filter !== 'unmatched' && (
                    <th className="text-left px-4 py-2 font-medium text-text-secondary">Bank Match</th>
                  )}
                </tr>
              </thead>
              <tbody>
                {data.items.map(item => (
                  <tr key={item.id} className="border-b border-border last:border-0 hover:bg-bg-hover">
                    <td className="px-4 py-2 text-text-secondary tabular-nums whitespace-nowrap">
                      {item.transaction_date?.slice(0, 10) || '—'}
                    </td>
                    <td className="px-4 py-2">
                      {item.description}
                    </td>
                    <td className="px-4 py-2 text-text-secondary">
                      {item.counterparty || '—'}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">
                      {item.amount != null ? (
                        <CurrencyAmount amount={item.amount} currency={item.currency} />
                      ) : '—'}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums text-text-secondary">
                      {item.fee != null && item.fee !== 0 ? fmt(item.fee, item.currency) : '—'}
                    </td>
                    <td className="px-4 py-2">
                      {item.match_id ? (
                        <Badge variant="income">Matched</Badge>
                      ) : (
                        <Badge variant="expense">Unmatched</Badge>
                      )}
                    </td>
                    {filter !== 'unmatched' && (
                      <td className="px-4 py-2 text-text-secondary text-xs">
                        {item.matched_merchant ? (
                          <span title={item.matched_transaction_id || ''}>
                            {item.matched_merchant} ({item.matched_date?.slice(0, 10)})
                          </span>
                        ) : '—'}
                      </td>
                    )}
                  </tr>
                ))}
                {data.items.length === 0 && (
                  <tr>
                    <td colSpan={7} className="px-4 py-8 text-center text-text-secondary">
                      No {filter === 'all' ? '' : filter} PayPal transactions
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {data.total > limit && (
            <div className="flex items-center justify-between mt-4">
              <button
                onClick={() => setOffset(Math.max(0, offset - limit))}
                disabled={offset === 0}
                className="px-3 py-1.5 text-sm rounded-md border border-border text-text-secondary hover:text-text-primary disabled:opacity-40"
              >
                Previous
              </button>
              <span className="text-sm text-text-secondary">
                {offset + 1}–{Math.min(offset + limit, data.total)} of {data.total}
              </span>
              <button
                onClick={() => setOffset(offset + limit)}
                disabled={offset + limit >= data.total}
                className="px-3 py-1.5 text-sm rounded-md border border-border text-text-secondary hover:text-text-primary disabled:opacity-40"
              >
                Next
              </button>
            </div>
          )}
        </>
      )}
    </div>
  )
}
