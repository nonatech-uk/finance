import { useState, useMemo, useCallback, useEffect } from 'react'
import { useTransactions, useTransaction } from '../hooks/useTransactions'
import { useOverview } from '../hooks/useStats'
import CurrencyAmount from '../components/common/CurrencyAmount'
import Badge from '../components/common/Badge'
import LoadingSpinner from '../components/common/LoadingSpinner'
import JsonViewer from '../components/common/JsonViewer'
import type { TransactionItem } from '../api/types'

export default function Transactions() {
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [institution, setInstitution] = useState('')
  const [currency, setCurrency] = useState('')
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [selectedId, setSelectedId] = useState<string | null>(null)

  const { data: overview } = useOverview()

  // Debounce search
  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 300)
    return () => clearTimeout(t)
  }, [search])

  const filters = useMemo(() => ({
    limit: 100,
    search: debouncedSearch || undefined,
    institution: institution || undefined,
    currency: currency || undefined,
    date_from: dateFrom || undefined,
    date_to: dateTo || undefined,
  }), [debouncedSearch, institution, currency, dateFrom, dateTo])

  const { data, isLoading, fetchNextPage, hasNextPage, isFetchingNextPage } = useTransactions(filters)
  const { data: detail, isLoading: detailLoading } = useTransaction(selectedId)

  const allItems = useMemo(() => {
    if (!data) return []
    return data.pages.flatMap(p => p.items)
  }, [data])

  const clearFilters = useCallback(() => {
    setSearch('')
    setInstitution('')
    setCurrency('')
    setDateFrom('')
    setDateTo('')
  }, [])

  return (
    <div className="flex gap-0 h-[calc(100vh-3rem)]">
      {/* Main list */}
      <div className={`flex-1 flex flex-col min-w-0 ${selectedId ? 'mr-[480px]' : ''}`}>
        <h2 className="text-xl font-semibold mb-4">Transactions</h2>

        {/* Filter bar */}
        <div className="flex flex-wrap gap-3 mb-4">
          <input
            type="text"
            placeholder="Search merchants..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary placeholder:text-text-secondary focus:outline-none focus:border-accent w-56"
          />
          <select
            value={institution}
            onChange={e => setInstitution(e.target.value)}
            className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary focus:outline-none focus:border-accent"
          >
            <option value="">All institutions</option>
            {overview?.institutions.map(i => (
              <option key={i} value={i}>{i}</option>
            ))}
          </select>
          <select
            value={currency}
            onChange={e => setCurrency(e.target.value)}
            className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary focus:outline-none focus:border-accent"
          >
            <option value="">All currencies</option>
            {['GBP', 'CHF', 'EUR', 'USD', 'PLN', 'NOK'].map(c => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
          <input type="date" value={dateFrom} onChange={e => setDateFrom(e.target.value)} className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary focus:outline-none focus:border-accent" />
          <input type="date" value={dateTo} onChange={e => setDateTo(e.target.value)} className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary focus:outline-none focus:border-accent" />
          <button onClick={clearFilters} className="text-text-secondary hover:text-text-primary text-sm px-2">Clear</button>
        </div>

        {/* Table */}
        {isLoading ? <LoadingSpinner /> : (
          <div className="flex-1 overflow-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-bg-primary">
                <tr className="text-text-secondary text-left text-xs uppercase tracking-wider">
                  <th className="pb-2 pr-4">Date</th>
                  <th className="pb-2 pr-4">Merchant</th>
                  <th className="pb-2 pr-4">Category</th>
                  <th className="pb-2 pr-4 text-right">Amount</th>
                  <th className="pb-2 pr-4">Source</th>
                </tr>
              </thead>
              <tbody>
                {allItems.map(txn => (
                  <TransactionRow key={txn.id} txn={txn} isSelected={txn.id === selectedId} onClick={() => setSelectedId(txn.id === selectedId ? null : txn.id)} />
                ))}
              </tbody>
            </table>
            {hasNextPage && (
              <div className="py-4 text-center">
                <button
                  onClick={() => fetchNextPage()}
                  disabled={isFetchingNextPage}
                  className="text-accent hover:text-accent-hover text-sm px-4 py-2 border border-accent/30 rounded-md hover:bg-accent/10 disabled:opacity-50"
                >
                  {isFetchingNextPage ? 'Loading...' : `Load more (${allItems.length} shown)`}
                </button>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Detail panel */}
      {selectedId && (
        <div className="fixed right-0 top-0 h-full w-[480px] bg-bg-secondary border-l border-border overflow-auto p-5 z-10">
          <div className="flex justify-between items-center mb-4">
            <h3 className="text-lg font-semibold">Transaction Detail</h3>
            <button onClick={() => setSelectedId(null)} className="text-text-secondary hover:text-text-primary text-xl">&times;</button>
          </div>
          {detailLoading ? <LoadingSpinner /> : detail && <TransactionDetailContent detail={detail} />}
        </div>
      )}
    </div>
  )
}

function TransactionRow({ txn, isSelected, onClick }: { txn: TransactionItem; isSelected: boolean; onClick: () => void }) {
  return (
    <tr
      onClick={onClick}
      className={`border-b border-border/50 cursor-pointer transition-colors ${isSelected ? 'bg-accent/10' : 'hover:bg-bg-hover'}`}
    >
      <td className="py-2 pr-4 whitespace-nowrap text-text-secondary">{txn.posted_at}</td>
      <td className="py-2 pr-4">
        <div className="truncate max-w-[300px]">{txn.canonical_merchant_name || txn.cleaned_merchant || txn.raw_merchant || '—'}</div>
      </td>
      <td className="py-2 pr-4">
        {txn.category_name ? (
          <Badge variant="accent">{txn.category_name}</Badge>
        ) : (
          <span className="text-text-secondary text-xs">—</span>
        )}
      </td>
      <td className="py-2 pr-4 text-right">
        <CurrencyAmount amount={txn.amount} currency={txn.currency} showSign={false} />
      </td>
      <td className="py-2 pr-4">
        <Badge>{txn.source}</Badge>
      </td>
    </tr>
  )
}

function TransactionDetailContent({ detail }: { detail: import('../api/types').TransactionDetail }) {
  return (
    <div className="space-y-5 text-sm">
      {/* Basic info */}
      <section>
        <h4 className="text-xs uppercase text-text-secondary mb-2">Basic Info</h4>
        <div className="grid grid-cols-2 gap-2">
          <div><span className="text-text-secondary">Date:</span> {detail.posted_at}</div>
          <div><span className="text-text-secondary">Amount:</span> <CurrencyAmount amount={detail.amount} currency={detail.currency} /></div>
          <div><span className="text-text-secondary">Institution:</span> {detail.institution}</div>
          <div><span className="text-text-secondary">Account:</span> {detail.account_ref}</div>
          <div><span className="text-text-secondary">Source:</span> <Badge>{detail.source}</Badge></div>
          <div><span className="text-text-secondary">Currency:</span> {detail.currency}</div>
        </div>
      </section>

      {/* Merchant chain */}
      <section>
        <h4 className="text-xs uppercase text-text-secondary mb-2">Merchant</h4>
        <div className="space-y-1">
          <div><span className="text-text-secondary">Raw:</span> {detail.raw_merchant || '—'}</div>
          <div><span className="text-text-secondary">Cleaned:</span> {detail.cleaned_merchant || '—'}</div>
          <div><span className="text-text-secondary">Canonical:</span> {detail.canonical_merchant_name || '—'}</div>
          <div><span className="text-text-secondary">Match:</span> {detail.merchant_match_type || '—'}</div>
          <div><span className="text-text-secondary">Category:</span> {detail.category_path || '—'}</div>
        </div>
      </section>

      {detail.raw_memo && (
        <section>
          <h4 className="text-xs uppercase text-text-secondary mb-2">Memo</h4>
          <div>{detail.raw_memo}</div>
        </section>
      )}

      {/* Dedup group */}
      {detail.dedup_group && (
        <section>
          <h4 className="text-xs uppercase text-text-secondary mb-2">Dedup Group</h4>
          <div className="mb-2 text-text-secondary text-xs">
            Rule: {detail.dedup_group.match_rule} · Confidence: {detail.dedup_group.confidence}
          </div>
          <div className="space-y-1">
            {detail.dedup_group.members.map(m => (
              <div key={m.raw_transaction_id} className="flex items-center gap-2">
                <Badge variant={m.is_preferred ? 'income' : 'default'}>{m.is_preferred ? 'preferred' : 'duplicate'}</Badge>
                <span className="text-text-secondary">{m.source}</span>
                <span className="text-text-secondary text-xs truncate">{m.raw_transaction_id}</span>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Economic event */}
      {detail.economic_event && (
        <section>
          <h4 className="text-xs uppercase text-text-secondary mb-2">Economic Event</h4>
          <div className="mb-2">
            <span className="text-text-secondary">Type:</span> {detail.economic_event.event_type}
            {detail.economic_event.description && <span> · {detail.economic_event.description}</span>}
          </div>
          <div className="space-y-1">
            {detail.economic_event.legs.map((leg, i) => (
              <div key={i} className="flex gap-3 text-text-secondary">
                <Badge>{leg.leg_type}</Badge>
                <CurrencyAmount amount={leg.amount} currency={leg.currency} />
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Raw data */}
      {detail.raw_data && (
        <section>
          <h4 className="text-xs uppercase text-text-secondary mb-2">Raw Data</h4>
          <div className="bg-bg-primary rounded p-3 text-xs font-mono overflow-auto max-h-64">
            <JsonViewer data={detail.raw_data} />
          </div>
        </section>
      )}
    </div>
  )
}
