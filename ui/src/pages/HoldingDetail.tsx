import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useHoldingDetail, useCreateTrade, useDeleteTrade } from '../hooks/useStocks'
import StatCard from '../components/common/StatCard'
import CurrencyAmount from '../components/common/CurrencyAmount'
import LoadingSpinner from '../components/common/LoadingSpinner'

export default function HoldingDetail() {
  const { holdingId } = useParams<{ holdingId: string }>()
  const { data, isLoading, error } = useHoldingDetail(holdingId || '')
  const createTrade = useCreateTrade()
  const deleteTrade = useDeleteTrade()
  const [showForm, setShowForm] = useState(false)
  const [form, setForm] = useState({
    trade_type: 'buy',
    trade_date: new Date().toISOString().slice(0, 10),
    quantity: '',
    price_per_share: '',
    fees: '0',
    notes: '',
  })

  const handleCreate = (e: React.FormEvent) => {
    e.preventDefault()
    if (!holdingId) return
    createTrade.mutate(
      { holdingId, ...form },
      {
        onSuccess: () => {
          setShowForm(false)
          setForm({ trade_type: 'buy', trade_date: new Date().toISOString().slice(0, 10), quantity: '', price_per_share: '', fees: '0', notes: '' })
        },
      },
    )
  }

  const handleDelete = (tradeId: string) => {
    if (confirm('Delete this trade?')) {
      deleteTrade.mutate(tradeId)
    }
  }

  if (isLoading) return <LoadingSpinner />
  if (error || !data) return <div className="text-expense">Holding not found</div>

  const pnl = parseFloat(data.unrealised_pnl || '0')
  const shares = parseFloat(data.current_shares || '0')

  return (
    <div className="space-y-6">
      <div>
        <Link to="/stocks" className="text-sm text-text-secondary hover:text-text-primary">&larr; Back to Portfolio</Link>
      </div>

      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold">{data.symbol} &mdash; {data.name}</h2>
          <div className="text-sm text-text-secondary mt-0.5">
            {data.country} &middot; {data.currency}
          </div>
        </div>
        <button
          onClick={() => setShowForm(!showForm)}
          className="px-3 py-1.5 text-sm rounded-md bg-accent text-white hover:bg-accent/90"
        >
          + Add Trade
        </button>
      </div>

      <div className="grid grid-cols-4 gap-4">
        <StatCard label="Shares Held" value={shares.toString()} />
        <StatCard label="Avg Cost" value={data.average_cost ? `$${parseFloat(data.average_cost).toFixed(2)}` : '-'} />
        <StatCard label="Current Price" value={data.current_price ? `$${parseFloat(data.current_price).toFixed(2)}` : '-'} subtitle={data.price_date ? `as of ${data.price_date}` : undefined} />
        <StatCard
          label="Unrealised P&L"
          value={data.unrealised_pnl ? `${pnl >= 0 ? '+' : ''}$${pnl.toLocaleString('en-GB', { minimumFractionDigits: 2 })}` : '-'}
          subtitle={data.unrealised_pnl_pct ? `${parseFloat(data.unrealised_pnl_pct) >= 0 ? '+' : ''}${data.unrealised_pnl_pct}%` : undefined}
          className={pnl >= 0 ? 'border-income/30' : 'border-expense/30'}
        />
      </div>

      {showForm && (
        <form onSubmit={handleCreate} className="bg-bg-card border border-border rounded-lg p-4 flex gap-3 items-end flex-wrap">
          <div>
            <label className="block text-xs text-text-secondary mb-1">Type</label>
            <select
              value={form.trade_type}
              onChange={e => setForm({ ...form, trade_type: e.target.value })}
              className="px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
            >
              <option value="buy">Buy</option>
              <option value="sell">Sell</option>
            </select>
          </div>
          <div>
            <label className="block text-xs text-text-secondary mb-1">Date</label>
            <input
              type="date"
              value={form.trade_date}
              onChange={e => setForm({ ...form, trade_date: e.target.value })}
              required
              className="px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
            />
          </div>
          <div>
            <label className="block text-xs text-text-secondary mb-1">Quantity</label>
            <input
              type="number"
              step="any"
              value={form.quantity}
              onChange={e => setForm({ ...form, quantity: e.target.value })}
              placeholder="10"
              required
              className="w-24 px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
            />
          </div>
          <div>
            <label className="block text-xs text-text-secondary mb-1">Price per share</label>
            <input
              type="number"
              step="any"
              value={form.price_per_share}
              onChange={e => setForm({ ...form, price_per_share: e.target.value })}
              placeholder="150.00"
              required
              className="w-28 px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
            />
          </div>
          <div>
            <label className="block text-xs text-text-secondary mb-1">Fees</label>
            <input
              type="number"
              step="any"
              value={form.fees}
              onChange={e => setForm({ ...form, fees: e.target.value })}
              className="w-20 px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
            />
          </div>
          <div className="flex-1">
            <label className="block text-xs text-text-secondary mb-1">Notes</label>
            <input
              value={form.notes}
              onChange={e => setForm({ ...form, notes: e.target.value })}
              placeholder="Optional"
              className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
            />
          </div>
          <button
            type="submit"
            disabled={createTrade.isPending}
            className="px-3 py-1.5 text-sm rounded-md bg-accent text-white hover:bg-accent/90 disabled:opacity-50"
          >
            {createTrade.isPending ? 'Adding...' : 'Add Trade'}
          </button>
          <button
            type="button"
            onClick={() => setShowForm(false)}
            className="px-3 py-1.5 text-sm rounded-md border border-border text-text-secondary hover:bg-bg-hover"
          >
            Cancel
          </button>
        </form>
      )}

      <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-border">
          <h3 className="text-sm font-medium text-text-secondary">Trade History</h3>
        </div>
        {data.trades && data.trades.length > 0 ? (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-bg-secondary">
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Date</th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Type</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Qty</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Price</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Total</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Fees</th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Notes</th>
                <th className="px-4 py-2"></th>
              </tr>
            </thead>
            <tbody>
              {data.trades.map(t => (
                <tr key={t.id} className="border-b border-border last:border-0 hover:bg-bg-hover">
                  <td className="px-4 py-2 tabular-nums">{t.trade_date}</td>
                  <td className="px-4 py-2">
                    <span className={`px-1.5 py-0.5 text-xs rounded font-medium ${
                      t.trade_type === 'buy' ? 'bg-income/15 text-income' : 'bg-expense/15 text-expense'
                    }`}>
                      {t.trade_type.toUpperCase()}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums">{parseFloat(t.quantity)}</td>
                  <td className="px-4 py-2 text-right tabular-nums">${parseFloat(t.price_per_share).toFixed(2)}</td>
                  <td className="px-4 py-2 text-right">
                    <CurrencyAmount amount={t.total_cost} currency="USD" showSign={false} />
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-text-secondary">
                    {parseFloat(t.fees) > 0 ? `$${parseFloat(t.fees).toFixed(2)}` : '-'}
                  </td>
                  <td className="px-4 py-2 text-text-secondary truncate max-w-[150px]">{t.notes || '-'}</td>
                  <td className="px-4 py-2 text-right">
                    <button
                      onClick={() => handleDelete(t.id)}
                      className="text-text-secondary hover:text-expense text-xs"
                      title="Delete trade"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="px-4 py-6 text-center text-text-secondary">
            No trades yet. Add one above.
          </div>
        )}
      </div>

      <div className="bg-bg-card border border-border rounded-lg p-5">
        <h3 className="text-sm font-medium text-text-secondary mb-2">Dividends</h3>
        <p className="text-text-secondary text-sm">Coming in a future update.</p>
      </div>
    </div>
  )
}
