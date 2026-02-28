import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useAssetsSummary, useCreateAssetHolding } from '../hooks/useAssets'
import LoadingSpinner from '../components/common/LoadingSpinner'
import StatCard from '../components/common/StatCard'

function fmt(v: string | null): string {
  if (!v) return '—'
  return parseFloat(v).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

export default function Assets() {
  const { data, isLoading } = useAssetsSummary()
  const createHolding = useCreateAssetHolding()
  const [showForm, setShowForm] = useState(false)
  const [form, setForm] = useState({ name: '', asset_type: 'other', notes: '' })

  const handleCreate = (e: React.FormEvent) => {
    e.preventDefault()
    createHolding.mutate(
      { name: form.name, asset_type: form.asset_type, notes: form.notes || undefined },
      { onSuccess: () => { setForm({ name: '', asset_type: 'other', notes: '' }); setShowForm(false) } },
    )
  }

  if (isLoading) return <LoadingSpinner />

  const holdings = data?.holdings ?? []

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Other Assets</h2>
        <button
          onClick={() => setShowForm(!showForm)}
          className="px-3 py-1.5 text-sm rounded-md bg-accent text-white hover:bg-accent/90"
        >
          {showForm ? 'Cancel' : 'Add Asset'}
        </button>
      </div>

      {showForm && (
        <form onSubmit={handleCreate} className="bg-bg-card border border-border rounded-lg p-4 space-y-3">
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className="block text-xs text-text-secondary mb-1">Name</label>
              <input
                required
                value={form.name}
                onChange={e => setForm({ ...form, name: e.target.value })}
                placeholder="e.g. 42 Acacia Avenue"
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
            <div>
              <label className="block text-xs text-text-secondary mb-1">Type</label>
              <select
                value={form.asset_type}
                onChange={e => setForm({ ...form, asset_type: e.target.value })}
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              >
                <option value="real_estate">Real Estate</option>
                <option value="vehicle">Vehicle</option>
                <option value="pension">Pension</option>
                <option value="collectible">Collectible</option>
                <option value="other">Other</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-text-secondary mb-1">Notes</label>
              <input
                value={form.notes}
                onChange={e => setForm({ ...form, notes: e.target.value })}
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
          </div>
          <button
            type="submit"
            disabled={createHolding.isPending || !form.name}
            className="px-3 py-1.5 text-sm rounded-md bg-accent text-white hover:bg-accent/90 disabled:opacity-50"
          >
            {createHolding.isPending ? 'Creating...' : 'Create'}
          </button>
        </form>
      )}

      {data && (
        <div className="grid grid-cols-3 gap-4">
          <StatCard label="Total Gross Value" value={`£${fmt(data.total_gross_value)}`} />
          <StatCard label="Total Tax Payable" value={`£${fmt(data.total_tax_payable)}`} />
          <StatCard label="Total Net Value" value={`£${fmt(data.total_net_value)}`} />
        </div>
      )}

      {holdings.length > 0 ? (
        <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-bg-secondary">
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Name</th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Type</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Gross Value</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Tax Payable</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Net Value</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Last Valued</th>
              </tr>
            </thead>
            <tbody>
              {holdings.map(h => (
                <tr key={h.id} className="border-b border-border last:border-0 hover:bg-bg-hover">
                  <td className="px-4 py-2">
                    <Link to={`/assets/${h.id}`} className="font-medium text-accent hover:underline">
                      {h.name}
                    </Link>
                  </td>
                  <td className="px-4 py-2 text-text-secondary capitalize">{h.asset_type.replace('_', ' ')}</td>
                  <td className="px-4 py-2 text-right tabular-nums">£{fmt(h.latest_gross_value)}</td>
                  <td className="px-4 py-2 text-right tabular-nums text-expense">£{fmt(h.latest_tax_payable)}</td>
                  <td className="px-4 py-2 text-right tabular-nums font-medium">£{fmt(h.latest_net_value)}</td>
                  <td className="px-4 py-2 text-right text-text-secondary">{h.valuation_date ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : !showForm ? (
        <div className="bg-bg-card border border-border rounded-lg p-8 text-center text-text-secondary">
          No assets yet. Click "Add Asset" to get started.
        </div>
      ) : null}
    </div>
  )
}
