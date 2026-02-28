import { useState } from 'react'
import { useParams, Link, useNavigate } from 'react-router-dom'
import { useAssetDetail, useAddAssetValuation, useUpdateAssetHolding, useDeleteAssetHolding } from '../hooks/useAssets'
import LoadingSpinner from '../components/common/LoadingSpinner'
import StatCard from '../components/common/StatCard'

function fmt(v: string | null): string {
  if (!v) return '—'
  return parseFloat(v).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

export default function AssetDetail() {
  const { holdingId } = useParams<{ holdingId: string }>()
  const navigate = useNavigate()
  const { data, isLoading } = useAssetDetail(holdingId!)
  const addValuation = useAddAssetValuation()
  const updateHolding = useUpdateAssetHolding()
  const deleteHolding = useDeleteAssetHolding()

  const [showForm, setShowForm] = useState(false)
  const [form, setForm] = useState({
    valuation_date: new Date().toISOString().slice(0, 10),
    gross_value: '',
    tax_payable: '0',
    notes: '',
  })

  // Rename state
  const [editing, setEditing] = useState(false)
  const [editName, setEditName] = useState('')

  // Delete confirmation
  const [confirmDelete, setConfirmDelete] = useState(false)

  const handleAdd = (e: React.FormEvent) => {
    e.preventDefault()
    addValuation.mutate(
      {
        holdingId: holdingId!,
        valuation_date: form.valuation_date,
        gross_value: form.gross_value,
        tax_payable: form.tax_payable || '0',
        notes: form.notes || undefined,
      },
      {
        onSuccess: () => {
          setForm({ valuation_date: new Date().toISOString().slice(0, 10), gross_value: '', tax_payable: '0', notes: '' })
          setShowForm(false)
        },
      },
    )
  }

  const handleRename = () => {
    if (!editName.trim() || editName === data?.name) {
      setEditing(false)
      return
    }
    updateHolding.mutate(
      { id: holdingId!, name: editName.trim() },
      { onSuccess: () => setEditing(false) },
    )
  }

  const handleDelete = () => {
    deleteHolding.mutate(holdingId!, {
      onSuccess: () => navigate('/assets'),
    })
  }

  if (isLoading || !data) return <LoadingSpinner />

  const valuations = data.valuations ?? []

  return (
    <div className="space-y-6">
      <div>
        <Link to="/assets" className="text-sm text-text-secondary hover:text-text-primary">&larr; Back to Assets</Link>
      </div>

      <div className="flex items-center justify-between">
        <div>
          {editing ? (
            <div className="flex items-center gap-2">
              <input
                autoFocus
                value={editName}
                onChange={e => setEditName(e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter') handleRename(); if (e.key === 'Escape') setEditing(false) }}
                className="text-xl font-semibold px-2 py-0.5 rounded border border-border bg-bg-primary text-text-primary"
              />
              <button onClick={handleRename} className="text-sm text-accent hover:underline">Save</button>
              <button onClick={() => setEditing(false)} className="text-sm text-text-secondary hover:underline">Cancel</button>
            </div>
          ) : (
            <div className="flex items-center gap-2">
              <h2 className="text-xl font-semibold">{data.name}</h2>
              <button
                onClick={() => { setEditName(data.name); setEditing(true) }}
                className="text-xs text-text-secondary hover:text-text-primary"
                title="Rename"
              >
                Edit
              </button>
            </div>
          )}
          <p className="text-sm text-text-secondary capitalize">{data.asset_type.replace('_', ' ')}{data.notes ? ` — ${data.notes}` : ''}</p>
        </div>
        <div className="flex items-center gap-2">
          {confirmDelete ? (
            <div className="flex items-center gap-2">
              <span className="text-sm text-expense">Delete this asset?</span>
              <button
                onClick={handleDelete}
                disabled={deleteHolding.isPending}
                className="px-3 py-1.5 text-sm rounded-md bg-red-600 text-white hover:bg-red-700 disabled:opacity-50"
              >
                {deleteHolding.isPending ? 'Deleting...' : 'Confirm'}
              </button>
              <button
                onClick={() => setConfirmDelete(false)}
                className="px-3 py-1.5 text-sm rounded-md border border-border text-text-secondary hover:text-text-primary"
              >
                Cancel
              </button>
            </div>
          ) : (
            <button
              onClick={() => setConfirmDelete(true)}
              className="px-3 py-1.5 text-sm rounded-md border border-border text-text-secondary hover:text-expense hover:border-expense"
            >
              Delete
            </button>
          )}
          <button
            onClick={() => setShowForm(!showForm)}
            className="px-3 py-1.5 text-sm rounded-md bg-accent text-white hover:bg-accent/90"
          >
            {showForm ? 'Cancel' : 'Add Valuation'}
          </button>
        </div>
      </div>

      {showForm && (
        <form onSubmit={handleAdd} className="bg-bg-card border border-border rounded-lg p-4 space-y-3">
          <div className="grid grid-cols-4 gap-3">
            <div>
              <label className="block text-xs text-text-secondary mb-1">Date</label>
              <input
                type="date"
                required
                value={form.valuation_date}
                onChange={e => setForm({ ...form, valuation_date: e.target.value })}
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
            <div>
              <label className="block text-xs text-text-secondary mb-1">Gross Value</label>
              <div className="flex items-center">
                <span className="text-text-secondary mr-1">&pound;</span>
                <input
                  type="number"
                  step="0.01"
                  required
                  value={form.gross_value}
                  onChange={e => setForm({ ...form, gross_value: e.target.value })}
                  placeholder="250000"
                  className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
                />
              </div>
            </div>
            <div>
              <label className="block text-xs text-text-secondary mb-1">Tax Payable</label>
              <div className="flex items-center">
                <span className="text-text-secondary mr-1">&pound;</span>
                <input
                  type="number"
                  step="0.01"
                  value={form.tax_payable}
                  onChange={e => setForm({ ...form, tax_payable: e.target.value })}
                  className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
                />
              </div>
            </div>
            <div>
              <label className="block text-xs text-text-secondary mb-1">Notes</label>
              <input
                value={form.notes}
                onChange={e => setForm({ ...form, notes: e.target.value })}
                placeholder="e.g. Zoopla estimate"
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
          </div>
          <button
            type="submit"
            disabled={addValuation.isPending || !form.gross_value}
            className="px-3 py-1.5 text-sm rounded-md bg-accent text-white hover:bg-accent/90 disabled:opacity-50"
          >
            {addValuation.isPending ? 'Saving...' : 'Save Valuation'}
          </button>
        </form>
      )}

      <div className="grid grid-cols-3 gap-4">
        <StatCard label="Gross Value" value={`£${fmt(data.latest_gross_value)}`} />
        <StatCard label="Tax Payable" value={`£${fmt(data.latest_tax_payable)}`} />
        <StatCard label="Net Value" value={`£${fmt(data.latest_net_value)}`} />
      </div>

      {/* Valuation History */}
      <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-border">
          <h3 className="text-sm font-medium text-text-secondary">Valuation History</h3>
        </div>
        {valuations.length > 0 ? (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-bg-secondary">
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Date</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Gross Value</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Tax Payable</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Net Value</th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Notes</th>
              </tr>
            </thead>
            <tbody>
              {valuations.map(v => (
                <tr key={v.id} className="border-b border-border last:border-0 hover:bg-bg-hover">
                  <td className="px-4 py-2 tabular-nums">{v.valuation_date}</td>
                  <td className="px-4 py-2 text-right tabular-nums">£{fmt(v.gross_value)}</td>
                  <td className="px-4 py-2 text-right tabular-nums text-expense">£{fmt(v.tax_payable)}</td>
                  <td className="px-4 py-2 text-right tabular-nums font-medium">£{fmt(v.net_value)}</td>
                  <td className="px-4 py-2 text-text-secondary">{v.notes ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="p-8 text-center text-text-secondary">
            No valuations yet. Click "Add Valuation" to record the first one.
          </div>
        )}
      </div>
    </div>
  )
}
