import { useState } from 'react'
import {
  useTagRules,
  useCreateTagRule,
  useUpdateTagRule,
  useDeleteTagRule,
  useApplyTagRules,
} from '../hooks/useTagRules'
import { useAccounts } from '../hooks/useAccounts'
import { useScope } from '../contexts/ScopeContext'
import LoadingSpinner from '../components/common/LoadingSpinner'
import type { TagRuleItem, TagRuleCreate } from '../api/types'

const EMPTY_FORM: TagRuleCreate = {
  name: '',
  date_from: null,
  date_to: null,
  account_ids: [],
  merchant_pattern: null,
  category_pattern: null,
  tags: [],
  is_active: true,
  priority: 100,
}

export default function TagRules() {
  const { scope } = useScope()
  const { data, isLoading } = useTagRules()
  const { data: accountsData } = useAccounts(false, scope)
  const createRule = useCreateTagRule()
  const updateRule = useUpdateTagRule()
  const deleteRule = useDeleteTagRule()
  const applyRules = useApplyTagRules()

  const [showForm, setShowForm] = useState(false)
  const [editingId, setEditingId] = useState<number | null>(null)
  const [form, setForm] = useState<TagRuleCreate>(EMPTY_FORM)
  const [tagInput, setTagInput] = useState('')
  const [applyResult, setApplyResult] = useState<{ rules_applied: number; tags_created: number; tags_removed: number } | null>(null)

  const accounts = (accountsData?.items ?? []).filter(a => a.id != null)

  const openCreate = () => {
    setEditingId(null)
    setForm(EMPTY_FORM)
    setTagInput('')
    setShowForm(true)
  }

  const openEdit = (rule: TagRuleItem) => {
    setEditingId(rule.id)
    setForm({
      name: rule.name,
      date_from: rule.date_from,
      date_to: rule.date_to,
      account_ids: rule.account_ids,
      merchant_pattern: rule.merchant_pattern,
      category_pattern: rule.category_pattern,
      tags: rule.tags,
      is_active: rule.is_active,
      priority: rule.priority,
    })
    setTagInput(rule.tags.join(', '))
    setShowForm(true)
  }

  const cancelForm = () => {
    setShowForm(false)
    setEditingId(null)
  }

  const parseTags = (input: string): string[] =>
    input
      .split(',')
      .map(t => t.trim())
      .filter(Boolean)

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const tags = parseTags(tagInput)
    if (!tags.length) return

    const payload: TagRuleCreate = {
      ...form,
      tags,
      date_from: form.date_from || undefined,
      date_to: form.date_to || undefined,
      merchant_pattern: form.merchant_pattern || undefined,
      category_pattern: form.category_pattern || undefined,
    }

    if (editingId) {
      updateRule.mutate(
        { id: editingId, ...payload },
        { onSuccess: () => cancelForm() },
      )
    } else {
      createRule.mutate(payload, { onSuccess: () => cancelForm() })
    }
  }

  const handleApply = () => {
    setApplyResult(null)
    applyRules.mutate(undefined, {
      onSuccess: (result) => setApplyResult(result),
    })
  }

  const handleDelete = (id: number) => {
    deleteRule.mutate(id)
  }

  const toggleActive = (rule: TagRuleItem) => {
    updateRule.mutate({ id: rule.id, is_active: !rule.is_active })
  }

  const toggleAccount = (accountId: string) => {
    const ids = form.account_ids ?? []
    setForm({
      ...form,
      account_ids: ids.includes(accountId)
        ? ids.filter(id => id !== accountId)
        : [...ids, accountId],
    })
  }

  const accountLabel = (id: string) => {
    const a = accounts.find(a => a.id === id)
    if (!a) return id.slice(0, 8)
    return a.display_name || a.name || `${a.institution}/${a.account_ref}`
  }

  if (isLoading) return <LoadingSpinner />

  const rules = data?.items ?? []

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Tag Rules</h2>
        <div className="flex gap-2">
          <button
            onClick={showForm && !editingId ? cancelForm : openCreate}
            className="px-3 py-1.5 text-sm rounded-md bg-accent text-white hover:bg-accent/90"
          >
            {showForm && !editingId ? 'Cancel' : 'Add Rule'}
          </button>
          <button
            onClick={handleApply}
            disabled={applyRules.isPending}
            className="px-3 py-1.5 text-sm rounded-md bg-income text-white hover:bg-income/90 disabled:opacity-50"
          >
            {applyRules.isPending ? 'Applying...' : 'Apply All Rules'}
          </button>
        </div>
      </div>

      {/* Apply result banner */}
      {applyResult && (
        <div className="bg-income/10 border border-income/30 rounded-lg px-4 py-3 text-sm">
          Applied {applyResult.rules_applied} rule{applyResult.rules_applied !== 1 ? 's' : ''}:
          {' '}{applyResult.tags_created} tag{applyResult.tags_created !== 1 ? 's' : ''} created,
          {' '}{applyResult.tags_removed} removed.
          <button onClick={() => setApplyResult(null)} className="ml-2 text-text-secondary hover:text-text-primary">&times;</button>
        </div>
      )}

      {/* Create / Edit form */}
      {showForm && (
        <form onSubmit={handleSubmit} className="bg-bg-card border border-border rounded-lg p-4 space-y-4">
          <div className="text-sm font-medium text-text-secondary mb-1">
            {editingId ? 'Edit Rule' : 'New Rule'}
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs text-text-secondary mb-1">Name</label>
              <input
                required
                value={form.name}
                onChange={e => setForm({ ...form, name: e.target.value })}
                placeholder="e.g. Holiday 2025"
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
            <div>
              <label className="block text-xs text-text-secondary mb-1">Priority</label>
              <input
                type="number"
                value={form.priority}
                onChange={e => setForm({ ...form, priority: parseInt(e.target.value) || 100 })}
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs text-text-secondary mb-1">Date From</label>
              <input
                type="date"
                value={form.date_from ?? ''}
                onChange={e => setForm({ ...form, date_from: e.target.value || null })}
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
            <div>
              <label className="block text-xs text-text-secondary mb-1">Date To</label>
              <input
                type="date"
                value={form.date_to ?? ''}
                onChange={e => setForm({ ...form, date_to: e.target.value || null })}
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs text-text-secondary mb-1">Merchant Pattern (regex, optional)</label>
              <input
                value={form.merchant_pattern ?? ''}
                onChange={e => setForm({ ...form, merchant_pattern: e.target.value || null })}
                placeholder="e.g. ^Amazon.*"
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary font-mono"
              />
            </div>
            <div>
              <label className="block text-xs text-text-secondary mb-1">Category Prefix (optional)</label>
              <input
                value={form.category_pattern ?? ''}
                onChange={e => setForm({ ...form, category_pattern: e.target.value || null })}
                placeholder="e.g. Skiing"
                className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
              />
            </div>
          </div>

          <div>
            <label className="block text-xs text-text-secondary mb-1">Tags (comma-separated)</label>
            <input
              required
              value={tagInput}
              onChange={e => setTagInput(e.target.value)}
              placeholder="e.g. holiday-2025, travel"
              className="w-full px-2 py-1.5 text-sm rounded border border-border bg-bg-primary text-text-primary"
            />
            {tagInput && (
              <div className="flex gap-1 mt-1 flex-wrap">
                {parseTags(tagInput).map(t => (
                  <span key={t} className="px-1.5 py-0.5 text-xs rounded bg-accent/15 text-accent">
                    {t}
                  </span>
                ))}
              </div>
            )}
          </div>

          {/* Account multi-select */}
          <div>
            <label className="block text-xs text-text-secondary mb-1">
              Accounts {!(form.account_ids?.length) && <span className="text-text-secondary">(all accounts if none selected)</span>}
            </label>
            <div className="max-h-40 overflow-y-auto border border-border rounded p-2 space-y-1 bg-bg-primary">
              {accounts.map(a => (
                <label key={a.id} className="flex items-center gap-2 text-sm cursor-pointer hover:bg-bg-hover rounded px-1">
                  <input
                    type="checkbox"
                    checked={(form.account_ids ?? []).includes(a.id!)}
                    onChange={() => toggleAccount(a.id!)}
                    className="rounded"
                  />
                  <span className="text-text-primary">
                    {a.display_name || a.name || a.account_ref}
                  </span>
                  <span className="text-text-secondary text-xs ml-auto">{a.institution} &middot; {a.currency}</span>
                </label>
              ))}
              {accounts.length === 0 && (
                <div className="text-xs text-text-secondary">No accounts with metadata found.</div>
              )}
            </div>
          </div>

          <div className="flex items-center gap-3">
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={form.is_active}
                onChange={e => setForm({ ...form, is_active: e.target.checked })}
                className="rounded"
              />
              Active
            </label>
          </div>

          <div className="flex gap-2">
            <button
              type="submit"
              disabled={createRule.isPending || updateRule.isPending || !form.name || !tagInput}
              className="px-3 py-1.5 text-sm rounded-md bg-accent text-white hover:bg-accent/90 disabled:opacity-50"
            >
              {(createRule.isPending || updateRule.isPending) ? 'Saving...' : editingId ? 'Update' : 'Create'}
            </button>
            <button
              type="button"
              onClick={cancelForm}
              className="px-3 py-1.5 text-sm rounded-md border border-border text-text-secondary hover:bg-bg-hover"
            >
              Cancel
            </button>
          </div>
        </form>
      )}

      {/* Rules table */}
      {rules.length > 0 ? (
        <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-bg-secondary">
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Name</th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Date Range</th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Accounts</th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Filters</th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">Tags</th>
                <th className="text-center px-4 py-2 font-medium text-text-secondary">Active</th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary">Actions</th>
              </tr>
            </thead>
            <tbody>
              {rules.map(rule => (
                <tr key={rule.id} className="border-b border-border last:border-0 hover:bg-bg-hover">
                  <td className="px-4 py-2 font-medium">{rule.name}</td>
                  <td className="px-4 py-2 text-text-secondary tabular-nums">
                    {rule.date_from || '*'} &rarr; {rule.date_to || '*'}
                  </td>
                  <td className="px-4 py-2">
                    {rule.account_ids.length === 0 ? (
                      <span className="text-text-secondary">All</span>
                    ) : (
                      <div className="flex flex-wrap gap-1">
                        {rule.account_ids.map(id => (
                          <span key={id} className="px-1.5 py-0.5 text-xs rounded bg-bg-secondary text-text-secondary">
                            {accountLabel(id)}
                          </span>
                        ))}
                      </div>
                    )}
                  </td>
                  <td className="px-4 py-2 text-xs text-text-secondary space-y-0.5">
                    {rule.merchant_pattern && <div>merchant: <span className="font-mono">{rule.merchant_pattern}</span></div>}
                    {rule.category_pattern && <div>category: {rule.category_pattern}*</div>}
                    {!rule.merchant_pattern && !rule.category_pattern && '—'}
                  </td>
                  <td className="px-4 py-2">
                    <div className="flex flex-wrap gap-1">
                      {rule.tags.map(t => (
                        <span key={t} className="px-1.5 py-0.5 text-xs rounded bg-accent/15 text-accent">
                          {t}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td className="px-4 py-2 text-center">
                    <button
                      onClick={() => toggleActive(rule)}
                      className={`w-8 h-4 rounded-full relative transition-colors ${
                        rule.is_active ? 'bg-income' : 'bg-border'
                      }`}
                    >
                      <span
                        className={`absolute top-0.5 w-3 h-3 rounded-full bg-white transition-transform ${
                          rule.is_active ? 'left-4' : 'left-0.5'
                        }`}
                      />
                    </button>
                  </td>
                  <td className="px-4 py-2 text-right">
                    <div className="flex gap-1 justify-end">
                      <button
                        onClick={() => openEdit(rule)}
                        className="px-2 py-1 text-xs rounded border border-border text-text-secondary hover:bg-bg-hover"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => handleDelete(rule.id)}
                        className="px-2 py-1 text-xs rounded border border-expense/30 text-expense hover:bg-expense/10"
                      >
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : !showForm ? (
        <div className="bg-bg-card border border-border rounded-lg p-8 text-center text-text-secondary">
          No tag rules yet. Click "Add Rule" to create your first auto-tagging rule.
        </div>
      ) : null}
    </div>
  )
}
