import { useState, useEffect, useMemo, useRef } from 'react'
import {
  useMerchants,
  useUpdateMerchantMapping,
  useUpdateMerchantName,
  useMerchantDetail,
  useSuggestions,
  useReviewSuggestion,
  useRunCategorisation,
  useMergeMerchant,
  useSplitAlias,
  useBulkMergeMerchants,
  useDisplayRules,
  useCreateRule,
  useUpdateRule,
  useDeleteRule,
} from '../hooks/useMerchants'
import { useCategories } from '../hooks/useCategories'
import Badge from '../components/common/Badge'
import LoadingSpinner from '../components/common/LoadingSpinner'
import type { CategoryItem, MerchantItem } from '../api/types'

type CategoryOption = { path: string; name: string; categoryType: string | null }

function flattenCategories(
  items: CategoryItem[],
  prefix = '',
  inheritedType: string | null = null,
): CategoryOption[] {
  const result: CategoryOption[] = []
  for (const cat of items) {
    // Use parent's type for children (DB has some mis-typed children)
    const effectiveType = inheritedType || cat.category_type
    result.push({ path: cat.full_path, name: prefix ? `${prefix} › ${cat.name}` : cat.name, categoryType: effectiveType })
    if (cat.children.length > 0) {
      result.push(...flattenCategories(cat.children, cat.full_path, effectiveType))
    }
  }
  return result
}

function CategorySelect({
  value,
  onChange,
  options,
  className,
}: {
  value: string
  onChange: (value: string) => void
  options: CategoryOption[]
  className?: string
}) {
  const expenseOpts = options.filter(o => o.categoryType === 'expense')
  const incomeOpts = options.filter(o => o.categoryType === 'income')

  return (
    <select
      value={value}
      onChange={e => onChange(e.target.value)}
      className={className}
    >
      <option value="">-- None --</option>
      {expenseOpts.length > 0 && (
        <optgroup label="Expense">
          {expenseOpts.map(opt => (
            <option key={opt.path} value={opt.path}>{opt.path}</option>
          ))}
        </optgroup>
      )}
      {incomeOpts.length > 0 && (
        <optgroup label="Income">
          {incomeOpts.map(opt => (
            <option key={opt.path} value={opt.path}>{opt.path}</option>
          ))}
        </optgroup>
      )}
    </select>
  )
}

function SortableHeader({
  label, sortKey, currentSort, currentDir, onSort, align = 'left',
}: {
  label: string
  sortKey: string
  currentSort: string
  currentDir: 'asc' | 'desc'
  onSort: (key: string) => void
  align?: 'left' | 'right'
}) {
  const isActive = currentSort === sortKey
  const arrow = isActive ? (currentDir === 'asc' ? ' ▲' : ' ▼') : ''
  return (
    <th
      className={`pb-2 pr-4 cursor-pointer hover:text-accent select-none ${align === 'right' ? 'text-right' : ''}`}
      onClick={() => onSort(sortKey)}
    >
      {label}{arrow}
    </th>
  )
}

function confidenceBadge(confidence: string | null, method: string | null) {
  if (!confidence || !method) return null
  const conf = parseFloat(confidence)
  const variant = conf >= 0.85 ? 'income' as const : conf >= 0.50 ? 'warning' as const : 'expense' as const
  const label = method === 'human' ? 'Human' : method === 'ibank_history' ? 'iBank' : method === 'source_hint' ? 'Hint' : method === 'llm' ? 'LLM' : method
  return (
    <span className="inline-flex items-center gap-1">
      <Badge variant={variant}>{Math.round(conf * 100)}%</Badge>
      <span className="text-xs text-text-secondary">{label}</span>
    </span>
  )
}

// ── Suggestion Review Panel ──

function SuggestionReviewPanel() {
  const { data, isLoading } = useSuggestions('pending')
  const reviewMutation = useReviewSuggestion()

  if (isLoading) return <LoadingSpinner />
  if (!data || data.items.length === 0) return null

  return (
    <div className="bg-amber-500/10 border border-amber-500/30 rounded-lg p-4 space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-amber-400">
          {data.total} category suggestion{data.total !== 1 ? 's' : ''} to review
        </h3>
      </div>
      <div className="space-y-2 max-h-96 overflow-y-auto">
        {data.items.map(s => (
          <div key={s.id} className="flex items-center gap-3 bg-bg-card rounded px-3 py-2 text-sm">
            <div className="flex-1 min-w-0">
              <span className="font-medium truncate block">{s.merchant_name}</span>
              <span className="text-text-secondary text-xs">{s.reasoning}</span>
            </div>
            <span className="text-xs text-accent font-medium whitespace-nowrap">{s.suggested_category_path.replace(/:/g, ' › ')}</span>
            <Badge variant={parseFloat(s.confidence) >= 0.85 ? 'income' : 'warning'}>
              {Math.round(parseFloat(s.confidence) * 100)}%
            </Badge>
            <div className="flex gap-1">
              <button
                onClick={() => reviewMutation.mutate({ id: s.id, status: 'accepted' })}
                disabled={reviewMutation.isPending}
                className="px-2 py-1 text-xs bg-green-600/20 text-green-400 rounded hover:bg-green-600/30"
              >
                ✓
              </button>
              <button
                onClick={() => reviewMutation.mutate({ id: s.id, status: 'rejected' })}
                disabled={reviewMutation.isPending}
                className="px-2 py-1 text-xs bg-red-600/20 text-red-400 rounded hover:bg-red-600/30"
              >
                ✗
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Merchant Detail Slide-over ──

function MerchantSlideOver({
  merchantId,
  onClose,
  categoryOptions,
}: {
  merchantId: string
  onClose: () => void
  categoryOptions: CategoryOption[]
}) {
  const { data, isLoading } = useMerchantDetail(merchantId)
  const nameMutation = useUpdateMerchantName()
  const mappingMutation = useUpdateMerchantMapping()
  const mergeMutation = useMergeMerchant()
  const splitMutation = useSplitAlias()
  const [displayName, setDisplayName] = useState('')
  const [mergeSearch, setMergeSearch] = useState('')
  const [showMerge, setShowMerge] = useState(false)
  const [confirmingAlias, setConfirmingAlias] = useState<string | null>(null)
  const [confirmingMerge, setConfirmingMerge] = useState<string | null>(null)

  const { data: mergeResults } = useMerchants({
    search: mergeSearch || undefined,
    limit: 10,
  })
  const mergeItems = useMemo(
    () => mergeResults?.pages.flatMap(p => p.items).filter(m => m.id !== merchantId) || [],
    [mergeResults, merchantId]
  )

  useEffect(() => {
    if (data) setDisplayName(data.display_name || '')
  }, [data])

  if (isLoading || !data) return null

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <div className="relative w-96 bg-bg-primary border-l border-border overflow-y-auto p-6 space-y-5">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold truncate">{data.display_name || data.name}</h3>
          <button onClick={onClose} className="text-text-secondary hover:text-text-primary text-xl">&times;</button>
        </div>

        {/* Canonical name */}
        <div>
          <label className="text-xs text-text-secondary uppercase tracking-wider">Canonical Name</label>
          <p className="text-sm">{data.name}</p>
        </div>

        {/* Display name */}
        <div>
          <label className="text-xs text-text-secondary uppercase tracking-wider block mb-1">Display Name</label>
          <div className="flex gap-2">
            <input
              type="text"
              value={displayName}
              onChange={e => setDisplayName(e.target.value)}
              placeholder="Set a friendly name..."
              className="flex-1 bg-bg-card border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
            />
            <button
              onClick={() => nameMutation.mutate({ id: merchantId, displayName: displayName || null })}
              disabled={nameMutation.isPending}
              className="px-3 py-1 text-xs bg-accent/20 text-accent rounded hover:bg-accent/30"
            >
              Save
            </button>
          </div>
        </div>

        {/* Category */}
        <div>
          <label className="text-xs text-text-secondary uppercase tracking-wider block mb-1">Category</label>
          <CategorySelect
            value={data.category_hint || ''}
            onChange={v => mappingMutation.mutate({ id: merchantId, categoryHint: v || null })}
            options={categoryOptions}
            className="w-full bg-bg-card border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
          />
          {data.category_method && (
            <div className="mt-1">
              {confidenceBadge(data.category_confidence, data.category_method)}
            </div>
          )}
        </div>

        {/* Aliases */}
        <div>
          <label className="text-xs text-text-secondary uppercase tracking-wider block mb-1">
            Aliases ({data.aliases.length})
          </label>
          <div className="space-y-1 max-h-40 overflow-y-auto">
            {data.aliases.map(a => (
              <div key={a} className="flex items-center gap-1 text-xs text-text-secondary bg-bg-card rounded px-2 py-1">
                {confirmingAlias === a ? (
                  <>
                    <span className="flex-1 text-text-primary">Split &ldquo;{a}&rdquo;?</span>
                    <button
                      onClick={() => {
                        splitMutation.mutate({ merchantId, alias: a }, {
                          onSettled: () => setConfirmingAlias(null),
                        })
                      }}
                      disabled={splitMutation.isPending}
                      className="shrink-0 text-green-400 hover:text-green-300 font-bold disabled:opacity-50"
                    >
                      {splitMutation.isPending ? '...' : '✓'}
                    </button>
                    <button
                      onClick={() => setConfirmingAlias(null)}
                      className="shrink-0 text-text-secondary hover:text-text-primary"
                    >
                      ✗
                    </button>
                  </>
                ) : (
                  <>
                    <span className="flex-1 truncate">{a}</span>
                    {data.aliases.length > 1 && (
                      <button
                        onClick={() => setConfirmingAlias(a)}
                        className="shrink-0 text-text-secondary hover:text-red-400 transition-colors"
                        title="Split into separate merchant"
                      >
                        ×
                      </button>
                    )}
                  </>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Recent Transactions */}
        {data.recent_transactions && data.recent_transactions.length > 0 && (
          <div>
            <label className="text-xs text-text-secondary uppercase tracking-wider block mb-1">
              Recent Transactions ({data.recent_transactions.length})
            </label>
            <div className="space-y-1 max-h-60 overflow-y-auto">
              {data.recent_transactions.map(tx => (
                <div key={tx.id} className="flex items-center justify-between bg-bg-card rounded px-2 py-1.5 text-xs">
                  <div className="min-w-0 flex-1">
                    <div className="text-text-primary truncate">{tx.raw_merchant || '—'}</div>
                    <div className="text-text-secondary">{tx.posted_at} · {tx.source}</div>
                  </div>
                  <div className={`shrink-0 ml-2 font-mono ${parseFloat(tx.amount) < 0 ? 'text-expense' : 'text-income'}`}>
                    {parseFloat(tx.amount) < 0 ? '' : '+'}{parseFloat(tx.amount).toFixed(2)} {tx.currency}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Merge */}
        <div>
          <button
            onClick={() => setShowMerge(!showMerge)}
            className="text-xs text-accent hover:text-accent-hover"
          >
            {showMerge ? 'Cancel merge' : 'Merge another merchant into this one...'}
          </button>
          {showMerge && (
            <div className="mt-2 space-y-2">
              <input
                type="text"
                value={mergeSearch}
                onChange={e => setMergeSearch(e.target.value)}
                placeholder="Search merchant to merge..."
                className="w-full bg-bg-card border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
              />
              {mergeSearch && mergeItems.map(m => (
                <div key={m.id} className="flex items-center justify-between bg-bg-card rounded px-2 py-1">
                  <span className="text-xs truncate">{m.display_name || m.name}</span>
                  {confirmingMerge === m.id ? (
                    <span className="flex items-center gap-1 ml-2 shrink-0">
                      <span className="text-xs text-text-secondary">Sure?</span>
                      <button
                        onClick={() => {
                          mergeMutation.mutate(
                            { survivingId: merchantId, mergeFromId: m.id },
                            { onSuccess: () => { setShowMerge(false); setMergeSearch(''); setConfirmingMerge(null) } }
                          )
                        }}
                        disabled={mergeMutation.isPending}
                        className="text-xs px-1.5 py-0.5 bg-red-600/20 text-red-400 rounded hover:bg-red-600/30 disabled:opacity-50"
                      >
                        {mergeMutation.isPending ? '...' : 'Yes'}
                      </button>
                      <button
                        onClick={() => setConfirmingMerge(null)}
                        className="text-xs px-1.5 py-0.5 text-text-secondary hover:text-text-primary"
                      >
                        No
                      </button>
                    </span>
                  ) : (
                    <button
                      onClick={() => setConfirmingMerge(m.id)}
                      className="text-xs px-2 py-0.5 bg-red-600/20 text-red-400 rounded hover:bg-red-600/30 ml-2 shrink-0"
                    >
                      Merge
                    </button>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="text-xs text-text-secondary">
          {data.mapping_count} raw merchant mapping{data.mapping_count !== 1 ? 's' : ''}
        </div>
      </div>
    </div>
  )
}

// ── Display Rules Panel ──

function RuleSortHeader({
  label, sortKey, currentSort, currentDir, onSort,
}: {
  label: string
  sortKey: string
  currentSort: string
  currentDir: 'asc' | 'desc'
  onSort: (key: string) => void
}) {
  const isActive = currentSort === sortKey
  const arrow = isActive ? (currentDir === 'asc' ? ' ▲' : ' ▼') : ''
  return (
    <th
      className="pb-1 pr-2 cursor-pointer hover:text-accent select-none"
      onClick={() => onSort(sortKey)}
    >
      {label}{arrow}
    </th>
  )
}

function DisplayRulesPanel({ categoryOptions }: { categoryOptions: CategoryOption[] }) {
  const { data, isLoading } = useDisplayRules()
  const createMutation = useCreateRule()
  const updateMutation = useUpdateRule()
  const deleteMutation = useDeleteRule()
  const [showForm, setShowForm] = useState(false)
  const [expanded, setExpanded] = useState(false)
  const [ruleSortBy, setRuleSortBy] = useState('priority')
  const [ruleSortDir, setRuleSortDir] = useState<'asc' | 'desc'>('asc')
  const [editingId, setEditingId] = useState<number | null>(null)
  const [confirmingDeleteRule, setConfirmingDeleteRule] = useState<number | null>(null)
  const panelRef = useRef<HTMLDivElement>(null)
  const editingIdRef = useRef(editingId)
  const editStateRef = useRef<{ pattern: string; display_name: string; merge_group: boolean; category_hint: string | null; priority: number }>({ pattern: '', display_name: '', merge_group: true, category_hint: null, priority: 100 })

  const [editState, setEditState] = useState({
    pattern: '',
    display_name: '',
    merge_group: true,
    category_hint: '' as string | null,
    priority: 100,
  })

  // Keep refs in sync
  editingIdRef.current = editingId
  editStateRef.current = editState

  useEffect(() => {
    if (editingId === null) return
    const handler = (e: MouseEvent) => {
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        const id = editingIdRef.current
        const state = editStateRef.current
        if (id !== null && state.pattern && state.display_name) {
          updateMutation.mutate(
            { id, rule: { ...state, category_hint: state.category_hint || null } },
          )
        }
        setEditingId(null)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [editingId])
  const [newRule, setNewRule] = useState({
    pattern: '',
    display_name: '',
    merge_group: true,
    category_hint: '' as string | null,
    priority: 100,
  })

  const saveCurrentEdit = () => {
    const id = editingIdRef.current
    const state = editStateRef.current
    if (id !== null && state.pattern && state.display_name) {
      updateMutation.mutate(
        { id, rule: { ...state, category_hint: state.category_hint || null } },
      )
    }
  }

  const startEdit = (r: { id: number; pattern: string; display_name: string; merge_group: boolean; category_hint: string | null; priority: number }) => {
    if (editingId !== null && editingId !== r.id) {
      saveCurrentEdit()
    }
    setEditingId(r.id)
    setEditState({
      pattern: r.pattern,
      display_name: r.display_name,
      merge_group: r.merge_group,
      category_hint: r.category_hint || '',
      priority: r.priority,
    })
  }

  const handleSaveEdit = () => {
    if (editingId === null || !editState.pattern || !editState.display_name) return
    updateMutation.mutate(
      { id: editingId, rule: { ...editState, category_hint: editState.category_hint || null } },
    )
    setEditingId(null)
  }

  const handleCreate = () => {
    if (!newRule.pattern || !newRule.display_name) return
    createMutation.mutate(
      { ...newRule, category_hint: newRule.category_hint || null },
      {
        onSettled: () => {
          setNewRule({ pattern: '', display_name: '', merge_group: true, category_hint: '', priority: 100 })
          setShowForm(false)
          setEditingId(null)
        },
      }
    )
  }

  const rules = data?.items || []

  const handleRuleSort = (key: string) => {
    if (ruleSortBy === key) {
      setRuleSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setRuleSortBy(key)
      setRuleSortDir('asc')
    }
  }

  const sortedRules = useMemo(() => {
    const sorted = [...rules]
    const dir = ruleSortDir === 'asc' ? 1 : -1
    sorted.sort((a, b) => {
      switch (ruleSortBy) {
        case 'pattern': return dir * a.pattern.localeCompare(b.pattern)
        case 'display_name': return dir * a.display_name.localeCompare(b.display_name)
        case 'category': return dir * (a.category_hint || '').localeCompare(b.category_hint || '')
        case 'merge': return dir * (Number(a.merge_group) - Number(b.merge_group))
        case 'priority': return dir * (a.priority - b.priority)
        default: return 0
      }
    })
    return sorted
  }, [rules, ruleSortBy, ruleSortDir])

  if (isLoading) return null

  const inputClass = "w-full bg-bg-card border border-border rounded px-2 py-1 text-xs text-text-primary focus:outline-none focus:border-accent"

  return (
    <div ref={panelRef} className="bg-bg-card border border-border rounded-lg p-4 space-y-3">
      <div className="flex items-center justify-between">
        <button
          onClick={() => setExpanded(!expanded)}
          className="text-sm font-semibold text-text-primary hover:text-accent flex items-center gap-1"
        >
          <span className="text-xs">{expanded ? '▼' : '▶'}</span>
          Display Rules ({rules.length})
        </button>
        {expanded && (
          <button
            onClick={() => { if (editingId !== null) saveCurrentEdit(); setShowForm(!showForm); setEditingId(null) }}
            className="px-2 py-1 text-xs bg-accent/20 text-accent rounded hover:bg-accent/30"
          >
            {showForm ? 'Cancel' : '+ New Rule'}
          </button>
        )}
      </div>

      {expanded && (
        <>
          {showForm && (
            <div className="bg-bg-primary rounded p-3 space-y-2 border border-border">
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <label className="text-xs text-text-secondary block mb-0.5">Regex Pattern</label>
                  <input
                    type="text"
                    value={newRule.pattern}
                    onChange={e => setNewRule(r => ({ ...r, pattern: e.target.value }))}
                    placeholder=".*T ROWE P.*"
                    className={`${inputClass} font-mono`}
                  />
                </div>
                <div>
                  <label className="text-xs text-text-secondary block mb-0.5">Display Name</label>
                  <input
                    type="text"
                    value={newRule.display_name}
                    onChange={e => setNewRule(r => ({ ...r, display_name: e.target.value }))}
                    placeholder="T. Rowe Price"
                    className={inputClass}
                  />
                </div>
              </div>
              <div className="grid grid-cols-3 gap-2">
                <div>
                  <label className="text-xs text-text-secondary block mb-0.5">Category (optional)</label>
                  <CategorySelect
                    value={newRule.category_hint || ''}
                    onChange={v => setNewRule(r => ({ ...r, category_hint: v || null }))}
                    options={categoryOptions}
                    className={inputClass}
                  />
                </div>
                <div>
                  <label className="text-xs text-text-secondary block mb-0.5">Priority</label>
                  <input
                    type="number"
                    value={newRule.priority}
                    onChange={e => setNewRule(r => ({ ...r, priority: parseInt(e.target.value) || 100 }))}
                    className={inputClass}
                  />
                </div>
                <div className="flex items-end gap-2">
                  <label className="flex items-center gap-1.5 text-xs text-text-secondary cursor-pointer pb-1">
                    <input
                      type="checkbox"
                      checked={newRule.merge_group}
                      onChange={e => setNewRule(r => ({ ...r, merge_group: e.target.checked }))}
                      className="accent-accent"
                    />
                    Merge matches
                  </label>
                  <button
                    onClick={handleCreate}
                    disabled={createMutation.isPending || !newRule.pattern || !newRule.display_name}
                    className="px-3 py-1 text-xs bg-accent text-white rounded hover:bg-accent-hover disabled:opacity-50 ml-auto"
                  >
                    Create
                  </button>
                </div>
              </div>
            </div>
          )}

          {rules.length > 0 && (
            <table className="w-full text-xs">
              <thead>
                <tr className="text-text-secondary text-left uppercase tracking-wider">
                  <RuleSortHeader label="Pattern" sortKey="pattern" currentSort={ruleSortBy} currentDir={ruleSortDir} onSort={handleRuleSort} />
                  <RuleSortHeader label="Display Name" sortKey="display_name" currentSort={ruleSortBy} currentDir={ruleSortDir} onSort={handleRuleSort} />
                  <RuleSortHeader label="Category" sortKey="category" currentSort={ruleSortBy} currentDir={ruleSortDir} onSort={handleRuleSort} />
                  <RuleSortHeader label="Merge" sortKey="merge" currentSort={ruleSortBy} currentDir={ruleSortDir} onSort={handleRuleSort} />
                  <RuleSortHeader label="Priority" sortKey="priority" currentSort={ruleSortBy} currentDir={ruleSortDir} onSort={handleRuleSort} />
                  <th className="pb-1"></th>
                </tr>
              </thead>
              <tbody>
                {sortedRules.map(r => editingId === r.id ? (
                  <tr key={r.id} className="border-t border-accent/30 bg-accent/5">
                    <td className="py-1.5 pr-2">
                      <input
                        type="text"
                        value={editState.pattern}
                        onChange={e => setEditState(s => ({ ...s, pattern: e.target.value }))}
                        className={`${inputClass} font-mono`}
                      />
                    </td>
                    <td className="py-1.5 pr-2">
                      <input
                        type="text"
                        value={editState.display_name}
                        onChange={e => setEditState(s => ({ ...s, display_name: e.target.value }))}
                        className={inputClass}
                      />
                    </td>
                    <td className="py-1.5 pr-2">
                      <CategorySelect
                        value={editState.category_hint || ''}
                        onChange={v => setEditState(s => ({ ...s, category_hint: v || null }))}
                        options={categoryOptions}
                        className={inputClass}
                      />
                    </td>
                    <td className="py-1.5 pr-2">
                      <input
                        type="checkbox"
                        checked={editState.merge_group}
                        onChange={e => setEditState(s => ({ ...s, merge_group: e.target.checked }))}
                        className="accent-accent"
                      />
                    </td>
                    <td className="py-1.5 pr-2">
                      <input
                        type="number"
                        value={editState.priority}
                        onChange={e => setEditState(s => ({ ...s, priority: parseInt(e.target.value) || 0 }))}
                        className={`${inputClass} w-16`}
                      />
                    </td>
                    <td className="py-1.5">
                      <div className="flex gap-1">
                        <button
                          onClick={handleSaveEdit}
                          disabled={updateMutation.isPending || !editState.pattern || !editState.display_name}
                          className="text-green-400 hover:text-green-300 disabled:opacity-50"
                        >
                          {updateMutation.isPending ? '...' : '✓'}
                        </button>
                        <button
                          onClick={() => setEditingId(null)}
                          className="text-text-secondary hover:text-text-primary"
                        >
                          ✗
                        </button>
                      </div>
                    </td>
                  </tr>
                ) : (
                  <tr
                    key={r.id}
                    className="border-t border-border/50 hover:bg-bg-hover cursor-pointer"
                    onClick={() => startEdit(r)}
                  >
                    <td className="py-1.5 pr-2 font-mono text-accent">{r.pattern}</td>
                    <td className="py-1.5 pr-2">{r.display_name}</td>
                    <td className="py-1.5 pr-2 text-text-secondary">{r.category_hint || '—'}</td>
                    <td className="py-1.5 pr-2">{r.merge_group ? '✓' : '—'}</td>
                    <td className="py-1.5 pr-2 text-text-secondary">{r.priority}</td>
                    <td className="py-1.5">
                      {confirmingDeleteRule === r.id ? (
                        <span className="flex items-center gap-1">
                          <button
                            onClick={e => { e.stopPropagation(); deleteMutation.mutate(r.id, { onSettled: () => setConfirmingDeleteRule(null) }) }}
                            disabled={deleteMutation.isPending}
                            className="text-red-400 hover:text-red-300 disabled:opacity-50"
                          >
                            {deleteMutation.isPending ? '...' : '✓'}
                          </button>
                          <button
                            onClick={e => { e.stopPropagation(); setConfirmingDeleteRule(null) }}
                            className="text-text-secondary hover:text-text-primary"
                          >
                            ✗
                          </button>
                        </span>
                      ) : (
                        <button
                          onClick={e => { e.stopPropagation(); setConfirmingDeleteRule(r.id) }}
                          className="text-red-400 hover:text-red-300"
                        >
                          ✗
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}
    </div>
  )
}

// ── Main Merchants Page ──

export default function Merchants() {
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [unmapped, setUnmapped] = useState(false)
  const [searchAliases, setSearchAliases] = useState(false)
  const [selectedMerchantId, setSelectedMerchantId] = useState<string | null>(null)
  const [selectMode, setSelectMode] = useState(false)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [mergeName, setMergeName] = useState('')
  const [sortBy, setSortBy] = useState('name')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')
  const [datePreset, setDatePreset] = useState<'12m' | '2y' | 'all'>('all')
  const runCategorisation = useRunCategorisation()
  const bulkMerge = useBulkMergeMerchants()

  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 300)
    return () => clearTimeout(t)
  }, [search])

  const lastUsedAfter = useMemo(() => {
    if (datePreset === 'all') return undefined
    const d = new Date()
    if (datePreset === '12m') d.setFullYear(d.getFullYear() - 1)
    else if (datePreset === '2y') d.setFullYear(d.getFullYear() - 2)
    return d.toISOString().slice(0, 10)
  }, [datePreset])

  const filters = useMemo(() => ({
    limit: 100,
    search: debouncedSearch || undefined,
    search_aliases: (debouncedSearch && searchAliases) || undefined,
    unmapped: unmapped || undefined,
    last_used_after: lastUsedAfter,
    sort_by: sortBy,
    sort_dir: sortDir,
  }), [debouncedSearch, searchAliases, unmapped, lastUsedAfter, sortBy, sortDir])

  const handleSort = (key: string) => {
    if (sortBy === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(key)
      setSortDir('asc')
    }
  }

  const { data, isLoading, fetchNextPage, hasNextPage, isFetchingNextPage } = useMerchants(filters)
  const { data: categoryTree } = useCategories()
  const mappingMutation = useUpdateMerchantMapping()

  const allItems = useMemo(() => data?.pages.flatMap(p => p.items) || [], [data])
  const categoryOptions = useMemo(() => categoryTree ? flattenCategories(categoryTree.items) : [], [categoryTree])

  const handleCategoryChange = (merchantId: string, value: string) => {
    mappingMutation.mutate({ id: merchantId, categoryHint: value || null })
  }

  const toggleSelect = (id: string) => {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const toggleSelectAll = () => {
    if (selected.size === allItems.length) setSelected(new Set())
    else setSelected(new Set(allItems.map(m => m.id)))
  }

  const exitSelectMode = () => {
    setSelectMode(false)
    setSelected(new Set())
    setMergeName('')
  }

  const handleMerge = (ids: string[], name: string) => {
    if (ids.length < 2 || !name.trim()) return
    bulkMerge.mutate({ merchantIds: ids, displayName: name.trim() }, {
      onSuccess: () => {
        exitSelectMode()
      },
    })
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Merchants</h2>
        <div className="flex items-center gap-2">
          <button
            onClick={() => {
              fetch('/api/v1/merchants/export')
                .then(r => r.blob())
                .then(blob => {
                  const url = URL.createObjectURL(blob)
                  const a = document.createElement('a')
                  a.href = url
                  a.download = 'merchants.csv'
                  a.click()
                  URL.revokeObjectURL(url)
                })
            }}
            className="px-3 py-1.5 text-xs border border-border text-text-secondary rounded-md hover:text-text-primary hover:border-accent/30"
          >
            Export CSV
          </button>
          <button
            onClick={() => runCategorisation.mutate({ includeLlm: false })}
            disabled={runCategorisation.isPending}
            className="px-3 py-1.5 text-xs bg-accent/20 text-accent rounded-md hover:bg-accent/30 disabled:opacity-50"
          >
            {runCategorisation.isPending ? 'Running...' : 'Run Auto-Categorisation'}
          </button>
        </div>
      </div>

      {/* Suggestion review panel */}
      <SuggestionReviewPanel />

      {/* Display Rules */}
      <DisplayRulesPanel categoryOptions={categoryOptions} />

      {/* Filters */}
      <div className="flex gap-3 items-center">
        <input
          type="text"
          placeholder="Search merchants..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary placeholder:text-text-secondary focus:outline-none focus:border-accent w-64"
        />
        <label className="flex items-center gap-2 text-sm text-text-secondary cursor-pointer">
          <input
            type="checkbox"
            checked={searchAliases}
            onChange={e => setSearchAliases(e.target.checked)}
            className="accent-accent"
          />
          Include aliases
        </label>
        <label className="flex items-center gap-2 text-sm text-text-secondary cursor-pointer">
          <input
            type="checkbox"
            checked={unmapped}
            onChange={e => setUnmapped(e.target.checked)}
            className="accent-accent"
          />
          Unmapped only
        </label>
        <div className="flex items-center gap-1 ml-2">
          {([['12m', 'Last 12m'], ['2y', 'Last 2y'], ['all', 'All time']] as const).map(([key, label]) => (
            <button
              key={key}
              onClick={() => setDatePreset(key)}
              className={`px-2 py-1 text-xs rounded-md border transition-colors ${
                datePreset === key
                  ? 'bg-accent/20 text-accent border-accent/40'
                  : 'border-border text-text-secondary hover:text-text-primary hover:border-accent/30'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
        <span className="text-text-secondary text-sm ml-auto">{allItems.length} merchants shown</span>
        <button
          onClick={() => {
            if (selectMode) {
              exitSelectMode()
            } else {
              setSelectMode(true)
              // Auto-select all visible items when entering select mode
              setSelected(new Set(allItems.map(m => m.id)))
            }
          }}
          className="text-xs px-3 py-1.5 border border-border rounded-md text-text-secondary hover:text-text-primary hover:border-accent"
        >
          {selectMode ? 'Exit Select' : 'Select'}
        </button>
      </div>

      {/* Merge all search results shortcut */}
      {debouncedSearch && allItems.length >= 2 && !selectMode && (
        <MergeAllBar
          count={allItems.length}
          searchTerm={debouncedSearch}
          merchantIds={allItems.map(m => m.id)}
          onMerge={handleMerge}
          isPending={bulkMerge.isPending}
        />
      )}

      {/* Categorisation result toast */}
      {runCategorisation.data && (
        <div className="bg-green-500/10 border border-green-500/30 rounded-lg px-4 py-2 text-sm text-green-400">
          Auto-categorisation complete:{' '}
          {runCategorisation.data.rules_merchants_merged > 0 && <>{runCategorisation.data.rules_merchants_merged} merged by rules, </>}
          {runCategorisation.data.rules_merchants_renamed > 0 && <>{runCategorisation.data.rules_merchants_renamed} renamed by rules, </>}
          {runCategorisation.data.auto_accepted} auto-accepted,{' '}
          {runCategorisation.data.queued_for_review} queued for review,{' '}
          {runCategorisation.data.display_names_set} display names set.
        </div>
      )}

      {/* Merchant table */}
      {isLoading ? <LoadingSpinner /> : (
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-bg-primary">
            <tr className="text-text-secondary text-left text-xs uppercase tracking-wider">
              {selectMode && (
                <th className="pb-2 pr-2 w-8">
                  <input
                    type="checkbox"
                    checked={selected.size === allItems.length && allItems.length > 0}
                    ref={el => { if (el) el.indeterminate = selected.size > 0 && selected.size < allItems.length }}
                    onChange={toggleSelectAll}
                    className="accent-accent"
                  />
                </th>
              )}
              <SortableHeader label="Merchant" sortKey="name" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
              <SortableHeader label="Category" sortKey="category" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
              <SortableHeader label="Confidence" sortKey="confidence" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
              <SortableHeader label="Mappings" sortKey="mappings" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} align="right" />
              <th className="pb-2">Assign Category</th>
            </tr>
          </thead>
          <tbody>
            {allItems.map(m => (
              <MerchantRow
                key={m.id}
                merchant={m}
                categoryOptions={categoryOptions}
                onCategoryChange={handleCategoryChange}
                onSelect={() => selectMode ? toggleSelect(m.id) : setSelectedMerchantId(m.id)}
                selectMode={selectMode}
                isSelected={selected.has(m.id)}
                onToggle={() => toggleSelect(m.id)}
              />
            ))}
          </tbody>
        </table>
      )}

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

      {/* Merge toolbar (select mode) */}
      {selectMode && selected.size >= 2 && (
        <div className="fixed bottom-0 left-0 right-0 bg-bg-card border-t border-border px-6 py-3 flex items-center gap-3 z-50">
          <span className="text-sm font-medium">{selected.size} merchants selected</span>
          <span className="text-border">|</span>
          <label className="text-sm text-text-secondary">Merge as:</label>
          <input
            type="text"
            value={mergeName}
            onChange={e => setMergeName(e.target.value)}
            placeholder="Display name for merged merchant..."
            className="bg-bg-primary border border-border rounded-md px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent w-64"
            autoFocus
          />
          <button
            onClick={() => handleMerge(Array.from(selected), mergeName)}
            disabled={bulkMerge.isPending || !mergeName.trim()}
            className="text-xs px-3 py-1.5 bg-accent text-white rounded-md hover:bg-accent-hover disabled:opacity-50"
          >
            {bulkMerge.isPending ? 'Merging...' : `Merge ${selected.size} merchants`}
          </button>
          <button
            onClick={exitSelectMode}
            className="text-xs px-3 py-1.5 border border-border rounded-md text-text-secondary hover:text-text-primary ml-auto"
          >
            Cancel
          </button>
        </div>
      )}

      {/* Detail slide-over */}
      {selectedMerchantId && (
        <MerchantSlideOver
          merchantId={selectedMerchantId}
          onClose={() => setSelectedMerchantId(null)}
          categoryOptions={categoryOptions}
        />
      )}
    </div>
  )
}

// ── Merge All Search Results Bar ──

function MergeAllBar({
  count, searchTerm, merchantIds, onMerge, isPending,
}: {
  count: number
  searchTerm: string
  merchantIds: string[]
  onMerge: (ids: string[], name: string) => void
  isPending: boolean
}) {
  const [name, setName] = useState(searchTerm)
  return (
    <div className="bg-accent/10 border border-accent/30 rounded-lg px-4 py-2 flex items-center gap-3">
      <span className="text-sm text-accent">{count} merchants match — merge into one?</span>
      <input
        type="text"
        value={name}
        onChange={e => setName(e.target.value)}
        placeholder="Display name..."
        className="bg-bg-primary border border-border rounded-md px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent w-48"
      />
      <button
        onClick={() => onMerge(merchantIds, name)}
        disabled={isPending || !name.trim()}
        className="text-xs px-3 py-1.5 bg-accent text-white rounded-md hover:bg-accent-hover disabled:opacity-50"
      >
        {isPending ? 'Merging...' : `Merge all ${count}`}
      </button>
    </div>
  )
}

function MerchantRow({
  merchant: m,
  categoryOptions,
  onCategoryChange,
  onSelect,
  selectMode = false,
  isSelected = false,
  onToggle,
}: {
  merchant: MerchantItem
  categoryOptions: CategoryOption[]
  onCategoryChange: (id: string, value: string) => void
  onSelect: () => void
  selectMode?: boolean
  isSelected?: boolean
  onToggle?: () => void
}) {
  return (
    <tr className={`border-b border-border/50 hover:bg-bg-hover ${isSelected ? 'bg-accent/5' : ''}`}>
      {selectMode && (
        <td className="py-2 pr-2 w-8">
          <input type="checkbox" checked={isSelected} onChange={onToggle} className="accent-accent" />
        </td>
      )}
      <td className="py-2 pr-4">
        <button
          onClick={onSelect}
          className="text-left hover:text-accent transition-colors"
        >
          <span className="font-medium">{m.display_name || m.name}</span>
          {m.display_name && m.display_name !== m.name && (
            <span className="text-xs text-text-secondary ml-2">({m.name})</span>
          )}
        </button>
      </td>
      <td className="py-2 pr-4">
        {m.category_hint ? (
          <Badge variant="accent">{m.category_hint}</Badge>
        ) : (
          <Badge variant="warning">Unmapped</Badge>
        )}
      </td>
      <td className="py-2 pr-4">
        {confidenceBadge(m.category_confidence, m.category_method)}
      </td>
      <td className="py-2 pr-4 text-right text-text-secondary">{m.mapping_count}</td>
      <td className="py-2">
        <CategorySelect
          value={m.category_hint || ''}
          onChange={v => onCategoryChange(m.id, v)}
          options={categoryOptions}
          className="bg-bg-card border border-border rounded px-2 py-1 text-xs text-text-primary focus:outline-none focus:border-accent w-48"
        />
      </td>
    </tr>
  )
}
