import { useState, useEffect, useMemo } from 'react'
import {
  useMerchants,
  useUpdateMerchantMapping,
  useUpdateMerchantName,
  useMerchantDetail,
  useSuggestions,
  useReviewSuggestion,
  useRunCategorisation,
  useMergeMerchant,
  useDisplayRules,
  useCreateRule,
  useDeleteRule,
} from '../hooks/useMerchants'
import { useCategories } from '../hooks/useCategories'
import Badge from '../components/common/Badge'
import LoadingSpinner from '../components/common/LoadingSpinner'
import type { CategoryItem, MerchantItem } from '../api/types'

function flattenCategories(items: CategoryItem[], prefix = ''): { path: string; name: string }[] {
  const result: { path: string; name: string }[] = []
  for (const cat of items) {
    result.push({ path: cat.full_path, name: prefix ? `${prefix} › ${cat.name}` : cat.name })
    if (cat.children.length > 0) {
      result.push(...flattenCategories(cat.children, cat.full_path))
    }
  }
  return result
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
  categoryOptions: { path: string; name: string }[]
}) {
  const { data, isLoading } = useMerchantDetail(merchantId)
  const nameMutation = useUpdateMerchantName()
  const mappingMutation = useUpdateMerchantMapping()
  const mergeMutation = useMergeMerchant()
  const [displayName, setDisplayName] = useState('')
  const [mergeSearch, setMergeSearch] = useState('')
  const [showMerge, setShowMerge] = useState(false)

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
          <select
            value={data.category_hint || ''}
            onChange={e => mappingMutation.mutate({ id: merchantId, categoryHint: e.target.value || null })}
            className="w-full bg-bg-card border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
          >
            <option value="">-- None --</option>
            {categoryOptions.map(opt => (
              <option key={opt.path} value={opt.path}>{opt.path}</option>
            ))}
          </select>
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
              <div key={a} className="text-xs text-text-secondary bg-bg-card rounded px-2 py-1">{a}</div>
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
                  <button
                    onClick={() => {
                      if (confirm(`Merge "${m.name}" into "${data.name}"?`)) {
                        mergeMutation.mutate(
                          { survivingId: merchantId, mergeFromId: m.id },
                          { onSuccess: () => { setShowMerge(false); setMergeSearch('') } }
                        )
                      }
                    }}
                    className="text-xs px-2 py-0.5 bg-red-600/20 text-red-400 rounded hover:bg-red-600/30 ml-2 shrink-0"
                  >
                    Merge
                  </button>
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

function DisplayRulesPanel({ categoryOptions }: { categoryOptions: { path: string; name: string }[] }) {
  const { data, isLoading } = useDisplayRules()
  const createMutation = useCreateRule()
  const deleteMutation = useDeleteRule()
  const [showForm, setShowForm] = useState(false)
  const [expanded, setExpanded] = useState(false)
  const [newRule, setNewRule] = useState({
    pattern: '',
    display_name: '',
    merge_group: true,
    category_hint: '' as string | null,
    priority: 100,
  })

  const handleCreate = () => {
    if (!newRule.pattern || !newRule.display_name) return
    createMutation.mutate(
      { ...newRule, category_hint: newRule.category_hint || null },
      {
        onSuccess: () => {
          setNewRule({ pattern: '', display_name: '', merge_group: true, category_hint: '', priority: 100 })
          setShowForm(false)
        },
      }
    )
  }

  if (isLoading) return null
  const rules = data?.items || []

  return (
    <div className="bg-bg-card border border-border rounded-lg p-4 space-y-3">
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
            onClick={() => setShowForm(!showForm)}
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
                    className="w-full bg-bg-card border border-border rounded px-2 py-1 text-sm text-text-primary font-mono focus:outline-none focus:border-accent"
                  />
                </div>
                <div>
                  <label className="text-xs text-text-secondary block mb-0.5">Display Name</label>
                  <input
                    type="text"
                    value={newRule.display_name}
                    onChange={e => setNewRule(r => ({ ...r, display_name: e.target.value }))}
                    placeholder="T. Rowe Price"
                    className="w-full bg-bg-card border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
                  />
                </div>
              </div>
              <div className="grid grid-cols-3 gap-2">
                <div>
                  <label className="text-xs text-text-secondary block mb-0.5">Category (optional)</label>
                  <select
                    value={newRule.category_hint || ''}
                    onChange={e => setNewRule(r => ({ ...r, category_hint: e.target.value || null }))}
                    className="w-full bg-bg-card border border-border rounded px-2 py-1 text-xs text-text-primary focus:outline-none focus:border-accent"
                  >
                    <option value="">-- None --</option>
                    {categoryOptions.map(opt => (
                      <option key={opt.path} value={opt.path}>{opt.path}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="text-xs text-text-secondary block mb-0.5">Priority</label>
                  <input
                    type="number"
                    value={newRule.priority}
                    onChange={e => setNewRule(r => ({ ...r, priority: parseInt(e.target.value) || 100 }))}
                    className="w-full bg-bg-card border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
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
                  <th className="pb-1 pr-2">Pattern</th>
                  <th className="pb-1 pr-2">Display Name</th>
                  <th className="pb-1 pr-2">Category</th>
                  <th className="pb-1 pr-2">Merge</th>
                  <th className="pb-1 pr-2">Priority</th>
                  <th className="pb-1"></th>
                </tr>
              </thead>
              <tbody>
                {rules.map(r => (
                  <tr key={r.id} className="border-t border-border/50">
                    <td className="py-1.5 pr-2 font-mono text-accent">{r.pattern}</td>
                    <td className="py-1.5 pr-2">{r.display_name}</td>
                    <td className="py-1.5 pr-2 text-text-secondary">{r.category_hint || '—'}</td>
                    <td className="py-1.5 pr-2">{r.merge_group ? '✓' : '—'}</td>
                    <td className="py-1.5 pr-2 text-text-secondary">{r.priority}</td>
                    <td className="py-1.5">
                      <button
                        onClick={() => { if (confirm(`Delete rule "${r.pattern}"?`)) deleteMutation.mutate(r.id) }}
                        disabled={deleteMutation.isPending}
                        className="text-red-400 hover:text-red-300"
                      >
                        ✗
                      </button>
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
  const [selectedMerchantId, setSelectedMerchantId] = useState<string | null>(null)
  const runCategorisation = useRunCategorisation()

  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 300)
    return () => clearTimeout(t)
  }, [search])

  const filters = useMemo(() => ({
    limit: 100,
    search: debouncedSearch || undefined,
    unmapped: unmapped || undefined,
  }), [debouncedSearch, unmapped])

  const { data, isLoading, fetchNextPage, hasNextPage, isFetchingNextPage } = useMerchants(filters)
  const { data: categoryTree } = useCategories()
  const mappingMutation = useUpdateMerchantMapping()

  const allItems = useMemo(() => data?.pages.flatMap(p => p.items) || [], [data])
  const categoryOptions = useMemo(() => categoryTree ? flattenCategories(categoryTree.items) : [], [categoryTree])

  const handleCategoryChange = (merchantId: string, value: string) => {
    mappingMutation.mutate({ id: merchantId, categoryHint: value || null })
  }

  if (isLoading) return <LoadingSpinner />

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Merchants</h2>
        <button
          onClick={() => runCategorisation.mutate({ includeLlm: false })}
          disabled={runCategorisation.isPending}
          className="px-3 py-1.5 text-xs bg-accent/20 text-accent rounded-md hover:bg-accent/30 disabled:opacity-50"
        >
          {runCategorisation.isPending ? 'Running...' : 'Run Auto-Categorisation'}
        </button>
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
            checked={unmapped}
            onChange={e => setUnmapped(e.target.checked)}
            className="accent-accent"
          />
          Unmapped only
        </label>
        <span className="text-text-secondary text-sm ml-auto">{allItems.length} merchants shown</span>
      </div>

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
      <table className="w-full text-sm">
        <thead className="sticky top-0 bg-bg-primary">
          <tr className="text-text-secondary text-left text-xs uppercase tracking-wider">
            <th className="pb-2 pr-4">Merchant</th>
            <th className="pb-2 pr-4">Category</th>
            <th className="pb-2 pr-4">Confidence</th>
            <th className="pb-2 pr-4 text-right">Mappings</th>
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
              onSelect={() => setSelectedMerchantId(m.id)}
            />
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

function MerchantRow({
  merchant: m,
  categoryOptions,
  onCategoryChange,
  onSelect,
}: {
  merchant: MerchantItem
  categoryOptions: { path: string; name: string }[]
  onCategoryChange: (id: string, value: string) => void
  onSelect: () => void
}) {
  return (
    <tr className="border-b border-border/50 hover:bg-bg-hover">
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
        <select
          value={m.category_hint || ''}
          onChange={e => onCategoryChange(m.id, e.target.value)}
          className="bg-bg-card border border-border rounded px-2 py-1 text-xs text-text-primary focus:outline-none focus:border-accent w-48"
        >
          <option value="">-- None --</option>
          {categoryOptions.map(opt => (
            <option key={opt.path} value={opt.path}>{opt.path}</option>
          ))}
        </select>
      </td>
    </tr>
  )
}
