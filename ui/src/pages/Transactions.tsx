import { useState, useMemo, useCallback, useEffect, useRef } from 'react'
import { useTransactions, useTransaction, useUpdateNote, useUpdateTransactionCategory, useLinkTransfer, useUnlinkEvent } from '../hooks/useTransactions'
import { useUpdateMerchantName } from '../hooks/useMerchants'
import { useCategories } from '../hooks/useCategories'
import { useOverview } from '../hooks/useStats'
import CurrencyAmount from '../components/common/CurrencyAmount'
import Badge from '../components/common/Badge'
import LoadingSpinner from '../components/common/LoadingSpinner'
import JsonViewer from '../components/common/JsonViewer'
import type { TransactionItem, TransactionDetail, CategoryItem } from '../api/types'

export default function Transactions() {
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [account, setAccount] = useState('')  // "institution/account_ref" or ""
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

  const [filterInstitution, filterAccountRef] = useMemo(() => {
    if (!account) return [undefined, undefined]
    const [inst, ...rest] = account.split('/')
    return [inst, rest.join('/')]
  }, [account])

  const filters = useMemo(() => ({
    limit: 100,
    search: debouncedSearch || undefined,
    institution: filterInstitution,
    account_ref: filterAccountRef,
    currency: currency || undefined,
    date_from: dateFrom || undefined,
    date_to: dateTo || undefined,
  }), [debouncedSearch, filterInstitution, filterAccountRef, currency, dateFrom, dateTo])

  const { data, isLoading, fetchNextPage, hasNextPage, isFetchingNextPage } = useTransactions(filters)
  const { data: detail, isLoading: detailLoading } = useTransaction(selectedId)

  const allItems = useMemo(() => {
    if (!data) return []
    return data.pages.flatMap(p => p.items)
  }, [data])

  const clearFilters = useCallback(() => {
    setSearch('')
    setAccount('')
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
            placeholder="Search merchants, notes, amounts..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary placeholder:text-text-secondary focus:outline-none focus:border-accent w-56"
          />
          <select
            value={account}
            onChange={e => setAccount(e.target.value)}
            className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary focus:outline-none focus:border-accent"
          >
            <option value="">All accounts</option>
            {overview?.accounts.map(a => (
              <option key={`${a.institution}/${a.account_ref}`} value={`${a.institution}/${a.account_ref}`}>{a.label}</option>
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
        <div className="truncate max-w-[300px] flex items-center gap-1.5">
          <span className="truncate">{txn.canonical_merchant_name || txn.cleaned_merchant || txn.raw_merchant || '—'}</span>
          {txn.note && (
            <span className="flex-shrink-0" title={txn.note}>
              <svg className="w-3.5 h-3.5 text-text-secondary opacity-50" viewBox="0 0 16 16" fill="currentColor">
                <path d="M2 3.5A1.5 1.5 0 013.5 2h9A1.5 1.5 0 0114 3.5v9a1.5 1.5 0 01-1.5 1.5h-9A1.5 1.5 0 012 12.5v-9zM4 5h8v1H4V5zm0 2.5h8v1H4v-1zM4 10h5v1H4v-1z"/>
              </svg>
            </span>
          )}
        </div>
      </td>
      <td className="py-2 pr-4">
        {txn.category_name ? (
          <span title={txn.category_path || undefined}><Badge variant="accent">{txn.category_name}</Badge></span>
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

      {/* Merchant + Category */}
      <MerchantSection detail={detail} />

      {detail.raw_memo && (
        <section>
          <h4 className="text-xs uppercase text-text-secondary mb-2">Memo</h4>
          <div>{detail.raw_memo}</div>
        </section>
      )}

      {/* Note */}
      <NoteSection transactionId={detail.id} note={detail.note} noteSource={detail.note_source} />

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

      {/* Economic event / Transfer linking */}
      <TransferSection detail={detail} />

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

function flattenCategories(items: CategoryItem[], prefix = ''): { path: string; name: string }[] {
  const result: { path: string; name: string }[] = []
  for (const cat of items) {
    result.push({ path: cat.full_path, name: prefix ? `${prefix} > ${cat.name}` : cat.name })
    if (cat.children.length > 0) {
      result.push(...flattenCategories(cat.children, cat.full_path))
    }
  }
  return result
}

function MerchantSection({ detail }: { detail: TransactionDetail }) {
  const [displayName, setDisplayName] = useState(detail.canonical_merchant_name || '')
  const [editingName, setEditingName] = useState(false)
  const nameMutation = useUpdateMerchantName()
  const categoryMutation = useUpdateTransactionCategory()
  const { data: categoryTree } = useCategories()

  const categoryOptions = useMemo(() => {
    if (!categoryTree) return []
    return flattenCategories(categoryTree.items)
  }, [categoryTree])

  // Reset display name when detail changes
  useEffect(() => {
    if (!editingName) setDisplayName(detail.canonical_merchant_name || '')
  }, [detail.canonical_merchant_id, editingName])

  const handleNameSave = () => {
    if (!detail.canonical_merchant_id) return
    nameMutation.mutate(
      { id: detail.canonical_merchant_id, displayName: displayName.trim() || null },
      { onSuccess: () => setEditingName(false) },
    )
  }

  const handleCategoryChange = (path: string) => {
    categoryMutation.mutate({ id: detail.id, categoryPath: path })
  }

  return (
    <section>
      <h4 className="text-xs uppercase text-text-secondary mb-2">Merchant</h4>
      <div className="space-y-3">
        {/* Display name (editable if canonical merchant exists) */}
        {detail.canonical_merchant_id ? (
          <div>
            <label className="text-text-secondary text-xs block mb-1">Display Name</label>
            {editingName ? (
              <div className="flex gap-2">
                <input
                  type="text"
                  value={displayName}
                  onChange={e => setDisplayName(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter') handleNameSave(); if (e.key === 'Escape') { setDisplayName(detail.canonical_merchant_name || ''); setEditingName(false) } }}
                  placeholder="Set a display name..."
                  className="flex-1 bg-bg-primary border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
                  autoFocus
                />
                <button
                  onClick={handleNameSave}
                  disabled={nameMutation.isPending}
                  className="px-3 py-1 text-xs bg-accent/20 text-accent rounded hover:bg-accent/30 disabled:opacity-50"
                >
                  {nameMutation.isPending ? '...' : 'Save'}
                </button>
                <button
                  onClick={() => { setDisplayName(detail.canonical_merchant_name || ''); setEditingName(false) }}
                  className="text-xs text-text-secondary hover:text-text-primary"
                >
                  Cancel
                </button>
              </div>
            ) : (
              <div className="flex items-center gap-2">
                <span>{detail.canonical_merchant_name || '—'}</span>
                <button
                  onClick={() => setEditingName(true)}
                  className="text-text-secondary hover:text-accent text-xs ml-auto"
                >
                  Edit
                </button>
              </div>
            )}
          </div>
        ) : (
          <div className="text-text-secondary text-xs italic">No merchant linked</div>
        )}

        {/* Category (transaction-level override) */}
        <div>
          <label className="text-text-secondary text-xs block mb-1">
            Category
            {detail.category_is_override && (
              <span className="ml-1.5 text-[10px] bg-accent/20 text-accent px-1.5 py-0.5 rounded">override</span>
            )}
          </label>
          <select
            value={detail.category_path || ''}
            onChange={e => handleCategoryChange(e.target.value)}
            disabled={categoryMutation.isPending}
            className="w-full bg-bg-primary border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent disabled:opacity-50"
          >
            <option value="">-- None --</option>
            {categoryOptions.map(opt => (
              <option key={opt.path} value={opt.path}>{opt.path}</option>
            ))}
          </select>
        </div>

        {/* Raw details */}
        <div className="text-xs text-text-secondary space-y-0.5 pt-1 border-t border-border/50">
          <div>Raw: {detail.raw_merchant || '—'}</div>
          <div>Cleaned: {detail.cleaned_merchant || '—'}</div>
          <div>Match: {detail.merchant_match_type || '—'}</div>
        </div>
      </div>
    </section>
  )
}

function TransferSection({ detail }: { detail: TransactionDetail }) {
  const [linking, setLinking] = useState(false)
  const [counterpartId, setCounterpartId] = useState('')
  const linkMutation = useLinkTransfer()
  const unlinkMutation = useUnlinkEvent()

  const handleLink = () => {
    const id = counterpartId.trim()
    if (!id) return
    linkMutation.mutate(
      { id: detail.id, counterpartId: id },
      { onSuccess: () => { setLinking(false); setCounterpartId('') } },
    )
  }

  const handleUnlink = () => {
    if (!detail.economic_event) return
    unlinkMutation.mutate({ eventId: detail.economic_event.event_id })
  }

  if (detail.economic_event) {
    return (
      <section>
        <div className="flex items-center gap-2 mb-2">
          <h4 className="text-xs uppercase text-text-secondary">Transfer</h4>
          <button
            onClick={handleUnlink}
            disabled={unlinkMutation.isPending}
            className="text-text-secondary hover:text-red-400 text-xs ml-auto disabled:opacity-50"
          >
            {unlinkMutation.isPending ? '...' : 'Unlink'}
          </button>
        </div>
        <div className="mb-2">
          <Badge variant="accent">{detail.economic_event.event_type.replace(/_/g, ' ')}</Badge>
          {detail.economic_event.description && (
            <span className="ml-2 text-text-secondary text-xs">{detail.economic_event.description}</span>
          )}
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
    )
  }

  return (
    <section>
      <div className="flex items-center gap-2 mb-2">
        <h4 className="text-xs uppercase text-text-secondary">Transfer</h4>
        {!linking && (
          <button
            onClick={() => setLinking(true)}
            className="text-text-secondary hover:text-accent text-xs ml-auto"
          >
            + Link as transfer
          </button>
        )}
      </div>
      {linking ? (
        <div className="space-y-2">
          <input
            type="text"
            value={counterpartId}
            onChange={e => setCounterpartId(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter') handleLink(); if (e.key === 'Escape') { setLinking(false); setCounterpartId('') } }}
            placeholder="Counterpart transaction ID..."
            className="w-full bg-bg-primary border border-border rounded-md px-3 py-1.5 text-sm text-text-primary placeholder:text-text-secondary focus:outline-none focus:border-accent font-mono"
            autoFocus
          />
          {linkMutation.isError && (
            <div className="text-red-400 text-xs">{(linkMutation.error as Error).message || 'Failed to link'}</div>
          )}
          <div className="flex gap-2">
            <button
              onClick={handleLink}
              disabled={linkMutation.isPending || !counterpartId.trim()}
              className="text-xs px-3 py-1 bg-accent text-white rounded-md hover:bg-accent-hover disabled:opacity-50"
            >
              {linkMutation.isPending ? 'Linking...' : 'Link'}
            </button>
            <button
              onClick={() => { setLinking(false); setCounterpartId('') }}
              className="text-xs px-3 py-1 text-text-secondary hover:text-text-primary"
            >
              Cancel
            </button>
          </div>
        </div>
      ) : (
        <div className="text-text-secondary text-xs italic">No linked transfer</div>
      )}
    </section>
  )
}

function NoteSection({ transactionId, note, noteSource }: { transactionId: string; note: string | null; noteSource: string | null }) {
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState(note || '')
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const updateNote = useUpdateNote()

  // Reset draft when note changes (e.g. after save)
  useEffect(() => {
    if (!editing) setDraft(note || '')
  }, [note, editing])

  // Focus textarea when entering edit mode
  useEffect(() => {
    if (editing && textareaRef.current) {
      textareaRef.current.focus()
      textareaRef.current.selectionStart = textareaRef.current.value.length
    }
  }, [editing])

  const handleSave = () => {
    updateNote.mutate({ id: transactionId, note: draft }, {
      onSuccess: () => setEditing(false),
    })
  }

  const handleCancel = () => {
    setDraft(note || '')
    setEditing(false)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && e.metaKey) {
      e.preventDefault()
      handleSave()
    }
    if (e.key === 'Escape') {
      handleCancel()
    }
  }

  return (
    <section>
      <div className="flex items-center gap-2 mb-2">
        <h4 className="text-xs uppercase text-text-secondary">Note</h4>
        {noteSource === 'ibank_import' && (
          <span className="text-[10px] text-text-secondary bg-bg-primary px-1.5 py-0.5 rounded">iBank</span>
        )}
        {!editing && (
          <button
            onClick={() => setEditing(true)}
            className="text-text-secondary hover:text-accent text-xs ml-auto"
          >
            {note ? 'Edit' : '+ Add note'}
          </button>
        )}
      </div>

      {editing ? (
        <div className="space-y-2">
          <textarea
            ref={textareaRef}
            value={draft}
            onChange={e => setDraft(e.target.value)}
            onKeyDown={handleKeyDown}
            rows={3}
            className="w-full bg-bg-primary border border-border rounded-md px-3 py-2 text-sm text-text-primary placeholder:text-text-secondary focus:outline-none focus:border-accent resize-y"
            placeholder="Add a note..."
          />
          <div className="flex gap-2 items-center">
            <button
              onClick={handleSave}
              disabled={updateNote.isPending}
              className="text-xs px-3 py-1 bg-accent text-white rounded-md hover:bg-accent-hover disabled:opacity-50"
            >
              {updateNote.isPending ? 'Saving...' : 'Save'}
            </button>
            <button
              onClick={handleCancel}
              className="text-xs px-3 py-1 text-text-secondary hover:text-text-primary"
            >
              Cancel
            </button>
            <span className="text-[10px] text-text-secondary ml-auto">Cmd+Enter to save</span>
          </div>
        </div>
      ) : note ? (
        <div className="text-text-primary whitespace-pre-wrap">{note}</div>
      ) : (
        <div className="text-text-secondary text-xs italic">No note</div>
      )}
    </section>
  )
}
