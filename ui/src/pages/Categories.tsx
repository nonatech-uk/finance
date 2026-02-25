import { useState, useMemo } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'
import { useCategories, useSpending, useRenameCategory, useCreateCategory, useDeleteCategory } from '../hooks/useCategories'
import Badge from '../components/common/Badge'
import CurrencyAmount from '../components/common/CurrencyAmount'
import LoadingSpinner from '../components/common/LoadingSpinner'
import type { CategoryItem } from '../api/types'

function sixMonthsAgo() {
  const d = new Date()
  d.setMonth(d.getMonth() - 6)
  return d.toISOString().slice(0, 10)
}
function today() {
  return new Date().toISOString().slice(0, 10)
}

// Flatten tree into a list for dropdowns
function flattenTree(items: CategoryItem[], prefix = ''): { id: string; path: string }[] {
  const result: { id: string; path: string }[] = []
  for (const cat of items) {
    result.push({ id: cat.id, path: cat.full_path })
    if (cat.children.length > 0) {
      result.push(...flattenTree(cat.children, cat.full_path))
    }
  }
  return result
}

export default function Categories() {
  const { data: tree, isLoading: treeLoading } = useCategories()
  const [dateFrom, setDateFrom] = useState(sixMonthsAgo())
  const [dateTo, setDateTo] = useState(today())
  const [currency, setCurrency] = useState('GBP')
  const [selectedId, setSelectedId] = useState<string | null>(null)

  const { data: spending, isLoading: spendingLoading } = useSpending({ date_from: dateFrom, date_to: dateTo, currency })

  const chartData = useMemo(() => {
    if (!spending) return []
    return spending.items
      .filter(s => parseFloat(s.total) < 0 && s.category_path !== 'Uncategorised' && s.category_path !== 'Ignore')
      .slice(0, 15)
      .map(s => ({
        name: s.category_name,
        amount: Math.abs(parseFloat(s.total)),
        count: s.transaction_count,
      }))
  }, [spending])

  // Find selected category from tree
  const flatCats = useMemo(() => tree ? flattenTree(tree.items) : [], [tree])
  const findCat = (items: CategoryItem[], id: string): CategoryItem | null => {
    for (const cat of items) {
      if (cat.id === id) return cat
      const found = findCat(cat.children, id)
      if (found) return found
    }
    return null
  }
  const selectedCat = selectedId && tree ? findCat(tree.items, selectedId) : null

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold">Categories</h2>

      <div className="grid grid-cols-[minmax(280px,1fr)_2fr] gap-6">
        {/* Tree + management panel */}
        <div className="space-y-4">
          <div className="bg-bg-card border border-border rounded-lg p-4 overflow-auto max-h-[calc(100vh-16rem)]">
            <h3 className="text-sm font-medium text-text-secondary mb-3">Category Tree</h3>
            {treeLoading ? <LoadingSpinner /> : (
              <div className="space-y-0.5">
                {tree?.items.map(cat => (
                  <CategoryNode
                    key={cat.id}
                    cat={cat}
                    depth={0}
                    selectedId={selectedId}
                    onSelect={setSelectedId}
                  />
                ))}
              </div>
            )}
          </div>

          {/* Management panel */}
          {selectedCat && (
            <CategoryManagePanel
              cat={selectedCat}
              allCategories={flatCats}
              onDeselect={() => setSelectedId(null)}
            />
          )}

          <CreateCategoryPanel
            allCategories={flatCats}
          />
        </div>

        {/* Spending report */}
        <div className="space-y-4">
          <div className="flex gap-3 items-center">
            <input type="date" value={dateFrom} onChange={e => setDateFrom(e.target.value)} className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary focus:outline-none focus:border-accent" />
            <span className="text-text-secondary">to</span>
            <input type="date" value={dateTo} onChange={e => setDateTo(e.target.value)} className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary focus:outline-none focus:border-accent" />
            <select value={currency} onChange={e => setCurrency(e.target.value)} className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary focus:outline-none focus:border-accent">
              {['GBP', 'CHF', 'EUR', 'USD'].map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>

          {spending && (
            <div className="flex gap-4">
              <div className="bg-bg-card border border-border rounded-lg p-4 flex-1">
                <div className="text-text-secondary text-xs mb-1">Total Income</div>
                <CurrencyAmount amount={spending.total_income} currency={currency} className="text-lg" />
              </div>
              <div className="bg-bg-card border border-border rounded-lg p-4 flex-1">
                <div className="text-text-secondary text-xs mb-1">Total Expense</div>
                <CurrencyAmount amount={spending.total_expense} currency={currency} className="text-lg" />
              </div>
            </div>
          )}

          <div className="bg-bg-card border border-border rounded-lg p-5">
            <h3 className="text-sm font-medium text-text-secondary mb-4">Top Spending by Category</h3>
            {spendingLoading ? <LoadingSpinner /> : (
              <ResponsiveContainer width="100%" height={400}>
                <BarChart data={chartData} layout="vertical" margin={{ left: 100 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e4e8" horizontal={false} />
                  <XAxis type="number" tick={{ fill: '#6b7280', fontSize: 11 }} tickFormatter={v => `£${(v / 1000).toFixed(0)}k`} />
                  <YAxis type="category" dataKey="name" tick={{ fill: '#6b7280', fontSize: 11 }} width={100} />
                  <Tooltip
                    contentStyle={{ background: '#ffffff', border: '1px solid #e2e4e8', borderRadius: 8, color: '#1a1a2e' }}
                    formatter={(value: number) => [`£${value.toLocaleString('en-GB', { minimumFractionDigits: 2 })}`, 'Spent']}
                  />
                  <Bar dataKey="amount" fill="#dc2626" radius={[0, 2, 2, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>

          {/* Spending table */}
          {spending && (
            <div className="bg-bg-card border border-border rounded-lg p-4">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-text-secondary text-left text-xs uppercase tracking-wider">
                    <th className="pb-2">Category</th>
                    <th className="pb-2 text-right">Total</th>
                    <th className="pb-2 text-right">Transactions</th>
                  </tr>
                </thead>
                <tbody>
                  {spending.items.map(s => (
                    <tr key={s.category_path} className="border-b border-border/50">
                      <td className="py-1.5">{s.category_path}</td>
                      <td className="py-1.5 text-right"><CurrencyAmount amount={s.total} currency={currency} /></td>
                      <td className="py-1.5 text-right text-text-secondary">{s.transaction_count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Category Tree Node ──

function CategoryNode({
  cat, depth, selectedId, onSelect,
}: {
  cat: CategoryItem
  depth: number
  selectedId: string | null
  onSelect: (id: string | null) => void
}) {
  const [expanded, setExpanded] = useState(depth < 1)
  const hasChildren = cat.children.length > 0
  const isSelected = cat.id === selectedId

  return (
    <div>
      <div
        className={`flex items-center gap-1.5 py-0.5 rounded px-1 cursor-pointer text-sm ${
          isSelected ? 'bg-accent/15 text-accent' : 'hover:bg-bg-hover'
        }`}
        style={{ paddingLeft: depth * 16 }}
        onClick={() => {
          if (isSelected) {
            onSelect(null)
          } else {
            onSelect(cat.id)
          }
        }}
      >
        <span
          className="text-text-secondary text-xs w-4"
          onClick={e => {
            if (hasChildren) {
              e.stopPropagation()
              setExpanded(!expanded)
            }
          }}
        >
          {hasChildren ? (expanded ? '▼' : '▶') : '·'}
        </span>
        <span>{cat.name}</span>
        {cat.category_type && (
          <Badge variant={cat.category_type === 'income' ? 'income' : 'expense'} className="ml-auto text-[10px]">
            {cat.category_type}
          </Badge>
        )}
      </div>
      {expanded && cat.children.map(child => (
        <CategoryNode key={child.id} cat={child} depth={depth + 1} selectedId={selectedId} onSelect={onSelect} />
      ))}
    </div>
  )
}

// ── Category Management Panel (Rename + Delete) ──

function CategoryManagePanel({
  cat, allCategories, onDeselect,
}: {
  cat: CategoryItem
  allCategories: { id: string; path: string }[]
  onDeselect: () => void
}) {
  const [renameName, setRenameName] = useState(cat.name)
  const [reassignTo, setReassignTo] = useState('')
  const [confirmingDelete, setConfirmingDelete] = useState(false)
  const renameMutation = useRenameCategory()
  const deleteMutation = useDeleteCategory()

  // Reset rename field when selection changes
  const [prevId, setPrevId] = useState(cat.id)
  if (cat.id !== prevId) {
    setPrevId(cat.id)
    setRenameName(cat.name)
    setReassignTo('')
    setConfirmingDelete(false)
  }

  const handleRename = () => {
    if (!renameName.trim() || renameName === cat.name) return
    renameMutation.mutate({ id: cat.id, newName: renameName.trim() })
  }

  const handleDelete = () => {
    if (!reassignTo) return
    if (!confirmingDelete) {
      setConfirmingDelete(true)
      return
    }
    deleteMutation.mutate({ id: cat.id, reassignTo }, {
      onSuccess: () => { setConfirmingDelete(false); onDeselect() },
    })
  }

  return (
    <div className="bg-bg-card border border-accent/30 rounded-lg p-4 space-y-4">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-semibold text-accent truncate">{cat.full_path}</h4>
        <button onClick={onDeselect} className="text-text-secondary hover:text-text-primary text-xs">close</button>
      </div>

      {/* Rename */}
      <div className="space-y-1.5">
        <label className="text-xs text-text-secondary uppercase tracking-wider">Rename</label>
        <div className="flex gap-2">
          <input
            type="text"
            value={renameName}
            onChange={e => setRenameName(e.target.value)}
            className="flex-1 bg-bg-primary border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
            onKeyDown={e => e.key === 'Enter' && handleRename()}
          />
          <button
            onClick={handleRename}
            disabled={renameMutation.isPending || !renameName.trim() || renameName === cat.name}
            className="px-3 py-1 text-xs bg-accent/20 text-accent rounded hover:bg-accent/30 disabled:opacity-50"
          >
            {renameMutation.isPending ? 'Saving...' : 'Save'}
          </button>
        </div>
        {renameMutation.isSuccess && renameMutation.data.renamed && (
          <p className="text-xs text-green-400">Renamed to {renameMutation.data.new_path}</p>
        )}
      </div>

      {/* Delete */}
      <div className="space-y-1.5">
        <label className="text-xs text-text-secondary uppercase tracking-wider">Delete &amp; reassign to</label>
        <select
          value={reassignTo}
          onChange={e => setReassignTo(e.target.value)}
          className="w-full bg-bg-primary border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
        >
          <option value="">-- Select target --</option>
          {allCategories
            .filter(c => c.id !== cat.id && !c.path.startsWith(cat.full_path + ':'))
            .map(c => (
              <option key={c.id} value={c.id}>{c.path}</option>
            ))}
        </select>
        <div className="flex items-center gap-2">
          <button
            onClick={handleDelete}
            disabled={deleteMutation.isPending || !reassignTo}
            className="px-3 py-1 text-xs bg-red-600/20 text-red-400 rounded hover:bg-red-600/30 disabled:opacity-50"
          >
            {deleteMutation.isPending ? 'Deleting...' : confirmingDelete ? 'Confirm Delete' : 'Delete'}
          </button>
          {confirmingDelete && (
            <button
              onClick={() => setConfirmingDelete(false)}
              className="px-2 py-1 text-xs text-text-secondary hover:text-text-primary"
            >
              Cancel
            </button>
          )}
        </div>
        {deleteMutation.isSuccess && (
          <p className="text-xs text-green-400">
            Deleted. {deleteMutation.data.merchants_moved} merchants reassigned.
          </p>
        )}
      </div>
    </div>
  )
}

// ── Create Category Panel ──

function CreateCategoryPanel({
  allCategories,
}: {
  allCategories: { id: string; path: string }[]
}) {
  const [expanded, setExpanded] = useState(false)
  const [name, setName] = useState('')
  const [parentId, setParentId] = useState<string>('')
  const [categoryType, setCategoryType] = useState<string>('expense')
  const createMutation = useCreateCategory()

  const handleCreate = () => {
    if (!name.trim()) return
    createMutation.mutate(
      { name: name.trim(), parentId: parentId || null, categoryType },
      {
        onSuccess: () => {
          setName('')
          setParentId('')
        },
      },
    )
  }

  return (
    <div className="bg-bg-card border border-border rounded-lg p-4 space-y-3">
      <button
        onClick={() => setExpanded(!expanded)}
        className="text-sm font-semibold text-text-primary hover:text-accent flex items-center gap-1"
      >
        <span className="text-xs">{expanded ? '▼' : '▶'}</span>
        New Category
      </button>

      {expanded && (
        <div className="space-y-2">
          <div>
            <label className="text-xs text-text-secondary block mb-0.5">Name</label>
            <input
              type="text"
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="Category name..."
              className="w-full bg-bg-primary border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
              onKeyDown={e => e.key === 'Enter' && handleCreate()}
            />
          </div>
          <div>
            <label className="text-xs text-text-secondary block mb-0.5">Parent (optional)</label>
            <select
              value={parentId}
              onChange={e => setParentId(e.target.value)}
              className="w-full bg-bg-primary border border-border rounded px-2 py-1 text-sm text-text-primary focus:outline-none focus:border-accent"
            >
              <option value="">-- Root level --</option>
              {allCategories.map(c => (
                <option key={c.id} value={c.id}>{c.path}</option>
              ))}
            </select>
          </div>
          <div className="flex items-center gap-4">
            <label className="text-xs text-text-secondary">Type:</label>
            <label className="flex items-center gap-1.5 text-sm cursor-pointer">
              <input
                type="radio"
                name="catType"
                value="expense"
                checked={categoryType === 'expense'}
                onChange={() => setCategoryType('expense')}
                className="accent-accent"
              />
              Expense
            </label>
            <label className="flex items-center gap-1.5 text-sm cursor-pointer">
              <input
                type="radio"
                name="catType"
                value="income"
                checked={categoryType === 'income'}
                onChange={() => setCategoryType('income')}
                className="accent-accent"
              />
              Income
            </label>
          </div>
          <button
            onClick={handleCreate}
            disabled={createMutation.isPending || !name.trim()}
            className="px-3 py-1 text-xs bg-accent text-white rounded hover:bg-accent-hover disabled:opacity-50"
          >
            {createMutation.isPending ? 'Creating...' : 'Create'}
          </button>
          {createMutation.isSuccess && (
            <p className="text-xs text-green-400">Created: {createMutation.data.full_path}</p>
          )}
        </div>
      )}
    </div>
  )
}
