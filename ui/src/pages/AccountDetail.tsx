import { useState, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useAccountDetail } from '../hooks/useAccounts'
import { useTransaction } from '../hooks/useTransactions'
import { useScope } from '../contexts/ScopeContext'
import { useCategories } from '../hooks/useCategories'
import { useCreateCashTransaction, useResetCashBalance } from '../hooks/useCash'
import { TransactionDetailContent } from './Transactions'
import StatCard from '../components/common/StatCard'
import CurrencyAmount from '../components/common/CurrencyAmount'
import Badge from '../components/common/Badge'
import LoadingSpinner from '../components/common/LoadingSpinner'
import type { CategoryItem } from '../api/types'

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

function today() {
  return new Date().toISOString().slice(0, 10)
}

function AddTransactionModal({
  accountRef,
  onClose,
}: {
  accountRef: string
  onClose: () => void
}) {
  const [date, setDate] = useState(today())
  const [amount, setAmount] = useState('')
  const [description, setDescription] = useState('')
  const [categoryPath, setCategoryPath] = useState('')
  const [tagInput, setTagInput] = useState('')
  const [tags, setTags] = useState<string[]>([])
  const [note, setNote] = useState('')
  const createTxn = useCreateCashTransaction()
  const { data: categoryTree } = useCategories()

  const categoryOptions = useMemo(() => {
    if (!categoryTree) return []
    return flattenCategories(categoryTree.items)
  }, [categoryTree])

  const handleAddTag = () => {
    const t = tagInput.trim()
    if (t && !tags.includes(t)) {
      setTags([...tags, t])
      setTagInput('')
    }
  }

  const handleTagKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      handleAddTag()
    }
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const numAmount = parseFloat(amount)
    if (isNaN(numAmount) || !description.trim()) return
    createTxn.mutate(
      {
        account_ref: accountRef,
        posted_at: date,
        amount: numAmount,
        description: description.trim(),
        category_path: categoryPath || null,
        tags: tags.length > 0 ? tags : undefined,
        note: note.trim() || null,
      },
      { onSuccess: () => onClose() },
    )
  }

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div
        className="bg-bg-card border border-border rounded-lg p-6 w-full max-w-md space-y-4 max-h-[90vh] overflow-y-auto"
        onClick={e => e.stopPropagation()}
      >
        <h3 className="text-lg font-semibold text-text-primary">Add Cash Transaction</h3>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm text-text-secondary mb-1">Date</label>
            <input
              type="date"
              value={date}
              onChange={e => setDate(e.target.value)}
              className="w-full px-3 py-1.5 text-sm bg-bg-secondary border border-border rounded-md text-text-primary focus:outline-none focus:ring-1 focus:ring-accent"
            />
          </div>
          <div>
            <label className="block text-sm text-text-secondary mb-1">Amount</label>
            <input
              type="number"
              step="0.01"
              value={amount}
              onChange={e => setAmount(e.target.value)}
              placeholder="-15.50 (negative = spending)"
              className="w-full px-3 py-1.5 text-sm bg-bg-secondary border border-border rounded-md text-text-primary placeholder:text-text-secondary/50 focus:outline-none focus:ring-1 focus:ring-accent"
            />
            <p className="text-xs text-text-secondary mt-1">
              Use negative for spending, positive for deposits.
            </p>
          </div>
          <div>
            <label className="block text-sm text-text-secondary mb-1">Description</label>
            <input
              type="text"
              value={description}
              onChange={e => setDescription(e.target.value)}
              placeholder="Coffee, groceries, etc."
              className="w-full px-3 py-1.5 text-sm bg-bg-secondary border border-border rounded-md text-text-primary placeholder:text-text-secondary/50 focus:outline-none focus:ring-1 focus:ring-accent"
            />
          </div>
          <div>
            <label className="block text-sm text-text-secondary mb-1">Category</label>
            <select
              value={categoryPath}
              onChange={e => setCategoryPath(e.target.value)}
              className="w-full px-3 py-1.5 text-sm bg-bg-secondary border border-border rounded-md text-text-primary focus:outline-none focus:ring-1 focus:ring-accent"
            >
              <option value="">-- None --</option>
              {categoryOptions.map(opt => (
                <option key={opt.path} value={opt.path}>{opt.path}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm text-text-secondary mb-1">Tags</label>
            <div className="flex gap-2">
              <input
                type="text"
                value={tagInput}
                onChange={e => setTagInput(e.target.value)}
                onKeyDown={handleTagKeyDown}
                placeholder="Add tag..."
                className="flex-1 px-3 py-1.5 text-sm bg-bg-secondary border border-border rounded-md text-text-primary placeholder:text-text-secondary/50 focus:outline-none focus:ring-1 focus:ring-accent"
              />
              <button
                type="button"
                onClick={handleAddTag}
                disabled={!tagInput.trim()}
                className="px-3 py-1.5 text-sm font-medium rounded-md border border-border text-text-primary hover:bg-bg-hover disabled:opacity-50 transition-colors"
              >
                Add
              </button>
            </div>
            {tags.length > 0 && (
              <div className="flex flex-wrap gap-1.5 mt-2">
                {tags.map(t => (
                  <span key={t} className="inline-flex items-center gap-1 px-2 py-0.5 text-xs bg-accent/15 text-accent rounded-full">
                    {t}
                    <button
                      type="button"
                      onClick={() => setTags(tags.filter(x => x !== t))}
                      className="hover:text-red-400"
                    >
                      &times;
                    </button>
                  </span>
                ))}
              </div>
            )}
          </div>
          <div>
            <label className="block text-sm text-text-secondary mb-1">Note</label>
            <textarea
              value={note}
              onChange={e => setNote(e.target.value)}
              placeholder="Optional note..."
              rows={2}
              className="w-full px-3 py-1.5 text-sm bg-bg-secondary border border-border rounded-md text-text-primary placeholder:text-text-secondary/50 focus:outline-none focus:ring-1 focus:ring-accent resize-none"
            />
          </div>
          <div className="flex items-center gap-3 pt-1">
            <button
              type="submit"
              disabled={createTxn.isPending || !amount || !description.trim()}
              className="px-4 py-1.5 text-sm font-medium rounded-md bg-accent text-white hover:bg-accent/90 disabled:opacity-50 transition-colors"
            >
              {createTxn.isPending ? 'Adding...' : 'Add Transaction'}
            </button>
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-1.5 text-sm font-medium rounded-md text-text-secondary hover:text-text-primary transition-colors"
            >
              Cancel
            </button>
            {createTxn.isError && (
              <span className="text-sm text-red-400">
                {createTxn.error?.message || 'Failed'}
              </span>
            )}
          </div>
        </form>
      </div>
    </div>
  )
}

function ResetBalanceModal({
  accountRef,
  currentBalance,
  currency,
  onClose,
}: {
  accountRef: string
  currentBalance: number
  currency: string
  onClose: () => void
}) {
  const [date, setDate] = useState(today())
  const [targetBalance, setTargetBalance] = useState('')
  const resetBalance = useResetCashBalance()
  const [result, setResult] = useState<{ adjustment: string; new_balance: string } | null>(null)

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const target = parseFloat(targetBalance)
    if (isNaN(target)) return
    resetBalance.mutate(
      {
        accountRef,
        body: { target_balance: target, posted_at: date },
      },
      {
        onSuccess: (data) => {
          if (data.adjustment === '0.00') {
            onClose()
          } else {
            setResult({ adjustment: data.adjustment!, new_balance: data.new_balance! })
          }
        },
      },
    )
  }

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div
        className="bg-bg-card border border-border rounded-lg p-6 w-full max-w-md space-y-4"
        onClick={e => e.stopPropagation()}
      >
        <h3 className="text-lg font-semibold text-text-primary">Reset Balance</h3>
        <p className="text-sm text-text-secondary">
          Current balance: <span className="text-text-primary font-medium">{currency} {currentBalance.toLocaleString('en-GB', { minimumFractionDigits: 2 })}</span>
        </p>
        {result ? (
          <div className="space-y-3">
            <p className="text-sm text-green-400">
              Balance reset. Adjustment: {currency} {Number(result.adjustment).toLocaleString('en-GB', { minimumFractionDigits: 2 })}
            </p>
            <button
              onClick={onClose}
              className="px-4 py-1.5 text-sm font-medium rounded-md bg-accent text-white hover:bg-accent/90 transition-colors"
            >
              Done
            </button>
          </div>
        ) : (
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label className="block text-sm text-text-secondary mb-1">Date</label>
              <input
                type="date"
                value={date}
                onChange={e => setDate(e.target.value)}
                className="w-full px-3 py-1.5 text-sm bg-bg-secondary border border-border rounded-md text-text-primary focus:outline-none focus:ring-1 focus:ring-accent"
              />
            </div>
            <div>
              <label className="block text-sm text-text-secondary mb-1">Target balance</label>
              <input
                type="number"
                step="0.01"
                value={targetBalance}
                onChange={e => setTargetBalance(e.target.value)}
                placeholder={`e.g. ${currentBalance.toFixed(0)}`}
                className="w-full px-3 py-1.5 text-sm bg-bg-secondary border border-border rounded-md text-text-primary placeholder:text-text-secondary/50 focus:outline-none focus:ring-1 focus:ring-accent"
              />
              <p className="text-xs text-text-secondary mt-1">
                A synthetic adjustment transaction will be created to match.
              </p>
            </div>
            <div className="flex items-center gap-3 pt-1">
              <button
                type="submit"
                disabled={resetBalance.isPending || !targetBalance}
                className="px-4 py-1.5 text-sm font-medium rounded-md bg-accent text-white hover:bg-accent/90 disabled:opacity-50 transition-colors"
              >
                {resetBalance.isPending ? 'Resetting...' : 'Reset Balance'}
              </button>
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-1.5 text-sm font-medium rounded-md text-text-secondary hover:text-text-primary transition-colors"
              >
                Cancel
              </button>
              {resetBalance.isError && (
                <span className="text-sm text-red-400">
                  {resetBalance.error?.message || 'Failed'}
                </span>
              )}
            </div>
          </form>
        )}
      </div>
    </div>
  )
}

export default function AccountDetail() {
  const { scope } = useScope()
  const { institution, accountRef } = useParams<{ institution: string; accountRef: string }>()
  const { data, isLoading } = useAccountDetail(institution || '', accountRef || '', scope)
  const [showAddTxn, setShowAddTxn] = useState(false)
  const [showResetBalance, setShowResetBalance] = useState(false)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const { data: detail, isLoading: detailLoading } = useTransaction(selectedId)

  if (isLoading) return <LoadingSpinner />
  if (!data) return <div className="text-text-secondary">Account not found</div>

  const isCash = institution === 'cash'

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Link to="/accounts" className="text-text-secondary hover:text-text-primary">&larr;</Link>
        <h2 className="text-xl font-semibold">
          {data.summary.display_name || data.summary.account_name || `${institution} / ${accountRef}`}
        </h2>
      </div>

      <div className="grid grid-cols-4 gap-4">
        <StatCard label="Balance" value={`${data.summary.currency} ${Number(data.summary.balance).toLocaleString('en-GB', { minimumFractionDigits: 2 })}`} />
        <StatCard label="Transactions" value={data.summary.transaction_count.toLocaleString()} />
        <StatCard label="Since" value={data.summary.earliest_date} />
        <StatCard label="Latest" value={data.summary.latest_date} />
      </div>

      {isCash && (
        <div className="flex gap-3">
          <button
            onClick={() => setShowAddTxn(true)}
            className="px-4 py-1.5 text-sm font-medium rounded-md bg-accent text-white hover:bg-accent/90 transition-colors"
          >
            Add Transaction
          </button>
          <button
            onClick={() => setShowResetBalance(true)}
            className="px-4 py-1.5 text-sm font-medium rounded-md border border-border text-text-primary hover:bg-bg-hover transition-colors"
          >
            Reset Balance
          </button>
        </div>
      )}

      <div>
        <h3 className="text-sm font-medium text-text-secondary mb-3">Recent Transactions</h3>
        <table className="w-full text-sm">
          <thead>
            <tr className="text-text-secondary text-left text-xs uppercase tracking-wider">
              <th className="pb-2 pr-4">Date</th>
              <th className="pb-2 pr-4">Merchant</th>
              <th className="pb-2 pr-4">Category</th>
              <th className="pb-2 pr-4 text-right">Amount</th>
              <th className="pb-2">Source</th>
            </tr>
          </thead>
          <tbody>
            {data.recent_transactions.map(txn => (
              <tr
                key={txn.id}
                className={`border-b border-border/50 hover:bg-bg-hover cursor-pointer ${txn.id === selectedId ? 'bg-bg-hover' : ''}`}
                onClick={() => setSelectedId(txn.id === selectedId ? null : txn.id)}
              >
                <td className="py-2 pr-4 text-text-secondary whitespace-nowrap">{txn.posted_at}</td>
                <td className="py-2 pr-4 truncate max-w-[250px]">{txn.canonical_merchant_name || txn.cleaned_merchant || txn.raw_merchant || '—'}</td>
                <td className="py-2 pr-4">{txn.category_name ? <Badge variant="accent">{txn.category_name}</Badge> : '—'}</td>
                <td className="py-2 pr-4 text-right"><CurrencyAmount amount={txn.amount} currency={txn.currency} showSign={false} /></td>
                <td className="py-2"><Badge>{txn.source}</Badge></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {showAddTxn && accountRef && (
        <AddTransactionModal
          accountRef={accountRef}
          onClose={() => setShowAddTxn(false)}
        />
      )}

      {showResetBalance && accountRef && (
        <ResetBalanceModal
          accountRef={accountRef}
          currentBalance={Number(data.summary.balance)}
          currency={data.summary.currency}
          onClose={() => setShowResetBalance(false)}
        />
      )}

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
