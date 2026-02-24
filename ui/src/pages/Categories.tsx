import { useState, useMemo } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'
import { useCategories, useSpending } from '../hooks/useCategories'
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

export default function Categories() {
  const { data: tree, isLoading: treeLoading } = useCategories()
  const [dateFrom, setDateFrom] = useState(sixMonthsAgo())
  const [dateTo, setDateTo] = useState(today())
  const [currency, setCurrency] = useState('GBP')

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

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold">Categories</h2>

      <div className="grid grid-cols-[minmax(280px,1fr)_2fr] gap-6">
        {/* Tree */}
        <div className="bg-bg-card border border-border rounded-lg p-4 overflow-auto max-h-[calc(100vh-8rem)]">
          <h3 className="text-sm font-medium text-text-secondary mb-3">Category Tree</h3>
          {treeLoading ? <LoadingSpinner /> : (
            <div className="space-y-0.5">
              {tree?.items.map(cat => <CategoryNode key={cat.id} cat={cat} depth={0} />)}
            </div>
          )}
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

function CategoryNode({ cat, depth }: { cat: CategoryItem; depth: number }) {
  const [expanded, setExpanded] = useState(depth < 1)
  const hasChildren = cat.children.length > 0

  return (
    <div>
      <div
        className="flex items-center gap-1.5 py-0.5 hover:bg-bg-hover rounded px-1 cursor-pointer text-sm"
        style={{ paddingLeft: depth * 16 }}
        onClick={() => hasChildren && setExpanded(!expanded)}
      >
        <span className="text-text-secondary text-xs w-4">
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
        <CategoryNode key={child.id} cat={child} depth={depth + 1} />
      ))}
    </div>
  )
}
