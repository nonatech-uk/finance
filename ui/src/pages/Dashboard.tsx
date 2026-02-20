import { useMemo } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'
import { useOverview, useMonthly } from '../hooks/useStats'
import { useSpending } from '../hooks/useCategories'
import StatCard from '../components/common/StatCard'
import LoadingSpinner from '../components/common/LoadingSpinner'

function threeMonthsAgo() {
  const d = new Date()
  d.setMonth(d.getMonth() - 3)
  return d.toISOString().slice(0, 10)
}

function today() {
  return new Date().toISOString().slice(0, 10)
}

export default function Dashboard() {
  const { data: overview, isLoading: overviewLoading } = useOverview()
  const { data: monthly, isLoading: monthlyLoading } = useMonthly({ months: 12, currency: 'GBP' })
  const { data: spending } = useSpending({ date_from: threeMonthsAgo(), date_to: today(), currency: 'GBP' })

  const chartData = useMemo(() => {
    if (!monthly) return []
    return [...monthly.items].reverse().map(m => ({
      month: m.month,
      income: parseFloat(m.income),
      expense: Math.abs(parseFloat(m.expense)),
      net: parseFloat(m.net),
    }))
  }, [monthly])

  const topSpending = useMemo(() => {
    if (!spending) return []
    return spending.items
      .filter(s => parseFloat(s.total) < 0 && s.category_path !== 'Uncategorised' && s.category_path !== 'Ignore')
      .slice(0, 12)
      .map(s => ({
        name: s.category_name,
        amount: Math.abs(parseFloat(s.total)),
      }))
  }, [spending])

  if (overviewLoading) return <LoadingSpinner />

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold">Dashboard</h2>

      {overview && (
        <div className="grid grid-cols-4 gap-4">
          <StatCard label="Active Transactions" value={overview.active_transactions.toLocaleString()} subtitle={`${overview.total_raw_transactions.toLocaleString()} raw`} />
          <StatCard label="Accounts" value={overview.active_accounts} subtitle={`${overview.total_accounts} total`} />
          <StatCard label="Dedup Savings" value={overview.removed_by_dedup.toLocaleString()} subtitle={`${overview.dedup_groups} groups`} />
          <StatCard label="Category Coverage" value={`${overview.category_coverage_pct}%`} subtitle={`${overview.date_range_from} — ${overview.date_range_to}`} />
        </div>
      )}

      <div className="grid grid-cols-2 gap-6">
        {/* Monthly Chart */}
        <div className="bg-bg-card border border-border rounded-lg p-5">
          <h3 className="text-sm font-medium text-text-secondary mb-4">Monthly Income / Expense (GBP)</h3>
          {monthlyLoading ? <LoadingSpinner /> : (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#2a2a3e" />
                <XAxis dataKey="month" tick={{ fill: '#8888a0', fontSize: 11 }} tickLine={false} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 11 }} tickLine={false} tickFormatter={v => `£${(v / 1000).toFixed(0)}k`} />
                <Tooltip
                  contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a3e', borderRadius: 8, color: '#e4e4ef' }}
                  formatter={(value: number) => [`£${value.toLocaleString('en-GB', { minimumFractionDigits: 0 })}`, '']}
                />
                <Bar dataKey="income" fill="#22c55e" radius={[2, 2, 0, 0]} />
                <Bar dataKey="expense" fill="#ef4444" radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Top Spending */}
        <div className="bg-bg-card border border-border rounded-lg p-5">
          <h3 className="text-sm font-medium text-text-secondary mb-4">Top Spending (Last 3 Months, GBP)</h3>
          {topSpending.length === 0 ? <LoadingSpinner /> : (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={topSpending} layout="vertical" margin={{ left: 80 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#2a2a3e" horizontal={false} />
                <XAxis type="number" tick={{ fill: '#8888a0', fontSize: 11 }} tickFormatter={v => `£${(v / 1000).toFixed(0)}k`} />
                <YAxis type="category" dataKey="name" tick={{ fill: '#8888a0', fontSize: 11 }} width={80} />
                <Tooltip
                  contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a3e', borderRadius: 8, color: '#e4e4ef' }}
                  formatter={(value: number) => [`£${value.toLocaleString('en-GB', { minimumFractionDigits: 2 })}`, 'Spent']}
                />
                <Bar dataKey="amount" fill="#ef4444" radius={[0, 2, 2, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>
    </div>
  )
}
