import { useParams, Link } from 'react-router-dom'
import { useAccountDetail } from '../hooks/useAccounts'
import { useScope } from '../contexts/ScopeContext'
import StatCard from '../components/common/StatCard'
import CurrencyAmount from '../components/common/CurrencyAmount'
import Badge from '../components/common/Badge'
import LoadingSpinner from '../components/common/LoadingSpinner'

export default function AccountDetail() {
  const { scope } = useScope()
  const { institution, accountRef } = useParams<{ institution: string; accountRef: string }>()
  const { data, isLoading } = useAccountDetail(institution || '', accountRef || '', scope)

  if (isLoading) return <LoadingSpinner />
  if (!data) return <div className="text-text-secondary">Account not found</div>

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
              <tr key={txn.id} className="border-b border-border/50 hover:bg-bg-hover">
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
    </div>
  )
}
