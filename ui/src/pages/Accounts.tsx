import { useMemo } from 'react'
import { Link } from 'react-router-dom'
import { useAccounts } from '../hooks/useAccounts'
import CurrencyAmount from '../components/common/CurrencyAmount'
import Badge from '../components/common/Badge'
import LoadingSpinner from '../components/common/LoadingSpinner'

export default function Accounts() {
  const { data, isLoading } = useAccounts()

  const grouped = useMemo(() => {
    if (!data) return {}
    const groups: Record<string, typeof data.items> = {}
    for (const item of data.items) {
      const key = item.institution
      if (!groups[key]) groups[key] = []
      groups[key].push(item)
    }
    return groups
  }, [data])

  if (isLoading) return <LoadingSpinner />

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold">Accounts</h2>

      {Object.entries(grouped).map(([institution, accounts]) => (
        <div key={institution}>
          <h3 className="text-sm font-medium text-text-secondary uppercase tracking-wider mb-3">{institution}</h3>
          <div className="grid grid-cols-3 gap-4">
            {accounts.map(acct => (
              <Link
                key={`${acct.institution}-${acct.account_ref}`}
                to={`/accounts/${acct.institution}/${acct.account_ref}`}
                className="bg-bg-card border border-border rounded-lg p-4 hover:border-accent/50 transition-colors block"
              >
                <div className="flex justify-between items-start mb-2">
                  <div className="font-medium truncate">{acct.account_name || acct.account_ref}</div>
                  <Badge>{acct.currency}</Badge>
                </div>
                {acct.balance && (
                  <div className="text-lg mb-2">
                    <CurrencyAmount amount={acct.balance} currency={acct.currency} showSign={false} />
                  </div>
                )}
                <div className="text-text-secondary text-xs space-y-0.5">
                  <div>{acct.transaction_count.toLocaleString()} transactions</div>
                  {acct.earliest_date && acct.latest_date && (
                    <div>{acct.earliest_date} — {acct.latest_date}</div>
                  )}
                  {acct.account_type && <Badge variant="default">{acct.account_type}</Badge>}
                </div>
              </Link>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}
