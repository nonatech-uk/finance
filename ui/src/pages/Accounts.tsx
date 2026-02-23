import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { useAccounts, useUpdateAccount } from '../hooks/useAccounts'
import CurrencyAmount from '../components/common/CurrencyAmount'
import Badge from '../components/common/Badge'
import LoadingSpinner from '../components/common/LoadingSpinner'
import type { AccountItem } from '../api/types'

function AccountEditModal({
  account,
  onClose,
}: {
  account: AccountItem
  onClose: () => void
}) {
  const updateAccount = useUpdateAccount()
  const [displayName, setDisplayName] = useState(account.display_name || '')
  const [isArchived, setIsArchived] = useState(account.is_archived)
  const [excludeFromReports, setExcludeFromReports] = useState(account.exclude_from_reports)

  const handleSave = () => {
    updateAccount.mutate(
      {
        institution: account.institution,
        accountRef: account.account_ref,
        body: {
          display_name: displayName || null,
          is_archived: isArchived,
          exclude_from_reports: excludeFromReports,
        },
      },
      { onSuccess: onClose },
    )
  }

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50" onClick={onClose}>
      <div
        className="bg-bg-card border border-border rounded-lg p-6 w-96 space-y-4"
        onClick={e => e.stopPropagation()}
      >
        <h3 className="text-lg font-semibold">
          Edit Account
        </h3>
        <p className="text-text-secondary text-sm">
          {account.institution} / {account.account_ref}
        </p>

        <div>
          <label className="block text-sm text-text-secondary mb-1">Display Name</label>
          <input
            type="text"
            value={displayName}
            onChange={e => setDisplayName(e.target.value)}
            placeholder={account.name || account.account_ref}
            className="w-full bg-bg-hover border border-border rounded px-3 py-2 text-sm text-text-primary focus:outline-none focus:border-accent"
          />
        </div>

        <label className="flex items-center gap-2 text-sm cursor-pointer">
          <input
            type="checkbox"
            checked={isArchived}
            onChange={e => setIsArchived(e.target.checked)}
            className="accent-accent"
          />
          <span>Archived</span>
          <span className="text-text-secondary text-xs">(hide from dashboard)</span>
        </label>

        <label className="flex items-center gap-2 text-sm cursor-pointer">
          <input
            type="checkbox"
            checked={excludeFromReports}
            onChange={e => setExcludeFromReports(e.target.checked)}
            className="accent-accent"
          />
          <span>Exclude from reports</span>
          <span className="text-text-secondary text-xs">(category spending)</span>
        </label>

        <div className="flex justify-end gap-2 pt-2">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm rounded border border-border hover:bg-bg-hover transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={updateAccount.isPending}
            className="px-4 py-2 text-sm rounded bg-accent text-white hover:bg-accent/80 transition-colors disabled:opacity-50"
          >
            {updateAccount.isPending ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  )
}

export default function Accounts() {
  const [showArchived, setShowArchived] = useState(false)
  const [editAccount, setEditAccount] = useState<AccountItem | null>(null)
  const { data, isLoading } = useAccounts(showArchived)

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
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Accounts</h2>
        <label className="flex items-center gap-2 text-sm text-text-secondary cursor-pointer">
          <input
            type="checkbox"
            checked={showArchived}
            onChange={e => setShowArchived(e.target.checked)}
            className="accent-accent"
          />
          Show archived
        </label>
      </div>

      {Object.entries(grouped).map(([institution, accounts]) => (
        <div key={institution}>
          <h3 className="text-sm font-medium text-text-secondary uppercase tracking-wider mb-3">{institution}</h3>
          <div className="grid grid-cols-3 gap-4">
            {accounts.map(acct => (
              <div
                key={`${acct.institution}-${acct.account_ref}`}
                className={`bg-bg-card border border-border rounded-lg p-4 transition-colors relative ${
                  acct.is_archived ? 'opacity-50' : 'hover:border-accent/50'
                }`}
              >
                <button
                  onClick={e => {
                    e.preventDefault()
                    setEditAccount(acct)
                  }}
                  className="absolute top-2 right-2 p-1.5 rounded hover:bg-bg-hover text-text-secondary hover:text-text-primary transition-colors"
                  title="Edit account"
                >
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" fill="currentColor" className="w-3.5 h-3.5">
                    <path d="M13.488 2.513a1.75 1.75 0 0 0-2.475 0L6.75 6.774a2.75 2.75 0 0 0-.596.892l-.848 2.047a.75.75 0 0 0 .98.98l2.047-.848a2.75 2.75 0 0 0 .892-.596l4.261-4.262a1.75 1.75 0 0 0 0-2.474Z" />
                    <path d="M4.75 3.5c-.69 0-1.25.56-1.25 1.25v6.5c0 .69.56 1.25 1.25 1.25h6.5c.69 0 1.25-.56 1.25-1.25V9A.75.75 0 0 1 14 9v2.25A2.75 2.75 0 0 1 11.25 14h-6.5A2.75 2.75 0 0 1 2 11.25v-6.5A2.75 2.75 0 0 1 4.75 2H7a.75.75 0 0 1 0 1.5H4.75Z" />
                  </svg>
                </button>

                <Link
                  to={`/accounts/${acct.institution}/${acct.account_ref}`}
                  className="block"
                >
                  <div className="flex justify-between items-start mb-2 pr-6">
                    <div className="font-medium truncate">
                      {acct.display_name || acct.name || acct.account_ref}
                    </div>
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
                    <div className="flex gap-1 flex-wrap">
                      {acct.account_type && <Badge variant="default">{acct.account_type}</Badge>}
                      {acct.is_archived && <Badge variant="warning">archived</Badge>}
                      {acct.exclude_from_reports && <Badge variant="accent">excl. reports</Badge>}
                    </div>
                  </div>
                </Link>
              </div>
            ))}
          </div>
        </div>
      ))}

      {editAccount && (
        <AccountEditModal account={editAccount} onClose={() => setEditAccount(null)} />
      )}
    </div>
  )
}
