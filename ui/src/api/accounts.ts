import { apiFetch } from './client'
import type { AccountList, AccountDetailResponse, AccountUpdate } from './types'

export function fetchAccounts(includeArchived = false) {
  const params = includeArchived ? '?include_archived=true' : ''
  return apiFetch<AccountList>(`/accounts${params}`)
}

export function fetchAccountDetail(institution: string, accountRef: string) {
  return apiFetch<AccountDetailResponse>(`/accounts/${institution}/${accountRef}`)
}

export function updateAccount(institution: string, accountRef: string, body: AccountUpdate) {
  return apiFetch<Record<string, unknown>>(`/accounts/${institution}/${accountRef}`, {
    method: 'PUT',
    body: JSON.stringify(body),
  })
}
