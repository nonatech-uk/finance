import { apiFetch } from './client'
import type { AccountList, AccountDetailResponse } from './types'

export function fetchAccounts() {
  return apiFetch<AccountList>('/accounts')
}

export function fetchAccountDetail(institution: string, accountRef: string) {
  return apiFetch<AccountDetailResponse>(`/accounts/${institution}/${accountRef}`)
}
