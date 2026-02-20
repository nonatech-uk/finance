import { apiFetch } from './client'
import type { TransactionList, TransactionDetail } from './types'

export interface TransactionFilters {
  cursor?: string
  limit?: number
  institution?: string
  account_ref?: string
  source?: string
  category?: string
  date_from?: string
  date_to?: string
  amount_min?: number
  amount_max?: number
  currency?: string
  search?: string
}

export function fetchTransactions(filters: TransactionFilters = {}) {
  const params = new URLSearchParams()
  Object.entries(filters).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') params.set(k, String(v))
  })
  const qs = params.toString()
  return apiFetch<TransactionList>(`/transactions${qs ? '?' + qs : ''}`)
}

export function fetchTransaction(id: string) {
  return apiFetch<TransactionDetail>(`/transactions/${id}`)
}
