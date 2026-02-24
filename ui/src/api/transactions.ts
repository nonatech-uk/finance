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

export function updateTransactionNote(id: string, note: string) {
  return apiFetch<{ ok: boolean }>(`/transactions/${id}/note`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ note }),
  })
}

export function updateTransactionCategory(id: string, categoryPath: string) {
  return apiFetch<{ ok: boolean }>(`/transactions/${id}/category`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ category_path: categoryPath }),
  })
}

export function linkTransfer(id: string, counterpartId: string) {
  return apiFetch<{ ok: boolean; event_id: string }>(`/transactions/${id}/link-transfer`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ counterpart_id: counterpartId }),
  })
}

export function unlinkEvent(eventId: string) {
  return apiFetch<{ ok: boolean }>(`/economic-events/${eventId}`, {
    method: 'DELETE',
  })
}
