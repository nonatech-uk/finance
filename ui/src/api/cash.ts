import { apiFetch } from './client'

export interface CashTransactionCreate {
  account_ref: string
  posted_at: string
  amount: number
  description: string
  category_path?: string | null
  tags?: string[]
  note?: string | null
}

export interface CashBalanceReset {
  target_balance: number
  posted_at: string
}

export interface CashResponse {
  ok: boolean
  transaction_id?: string
  adjustment?: string
  new_balance?: string
}

export function createCashTransaction(body: CashTransactionCreate) {
  return apiFetch<CashResponse>('/cash/transactions', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function resetCashBalance(accountRef: string, body: CashBalanceReset) {
  return apiFetch<CashResponse>(`/cash/${accountRef}/reset-balance`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
}
