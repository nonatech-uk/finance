import { apiFetch } from './client'
import type { CategoryTree, SpendingReport } from './types'

export function fetchCategories() {
  return apiFetch<CategoryTree>('/categories')
}

export interface SpendingFilters {
  date_from: string
  date_to: string
  currency?: string
  institution?: string
  account_ref?: string
}

export function fetchSpending(filters: SpendingFilters) {
  const params = new URLSearchParams()
  Object.entries(filters).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') params.set(k, String(v))
  })
  return apiFetch<SpendingReport>(`/categories/spending?${params.toString()}`)
}
