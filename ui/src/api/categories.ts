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

export function createCategory(name: string, parentId: string | null, categoryType: string) {
  return apiFetch<{ id: string; full_path: string; created: boolean }>('/categories', {
    method: 'POST',
    body: JSON.stringify({ name, parent_id: parentId, category_type: categoryType }),
  })
}

export function renameCategory(id: string, newName: string) {
  return apiFetch<{ id: string; old_path: string; new_path: string; renamed: boolean }>(
    `/categories/${id}/rename`,
    { method: 'PUT', body: JSON.stringify({ new_name: newName }) },
  )
}

export function deleteCategory(id: string, reassignTo: string) {
  return apiFetch<{ id: string; deleted_path: string; reassigned_to: string; merchants_moved: number }>(
    `/categories/${id}`,
    { method: 'DELETE', body: JSON.stringify({ reassign_to: reassignTo }) },
  )
}
