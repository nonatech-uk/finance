import { apiFetch } from './client'
import type { MerchantList } from './types'

export interface MerchantFilters {
  search?: string
  unmapped?: boolean
  cursor?: string
  limit?: number
}

export function fetchMerchants(filters: MerchantFilters = {}) {
  const params = new URLSearchParams()
  Object.entries(filters).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '' && v !== false)
      params.set(k, String(v))
  })
  const qs = params.toString()
  return apiFetch<MerchantList>(`/merchants${qs ? '?' + qs : ''}`)
}

export function updateMerchantMapping(id: string, categoryHint: string | null) {
  return apiFetch<{ id: string; name: string; category_hint: string | null; updated: boolean }>(
    `/merchants/${id}/mapping`,
    { method: 'PUT', body: JSON.stringify({ category_hint: categoryHint }) }
  )
}
