import { apiFetch } from './client'
import type { MerchantList, MerchantDetail, CategorySuggestionList, DisplayRuleList, DisplayRule } from './types'

export interface MerchantFilters {
  search?: string
  search_aliases?: boolean
  unmapped?: boolean
  has_suggestions?: boolean
  last_used_after?: string
  last_used_before?: string
  cursor?: string
  offset?: number
  sort_by?: string
  sort_dir?: 'asc' | 'desc'
  limit?: number
  scope?: string
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

export function fetchMerchantDetail(id: string, scope?: string) {
  const qs = scope ? `?scope=${scope}` : ''
  return apiFetch<MerchantDetail>(`/merchants/${id}${qs}`)
}

export function updateMerchantMapping(id: string, categoryHint: string | null) {
  return apiFetch<{ id: string; name: string; category_hint: string | null; updated: boolean }>(
    `/merchants/${id}/mapping`,
    { method: 'PUT', body: JSON.stringify({ category_hint: categoryHint }) }
  )
}

export function updateMerchantName(id: string, displayName: string | null) {
  return apiFetch<{ id: string; display_name: string | null; updated: boolean }>(
    `/merchants/${id}/name`,
    { method: 'PUT', body: JSON.stringify({ display_name: displayName }) }
  )
}

export function mergeMerchant(survivingId: string, mergeFromId: string) {
  return apiFetch<{ surviving_id: string; merged_from_id: string; mappings_moved: number }>(
    `/merchants/${survivingId}/merge`,
    { method: 'POST', body: JSON.stringify({ merge_from_id: mergeFromId }) }
  )
}

export function bulkMergeMerchants(merchantIds: string[], displayName?: string) {
  return apiFetch<{ surviving_id: string; merged: number; display_name: string | null }>(
    '/merchants/bulk-merge',
    { method: 'POST', body: JSON.stringify({ merchant_ids: merchantIds, display_name: displayName || null }) }
  )
}

export function splitAlias(merchantId: string, alias: string) {
  return apiFetch<{ original_merchant_id: string; new_merchant_id: string; alias: string }>(
    `/merchants/${merchantId}/split-alias`,
    { method: 'POST', body: JSON.stringify({ alias }) }
  )
}

export function fetchSuggestions(status = 'pending', limit = 50, scope?: string) {
  const params = new URLSearchParams({ status, limit: String(limit) })
  if (scope) params.set('scope', scope)
  return apiFetch<CategorySuggestionList>(`/merchants/suggestions?${params.toString()}`)
}

export function reviewSuggestion(id: number, status: 'accepted' | 'rejected') {
  return apiFetch<{ id: number; status: string; applied: boolean }>(
    `/merchants/suggestions/${id}`,
    { method: 'PUT', body: JSON.stringify({ status }) }
  )
}

export function runCategorisation(includeLlm = false) {
  return apiFetch<{
    display_names_set: number
    rules_merchants_merged: number
    rules_merchants_renamed: number
    source_hint_suggestions: number
    auto_accepted: number
    queued_for_review: number
    llm_queued: number
  }>(`/categorisation/run?include_llm=${includeLlm}`, { method: 'POST' })
}

// ── Display Rules ──

export function fetchRules() {
  return apiFetch<DisplayRuleList>('/merchants/rules')
}

export function createRule(rule: Omit<DisplayRule, 'id'>) {
  return apiFetch<DisplayRule>('/merchants/rules', {
    method: 'POST',
    body: JSON.stringify(rule),
  })
}

export function updateRule(id: number, rule: Omit<DisplayRule, 'id'>) {
  return apiFetch<DisplayRule>(`/merchants/rules/${id}`, {
    method: 'PUT',
    body: JSON.stringify(rule),
  })
}

export function deleteRule(id: number) {
  return apiFetch<{ id: number; deleted: boolean }>(`/merchants/rules/${id}`, { method: 'DELETE' })
}
