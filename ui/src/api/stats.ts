import { apiFetch } from './client'
import type { MonthlyReport, OverviewStats } from './types'

export function fetchOverview(scope?: string) {
  const qs = scope ? `?scope=${scope}` : ''
  return apiFetch<OverviewStats>(`/stats/overview${qs}`)
}

export interface MonthlyFilters {
  months?: number
  institution?: string
  account_ref?: string
  currency?: string
  scope?: string
}

export function fetchMonthly(filters: MonthlyFilters = {}) {
  const params = new URLSearchParams()
  Object.entries(filters).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') params.set(k, String(v))
  })
  const qs = params.toString()
  return apiFetch<MonthlyReport>(`/stats/monthly${qs ? '?' + qs : ''}`)
}
