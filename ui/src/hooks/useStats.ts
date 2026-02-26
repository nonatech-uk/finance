import { useQuery } from '@tanstack/react-query'
import { fetchOverview, fetchMonthly, type MonthlyFilters } from '../api/stats'

export function useOverview(scope?: string) {
  return useQuery({
    queryKey: ['stats', 'overview', scope],
    queryFn: () => fetchOverview(scope),
    staleTime: 5 * 60 * 1000,
  })
}

export function useMonthly(filters: MonthlyFilters = {}) {
  return useQuery({
    queryKey: ['stats', 'monthly', filters],
    queryFn: () => fetchMonthly(filters),
    staleTime: 5 * 60 * 1000,
  })
}
