import { useQuery } from '@tanstack/react-query'
import { fetchCategories, fetchSpending, type SpendingFilters } from '../api/categories'

export function useCategories() {
  return useQuery({
    queryKey: ['categories'],
    queryFn: fetchCategories,
    staleTime: 10 * 60 * 1000,
  })
}

export function useSpending(filters: SpendingFilters) {
  return useQuery({
    queryKey: ['spending', filters],
    queryFn: () => fetchSpending(filters),
    enabled: !!filters.date_from && !!filters.date_to,
    staleTime: 5 * 60 * 1000,
  })
}
