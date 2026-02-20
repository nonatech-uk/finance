import { useInfiniteQuery, useQuery } from '@tanstack/react-query'
import { fetchTransactions, fetchTransaction, type TransactionFilters } from '../api/transactions'

export function useTransactions(filters: Omit<TransactionFilters, 'cursor'>) {
  return useInfiniteQuery({
    queryKey: ['transactions', filters],
    queryFn: ({ pageParam }) => fetchTransactions({ ...filters, cursor: pageParam }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.has_more ? lastPage.next_cursor ?? undefined : undefined,
  })
}

export function useTransaction(id: string | null) {
  return useQuery({
    queryKey: ['transaction', id],
    queryFn: () => fetchTransaction(id!),
    enabled: !!id,
  })
}
