import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { fetchTransactions, fetchTransaction, updateTransactionNote, updateTransactionCategory, linkTransfer, unlinkEvent, addTransactionTag, removeTransactionTag, fetchAllTags, type TransactionFilters } from '../api/transactions'

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

export function useUpdateNote() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, note }: { id: string; note: string }) =>
      updateTransactionNote(id, note),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['transaction', variables.id] })
    },
  })
}

export function useUpdateTransactionCategory() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, categoryPath }: { id: string; categoryPath: string }) =>
      updateTransactionCategory(id, categoryPath),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['transaction', variables.id] })
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
    },
  })
}

export function useLinkTransfer() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, counterpartId }: { id: string; counterpartId: string }) =>
      linkTransfer(id, counterpartId),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['transaction', variables.id] })
      queryClient.invalidateQueries({ queryKey: ['transaction', variables.counterpartId] })
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
    },
  })
}

export function useUnlinkEvent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ eventId }: { eventId: string }) =>
      unlinkEvent(eventId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['transaction'] })
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
    },
  })
}

export function useAllTags() {
  return useQuery({
    queryKey: ['tags'],
    queryFn: fetchAllTags,
    staleTime: 60_000,
  })
}

export function useAddTag() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, tag }: { id: string; tag: string }) =>
      addTransactionTag(id, tag),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['transaction', variables.id] })
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
      queryClient.invalidateQueries({ queryKey: ['tags'] })
    },
  })
}

export function useRemoveTag() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, tag }: { id: string; tag: string }) =>
      removeTransactionTag(id, tag),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['transaction', variables.id] })
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
      queryClient.invalidateQueries({ queryKey: ['tags'] })
    },
  })
}
