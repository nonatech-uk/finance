import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { fetchTransactions, fetchTransaction, updateTransactionNote, updateTransactionCategory, linkTransfer, unlinkEvent, addTransactionTag, removeTransactionTag, fetchAllTags, bulkUpdateCategory, bulkUpdateMerchantName, bulkAddTags, bulkRemoveTag, bulkReplaceTags, bulkUpdateNote, type TransactionFilters } from '../api/transactions'

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

// ── Bulk Operations ──

export function useBulkUpdateCategory() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ ids, categoryPath }: { ids: string[]; categoryPath: string }) =>
      bulkUpdateCategory(ids, categoryPath),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
      queryClient.invalidateQueries({ queryKey: ['transaction'] })
    },
  })
}

export function useBulkUpdateMerchantName() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ ids, displayName }: { ids: string[]; displayName: string | null }) =>
      bulkUpdateMerchantName(ids, displayName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
      queryClient.invalidateQueries({ queryKey: ['transaction'] })
      queryClient.invalidateQueries({ queryKey: ['merchants'] })
    },
  })
}

export function useBulkAddTags() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ ids, tags }: { ids: string[]; tags: string[] }) =>
      bulkAddTags(ids, tags),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
      queryClient.invalidateQueries({ queryKey: ['transaction'] })
      queryClient.invalidateQueries({ queryKey: ['tags'] })
    },
  })
}

export function useBulkRemoveTag() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ ids, tag }: { ids: string[]; tag: string }) =>
      bulkRemoveTag(ids, tag),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
      queryClient.invalidateQueries({ queryKey: ['transaction'] })
      queryClient.invalidateQueries({ queryKey: ['tags'] })
    },
  })
}

export function useBulkReplaceTags() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ ids, tags }: { ids: string[]; tags: string[] }) =>
      bulkReplaceTags(ids, tags),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
      queryClient.invalidateQueries({ queryKey: ['transaction'] })
      queryClient.invalidateQueries({ queryKey: ['tags'] })
    },
  })
}

export function useBulkUpdateNote() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ ids, note, mode }: { ids: string[]; note: string; mode: 'replace' | 'append' }) =>
      bulkUpdateNote(ids, note, mode),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
      queryClient.invalidateQueries({ queryKey: ['transaction'] })
    },
  })
}
