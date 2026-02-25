import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  fetchMerchants,
  fetchMerchantDetail,
  updateMerchantMapping,
  updateMerchantName,
  mergeMerchant,
  bulkMergeMerchants,
  splitAlias,
  fetchSuggestions,
  reviewSuggestion,
  runCategorisation,
  fetchRules,
  createRule,
  updateRule,
  deleteRule,
  type MerchantFilters,
} from '../api/merchants'
import type { DisplayRule } from '../api/types'

export function useMerchants(filters: Omit<MerchantFilters, 'cursor' | 'offset'>) {
  const isNameSort = !filters.sort_by || filters.sort_by === 'name'

  return useInfiniteQuery({
    queryKey: ['merchants', filters],
    queryFn: ({ pageParam }) => {
      if (isNameSort) {
        return fetchMerchants({ ...filters, cursor: pageParam as string | undefined })
      }
      return fetchMerchants({ ...filters, offset: (pageParam as number) || 0 })
    },
    initialPageParam: isNameSort ? (undefined as string | undefined) : (0 as number | undefined),
    getNextPageParam: (lastPage, allPages) => {
      if (!lastPage.has_more) return undefined
      if (isNameSort) {
        return lastPage.next_cursor ?? undefined
      }
      return allPages.reduce((sum, p) => sum + p.items.length, 0)
    },
  })
}

export function useMerchantDetail(id: string | null) {
  return useQuery({
    queryKey: ['merchant', id],
    queryFn: () => fetchMerchantDetail(id!),
    enabled: !!id,
  })
}

export function useUpdateMerchantMapping() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, categoryHint }: { id: string; categoryHint: string | null }) =>
      updateMerchantMapping(id, categoryHint),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['merchants'] })
      queryClient.invalidateQueries({ queryKey: ['merchant'] })
    },
  })
}

export function useUpdateMerchantName() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, displayName }: { id: string; displayName: string | null }) =>
      updateMerchantName(id, displayName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['merchants'] })
      queryClient.invalidateQueries({ queryKey: ['merchant'] })
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
      queryClient.invalidateQueries({ queryKey: ['transaction'] })
    },
  })
}

export function useMergeMerchant() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ survivingId, mergeFromId }: { survivingId: string; mergeFromId: string }) =>
      mergeMerchant(survivingId, mergeFromId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['merchants'] })
      queryClient.invalidateQueries({ queryKey: ['merchant'] })
    },
  })
}

export function useBulkMergeMerchants() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ merchantIds, displayName }: { merchantIds: string[]; displayName?: string }) =>
      bulkMergeMerchants(merchantIds, displayName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['merchants'] })
      queryClient.invalidateQueries({ queryKey: ['merchant'] })
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
      queryClient.invalidateQueries({ queryKey: ['suggestions'] })
    },
  })
}

export function useSplitAlias() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ merchantId, alias }: { merchantId: string; alias: string }) =>
      splitAlias(merchantId, alias),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['merchants'] })
      queryClient.invalidateQueries({ queryKey: ['merchant'] })
      queryClient.invalidateQueries({ queryKey: ['transactions'] })
    },
  })
}

export function useSuggestions(status = 'pending') {
  return useQuery({
    queryKey: ['suggestions', status],
    queryFn: () => fetchSuggestions(status),
  })
}

export function useReviewSuggestion() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, status }: { id: number; status: 'accepted' | 'rejected' }) =>
      reviewSuggestion(id, status),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['suggestions'] })
      queryClient.invalidateQueries({ queryKey: ['merchants'] })
    },
  })
}

export function useRunCategorisation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ includeLlm }: { includeLlm: boolean }) =>
      runCategorisation(includeLlm),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['merchants'] })
      queryClient.invalidateQueries({ queryKey: ['suggestions'] })
    },
  })
}

export function useDisplayRules() {
  return useQuery({
    queryKey: ['display-rules'],
    queryFn: fetchRules,
  })
}

export function useCreateRule() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (rule: Omit<DisplayRule, 'id'>) => createRule(rule),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['display-rules'] })
    },
  })
}

export function useUpdateRule() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, rule }: { id: number; rule: Omit<DisplayRule, 'id'> }) => updateRule(id, rule),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['display-rules'] })
    },
  })
}

export function useDeleteRule() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => deleteRule(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['display-rules'] })
    },
  })
}
