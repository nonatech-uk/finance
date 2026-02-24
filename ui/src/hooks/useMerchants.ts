import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  fetchMerchants,
  fetchMerchantDetail,
  updateMerchantMapping,
  updateMerchantName,
  mergeMerchant,
  fetchSuggestions,
  reviewSuggestion,
  runCategorisation,
  fetchRules,
  createRule,
  deleteRule,
  type MerchantFilters,
} from '../api/merchants'
import type { DisplayRule } from '../api/types'

export function useMerchants(filters: Omit<MerchantFilters, 'cursor'>) {
  return useInfiniteQuery({
    queryKey: ['merchants', filters],
    queryFn: ({ pageParam }) => fetchMerchants({ ...filters, cursor: pageParam }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.has_more ? lastPage.next_cursor ?? undefined : undefined,
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

export function useDeleteRule() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => deleteRule(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['display-rules'] })
    },
  })
}
