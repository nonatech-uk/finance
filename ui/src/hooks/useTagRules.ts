import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchTagRules,
  createTagRule,
  updateTagRule,
  deleteTagRule,
  applyTagRules,
} from '../api/tagRules'
import type { TagRuleCreate, TagRuleUpdate } from '../api/types'

export function useTagRules() {
  return useQuery({
    queryKey: ['tag-rules'],
    queryFn: fetchTagRules,
    staleTime: 5 * 60_000,
  })
}

export function useCreateTagRule() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (data: TagRuleCreate) => createTagRule(data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['tag-rules'] }),
  })
}

export function useUpdateTagRule() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, ...data }: { id: number } & TagRuleUpdate) =>
      updateTagRule(id, data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['tag-rules'] }),
  })
}

export function useDeleteTagRule() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: deleteTagRule,
    onSuccess: () => qc.invalidateQueries({ queryKey: ['tag-rules'] }),
  })
}

export function useApplyTagRules() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: applyTagRules,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['tag-rules'] })
      qc.invalidateQueries({ queryKey: ['transactions'] })
      qc.invalidateQueries({ queryKey: ['tags'] })
    },
  })
}
