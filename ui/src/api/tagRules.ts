import { apiFetch } from './client'
import type {
  TagRuleItem,
  TagRuleList,
  TagRuleCreate,
  TagRuleUpdate,
  TagRuleApplyResult,
} from './types'

export function fetchTagRules() {
  return apiFetch<TagRuleList>('/tag-rules')
}

export function createTagRule(data: TagRuleCreate) {
  return apiFetch<TagRuleItem>('/tag-rules', {
    method: 'POST',
    body: JSON.stringify(data),
  })
}

export function updateTagRule(id: number, data: TagRuleUpdate) {
  return apiFetch<TagRuleItem>(`/tag-rules/${id}`, {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}

export function deleteTagRule(id: number) {
  return apiFetch<{ id: number; deleted: boolean; tags_removed: number }>(
    `/tag-rules/${id}`,
    { method: 'DELETE' },
  )
}

export function applyTagRules() {
  return apiFetch<TagRuleApplyResult>('/tag-rules/apply', {
    method: 'POST',
  })
}
