import { apiFetch } from './client'
import type { TagSummaryList } from './types'

export function fetchTags() {
  return apiFetch<TagSummaryList>('/tags')
}

export function renameTag(oldName: string, newName: string) {
  return apiFetch<{ ok: boolean; affected: number; rules_updated: number }>(
    `/tags/${encodeURIComponent(oldName)}`,
    { method: 'PUT', body: JSON.stringify({ new_name: newName }) },
  )
}

export function deleteTag(tagName: string) {
  return apiFetch<{ ok: boolean; affected: number }>(
    `/tags/${encodeURIComponent(tagName)}`,
    { method: 'DELETE' },
  )
}
