import { apiFetch } from './client'
import type { UserInfo } from './types'

export function fetchMe() {
  return apiFetch<UserInfo>('/auth/me')
}
