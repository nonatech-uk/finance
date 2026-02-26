import { createContext, useContext, useState, useEffect, type ReactNode } from 'react'
import { useMe } from '../hooks/useAuth'
import type { UserInfo } from '../api/types'

interface ScopeContextValue {
  scope: string
  setScope: (s: string) => void
  allowedScopes: string[]
  user: UserInfo | null
  isAdmin: boolean
  isLoading: boolean
}

const ScopeContext = createContext<ScopeContextValue | null>(null)

const STORAGE_KEY = 'finance-scope'

export function ScopeProvider({ children }: { children: ReactNode }) {
  const { data: user, isLoading } = useMe()
  const [scope, setScopeRaw] = useState(() => localStorage.getItem(STORAGE_KEY) || 'personal')

  const allowedScopes = user?.allowed_scopes ?? []

  // Auto-correct if stored scope is not allowed
  useEffect(() => {
    if (!user) return
    if (scope !== 'all' && !allowedScopes.includes(scope)) {
      const fallback = allowedScopes[0] || 'personal'
      setScopeRaw(fallback)
      localStorage.setItem(STORAGE_KEY, fallback)
    }
  }, [user, allowedScopes, scope])

  const setScope = (s: string) => {
    setScopeRaw(s)
    localStorage.setItem(STORAGE_KEY, s)
  }

  return (
    <ScopeContext.Provider
      value={{
        scope,
        setScope,
        allowedScopes,
        user: user ?? null,
        isAdmin: user?.role === 'admin',
        isLoading,
      }}
    >
      {children}
    </ScopeContext.Provider>
  )
}

export function useScope() {
  const ctx = useContext(ScopeContext)
  if (!ctx) throw new Error('useScope must be used within ScopeProvider')
  return ctx
}
