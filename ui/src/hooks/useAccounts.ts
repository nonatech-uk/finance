import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchAccounts, fetchAccountDetail, updateAccount } from '../api/accounts'
import type { AccountUpdate } from '../api/types'

export function useAccounts(includeArchived = false) {
  return useQuery({
    queryKey: ['accounts', { includeArchived }],
    queryFn: () => fetchAccounts(includeArchived),
    staleTime: 5 * 60 * 1000,
  })
}

export function useAccountDetail(institution: string, accountRef: string) {
  return useQuery({
    queryKey: ['account', institution, accountRef],
    queryFn: () => fetchAccountDetail(institution, accountRef),
    enabled: !!institution && !!accountRef,
  })
}

export function useUpdateAccount() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ institution, accountRef, body }: {
      institution: string
      accountRef: string
      body: AccountUpdate
    }) => updateAccount(institution, accountRef, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['accounts'] })
    },
  })
}
