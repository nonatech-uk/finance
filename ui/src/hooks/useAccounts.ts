import { useQuery } from '@tanstack/react-query'
import { fetchAccounts, fetchAccountDetail } from '../api/accounts'

export function useAccounts() {
  return useQuery({
    queryKey: ['accounts'],
    queryFn: fetchAccounts,
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
