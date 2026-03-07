import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  createCashTransaction,
  resetCashBalance,
  type CashTransactionCreate,
  type CashBalanceReset,
} from '../api/cash'

export function useCreateCashTransaction() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: CashTransactionCreate) => createCashTransaction(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['accounts'] })
      queryClient.invalidateQueries({ queryKey: ['account'] })
    },
  })
}

export function useResetCashBalance() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ accountRef, body }: { accountRef: string; body: CashBalanceReset }) =>
      resetCashBalance(accountRef, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['accounts'] })
      queryClient.invalidateQueries({ queryKey: ['account'] })
    },
  })
}
