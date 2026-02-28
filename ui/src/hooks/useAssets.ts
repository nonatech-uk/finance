import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchAssetsSummary,
  fetchAssetDetail,
  createAssetHolding,
  updateAssetHolding,
  deleteAssetHolding,
  addAssetValuation,
} from '../api/assets'

export function useAssetsSummary() {
  return useQuery({
    queryKey: ['assets', 'summary'],
    queryFn: fetchAssetsSummary,
    staleTime: 5 * 60 * 1000,
  })
}

export function useAssetDetail(id: string) {
  return useQuery({
    queryKey: ['assets', 'holding', id],
    queryFn: () => fetchAssetDetail(id),
    enabled: !!id,
  })
}

export function useCreateAssetHolding() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: createAssetHolding,
    onSuccess: () => qc.invalidateQueries({ queryKey: ['assets'] }),
  })
}

export function useUpdateAssetHolding() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, ...data }: { id: string; name?: string; asset_type?: string; is_active?: boolean; notes?: string }) =>
      updateAssetHolding(id, data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['assets'] }),
  })
}

export function useDeleteAssetHolding() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: deleteAssetHolding,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['assets'] })
      qc.invalidateQueries({ queryKey: ['accounts'] })
    },
  })
}

export function useAddAssetValuation() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ holdingId, ...data }: { holdingId: string; valuation_date: string; gross_value: string; tax_payable?: string; notes?: string }) =>
      addAssetValuation(holdingId, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['assets'] })
      qc.invalidateQueries({ queryKey: ['accounts'] })
    },
  })
}
