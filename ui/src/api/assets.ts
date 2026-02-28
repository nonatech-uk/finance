import { apiFetch } from './client'
import type {
  AssetHoldingItem,
  AssetValuationItem,
  AssetsSummary,
} from './types'

export function fetchAssetsSummary() {
  return apiFetch<AssetsSummary>('/assets/summary')
}

export function fetchAssetHoldings() {
  return apiFetch<{ items: AssetHoldingItem[] }>('/assets/holdings')
}

export function fetchAssetDetail(id: string) {
  return apiFetch<AssetHoldingItem & { valuations: AssetValuationItem[] }>(
    `/assets/holdings/${id}`,
  )
}

export function createAssetHolding(data: {
  name: string
  asset_type?: string
  currency?: string
  notes?: string
}) {
  return apiFetch<AssetHoldingItem>('/assets/holdings', {
    method: 'POST',
    body: JSON.stringify(data),
  })
}

export function updateAssetHolding(
  id: string,
  data: { name?: string; asset_type?: string; is_active?: boolean; notes?: string },
) {
  return apiFetch<AssetHoldingItem>(`/assets/holdings/${id}`, {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}

export function deleteAssetHolding(id: string) {
  return apiFetch<{ ok: boolean }>(`/assets/holdings/${id}`, {
    method: 'DELETE',
  })
}

export function addAssetValuation(
  holdingId: string,
  data: { valuation_date: string; gross_value: string; tax_payable?: string; notes?: string },
) {
  return apiFetch<AssetValuationItem>(`/assets/holdings/${holdingId}/valuations`, {
    method: 'POST',
    body: JSON.stringify(data),
  })
}
