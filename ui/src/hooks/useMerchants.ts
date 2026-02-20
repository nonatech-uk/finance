import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchMerchants, updateMerchantMapping, type MerchantFilters } from '../api/merchants'

export function useMerchants(filters: Omit<MerchantFilters, 'cursor'>) {
  return useInfiniteQuery({
    queryKey: ['merchants', filters],
    queryFn: ({ pageParam }) => fetchMerchants({ ...filters, cursor: pageParam }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.has_more ? lastPage.next_cursor ?? undefined : undefined,
  })
}

export function useUpdateMerchantMapping() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, categoryHint }: { id: string; categoryHint: string | null }) =>
      updateMerchantMapping(id, categoryHint),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['merchants'] })
    },
  })
}
