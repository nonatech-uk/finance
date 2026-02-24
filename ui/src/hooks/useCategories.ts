import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchCategories,
  fetchSpending,
  createCategory,
  renameCategory,
  deleteCategory,
  type SpendingFilters,
} from '../api/categories'

export function useCategories() {
  return useQuery({
    queryKey: ['categories'],
    queryFn: fetchCategories,
    staleTime: 10 * 60 * 1000,
  })
}

export function useSpending(filters: SpendingFilters) {
  return useQuery({
    queryKey: ['spending', filters],
    queryFn: () => fetchSpending(filters),
    enabled: !!filters.date_from && !!filters.date_to,
    staleTime: 5 * 60 * 1000,
  })
}

export function useCreateCategory() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ name, parentId, categoryType }: { name: string; parentId: string | null; categoryType: string }) =>
      createCategory(name, parentId, categoryType),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['categories'] }) },
  })
}

export function useRenameCategory() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, newName }: { id: string; newName: string }) => renameCategory(id, newName),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['categories'] })
      qc.invalidateQueries({ queryKey: ['spending'] })
      qc.invalidateQueries({ queryKey: ['merchants'] })
    },
  })
}

export function useDeleteCategory() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, reassignTo }: { id: string; reassignTo: string }) => deleteCategory(id, reassignTo),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['categories'] })
      qc.invalidateQueries({ queryKey: ['spending'] })
      qc.invalidateQueries({ queryKey: ['merchants'] })
    },
  })
}
