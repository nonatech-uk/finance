import { useState, useEffect, useMemo } from 'react'
import { useMerchants, useUpdateMerchantMapping } from '../hooks/useMerchants'
import { useCategories } from '../hooks/useCategories'
import Badge from '../components/common/Badge'
import LoadingSpinner from '../components/common/LoadingSpinner'
import type { CategoryItem } from '../api/types'

function flattenCategories(items: CategoryItem[], prefix = ''): { path: string; name: string }[] {
  const result: { path: string; name: string }[] = []
  for (const cat of items) {
    result.push({ path: cat.full_path, name: prefix ? `${prefix} › ${cat.name}` : cat.name })
    if (cat.children.length > 0) {
      result.push(...flattenCategories(cat.children, cat.full_path))
    }
  }
  return result
}

export default function Merchants() {
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [unmapped, setUnmapped] = useState(false)

  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 300)
    return () => clearTimeout(t)
  }, [search])

  const filters = useMemo(() => ({
    limit: 100,
    search: debouncedSearch || undefined,
    unmapped: unmapped || undefined,
  }), [debouncedSearch, unmapped])

  const { data, isLoading, fetchNextPage, hasNextPage, isFetchingNextPage } = useMerchants(filters)
  const { data: categoryTree } = useCategories()
  const mutation = useUpdateMerchantMapping()

  const allItems = useMemo(() => data?.pages.flatMap(p => p.items) || [], [data])
  const categoryOptions = useMemo(() => categoryTree ? flattenCategories(categoryTree.items) : [], [categoryTree])

  const handleCategoryChange = (merchantId: string, value: string) => {
    mutation.mutate({ id: merchantId, categoryHint: value || null })
  }

  if (isLoading) return <LoadingSpinner />

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold">Merchants</h2>

      <div className="flex gap-3 items-center">
        <input
          type="text"
          placeholder="Search merchants..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-bg-card border border-border rounded-md px-3 py-1.5 text-sm text-text-primary placeholder:text-text-secondary focus:outline-none focus:border-accent w-64"
        />
        <label className="flex items-center gap-2 text-sm text-text-secondary cursor-pointer">
          <input
            type="checkbox"
            checked={unmapped}
            onChange={e => setUnmapped(e.target.checked)}
            className="accent-accent"
          />
          Unmapped only
        </label>
        <span className="text-text-secondary text-sm ml-auto">{allItems.length} merchants shown</span>
      </div>

      <table className="w-full text-sm">
        <thead className="sticky top-0 bg-bg-primary">
          <tr className="text-text-secondary text-left text-xs uppercase tracking-wider">
            <th className="pb-2 pr-4">Merchant</th>
            <th className="pb-2 pr-4">Category</th>
            <th className="pb-2 pr-4 text-right">Mappings</th>
            <th className="pb-2">Assign Category</th>
          </tr>
        </thead>
        <tbody>
          {allItems.map(m => (
            <tr key={m.id} className="border-b border-border/50 hover:bg-bg-hover">
              <td className="py-2 pr-4 font-medium">{m.name}</td>
              <td className="py-2 pr-4">
                {m.category_hint ? (
                  <Badge variant="accent">{m.category_hint}</Badge>
                ) : (
                  <Badge variant="warning">Unmapped</Badge>
                )}
              </td>
              <td className="py-2 pr-4 text-right text-text-secondary">{m.mapping_count}</td>
              <td className="py-2">
                <select
                  value={m.category_hint || ''}
                  onChange={e => handleCategoryChange(m.id, e.target.value)}
                  className="bg-bg-card border border-border rounded px-2 py-1 text-xs text-text-primary focus:outline-none focus:border-accent w-48"
                >
                  <option value="">— None —</option>
                  {categoryOptions.map(opt => (
                    <option key={opt.path} value={opt.path}>{opt.path}</option>
                  ))}
                </select>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {hasNextPage && (
        <div className="py-4 text-center">
          <button
            onClick={() => fetchNextPage()}
            disabled={isFetchingNextPage}
            className="text-accent hover:text-accent-hover text-sm px-4 py-2 border border-accent/30 rounded-md hover:bg-accent/10 disabled:opacity-50"
          >
            {isFetchingNextPage ? 'Loading...' : `Load more (${allItems.length} shown)`}
          </button>
        </div>
      )}
    </div>
  )
}
