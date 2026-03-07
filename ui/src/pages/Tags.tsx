import { useState } from 'react'
import { useTags, useRenameTag, useDeleteTag } from '../hooks/useTags'
import LoadingSpinner from '../components/common/LoadingSpinner'
import type { TagSummaryItem } from '../api/types'

type SortKey = 'tag' | 'count'
type SortDir = 'asc' | 'desc'

const SOURCE_LABELS: Record<string, string> = {
  rule: 'Rule',
  user: 'Manual',
  migration: 'Migration',
  ibank_import: 'iBank',
  system: 'System',
}

const SOURCE_COLOURS: Record<string, string> = {
  rule: 'bg-accent/15 text-accent',
  user: 'bg-income/15 text-income',
  migration: 'bg-text-secondary/15 text-text-secondary',
  ibank_import: 'bg-text-secondary/15 text-text-secondary',
  system: 'bg-text-secondary/15 text-text-secondary',
}

export default function Tags() {
  const { data, isLoading } = useTags()
  const renameMut = useRenameTag()
  const deleteMut = useDeleteTag()

  const [sortKey, setSortKey] = useState<SortKey>('tag')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [search, setSearch] = useState('')
  const [editingTag, setEditingTag] = useState<string | null>(null)
  const [newName, setNewName] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir(key === 'count' ? 'desc' : 'asc')
    }
  }

  const sortIndicator = (key: SortKey) =>
    sortKey === key ? (sortDir === 'asc' ? ' \u25B2' : ' \u25BC') : ''

  const items = (data?.items ?? [])
    .filter(t => !search || t.tag.toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => {
      const mul = sortDir === 'asc' ? 1 : -1
      if (sortKey === 'count') return (a.count - b.count) * mul
      return a.tag.localeCompare(b.tag) * mul
    })

  const startEdit = (tag: TagSummaryItem) => {
    setEditingTag(tag.tag)
    setNewName(tag.tag)
    setConfirmDelete(null)
  }

  const cancelEdit = () => {
    setEditingTag(null)
    setNewName('')
  }

  const submitRename = () => {
    if (!editingTag || !newName.trim() || newName.trim() === editingTag) return
    renameMut.mutate(
      { oldName: editingTag, newName: newName.trim() },
      { onSuccess: () => cancelEdit() },
    )
  }

  const handleDelete = (tag: string) => {
    deleteMut.mutate(tag, {
      onSuccess: () => setConfirmDelete(null),
    })
  }

  if (isLoading) return <LoadingSpinner />

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Tags</h2>
        <span className="text-sm text-text-secondary">
          {items.length} tag{items.length !== 1 ? 's' : ''}
        </span>
      </div>

      {/* Search */}
      <div>
        <input
          type="text"
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Filter tags..."
          className="w-full max-w-sm px-3 py-1.5 text-sm rounded-md border border-border bg-bg-primary text-text-primary placeholder:text-text-secondary"
        />
      </div>

      {/* Table */}
      {items.length > 0 ? (
        <div className="bg-bg-card border border-border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-bg-secondary">
                <th
                  onClick={() => toggleSort('tag')}
                  className="text-left px-4 py-2 font-medium text-text-secondary cursor-pointer select-none"
                >
                  Tag{sortIndicator('tag')}
                </th>
                <th
                  onClick={() => toggleSort('count')}
                  className="text-right px-4 py-2 font-medium text-text-secondary cursor-pointer select-none w-24"
                >
                  Count{sortIndicator('count')}
                </th>
                <th className="text-left px-4 py-2 font-medium text-text-secondary">
                  Sources
                </th>
                <th className="text-right px-4 py-2 font-medium text-text-secondary w-40">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {items.map(t => (
                <tr
                  key={t.tag}
                  className="border-b border-border last:border-0 hover:bg-bg-hover"
                >
                  <td className="px-4 py-2">
                    {editingTag === t.tag ? (
                      <div className="flex items-center gap-2">
                        <input
                          autoFocus
                          value={newName}
                          onChange={e => setNewName(e.target.value)}
                          onKeyDown={e => {
                            if (e.key === 'Enter') submitRename()
                            if (e.key === 'Escape') cancelEdit()
                          }}
                          className="px-2 py-1 text-sm rounded border border-border bg-bg-primary text-text-primary w-64"
                        />
                        <button
                          onClick={submitRename}
                          disabled={renameMut.isPending || !newName.trim() || newName.trim() === editingTag}
                          className="px-2 py-1 text-xs rounded bg-accent text-white hover:bg-accent/90 disabled:opacity-50"
                        >
                          {renameMut.isPending ? 'Saving...' : 'Save'}
                        </button>
                        <button
                          onClick={cancelEdit}
                          className="px-2 py-1 text-xs rounded border border-border text-text-secondary hover:bg-bg-hover"
                        >
                          Cancel
                        </button>
                      </div>
                    ) : (
                      <span className="font-medium">{t.tag}</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-text-secondary">
                    {t.count}
                  </td>
                  <td className="px-4 py-2">
                    <div className="flex flex-wrap gap-1">
                      {Object.entries(t.sources).map(([src, cnt]) => (
                        <span
                          key={src}
                          className={`px-1.5 py-0.5 text-xs rounded ${SOURCE_COLOURS[src] ?? 'bg-bg-secondary text-text-secondary'}`}
                        >
                          {SOURCE_LABELS[src] ?? src} ({cnt})
                        </span>
                      ))}
                    </div>
                  </td>
                  <td className="px-4 py-2 text-right">
                    {confirmDelete === t.tag ? (
                      <div className="flex gap-1 justify-end">
                        <span className="text-xs text-text-secondary py-1">
                          Remove from {t.count} txn{t.count !== 1 ? 's' : ''}?
                        </span>
                        <button
                          onClick={() => handleDelete(t.tag)}
                          disabled={deleteMut.isPending}
                          className="px-2 py-1 text-xs rounded bg-expense text-white hover:bg-expense/90 disabled:opacity-50"
                        >
                          {deleteMut.isPending ? '...' : 'Confirm'}
                        </button>
                        <button
                          onClick={() => setConfirmDelete(null)}
                          className="px-2 py-1 text-xs rounded border border-border text-text-secondary hover:bg-bg-hover"
                        >
                          Cancel
                        </button>
                      </div>
                    ) : editingTag !== t.tag ? (
                      <div className="flex gap-1 justify-end">
                        <button
                          onClick={() => startEdit(t)}
                          className="px-2 py-1 text-xs rounded border border-border text-text-secondary hover:bg-bg-hover"
                        >
                          Rename
                        </button>
                        <button
                          onClick={() => { setConfirmDelete(t.tag); setEditingTag(null) }}
                          className="px-2 py-1 text-xs rounded border border-expense/30 text-expense hover:bg-expense/10"
                        >
                          Delete
                        </button>
                      </div>
                    ) : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="bg-bg-card border border-border rounded-lg p-8 text-center text-text-secondary">
          {search ? 'No tags matching your filter.' : 'No tags yet.'}
        </div>
      )}
    </div>
  )
}
