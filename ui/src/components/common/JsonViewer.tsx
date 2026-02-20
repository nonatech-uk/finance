import { useState } from 'react'

interface Props {
  data: unknown
  depth?: number
}

export default function JsonViewer({ data, depth = 0 }: Props) {
  const [collapsed, setCollapsed] = useState(depth > 1)

  if (data === null) return <span className="text-text-secondary">null</span>
  if (data === undefined) return <span className="text-text-secondary">undefined</span>
  if (typeof data === 'string') return <span className="text-income">"{data}"</span>
  if (typeof data === 'number') return <span className="text-accent">{data}</span>
  if (typeof data === 'boolean') return <span className="text-warning">{String(data)}</span>

  if (Array.isArray(data)) {
    if (data.length === 0) return <span className="text-text-secondary">[]</span>
    return (
      <div className="pl-4">
        <button onClick={() => setCollapsed(!collapsed)} className="text-text-secondary hover:text-text-primary text-xs">
          {collapsed ? `▶ Array(${data.length})` : '▼ ['}
        </button>
        {!collapsed && (
          <>
            {data.map((item, i) => (
              <div key={i} className="pl-2">
                <JsonViewer data={item} depth={depth + 1} />
                {i < data.length - 1 && <span className="text-text-secondary">,</span>}
              </div>
            ))}
            <span className="text-text-secondary">]</span>
          </>
        )}
      </div>
    )
  }

  if (typeof data === 'object') {
    const entries = Object.entries(data as Record<string, unknown>)
    if (entries.length === 0) return <span className="text-text-secondary">{'{}'}</span>
    return (
      <div className="pl-4">
        <button onClick={() => setCollapsed(!collapsed)} className="text-text-secondary hover:text-text-primary text-xs">
          {collapsed ? `▶ {${entries.length} keys}` : '▼ {'}
        </button>
        {!collapsed && (
          <>
            {entries.map(([key, val], i) => (
              <div key={key} className="pl-2">
                <span className="text-text-secondary">{key}: </span>
                <JsonViewer data={val} depth={depth + 1} />
                {i < entries.length - 1 && <span className="text-text-secondary">,</span>}
              </div>
            ))}
            <span className="text-text-secondary">{'}'}</span>
          </>
        )}
      </div>
    )
  }

  return <span>{String(data)}</span>
}
