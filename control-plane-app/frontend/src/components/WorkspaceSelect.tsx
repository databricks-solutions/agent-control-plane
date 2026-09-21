import { useState, useRef, useEffect, useMemo } from 'react'
import { Globe, Search, ChevronDown, Check } from 'lucide-react'

export interface WorkspaceOption {
  /** The workspace_id (or sentinel) passed back through onChange. */
  value: string
  /** Human-readable label shown in the list, e.g. "WS 123 · 4 endpoints". */
  label: string
  /** Optional extra text (name, counts) that search should also match against. */
  keywords?: string
}

/** Metadata shape from useWorkspaceDirectory (subset used for labelling). */
export interface WorkspaceDirMeta { name?: string; deployment_name?: string }

/**
 * Build a WorkspaceOption for a workspace id, leading with its human-readable
 * name (falling back to deployment name, then "WS <id>"). `suffix` appends a
 * per-page detail (e.g. "4 endpoints"). The id and deployment name are folded
 * into `keywords` so the picker still matches when a user types the numeric id.
 */
export function workspaceOption(
  id: string,
  dir?: Record<string, WorkspaceDirMeta>,
  suffix?: string,
): WorkspaceOption {
  const meta = (dir && dir[id]) || {}
  const name = (meta.name || meta.deployment_name || '').trim()
  const base = name || `WS ${id}`
  return {
    value: id,
    label: suffix ? `${base} · ${suffix}` : base,
    // Always searchable by id; include the deployment name and, when we lead with
    // a name, the "WS <id>" form too, so every prior search term still matches.
    keywords: `${id} ${meta.deployment_name || ''} ${name ? `WS ${id}` : ''}`.trim(),
  }
}

interface WorkspaceSelectProps {
  value: string
  onChange: (value: string) => void
  options: WorkspaceOption[]
  /** Sentinel value meaning "no filter"; rendered as the first option. */
  allValue: string
  allLabel?: string
  disabled?: boolean
  /** Extra classes for the trigger button so each page keeps its own sizing. */
  className?: string
  placeholder?: string
  showIcon?: boolean
}

/**
 * Searchable workspace picker.
 *
 * Replaces a plain <select> of workspaces: the trigger opens a panel with a
 * search box so a workspace can be found by typing its id/name instead of
 * scrolling a long list. Enter selects the first match. Falls back to the same
 * "All Workspaces" behaviour when nothing is typed.
 */
export function WorkspaceSelect({
  value,
  onChange,
  options,
  allValue,
  allLabel = 'All Workspaces',
  disabled = false,
  className = 'border rounded-lg px-3 py-2 text-sm min-w-[220px] dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200',
  placeholder = 'Search workspace id or name…',
  showIcon = true,
}: WorkspaceSelectProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [query, setQuery] = useState('')
  const containerRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  const allOption: WorkspaceOption = { value: allValue, label: allLabel }
  const everything = useMemo(() => [allOption, ...options], [options, allValue, allLabel])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return everything
    return everything.filter((o) =>
      o.value.toLowerCase().includes(q) ||
      o.label.toLowerCase().includes(q) ||
      (o.keywords || '').toLowerCase().includes(q),
    )
  }, [everything, query])

  const selectedLabel =
    everything.find((o) => o.value === value)?.label ?? allLabel

  // Close on outside click.
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  // Focus the search box when the panel opens.
  useEffect(() => {
    if (isOpen) {
      setQuery('')
      // Defer so the input exists in the DOM.
      const t = setTimeout(() => inputRef.current?.focus(), 0)
      return () => clearTimeout(t)
    }
  }, [isOpen])

  const commit = (v: string) => {
    onChange(v)
    setIsOpen(false)
    setQuery('')
  }

  return (
    <div ref={containerRef} className="relative">
      <button
        type="button"
        data-testid="workspace-select"
        data-value={value}
        disabled={disabled}
        onClick={() => setIsOpen((o) => !o)}
        className={`flex items-center gap-2 ${className} ${disabled ? 'opacity-60 cursor-not-allowed' : 'cursor-pointer'}`}
      >
        {showIcon && <Globe className="w-4 h-4 text-gray-400 dark:text-gray-500 flex-shrink-0" />}
        <span className="truncate flex-1 text-left">{selectedLabel}</span>
        <ChevronDown className="w-4 h-4 text-gray-400 dark:text-gray-500 flex-shrink-0" />
      </button>

      {isOpen && (
        <div className="absolute z-50 mt-1 min-w-full w-max max-w-[min(90vw,360px)] bg-white dark:bg-gray-800 border dark:border-gray-600 rounded-lg shadow-lg">
          <div className="p-2 border-b dark:border-gray-700">
            <div className="relative">
              <Search className="w-3.5 h-3.5 text-gray-400 absolute left-2 top-1/2 -translate-y-1/2" />
              <input
                ref={inputRef}
                type="text"
                data-testid="workspace-search"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && filtered.length > 0) {
                    e.preventDefault()
                    commit(filtered[0].value)
                  } else if (e.key === 'Escape') {
                    setIsOpen(false)
                  }
                }}
                placeholder={placeholder}
                className="w-full pl-7 pr-2 py-1.5 text-xs border rounded dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200 focus:ring-2 focus:ring-db-red/30 focus:border-db-red"
              />
            </div>
          </div>
          <div className="max-h-64 overflow-y-auto py-1">
            {filtered.length === 0 ? (
              <div className="px-3 py-2 text-xs text-gray-400">No matching workspace</div>
            ) : (
              filtered.map((o) => (
                <button
                  key={o.value}
                  type="button"
                  data-testid="workspace-option"
                  data-value={o.value}
                  onClick={() => commit(o.value)}
                  className="w-full text-left px-3 py-1.5 text-sm hover:bg-gray-100 dark:hover:bg-gray-700 flex items-center gap-2 dark:text-gray-200"
                >
                  <Check className={`w-3.5 h-3.5 flex-shrink-0 ${o.value === value ? 'text-db-red' : 'text-transparent'}`} />
                  <span className="truncate">{o.label}</span>
                </button>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  )
}
