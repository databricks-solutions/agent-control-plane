import { useState, useCallback, useMemo } from 'react'
import { ChevronDown, ChevronUp, ArrowUpDown } from 'lucide-react'

export type SortDir = 'asc' | 'desc'
export type SortState<K extends string = string> = { key: K; dir: SortDir } | null

export function useSort<K extends string>(defaultKey?: K, defaultDir: SortDir = 'desc') {
  const [sort, setSort] = useState<SortState<K>>(
    defaultKey ? { key: defaultKey, dir: defaultDir } : null,
  )
  const toggle = useCallback((key: K) => {
    setSort((prev) => {
      if (prev?.key === key) return prev.dir === 'desc' ? { key, dir: 'asc' } : null
      return { key, dir: 'desc' }
    })
  }, [])
  return { sort, toggle }
}

export function sortRows<T>(rows: T[], sort: SortState, accessor: (row: T, key: string) => any): T[] {
  if (!sort) return rows
  const { key, dir } = sort
  return [...rows].sort((a, b) => {
    const va = accessor(a, key)
    const vb = accessor(b, key)
    if (va == null && vb == null) return 0
    if (va == null) return 1
    if (vb == null) return -1
    const cmp = typeof va === 'string' ? va.localeCompare(vb) : va - vb
    return dir === 'asc' ? cmp : -cmp
  })
}

export function useSortedRows<T>(
  rows: T[],
  sort: SortState,
  accessor: (row: T, key: string) => any,
) {
  return useMemo(() => sortRows(rows, sort, accessor), [rows, sort, accessor])
}

export function SortableHeader({
  label,
  sortKey,
  current,
  onToggle,
  align = 'left',
  className = '',
}: {
  label: string
  sortKey: string
  current: SortState
  onToggle: (k: any) => void
  align?: 'left' | 'right'
  className?: string
}) {
  const active = current?.key === sortKey
  const icon = active
    ? current.dir === 'desc'
      ? <ChevronDown className="w-3 h-3 flex-shrink-0" />
      : <ChevronUp className="w-3 h-3 flex-shrink-0" />
    : <ArrowUpDown className="w-3 h-3 opacity-0 group-hover/sh:opacity-30 flex-shrink-0 transition-opacity" />

  return (
    <th
      className={`pb-2 font-medium cursor-pointer select-none group/sh hover:text-gray-700 dark:hover:text-gray-200 transition-colors ${align === 'right' ? 'text-right' : 'text-left'} ${className}`}
      onClick={() => onToggle(sortKey)}
    >
      <span className="inline-flex items-center gap-1">
        {align === 'right' && icon}
        {label}
        {align === 'left' && icon}
      </span>
    </th>
  )
}
