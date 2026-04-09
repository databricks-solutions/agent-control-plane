import { useState, useRef, useEffect, useCallback } from 'react'
import { useSearchPrincipals } from '@/api/hooks'
import { Badge } from '@/components/ui/badge'

interface PrincipalAutocompleteProps {
  value: string
  onChange: (value: string) => void
  onSelect?: (principal: { display_name: string; id: string; type: string; email?: string }) => void
  principalType?: string  // filter to specific type
  placeholder?: string
  className?: string
}

export function PrincipalAutocomplete({
  value,
  onChange,
  onSelect,
  principalType,
  placeholder = 'Search users, groups, or service principals...',
  className = '',
}: PrincipalAutocompleteProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [debouncedQuery, setDebouncedQuery] = useState('')
  const containerRef = useRef<HTMLDivElement>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout>>()

  // Debounce the search query by 300ms
  useEffect(() => {
    if (timerRef.current) clearTimeout(timerRef.current)
    timerRef.current = setTimeout(() => {
      setDebouncedQuery(value)
    }, 300)
    return () => { if (timerRef.current) clearTimeout(timerRef.current) }
  }, [value])

  const { data: results, isLoading } = useSearchPrincipals(debouncedQuery, principalType)

  // Close dropdown on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const handleSelect = useCallback((principal: { display_name: string; id: string; type: string; email?: string }) => {
    const displayValue = principal.email || principal.display_name
    onChange(displayValue)
    onSelect?.(principal)
    setIsOpen(false)
  }, [onChange, onSelect])

  const typeBadgeVariant = (type: string) => {
    if (type === 'group') return 'info'
    if (type === 'service_principal') return 'warning'
    return 'default'
  }

  return (
    <div ref={containerRef} className={`relative ${className}`}>
      <input
        type="text"
        value={value}
        onChange={(e) => { onChange(e.target.value); setIsOpen(true) }}
        onFocus={() => { if (value.length >= 2) setIsOpen(true) }}
        placeholder={placeholder}
        className="w-full px-2 py-1 text-xs border rounded dark:bg-gray-800 dark:border-gray-600 dark:text-gray-200 focus:ring-2 focus:ring-blue-500/30 focus:border-blue-500"
        onClick={(e) => e.stopPropagation()}
      />
      {isOpen && debouncedQuery.length >= 2 && (
        <div className="absolute z-50 mt-1 w-full bg-white dark:bg-gray-800 border dark:border-gray-600 rounded-lg shadow-lg max-h-48 overflow-y-auto">
          {isLoading ? (
            <div className="px-3 py-2 text-xs text-gray-400">Searching...</div>
          ) : results && results.length > 0 ? (
            results.map((p) => (
              <button
                key={`${p.type}-${p.id}`}
                onClick={(e) => { e.stopPropagation(); handleSelect(p) }}
                className="w-full text-left px-3 py-2 text-xs hover:bg-gray-100 dark:hover:bg-gray-700 flex items-center gap-2 border-b last:border-b-0 dark:border-gray-700"
              >
                <div className="w-6 h-6 bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-400 rounded-full flex items-center justify-center text-[10px] font-bold flex-shrink-0">
                  {p.display_name.charAt(0).toUpperCase()}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="font-medium dark:text-gray-200 truncate">{p.display_name}</div>
                  {p.email && p.email !== p.display_name && (
                    <div className="text-gray-400 dark:text-gray-500 truncate">{p.email}</div>
                  )}
                </div>
                <Badge variant={typeBadgeVariant(p.type)} className="text-[10px] flex-shrink-0">
                  {p.type === 'service_principal' ? 'SP' : p.type}
                </Badge>
              </button>
            ))
          ) : (
            <div className="px-3 py-2 text-xs text-gray-400">
              No workspace matches — you can still type any account principal
            </div>
          )}
        </div>
      )}
    </div>
  )
}
