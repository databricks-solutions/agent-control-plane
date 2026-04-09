import { RefreshCw } from 'lucide-react'

interface RefreshButtonProps {
  onRefresh: () => void
  isRefreshing?: boolean
  isPending?: boolean
  lastSynced?: string | null
  title?: string
}

/** Icon-only refresh button with optional cache-age label — matches Governance style. */
export function RefreshButton({
  onRefresh,
  isRefreshing = false,
  isPending = false,
  lastSynced,
  title = 'Refresh data',
}: RefreshButtonProps) {
  const busy = isRefreshing || isPending

  const cacheAge = lastSynced
    ? Math.round((Date.now() - new Date(lastSynced).getTime()) / 60_000)
    : null

  return (
    <div className="flex items-center gap-2 text-xs text-gray-400 dark:text-gray-500">
      {isRefreshing ? (
        <span className="flex items-center gap-1 text-blue-500">
          <RefreshCw className="w-3.5 h-3.5 animate-spin" />
          Refreshing…
        </span>
      ) : cacheAge !== null ? (
        <span>{cacheAge < 60 ? `${cacheAge}m ago` : `${Math.round(cacheAge / 60)}h ago`}</span>
      ) : null}
      <button
        onClick={onRefresh}
        disabled={busy}
        className="p-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-40 transition-colors"
        title={title}
      >
        <RefreshCw
          className={`w-4 h-4 ${isPending ? 'animate-spin text-blue-500' : 'text-gray-500 dark:text-gray-400'}`}
        />
      </button>
    </div>
  )
}
