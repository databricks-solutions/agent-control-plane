import { useState, useEffect, type ReactNode } from 'react'

/**
 * Defers rendering of heavy Recharts components until after the browser
 * has painted the rest of the page. This prevents charts from blocking
 * the initial render of tables, KPI cards, etc.
 */
export function LazyChart({
  height = 300,
  children,
}: {
  height?: number
  children: ReactNode
}) {
  const [ready, setReady] = useState(false)

  useEffect(() => {
    const id = requestAnimationFrame(() => {
      setReady(true)
    })
    return () => cancelAnimationFrame(id)
  }, [])

  if (!ready) {
    return (
      <div
        style={{ height }}
        className="flex items-center justify-center text-gray-300 dark:text-gray-600 text-sm"
      >
        <div className="animate-pulse">Loading chart…</div>
      </div>
    )
  }

  return <>{children}</>
}
