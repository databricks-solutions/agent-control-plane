import { useState, useCallback } from 'react'

const STORAGE_KEY = 'pinned_agents'

function loadPinned(): Set<string> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    return new Set(raw ? JSON.parse(raw) : [])
  } catch {
    return new Set()
  }
}

export function usePinnedAgents() {
  const [pinned, setPinned] = useState<Set<string>>(loadPinned)

  const togglePin = useCallback((id: string) => {
    setPinned((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      try { localStorage.setItem(STORAGE_KEY, JSON.stringify([...next])) } catch {}
      return next
    })
  }, [])

  const isPinned = useCallback((id: string) => pinned.has(id), [pinned])

  return { pinned, togglePin, isPinned }
}
