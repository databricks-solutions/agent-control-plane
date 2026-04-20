import { useState, useEffect } from 'react'

export function usePersistedWorkspaceFilter(
  key: string,
  defaultValue: string,
): [string, (v: string) => void] {
  const [value, setValue] = useState<string>(() => {
    try {
      return localStorage.getItem(key) ?? defaultValue
    } catch {
      return defaultValue
    }
  })
  useEffect(() => {
    try {
      localStorage.setItem(key, value)
    } catch {}
  }, [key, value])
  return [value, setValue]
}
