export const API_BASE_URL = import.meta.env.VITE_API_URL || '/api/v1'
export const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8080/ws/updates'

export const STATUS_COLORS = {
  ONLINE: 'bg-green-500',
  OFFLINE: 'bg-red-500',
  PROVISIONING: 'bg-yellow-500',
  UNKNOWN: 'bg-gray-500',
} as const

export const HEALTH_COLORS = {
  healthy: 'bg-green-500',
  warning: 'bg-yellow-500',
  critical: 'bg-red-500',
} as const
