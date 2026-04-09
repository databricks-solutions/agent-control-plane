import { WS_URL } from './constants'

export class WebSocketClient {
  private ws: WebSocket | null = null
  private reconnectAttempts = 0
  private maxReconnectAttempts = 5
  private listeners: Map<string, Set<(data: any) => void>> = new Map()

  connect() {
    try {
      this.ws = new WebSocket(WS_URL)
      
      this.ws.onopen = () => {
        console.log('WebSocket connected')
        this.reconnectAttempts = 0
      }

      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data)
          this.notifyListeners('message', data)
        } catch (e) {
          console.error('Failed to parse WebSocket message', e)
        }
      }

      this.ws.onerror = (error) => {
        console.error('WebSocket error', error)
        this.notifyListeners('error', error)
      }

      this.ws.onclose = () => {
        console.log('WebSocket closed')
        this.notifyListeners('close', null)
        this.reconnect()
      }
    } catch (error) {
      console.error('Failed to connect WebSocket', error)
      this.reconnect()
    }
  }

  private reconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++
      setTimeout(() => {
        console.log(`Reconnecting... (${this.reconnectAttempts}/${this.maxReconnectAttempts})`)
        this.connect()
      }, 1000 * this.reconnectAttempts)
    }
  }

  on(event: string, callback: (data: any) => void) {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set())
    }
    this.listeners.get(event)!.add(callback)
  }

  off(event: string, callback: (data: any) => void) {
    this.listeners.get(event)?.delete(callback)
  }

  private notifyListeners(event: string, data: any) {
    this.listeners.get(event)?.forEach(callback => callback(data))
  }

  disconnect() {
    if (this.ws) {
      this.ws.close()
      this.ws = null
    }
  }
}

export const wsClient = new WebSocketClient()
