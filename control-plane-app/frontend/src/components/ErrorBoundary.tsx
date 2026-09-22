import { Component, type ErrorInfo, type ReactNode } from 'react'

interface Props {
  children: ReactNode
}

interface State {
  hasError: boolean
  error: Error | null
}

/**
 * Top-level error boundary: catches render/lifecycle errors anywhere in the
 * tree so an unhandled exception shows a recoverable message instead of a
 * blank white screen.
 */
export default class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, error: null }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Surface to the console for local debugging / browser error reporting.
    console.error('Unhandled UI error:', error, info.componentStack)
  }

  private handleReload = () => {
    window.location.reload()
  }

  render() {
    if (!this.state.hasError) return this.props.children

    return (
      <div className="min-h-screen flex items-center justify-center bg-db-gray-50 dark:bg-gray-900 p-6">
        <div className="max-w-md w-full text-center space-y-4 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-8 shadow-sm">
          <h1 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
            Something went wrong
          </h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            An unexpected error occurred while rendering this page. Reloading
            usually fixes it — if it persists, the data source may be temporarily
            unavailable.
          </p>
          {this.state.error?.message && (
            <pre className="text-left text-xs text-gray-400 dark:text-gray-500 whitespace-pre-wrap break-words max-h-32 overflow-auto rounded bg-gray-50 dark:bg-gray-900 p-2">
              {this.state.error.message}
            </pre>
          )}
          <button
            onClick={this.handleReload}
            className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-medium bg-db-red text-white hover:opacity-90 transition-opacity"
          >
            Reload
          </button>
        </div>
      </div>
    )
  }
}
