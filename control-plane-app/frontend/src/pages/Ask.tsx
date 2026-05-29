import { useGenieSpaceInfo, useAppConfig } from '@/api/hooks'
import { Sparkles, ExternalLink } from 'lucide-react'

/**
 * "Ask" tab — embeds the curated Genie space's chat UI in an iframe.
 *
 * The space is created out-of-band by setup_genie_space.py; this page
 * just deep-links to it. Genie owns the chat UI, OBO flows through the
 * workspace session, and the user sees only what their UC permissions allow.
 *
 * Hidden entirely (via the nav) when FEATURE_GENIE_ENABLED is off.
 */
export default function AskPage() {
  const { data: config } = useAppConfig()
  const genieEnabled = !!config?.features?.genie_enabled
  const { data: info, isLoading, error } = useGenieSpaceInfo(genieEnabled)

  // Defensive: if someone hits this URL directly while the flag is off,
  // show a sensible message rather than a blank screen.
  if (!genieEnabled) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-gray-500 dark:text-gray-400">
        <Sparkles className="w-10 h-10 mb-3 opacity-40" />
        <p className="text-sm">Ask is not enabled in this workspace.</p>
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full text-gray-400">
        Loading Genie space…
      </div>
    )
  }

  if (error || !info?.available || !info.space_url) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-gray-500 dark:text-gray-400 max-w-md mx-auto text-center px-4">
        <Sparkles className="w-10 h-10 mb-3 opacity-40" />
        <p className="text-sm mb-2">Genie space is not configured.</p>
        <p className="text-xs">
          Run <code className="px-1 py-0.5 bg-gray-100 dark:bg-gray-800 rounded">setup_genie_space.py</code>{' '}
          to create one, then set <code className="px-1 py-0.5 bg-gray-100 dark:bg-gray-800 rounded">GENIE_SPACE_ID</code> in the app config.
        </p>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-full -m-6">
      {/* Compact header above the iframe so the user always has an out */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 flex-shrink-0">
        <div className="flex items-center gap-2 text-sm">
          <Sparkles className="w-4 h-4 text-db-red" />
          <span className="font-medium text-db-navy-900 dark:text-gray-100">Ask</span>
          <span className="text-gray-400 dark:text-gray-500 hidden sm:inline">
            — natural-language queries over ACP's analytical data
          </span>
        </div>
        <a
          href={info.space_url}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
        >
          Open in new tab <ExternalLink className="w-3 h-3" />
        </a>
      </div>
      <iframe
        src={info.space_url}
        title="ACP Analytics Genie space"
        className="flex-1 w-full border-0"
        // Allow same-origin since the iframe runs on the same workspace host
        // as the App, so the user's session cookie flows through and Genie
        // sees their identity for UC permission checks.
        sandbox="allow-same-origin allow-scripts allow-forms allow-popups allow-downloads"
      />
    </div>
  )
}
