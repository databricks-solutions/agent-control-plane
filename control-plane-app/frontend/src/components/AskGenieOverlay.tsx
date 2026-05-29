import { useEffect, useMemo, useRef, useState } from 'react'
import { useLocation } from 'react-router-dom'
import { Sparkles, X, Send, ExternalLink, Maximize2, Minimize2, ChevronDown, ChevronRight, AlertCircle, RotateCcw, Inbox, MessageSquarePlus, Square } from 'lucide-react'
import {
  useAppConfig,
  useGenieSpaceInfo,
  genieStartConversation,
  genieGetMessage,
  genieFollowUp,
  genieGetQueryResult,
  type GenieMessage,
  type GenieQueryResult,
} from '@/api/hooks'
import Markdown from './Markdown'
import { LineChart } from './charts/LineChart'
import { BarChart } from './charts/BarChart'
import { StackedBarChart } from './charts/StackedBarChart'
import { PieChart } from './charts/PieChart'
import {
  formatCompact,
  formatFull,
  formatDateShort,
  formatDateFull,
  isCurrencyColumn,
  isDateColumn,
  isPercentColumn,
  makeValueFormatter,
} from '@/lib/formatters'

type ChatMessage =
  | { kind: 'user'; text: string }
  | {
      kind: 'genie'
      status: 'loading' | 'done' | 'error'
      text?: string
      sql?: string
      result?: GenieQueryResult
      suggestions?: string[]
      error?: string
    }

const POLL_INTERVAL_MS = 1500
const POLL_TIMEOUT_MS = 90_000

/** Pull the message content out of Genie's mixed attachments shape. */
function parseAttachments(msg: GenieMessage): { text?: string; sql?: string; suggestions?: string[] } {
  const out: { text?: string; sql?: string; suggestions?: string[] } = {}
  for (const a of msg.attachments ?? []) {
    if (a.query?.query && !out.sql) out.sql = a.query.query
    if (a.text) {
      const t = typeof a.text === 'string' ? a.text : a.text.content
      if (t && !out.text) out.text = t
    }
    if (a.suggested_questions) {
      const sq = Array.isArray(a.suggested_questions)
        ? a.suggested_questions
        : a.suggested_questions.questions
      if (sq && sq.length) out.suggestions = sq
    }
  }
  return out
}

/** Per-route starter questions shown in the empty state. Picked to match the
 *  data the user is currently looking at, so the overlay feels integrated
 *  rather than a generic search box. */
const ROUTE_SUGGESTIONS: Array<{ match: (p: string) => boolean; questions: string[] }> = [
  {
    match: p => p.startsWith('/agents/detail/'),
    questions: [
      'How many invocations did this agent have in the past 7 days?',
      'Which users invoked this agent the most?',
      'Top traces by latency for this agent',
    ],
  },
  {
    match: p => p.startsWith('/agents'),
    questions: [
      'Which agents have the most invocations today?',
      'Top 10 agents by total tokens this week',
      'Which agents have failed traces in the past 24 hours?',
    ],
  },
  {
    match: p => p.startsWith('/ai-gateway'),
    questions: [
      'Which endpoints had the most requests today?',
      'Top 5 endpoints by token consumption this week',
      'Daily endpoint usage over the past 14 days',
    ],
  },
  {
    match: p => p.startsWith('/observability'),
    questions: [
      'Average trace latency by agent over the past 24 hours',
      'Top 10 slowest traces today',
      'Trace volume by hour over the past 7 days',
    ],
  },
  {
    match: p => p.startsWith('/workspaces'),
    questions: [
      'Top 5 workspaces by total spend this month',
      'Which workspaces have the most agents?',
      'Compare workspace activity over the past 14 days',
    ],
  },
  {
    match: p => p.startsWith('/tools'),
    questions: [
      'Most-used tools in the past 7 days',
      'Which agents use the most distinct tools?',
    ],
  },
  {
    // "Knowledge Bases" route — covers both vector search indexes and Lakebase instances.
    match: p => p.startsWith('/vector-search'),
    questions: [
      'Top 5 workspaces by Knowledge Bases cost this month',
      'Lakebase storage cost by instance',
      'Vector search query volume over the past 14 days',
    ],
  },
  {
    match: p => p.startsWith('/admin'),
    questions: [
      'How many resources have we discovered in total?',
      'Show data freshness across all caches',
    ],
  },
  // Governance (root) — budgets and spend focus
  {
    match: p => p === '/' || p.startsWith('/governance'),
    questions: [
      'Which users are above 80% of their budget?',
      'Top 5 most expensive users this month',
      'Daily token consumption over the past 14 days',
    ],
  },
]

const DEFAULT_SUGGESTIONS = [
  'Daily token consumption over the past 14 days',
  'Top 5 endpoints by spend this month',
  'Which agents had failures today?',
]

function suggestionsForPath(path: string): string[] {
  for (const rule of ROUTE_SUGGESTIONS) {
    if (rule.match(path)) return rule.questions
  }
  return DEFAULT_SUGGESTIONS
}

/** Turn raw API/SDK errors into a one-line user-facing sentence. Keeps the
 *  developer detail in the title attribute for inspection. */
function humanizeError(raw: string | undefined): string {
  if (!raw) return 'Something went wrong asking Genie.'
  const s = raw.toLowerCase()
  if (s.includes('timed out') || s.includes('timeout')) {
    return 'Genie took too long to respond. Try a simpler question or retry.'
  }
  if (s.includes('network') || s.includes('failed to fetch') || s.includes('econn')) {
    return "Couldn't reach Genie. Check your connection and try again."
  }
  if (s.includes('403') || s.includes('forbidden') || s.includes('permission')) {
    return "You don't have permission to query this Genie space."
  }
  if (s.includes('404') || s.includes('not found')) {
    return 'Genie space not found. The space may have been removed.'
  }
  if (s.includes('500') || s.includes('internal')) {
    return 'Genie hit an internal error. Retry in a moment.'
  }
  if (s.includes('cancelled') || s.includes('canceled')) {
    return 'Request was cancelled.'
  }
  // Fall back to raw if it's short enough to be readable
  return raw.length < 160 ? raw : 'Genie returned an error. Try again.'
}

class CancelledError extends Error {
  constructor() {
    super('Cancelled')
    this.name = 'CancelledError'
  }
}

async function pollMessage(cid: string, mid: string, signal?: AbortSignal): Promise<GenieMessage> {
  const start = Date.now()
  while (Date.now() - start < POLL_TIMEOUT_MS) {
    if (signal?.aborted) throw new CancelledError()
    const m = await genieGetMessage(cid, mid, signal)
    if (m.status === 'COMPLETED' || m.status === 'FAILED' || m.status === 'CANCELLED') return m
    // Cancel-aware sleep so users don't wait up to POLL_INTERVAL_MS after Stop.
    await new Promise<void>((resolve, reject) => {
      const t = window.setTimeout(resolve, POLL_INTERVAL_MS)
      const onAbort = () => { window.clearTimeout(t); reject(new CancelledError()) }
      signal?.addEventListener('abort', onAbort, { once: true })
    })
  }
  throw new Error('Genie response timed out')
}

/* ── sessionStorage persistence ────────────────────────────────── */

const STORAGE_KEY = 'acp.genie.thread.v1'
const STORAGE_VERSION = 1

type PersistedThread = {
  v: number
  conversationId: string | null
  messages: ChatMessage[]
}

/** Strip in-flight loading placeholders so a reload doesn't show a spinner
 *  for a message we can no longer poll. */
function persistableMessages(messages: ChatMessage[]): ChatMessage[] {
  return messages.filter(m => !(m.kind === 'genie' && m.status === 'loading'))
}

function loadThread(): PersistedThread | null {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw) as PersistedThread
    if (parsed?.v !== STORAGE_VERSION) return null
    return parsed
  } catch {
    return null
  }
}

function saveThread(conversationId: string | null, messages: ChatMessage[]) {
  try {
    const safe = persistableMessages(messages)
    if (!conversationId && safe.length === 0) {
      sessionStorage.removeItem(STORAGE_KEY)
      return
    }
    const payload: PersistedThread = { v: STORAGE_VERSION, conversationId, messages: safe }
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(payload))
  } catch {
    // Quota exceeded or sessionStorage disabled — silent best-effort.
  }
}

export default function AskGenieOverlay() {
  const { data: config } = useAppConfig()
  const genieEnabled = !!config?.features?.genie_enabled
  const { data: info } = useGenieSpaceInfo(genieEnabled)
  const location = useLocation()
  const suggestions = useMemo(() => suggestionsForPath(location.pathname), [location.pathname])

  const [open, setOpen] = useState(false)
  const [maximized, setMaximized] = useState(false)
  const [input, setInput] = useState('')
  // Lazy-init from sessionStorage so a hard refresh resumes the thread.
  const [messages, setMessages] = useState<ChatMessage[]>(() => loadThread()?.messages ?? [])
  const [conversationId, setConversationId] = useState<string | null>(() => loadThread()?.conversationId ?? null)
  const [busy, setBusy] = useState(false)
  const [atBottom, setAtBottom] = useState(true)
  const scrollRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)
  // Forces auto-scroll on the next render (e.g. after user submits a question).
  const stickToBottomRef = useRef(true)
  // Controller for the in-flight Genie request — call .abort() to cancel.
  const abortRef = useRef<AbortController | null>(null)

  // Persist on every change. `busy` deliberately not in deps — it's transient.
  useEffect(() => {
    saveThread(conversationId, messages)
  }, [conversationId, messages])

  // Auto-scroll only when user is already at (or near) the bottom — otherwise
  // they're reading earlier output and don't want to be yanked down.
  useEffect(() => {
    const el = scrollRef.current
    if (!el) return
    if (stickToBottomRef.current || atBottom) {
      el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' })
      stickToBottomRef.current = false
    }
  }, [messages, busy, atBottom])

  const handleScroll = () => {
    const el = scrollRef.current
    if (!el) return
    const distance = el.scrollHeight - el.scrollTop - el.clientHeight
    setAtBottom(distance < 40)
  }

  // Esc: cancel in-flight question first (if any), else close overlay.
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return
      if (busy && abortRef.current) {
        e.preventDefault()
        abortRef.current.abort()
        return
      }
      setOpen(false)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, busy])

  // Global Cmd/Ctrl+K toggles overlay open. Skipped while the user is typing
  // in another input/textarea/contenteditable so we don't hijack their flow.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (!(e.key === 'k' || e.key === 'K')) return
      if (!(e.metaKey || e.ctrlKey)) return
      const target = e.target as HTMLElement | null
      if (target) {
        const tag = target.tagName
        if (tag === 'INPUT' || tag === 'TEXTAREA' || target.isContentEditable) {
          // Allow Cmd+K only if the active input is *our* Genie input (toggles close)
          if (target !== inputRef.current) return
        }
      }
      e.preventDefault()
      setOpen(o => !o)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  // When the overlay opens, focus the input.
  useEffect(() => {
    if (open) {
      // Defer until after the panel transition kicks off so focus actually lands.
      const id = window.setTimeout(() => inputRef.current?.focus(), 50)
      return () => window.clearTimeout(id)
    }
  }, [open])

  const startNewChat = () => {
    setMessages([])
    setConversationId(null)
    setInput('')
    stickToBottomRef.current = true
    setAtBottom(true)
    // saveThread effect will clear sessionStorage on next tick.
    inputRef.current?.focus()
  }

  if (!genieEnabled || !info?.available) return null

  function cancel() {
    abortRef.current?.abort()
  }

  async function ask(question: string) {
    const q = question.trim()
    if (!q || busy) return
    setBusy(true)
    setInput('')
    // Sending a new question always pulls the view to the bottom, regardless
    // of where the user had scrolled.
    stickToBottomRef.current = true
    const controller = new AbortController()
    abortRef.current = controller
    setMessages(prev => [...prev, { kind: 'user', text: q }, { kind: 'genie', status: 'loading' }])
    try {
      let cid = conversationId
      let mid: string
      if (!cid) {
        const r = await genieStartConversation(q, controller.signal)
        cid = r.conversation?.id ?? r.conversation_id
        mid = r.message?.id ?? r.message?.message_id ?? r.message_id
        setConversationId(cid)
      } else {
        const r = await genieFollowUp(cid, q, controller.signal)
        mid = r.id ?? r.message_id ?? ''
      }
      const finalMsg = await pollMessage(cid!, mid, controller.signal)
      const parsed = parseAttachments(finalMsg)
      // Only fetch query-result when Genie actually wrote SQL.
      const result = parsed.sql && finalMsg.status === 'COMPLETED'
        ? await genieGetQueryResult(cid!, mid, controller.signal)
        : null
      setMessages(prev => {
        const next = [...prev]
        next[next.length - 1] = {
          kind: 'genie',
          status: finalMsg.status === 'COMPLETED' ? 'done' : 'error',
          text: parsed.text,
          sql: parsed.sql,
          result: result ?? undefined,
          suggestions: parsed.suggestions,
          error: finalMsg.status !== 'COMPLETED' ? finalMsg.content || finalMsg.status : undefined,
        }
        return next
      })
    } catch (e: any) {
      const aborted =
        e?.name === 'CancelledError' ||
        e?.name === 'CanceledError' ||
        e?.name === 'AbortError' ||
        e?.code === 'ERR_CANCELED' ||
        controller.signal.aborted
      setMessages(prev => {
        const next = [...prev]
        next[next.length - 1] = aborted
          ? { kind: 'genie', status: 'error', error: 'Cancelled' }
          : { kind: 'genie', status: 'error', error: e?.message || 'Request failed' }
        return next
      })
    } finally {
      if (abortRef.current === controller) abortRef.current = null
      setBusy(false)
    }
  }

  return (
    <>
      {/* Floating action button */}
      <button
        type="button"
        aria-label={open ? 'Close Ask Genie' : 'Open Ask Genie'}
        title="Ask Genie (⌘K)"
        onClick={() => setOpen(o => !o)}
        className={`fixed bottom-6 right-6 z-50 w-14 h-14 rounded-full shadow-lg flex items-center justify-center transition-all
          ${open ? 'bg-gray-700 dark:bg-gray-600 text-white' : 'bg-db-red text-white hover:bg-db-red/90 shadow-db-red/30 hover:scale-105'}`}
      >
        {open ? <X className="w-6 h-6" /> : <Sparkles className="w-6 h-6" />}
      </button>

      {/* Chat panel */}
      <div
        className={`fixed z-40 transition-all duration-200 ease-out shadow-2xl rounded-xl overflow-hidden
          bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 flex flex-col
          ${open ? 'opacity-100 translate-y-0' : 'pointer-events-none opacity-0 translate-y-4'}
          ${maximized
            ? 'top-6 left-6 right-6 bottom-24'
            : 'bottom-24 right-6 w-[440px] max-w-[calc(100vw-3rem)] h-[640px] max-h-[calc(100vh-7rem)]'}`}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-3 py-2 border-b border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 flex-shrink-0">
          <div className="flex items-center gap-2 text-sm font-medium text-db-navy-900 dark:text-gray-100">
            <Sparkles className="w-4 h-4 text-db-red" />
            <span>Ask Genie</span>
          </div>
          <div className="flex items-center gap-1">
            {messages.length > 0 && (
              <button
                type="button"
                onClick={startNewChat}
                disabled={busy}
                title="New chat"
                aria-label="Start a new chat"
                className="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <MessageSquarePlus className="w-4 h-4" />
              </button>
            )}
            {info.space_url && (
              <a
                href={info.space_url}
                target="_blank"
                rel="noopener noreferrer"
                title="Open Genie space in new tab"
                className="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
              >
                <ExternalLink className="w-4 h-4" />
              </a>
            )}
            <button
              type="button"
              onClick={() => setMaximized(m => !m)}
              title={maximized ? 'Restore' : 'Maximize'}
              className="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
            >
              {maximized ? <Minimize2 className="w-4 h-4" /> : <Maximize2 className="w-4 h-4" />}
            </button>
            <button
              type="button"
              onClick={() => setOpen(false)}
              title="Close"
              className="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Message list */}
        <div className="flex-1 relative flex flex-col min-h-0">
          <div
            ref={scrollRef}
            onScroll={handleScroll}
            className="flex-1 overflow-y-auto px-4 py-3 space-y-3 bg-gray-50 dark:bg-gray-950"
          >
            {messages.length === 0 && (
              <div className="py-6">
                <div className="text-center text-sm text-gray-500 dark:text-gray-400 mb-4">
                  <Sparkles className="w-8 h-8 mx-auto mb-2 opacity-50 text-db-red" />
                  <p>Ask anything about your agent estate.</p>
                  <p className="text-xs mt-1 opacity-70">Cost, usage, agents, traces, vector search.</p>
                </div>
                <div className="space-y-1.5 px-1">
                  <div className="text-[10px] uppercase tracking-wide text-gray-400 dark:text-gray-500 font-medium px-2">
                    Try asking
                  </div>
                  {suggestions.map((q, i) => (
                    <button
                      key={i}
                      type="button"
                      onClick={() => ask(q)}
                      className="w-full text-left text-sm px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-db-navy-900 dark:text-gray-200 hover:border-db-red hover:bg-db-red/5 dark:hover:bg-db-red/10 transition-colors flex items-center gap-2"
                    >
                      <Sparkles className="w-3.5 h-3.5 text-db-red flex-shrink-0 opacity-60" />
                      <span className="truncate">{q}</span>
                    </button>
                  ))}
                </div>
              </div>
            )}
            {messages.map((m, i) => {
              const prev = messages[i - 1]
              const previousQuestion = prev?.kind === 'user' ? prev.text : undefined
              return (
                <MessageBubble
                  key={i}
                  message={m}
                  previousQuestion={previousQuestion}
                  onSuggestion={ask}
                  onRetry={ask}
                />
              )
            })}
          </div>
          {!atBottom && messages.length > 0 && (
            <button
              type="button"
              onClick={() => {
                const el = scrollRef.current
                if (el) el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' })
              }}
              className="absolute bottom-3 left-1/2 -translate-x-1/2 px-3 py-1 rounded-full text-xs bg-db-red text-white shadow-md hover:bg-db-red/90 flex items-center gap-1"
            >
              <ChevronDown className="w-3 h-3" />
              Jump to latest
            </button>
          )}
        </div>

        {/* Input */}
        <form
          onSubmit={e => { e.preventDefault(); ask(input) }}
          className="flex gap-2 p-3 border-t border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 flex-shrink-0"
        >
          <input
            ref={inputRef}
            type="text"
            value={input}
            onChange={e => setInput(e.target.value)}
            placeholder={busy ? 'Genie is thinking…' : 'Ask Genie…'}
            disabled={busy}
            className="flex-1 px-3 py-2 text-sm rounded-md border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-900 text-db-navy-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-db-red/30 focus:border-db-red disabled:opacity-50"
          />
          {busy ? (
            <button
              type="button"
              onClick={cancel}
              title="Stop (Esc)"
              aria-label="Stop generating"
              className="px-3 py-2 rounded-md bg-gray-700 text-white text-sm hover:bg-gray-800 flex items-center gap-1"
            >
              <Square className="w-3.5 h-3.5 fill-current" />
            </button>
          ) : (
            <button
              type="submit"
              disabled={!input.trim()}
              title="Send (Enter)"
              aria-label="Send"
              className="px-3 py-2 rounded-md bg-db-red text-white text-sm hover:bg-db-red/90 disabled:opacity-50 flex items-center gap-1"
            >
              <Send className="w-4 h-4" />
            </button>
          )}
        </form>
      </div>
    </>
  )
}

function MessageBubble({
  message,
  previousQuestion,
  onSuggestion,
  onRetry,
}: {
  message: ChatMessage
  previousQuestion?: string
  onSuggestion: (q: string) => void
  onRetry: (q: string) => void
}) {
  const [showSql, setShowSql] = useState(false)

  if (message.kind === 'user') {
    return (
      <div className="flex justify-end">
        <div className="max-w-[85%] px-3 py-2 rounded-lg bg-db-red text-white text-sm whitespace-pre-wrap break-words">
          {message.text}
        </div>
      </div>
    )
  }

  // Genie message
  if (message.status === 'loading') {
    return (
      <div className="flex justify-start">
        <div className="max-w-[85%] px-3 py-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-sm text-gray-500 dark:text-gray-400">
          <span className="inline-flex gap-1">
            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-pulse" />
            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-pulse" style={{ animationDelay: '0.2s' }} />
            <span className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-pulse" style={{ animationDelay: '0.4s' }} />
          </span>
        </div>
      </div>
    )
  }

  if (message.status === 'error') {
    return (
      <div className="flex justify-start w-full">
        <div className="max-w-[95%] w-full px-3 py-2 rounded-lg bg-red-50 dark:bg-red-950/40 border border-red-200 dark:border-red-800 text-sm text-red-700 dark:text-red-300">
          <div className="flex items-start gap-2">
            <AlertCircle className="w-4 h-4 flex-shrink-0 mt-0.5" />
            <div className="flex-1 min-w-0">
              <div title={message.error}>{humanizeError(message.error)}</div>
              {previousQuestion && (
                <button
                  type="button"
                  onClick={() => onRetry(previousQuestion)}
                  className="mt-2 inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md bg-white dark:bg-gray-900 border border-red-200 dark:border-red-800 text-red-700 dark:text-red-300 hover:bg-red-100 dark:hover:bg-red-950 transition-colors"
                >
                  <RotateCcw className="w-3 h-3" />
                  Try again
                </button>
              )}
            </div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex justify-start">
      <div className="max-w-[95%] space-y-2 w-full">
        {message.text && (
          <div className="px-3 py-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-db-navy-900 dark:text-gray-100 break-words">
            <Markdown text={message.text} />
          </div>
        )}
        {message.result && message.result.rows.length > 0 && (
          <ResultView result={message.result} />
        )}
        {message.result && message.result.rows.length === 0 && message.sql && (
          <div className="px-3 py-3 rounded-lg border border-dashed border-gray-300 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/40 text-sm text-gray-600 dark:text-gray-400 flex items-start gap-2">
            <Inbox className="w-4 h-4 flex-shrink-0 mt-0.5 opacity-70" />
            <div>
              <div className="font-medium text-gray-700 dark:text-gray-300">No results</div>
              <div className="text-xs mt-0.5">The query ran but returned no rows. Try a wider time range or different filter.</div>
            </div>
          </div>
        )}
        {message.sql && (
          <details
            open={showSql}
            onToggle={e => setShowSql((e.target as HTMLDetailsElement).open)}
            className="text-xs"
          >
            <summary className="cursor-pointer list-none inline-flex items-center gap-1 text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200">
              {showSql ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
              {showSql ? 'Hide SQL' : 'Show SQL'}
            </summary>
            <pre className="mt-1 p-2 rounded bg-gray-100 dark:bg-gray-900 border border-gray-200 dark:border-gray-700 text-[11px] text-gray-700 dark:text-gray-300 overflow-x-auto whitespace-pre-wrap break-words">
              {message.sql}
            </pre>
          </details>
        )}
        {message.suggestions && message.suggestions.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {message.suggestions.slice(0, 4).map((q, i) => (
              <button
                key={i}
                type="button"
                onClick={() => onSuggestion(q)}
                className="text-[11px] px-2 py-1 rounded-full border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
              >
                {q}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

/* ── Result table ──────────────────────────────────────────────── */

const MAX_PREVIEW_ROWS = 20

function ResultTable({ result }: { result: GenieQueryResult }) {
  const [showAll, setShowAll] = useState(false)
  const total = result.total_row_count ?? result.rows.length
  const visible = showAll ? result.rows : result.rows.slice(0, MAX_PREVIEW_ROWS)
  const hidden = result.rows.length - visible.length

  // Per-column metadata: numeric (right-align + compact), currency, percent, date.
  const colMeta = result.columns.map(c => {
    const t = (c.type_text || '').toUpperCase()
    const numeric = /INT|LONG|BIGINT|DOUBLE|FLOAT|DECIMAL|NUMERIC|SHORT|BYTE/.test(t)
    const date = isDateColumn(t)
    return {
      numeric,
      date,
      currency: numeric && isCurrencyColumn(c.name),
      percent: numeric && isPercentColumn(c.name),
    }
  })

  /** Returns [compactDisplay, fullPrecisionTitle] for a cell. */
  const formatCell = (
    v: any,
    meta: { numeric: boolean; date: boolean; currency: boolean; percent: boolean },
  ): [React.ReactNode, string] => {
    if (v == null) return [<span className="text-gray-400">—</span>, '']
    if (meta.date) {
      return [formatDateShort(v), formatDateFull(v)]
    }
    if (!meta.numeric) {
      const s = String(v)
      return [s, s]
    }
    const n = typeof v === 'number' ? v : Number(v)
    if (!Number.isFinite(n)) return [String(v), String(v)]
    const suffix = meta.percent ? '%' : ''
    const compact = formatCompact(n, { currency: meta.currency }) + suffix
    const full = formatFull(n, { currency: meta.currency }) + suffix
    return [compact, full]
  }

  return (
    <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="bg-gray-50 dark:bg-gray-900 text-gray-600 dark:text-gray-300 border-b border-gray-200 dark:border-gray-700">
              {result.columns.map((c, i) => (
                <th
                  key={i}
                  className={`px-2 py-1.5 font-medium whitespace-nowrap ${colMeta[i].numeric ? 'text-right' : 'text-left'}`}
                  title={c.type_text || ''}
                >
                  {c.name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visible.map((row, ri) => (
              <tr key={ri} className="border-b border-gray-100 dark:border-gray-800 last:border-b-0">
                {row.map((cell: any, ci: number) => {
                  const meta = colMeta[ci]
                  const [display, title] = formatCell(cell, meta)
                  return (
                    <td
                      key={ci}
                      className={`px-2 py-1 ${meta.numeric ? 'text-right font-mono tabular-nums' : 'text-left'} text-db-navy-900 dark:text-gray-200 max-w-[240px] truncate`}
                      title={title}
                    >
                      {display}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="px-2 py-1 text-[10px] text-gray-500 dark:text-gray-400 bg-gray-50 dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700 flex items-center justify-between">
        <span>
          {visible.length} of {total.toLocaleString()} row{total === 1 ? '' : 's'}
          {result.truncated && ' (server-truncated)'}
        </span>
        {hidden > 0 && (
          <button
            type="button"
            onClick={() => setShowAll(true)}
            className="text-db-red hover:underline"
          >
            Show {hidden} more
          </button>
        )}
      </div>
    </div>
  )
}

/* ── Result view (chart-or-table) ──────────────────────────────── */

type ChartSpec =
  | { kind: 'line'; xCol: string; yCol: string }
  | { kind: 'line-multi'; dateCol: string; seriesCol: string; valueCol: string }
  | { kind: 'bar'; xCol: string; yCol: string }
  | { kind: 'stacked-bar'; xCol: string; seriesCol: string; valueCol: string }
  | { kind: 'donut'; xCol: string; yCol: string }

const BAR_TOPN_THRESHOLD = 25
const BAR_TOPN_KEEP = 15
const DONUT_MAX_SLICES = 6

const isDate = (t: string) => /DATE|TIMESTAMP/.test(t)
const isNumeric = (t: string) =>
  /INT|LONG|BIGINT|DOUBLE|FLOAT|DECIMAL|NUMERIC|SHORT|BYTE/.test(t)
const isString = (t: string) => !t || /STRING|VARCHAR|CHAR/.test(t)

function detectChart(result: GenieQueryResult): ChartSpec | null {
  const types = result.columns.map(c => (c.type_text || '').toUpperCase())

  // 2-column shapes
  if (result.columns.length === 2) {
    if (isDate(types[0]) && isNumeric(types[1])) {
      return { kind: 'line', xCol: result.columns[0].name, yCol: result.columns[1].name }
    }
    if (isString(types[0]) && isNumeric(types[1])) {
      // Small categorical share → donut. Otherwise bar (top-N'd as needed).
      if (result.rows.length <= DONUT_MAX_SLICES) {
        return { kind: 'donut', xCol: result.columns[0].name, yCol: result.columns[1].name }
      }
      return { kind: 'bar', xCol: result.columns[0].name, yCol: result.columns[1].name }
    }
  }

  // 3-column shapes
  if (result.columns.length === 3) {
    // (date, category, value) → multi-series line
    if (isDate(types[0]) && isString(types[1]) && isNumeric(types[2])) {
      return {
        kind: 'line-multi',
        dateCol: result.columns[0].name,
        seriesCol: result.columns[1].name,
        valueCol: result.columns[2].name,
      }
    }
    // (category, category, value) → stacked bar
    if (isString(types[0]) && isString(types[1]) && isNumeric(types[2])) {
      return {
        kind: 'stacked-bar',
        xCol: result.columns[0].name,
        seriesCol: result.columns[1].name,
        valueCol: result.columns[2].name,
      }
    }
  }

  return null
}

const toNum = (v: any): number => {
  if (v == null) return 0
  if (typeof v === 'number') return v
  const n = Number(v)
  return Number.isFinite(n) ? n : 0
}

function buildLineData(result: GenieQueryResult) {
  return result.rows
    .filter(r => r[0] != null && r[1] != null)
    .map(r => ({ timestamp: String(r[0]), value: toNum(r[1]) }))
}

function buildBarData(result: GenieQueryResult) {
  const all = result.rows
    .filter(r => r[0] != null && r[1] != null)
    .map(r => {
      const raw = String(r[0])
      return {
        name: raw.length > 20 ? raw.slice(0, 18) + '…' : raw,
        value: toNum(r[1]),
      }
    })
  // Sort desc by value so top-N is the top
  all.sort((a, b) => b.value - a.value)
  if (all.length > BAR_TOPN_THRESHOLD) {
    const top = all.slice(0, BAR_TOPN_KEEP)
    const otherTotal = all.slice(BAR_TOPN_KEEP).reduce((s, r) => s + r.value, 0)
    return [...top, { name: `Other (${all.length - BAR_TOPN_KEEP})`, value: otherTotal }]
  }
  return all
}

/** Pivot 3-col rows [(date, category, value), ...] into per-date records with
 *  one numeric column per category. Returns the chart data + series labels. */
function buildMultiLineData(result: GenieQueryResult): { data: any[]; series: string[] } {
  const pivot = new Map<string, Record<string, number>>()
  const seriesSet = new Set<string>()
  for (const r of result.rows) {
    if (r[0] == null || r[1] == null || r[2] == null) continue
    const date = String(r[0])
    const cat = String(r[1])
    if (!pivot.has(date)) pivot.set(date, {})
    pivot.get(date)![cat] = toNum(r[2])
    seriesSet.add(cat)
  }
  const series = Array.from(seriesSet).sort()
  const data = Array.from(pivot.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, vals]) => ({
      timestamp: date,
      ...Object.fromEntries(series.map(s => [s, vals[s] ?? 0])),
    }))
  return { data, series }
}

/** Pivot 3-col rows [(xCategory, seriesCategory, value), ...] into per-X
 *  records with one numeric column per series. Caps series cardinality so we
 *  don't render an unreadable mass of stack segments. */
const STACK_MAX_SERIES = 8
function buildStackedBarData(result: GenieQueryResult): { data: any[]; series: string[] } {
  // First pass: total each series so we can keep the largest contributors.
  const seriesTotals = new Map<string, number>()
  for (const r of result.rows) {
    if (r[0] == null || r[1] == null || r[2] == null) continue
    const cat = String(r[1])
    seriesTotals.set(cat, (seriesTotals.get(cat) ?? 0) + toNum(r[2]))
  }
  const ranked = Array.from(seriesTotals.entries()).sort(([, a], [, b]) => b - a)
  const keep = new Set(ranked.slice(0, STACK_MAX_SERIES).map(([s]) => s))
  const hasOther = ranked.length > STACK_MAX_SERIES
  const series = [...ranked.slice(0, STACK_MAX_SERIES).map(([s]) => s)]
  if (hasOther) series.push('Other')

  // Second pass: pivot + bucket the long tail into 'Other'.
  const pivot = new Map<string, Record<string, number>>()
  for (const r of result.rows) {
    if (r[0] == null || r[1] == null || r[2] == null) continue
    const x = String(r[0])
    const cat = String(r[1])
    const v = toNum(r[2])
    if (!pivot.has(x)) pivot.set(x, {})
    const row = pivot.get(x)!
    const key = keep.has(cat) ? cat : 'Other'
    row[key] = (row[key] ?? 0) + v
  }
  // Sort X by total descending so the biggest stacks are leftmost.
  const data = Array.from(pivot.entries())
    .map(([name, vals]) => ({
      name: name.length > 20 ? name.slice(0, 18) + '…' : name,
      total: Object.values(vals).reduce((s, v) => s + v, 0),
      ...Object.fromEntries(series.map(s => [s, vals[s] ?? 0])),
    }))
    .sort((a, b) => b.total - a.total)
    .slice(0, 20) // also cap X cardinality
  return { data, series }
}

/** 2-col (string, numeric) → donut-friendly {name, value} array. */
function buildDonutData(result: GenieQueryResult): Array<{ name: string; value: number }> {
  return result.rows
    .filter(r => r[0] != null && r[1] != null)
    .map(r => ({
      name: String(r[0]).length > 24 ? String(r[0]).slice(0, 22) + '…' : String(r[0]),
      value: toNum(r[1]),
    }))
}

function ResultView({ result }: { result: GenieQueryResult }) {
  const chart = detectChart(result)
  const [view, setView] = useState<'chart' | 'table'>(chart ? 'chart' : 'table')

  // Build chart data once (cheap, small)
  let chartContent: React.ReactNode = null
  if (chart) {
    if (chart.kind === 'line') {
      const fmt = makeValueFormatter(chart.yCol)
      const fullFmt = makeValueFormatter(chart.yCol, { compact: false })
      chartContent = (
        <LineChart
          data={buildLineData(result) as any}
          dataKey="value"
          name={chart.yCol}
          height={200}
          valueFormatter={fmt}
          tooltipFormatter={fullFmt}
        />
      )
    } else if (chart.kind === 'bar') {
      const fmt = makeValueFormatter(chart.yCol)
      const fullFmt = makeValueFormatter(chart.yCol, { compact: false })
      chartContent = (
        <BarChart
          data={buildBarData(result) as any}
          dataKey="value"
          nameKey="name"
          height={200}
          valueFormatter={fmt}
          tooltipFormatter={fullFmt}
        />
      )
    } else if (chart.kind === 'line-multi') {
      const { data, series } = buildMultiLineData(result)
      const [primary, ...rest] = series
      const seriesProp = Object.fromEntries(rest.map(s => [s, s]))
      const fmt = makeValueFormatter(chart.valueCol)
      const fullFmt = makeValueFormatter(chart.valueCol, { compact: false })
      chartContent = (
        <LineChart
          data={data as any}
          dataKey={primary}
          name={primary}
          series={seriesProp}
          height={220}
          valueFormatter={fmt}
          tooltipFormatter={fullFmt}
        />
      )
    } else if (chart.kind === 'stacked-bar') {
      const { data, series } = buildStackedBarData(result)
      const fmt = makeValueFormatter(chart.valueCol)
      const fullFmt = makeValueFormatter(chart.valueCol, { compact: false })
      chartContent = (
        <StackedBarChart
          data={data}
          nameKey="name"
          series={series}
          height={240}
          valueFormatter={fmt}
          tooltipFormatter={fullFmt}
        />
      )
    } else if (chart.kind === 'donut') {
      const fullFmt = makeValueFormatter(chart.yCol, { compact: false })
      chartContent = (
        <PieChart
          data={buildDonutData(result)}
          donut
          height={220}
          valueFormatter={fullFmt}
        />
      )
    }
  }

  return (
    <div className="space-y-1">
      {chart && (
        <div className="flex items-center gap-0.5 text-[11px]">
          <button
            type="button"
            onClick={() => setView('chart')}
            className={`px-2 py-0.5 rounded ${view === 'chart'
              ? 'bg-db-red text-white'
              : 'text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800'}`}
          >
            Chart
          </button>
          <button
            type="button"
            onClick={() => setView('table')}
            className={`px-2 py-0.5 rounded ${view === 'table'
              ? 'bg-db-red text-white'
              : 'text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800'}`}
          >
            Table
          </button>
        </div>
      )}
      {view === 'chart' && chartContent && (
        <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-2">
          {chartContent}
        </div>
      )}
      {view === 'table' && <ResultTable result={result} />}
    </div>
  )
}
