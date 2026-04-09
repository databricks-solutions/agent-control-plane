import { useState, useRef, useEffect, useMemo, useCallback } from 'react'
import {
  usePlaygroundSessions,
  usePlaygroundMessages,
  useSendPlaygroundMessage,
  useDeletePlaygroundSession,
  usePlaygroundEndpoints,
  type PlaygroundSession,
  type PlaygroundMessage,
  type PlaygroundEndpoint,
} from '@/api/hooks'
import { Card } from '@/components/ui/card'
import {
  MessageSquare,
  Send,
  Plus,
  Trash2,
  Bot,
  User,
  AlertCircle,
  Loader2,
  Clock,
  Zap,
  ChevronDown,
  Cpu,
  Sparkles,
  Search,
  AppWindow,
} from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'

/* ── Agent selector dropdown ────────────────────────────────────── */

interface AgentOption {
  endpoint_name: string
  agent_name: string
  type: string
  status: string
  app_url?: string
}

function AgentSelector({
  agents,
  selected,
  onSelect,
  disabled,
}: {
  agents: AgentOption[]
  selected: AgentOption | null
  onSelect: (agent: AgentOption) => void
  disabled?: boolean
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const filtered = useMemo(
    () =>
      agents.filter(
        (a) =>
          a.agent_name.toLowerCase().includes(search.toLowerCase()) ||
          a.endpoint_name.toLowerCase().includes(search.toLowerCase()),
      ),
    [agents, search],
  )

  const typeIcon = (type: string) => {
    switch (type) {
      case 'custom_model':
        return <Cpu className="w-3.5 h-3.5" />
      case 'external_model':
        return <Sparkles className="w-3.5 h-3.5" />
      case 'app':
        return <AppWindow className="w-3.5 h-3.5" />
      default:
        return <Bot className="w-3.5 h-3.5" />
    }
  }

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => !disabled && setOpen(!open)}
        disabled={disabled}
        className="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600
          bg-white dark:bg-gray-700 text-sm font-medium text-gray-700 dark:text-gray-200
          hover:bg-gray-50 dark:hover:bg-gray-600 transition-colors min-w-[260px]
          disabled:opacity-50 disabled:cursor-not-allowed"
      >
        {selected ? (
          <>
            {typeIcon(selected.type)}
            <span className="truncate">{selected.agent_name}</span>
            <span className="text-xs text-gray-400 dark:text-gray-500 truncate ml-auto">
              {selected.endpoint_name}
            </span>
          </>
        ) : (
          <>
            <Bot className="w-4 h-4 text-gray-400" />
            <span className="text-gray-400">Select an agent…</span>
          </>
        )}
        <ChevronDown className="w-4 h-4 text-gray-400 ml-1 flex-shrink-0" />
      </button>

      {open && (
        <div className="absolute top-full left-0 mt-1 w-full min-w-[340px] z-50 bg-white dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded-lg shadow-xl overflow-hidden">
          <div className="p-2 border-b border-gray-100 dark:border-gray-600">
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" />
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search agents…"
                autoFocus
                className="w-full pl-8 pr-3 py-1.5 text-sm bg-gray-50 dark:bg-gray-600 border-0 rounded-md
                  text-gray-700 dark:text-gray-200 placeholder-gray-400 dark:placeholder-gray-500
                  focus:outline-none focus:ring-1 focus:ring-db-red"
              />
            </div>
          </div>
          <div className="max-h-64 overflow-y-auto">
            {filtered.length === 0 ? (
              <div className="px-3 py-4 text-sm text-gray-400 text-center">No matching agents</div>
            ) : (
              filtered.map((a) => (
                <button
                  key={a.endpoint_name}
                  onClick={() => {
                    onSelect(a)
                    setOpen(false)
                    setSearch('')
                  }}
                  className={`w-full flex items-center gap-2.5 px-3 py-2 text-left text-sm
                    hover:bg-gray-50 dark:hover:bg-gray-600 transition-colors
                    ${selected?.endpoint_name === a.endpoint_name ? 'bg-red-50 dark:bg-red-900/20' : ''}`}
                >
                  {typeIcon(a.type)}
                  <div className="flex-1 min-w-0">
                    <div className="font-medium text-gray-800 dark:text-gray-100 truncate">{a.agent_name}</div>
                    <div className="text-xs text-gray-400 dark:text-gray-500 truncate">{a.endpoint_name}</div>
                  </div>
                  <span
                    className={`text-[10px] font-medium px-1.5 py-0.5 rounded-full ${
                      ['READY', 'ONLINE', 'ACTIVE', 'RUNNING'].includes(a.status.toUpperCase())
                        ? 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-400'
                        : 'bg-gray-100 text-gray-500 dark:bg-gray-600 dark:text-gray-400'
                    }`}
                  >
                    {a.status}
                  </span>
                </button>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  )
}

/* ── Chat message bubble ────────────────────────────────────────── */

function ChatBubble({ message }: { message: PlaygroundMessage }) {
  const isUser = message.role === 'user'
  const isError = message.role === 'error'

  return (
    <div className={`flex gap-3 ${isUser ? 'flex-row-reverse' : ''}`}>
      {/* Avatar */}
      <div
        className={`w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 ${
          isUser
            ? 'bg-db-red/10 text-db-red dark:bg-db-red/20'
            : isError
              ? 'bg-red-100 text-red-600 dark:bg-red-900/30 dark:text-red-400'
              : 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400'
        }`}
      >
        {isUser ? <User className="w-4 h-4" /> : isError ? <AlertCircle className="w-4 h-4" /> : <Bot className="w-4 h-4" />}
      </div>

      {/* Bubble */}
      <div className={`max-w-[75%] ${isUser ? 'text-right' : ''}`}>
        <div
          className={`inline-block px-4 py-2.5 rounded-2xl text-sm leading-relaxed whitespace-pre-wrap ${
            isUser
              ? 'bg-db-red text-white rounded-br-md'
              : isError
                ? 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-800 rounded-bl-md'
                : 'bg-gray-100 dark:bg-gray-700 text-gray-800 dark:text-gray-100 rounded-bl-md'
          }`}
        >
          {message.content || (isError ? 'An error occurred' : '')}
        </div>

        {/* Metadata line for assistant messages */}
        {message.role === 'assistant' && (
          <div className="flex items-center gap-3 mt-1 text-[11px] text-gray-400 dark:text-gray-500">
            {message.model && (
              <span className="flex items-center gap-1">
                <Cpu className="w-3 h-3" />
                {message.model}
              </span>
            )}
            {message.total_tokens != null && (
              <span className="flex items-center gap-1">
                <Zap className="w-3 h-3" />
                {message.total_tokens.toLocaleString()} tokens
                {message.input_tokens != null && message.output_tokens != null && (
                  <span className="text-gray-300 dark:text-gray-600">
                    ({message.input_tokens}→{message.output_tokens})
                  </span>
                )}
              </span>
            )}
            {message.latency_ms != null && (
              <span className="flex items-center gap-1">
                <Clock className="w-3 h-3" />
                {message.latency_ms >= 1000
                  ? `${(message.latency_ms / 1000).toFixed(1)}s`
                  : `${message.latency_ms}ms`}
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

/* ── Empty state ────────────────────────────────────────────────── */

function EmptyState() {
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-center px-8">
      <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-db-red/10 to-db-red/5 dark:from-db-red/20 dark:to-db-red/5 flex items-center justify-center mb-4">
        <MessageSquare className="w-8 h-8 text-db-red" />
      </div>
      <h3 className="text-lg font-semibold text-gray-800 dark:text-gray-100 mb-1">Agent Playground</h3>
      <p className="text-sm text-gray-500 dark:text-gray-400 max-w-sm">
        Select an agent from the dropdown above and start a conversation.
        Your chat history will be saved automatically.
      </p>
    </div>
  )
}

/* ── Main Page Component ────────────────────────────────────────── */

export default function PlaygroundPage() {
  const [activeSessionId, setActiveSessionId] = useState<string | null>(null)
  const [selectedAgent, setSelectedAgent] = useState<AgentOption | null>(null)
  const [inputValue, setInputValue] = useState('')
  const [sidebarSearch, setSidebarSearch] = useState('')

  const messagesEndRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  // Data hooks
  const { data: sessions } = usePlaygroundSessions()
  const { data: sessionDetail } = usePlaygroundMessages(activeSessionId)
  const { data: endpoints } = usePlaygroundEndpoints()
  const sendMessage = useSendPlaygroundMessage()
  const deleteSession = useDeletePlaygroundSession()

  // Build agent options from the permission-filtered backend endpoint
  const agentOptions: AgentOption[] = useMemo(() => {
    if (!endpoints) return []
    return endpoints.map((ep: PlaygroundEndpoint) => ({
      endpoint_name: ep.endpoint_name,
      agent_name: ep.agent_name || ep.endpoint_name,
      type: ep.type || 'unknown',
      status: ep.status || 'READY',
      app_url: ep.app_url,
    }))
  }, [endpoints])

  // Messages for the active session
  const messages: PlaygroundMessage[] = useMemo(() => sessionDetail?.messages || [], [sessionDetail])

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, sendMessage.isPending])

  // When switching sessions, update the agent selector to match
  useEffect(() => {
    if (sessionDetail && agentOptions.length) {
      const match = agentOptions.find((a) => a.endpoint_name === sessionDetail.endpoint_name)
      if (match) setSelectedAgent(match)
    }
  }, [sessionDetail, agentOptions])

  // Auto-resize textarea
  const autoResize = useCallback(() => {
    const ta = textareaRef.current
    if (ta) {
      ta.style.height = 'auto'
      ta.style.height = `${Math.min(ta.scrollHeight, 200)}px`
    }
  }, [])

  // Send handler
  const handleSend = useCallback(() => {
    if (!inputValue.trim() || !selectedAgent || sendMessage.isPending) return

    sendMessage.mutate(
      {
        endpoint_name: selectedAgent.endpoint_name,
        agent_name: selectedAgent.agent_name,
        session_id: activeSessionId,
        message: inputValue.trim(),
        app_url: selectedAgent.app_url ?? null,
      },
      {
        onSuccess: (data) => {
          if (!activeSessionId) {
            setActiveSessionId(data.session_id)
          }
        },
      },
    )
    setInputValue('')
    // Reset textarea height
    if (textareaRef.current) textareaRef.current.style.height = 'auto'
  }, [inputValue, selectedAgent, activeSessionId, sendMessage])

  // New conversation
  const handleNewChat = useCallback(() => {
    setActiveSessionId(null)
    setInputValue('')
  }, [])

  // Delete session
  const handleDelete = useCallback(
    (sessionId: string, e: React.MouseEvent) => {
      e.stopPropagation()
      if (activeSessionId === sessionId) {
        setActiveSessionId(null)
      }
      deleteSession.mutate(sessionId)
    },
    [activeSessionId, deleteSession],
  )

  // Filter sidebar sessions
  const filteredSessions = useMemo(() => {
    if (!sessions) return []
    if (!sidebarSearch) return sessions
    const q = sidebarSearch.toLowerCase()
    return sessions.filter(
      (s) =>
        s.title?.toLowerCase().includes(q) ||
        s.endpoint_name.toLowerCase().includes(q) ||
        s.agent_name?.toLowerCase().includes(q),
    )
  }, [sessions, sidebarSearch])

  return (
    <div className="flex h-[calc(100vh-8rem)] gap-4">
      {/* ── Session Sidebar ─────────────────────────────────────── */}
      <div className="w-72 flex-shrink-0 flex flex-col bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
        {/* New Chat Button */}
        <div className="p-3 border-b border-gray-100 dark:border-gray-700">
          <button
            onClick={handleNewChat}
            className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg text-sm font-medium
              bg-db-red text-white hover:bg-db-red/90 transition-colors shadow-sm"
          >
            <Plus className="w-4 h-4" />
            New Chat
          </button>
        </div>

        {/* Search */}
        <div className="px-3 pt-2 pb-1">
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" />
            <input
              type="text"
              value={sidebarSearch}
              onChange={(e) => setSidebarSearch(e.target.value)}
              placeholder="Search conversations…"
              className="w-full pl-8 pr-3 py-1.5 text-xs bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600
                rounded-md text-gray-700 dark:text-gray-200 placeholder-gray-400 dark:placeholder-gray-500
                focus:outline-none focus:ring-1 focus:ring-db-red"
            />
          </div>
        </div>

        {/* Session List */}
        <div className="flex-1 overflow-y-auto px-2 py-2 space-y-0.5">
          {filteredSessions.length === 0 ? (
            <div className="text-xs text-gray-400 dark:text-gray-500 text-center py-8">
              {sessions?.length ? 'No matches' : 'No conversations yet'}
            </div>
          ) : (
            filteredSessions.map((s: PlaygroundSession) => (
              <button
                key={s.session_id}
                onClick={() => setActiveSessionId(s.session_id)}
                className={`group w-full flex items-start gap-2 px-2.5 py-2 rounded-lg text-left transition-colors ${
                  activeSessionId === s.session_id
                    ? 'bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800'
                    : 'hover:bg-gray-50 dark:hover:bg-gray-700/50'
                }`}
              >
                <MessageSquare
                  className={`w-3.5 h-3.5 mt-0.5 flex-shrink-0 ${
                    activeSessionId === s.session_id ? 'text-db-red' : 'text-gray-400 dark:text-gray-500'
                  }`}
                />
                <div className="flex-1 min-w-0">
                  <div
                    className={`text-xs font-medium truncate ${
                      activeSessionId === s.session_id
                        ? 'text-db-red dark:text-red-300'
                        : 'text-gray-700 dark:text-gray-200'
                    }`}
                  >
                    {s.title || 'Untitled'}
                  </div>
                  <div className="text-[10px] text-gray-400 dark:text-gray-500 truncate mt-0.5">
                    {s.agent_name || s.endpoint_name}
                  </div>
                  <div className="text-[10px] text-gray-300 dark:text-gray-600 mt-0.5">
                    {formatDistanceToNow(new Date(s.updated_at), { addSuffix: true })}
                  </div>
                </div>
                <button
                  onClick={(e) => handleDelete(s.session_id, e)}
                  className="opacity-0 group-hover:opacity-100 p-1 rounded hover:bg-red-100 dark:hover:bg-red-900/30
                    text-gray-400 hover:text-red-600 dark:hover:text-red-400 transition-all"
                  title="Delete conversation"
                >
                  <Trash2 className="w-3 h-3" />
                </button>
              </button>
            ))
          )}
        </div>

        {/* Session count */}
        {sessions && sessions.length > 0 && (
          <div className="px-3 py-2 border-t border-gray-100 dark:border-gray-700 text-[10px] text-gray-400 dark:text-gray-500">
            {sessions.length} conversation{sessions.length !== 1 ? 's' : ''}
          </div>
        )}
      </div>

      {/* ── Chat Area ───────────────────────────────────────────── */}
      <div className="flex-1 flex flex-col bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
        {/* Top bar: agent selector */}
        <div className="flex items-center gap-3 px-4 py-3 border-b border-gray-100 dark:border-gray-700">
          <AgentSelector
            agents={agentOptions}
            selected={selectedAgent}
            onSelect={setSelectedAgent}
            disabled={!!activeSessionId}
          />
          {activeSessionId && sessionDetail && (
            <div className="text-xs text-gray-400 dark:text-gray-500 ml-auto">
              Session started{' '}
              {formatDistanceToNow(new Date(sessionDetail.created_at), { addSuffix: true })}
            </div>
          )}
        </div>

        {/* Messages area */}
        <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
          {!activeSessionId && messages.length === 0 ? (
            <EmptyState />
          ) : (
            <>
              {messages.map((m) => (
                <ChatBubble key={m.message_id} message={m} />
              ))}

              {/* Typing indicator while waiting for response */}
              {sendMessage.isPending && (
                <div className="flex gap-3">
                  <div className="w-8 h-8 rounded-full bg-green-100 dark:bg-green-900/30 flex items-center justify-center flex-shrink-0">
                    <Bot className="w-4 h-4 text-green-700 dark:text-green-400" />
                  </div>
                  <div className="inline-flex items-center gap-1.5 px-4 py-2.5 rounded-2xl rounded-bl-md bg-gray-100 dark:bg-gray-700">
                    <Loader2 className="w-4 h-4 text-gray-400 animate-spin" />
                    <span className="text-sm text-gray-400">Thinking…</span>
                  </div>
                </div>
              )}

              <div ref={messagesEndRef} />
            </>
          )}
        </div>

        {/* Input area */}
        <div className="border-t border-gray-100 dark:border-gray-700 px-4 py-3">
          {!selectedAgent && (
            <div className="text-xs text-amber-600 dark:text-amber-400 mb-2 flex items-center gap-1.5">
              <AlertCircle className="w-3.5 h-3.5" />
              Select an agent above to start chatting
            </div>
          )}
          <div className="flex items-end gap-2">
            <textarea
              ref={textareaRef}
              value={inputValue}
              onChange={(e) => {
                setInputValue(e.target.value)
                autoResize()
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault()
                  handleSend()
                }
              }}
              placeholder={selectedAgent ? `Message ${selectedAgent.agent_name}…` : 'Select an agent to start…'}
              disabled={!selectedAgent || sendMessage.isPending}
              rows={1}
              className="flex-1 resize-none px-4 py-2.5 rounded-xl border border-gray-200 dark:border-gray-600
                bg-gray-50 dark:bg-gray-700 text-sm text-gray-800 dark:text-gray-100
                placeholder-gray-400 dark:placeholder-gray-500
                focus:outline-none focus:ring-2 focus:ring-db-red/30 focus:border-db-red
                disabled:opacity-50 disabled:cursor-not-allowed
                transition-colors"
            />
            <button
              onClick={handleSend}
              disabled={!inputValue.trim() || !selectedAgent || sendMessage.isPending}
              className="flex-shrink-0 w-10 h-10 rounded-xl bg-db-red text-white flex items-center justify-center
                hover:bg-db-red/90 disabled:opacity-40 disabled:cursor-not-allowed
                transition-colors shadow-sm"
            >
              {sendMessage.isPending ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Send className="w-4 h-4" />
              )}
            </button>
          </div>
          <div className="mt-1.5 text-[10px] text-gray-300 dark:text-gray-600">
            Press Enter to send · Shift+Enter for new line
          </div>
        </div>
      </div>
    </div>
  )
}
