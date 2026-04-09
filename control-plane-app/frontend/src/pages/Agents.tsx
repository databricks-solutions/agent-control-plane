import { useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { useQueryClient, useIsFetching } from '@tanstack/react-query'
import { usePinnedAgents } from '@/lib/usePinnedAgents'
import {
  useAllAgentsMerged,
  useDiscoveryStatus,
  useSyncAgents,
  useGatewayPageData,
  useGatewayUsageSummary,
  useGatewayUsageTimeseries,
  useGatewayUsageByUser,
} from '@/api/hooks'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { StatusBadge } from '@/components/StatusBadge'
import { KpiCard } from '@/components/KpiCard'
import { SortableHeader, useSort, sortRows } from '@/components/SortableTable'
import { TablePagination } from '@/components/TablePagination'
import { LineChart } from '@/components/charts/LineChart'
import { BarChart } from '@/components/charts/BarChart'
import { DB_CHART } from '@/lib/brand'
import TopologyTab from '@/components/TopologyTab'

import OperationsRTPage from './OperationsRT'
import { RefreshButton } from '@/components/RefreshButton'
import {
  Bot,
  Server,
  Wrench,
  Globe,
  ExternalLink,
  ChevronDown,
  ChevronRight,
  AppWindow,
  Search,
  Sparkles,
  Brain,
  Network,
  Activity,
  LayoutDashboard,

  Pin,
  PinOff,
  Cpu,
  ScanText,
} from 'lucide-react'

/* ── tabs ─────────────────────────────────────────────────────── */

const TABS = [
  { key: 'overview',      label: 'Overview',   icon: LayoutDashboard },
  { key: 'operations',    label: 'Metrics',    icon: Activity },
  { key: 'operations_rt', label: 'Operations', icon: Server },
  { key: 'topology',      label: 'Topology',   icon: Network },

] as const

type TabKey = (typeof TABS)[number]['key']

/* ── main page ───────────────────────────────────────────────── */

export default function AgentsPage() {
  const [tab, setTab] = useState<TabKey>('overview')

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Agents</h2>
        <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
          Auto-discovered agents across workspaces — inventory, health & performance
        </p>
      </div>

      {/* Tab bar */}
      <div className="border-b dark:border-gray-700 flex gap-1 overflow-x-auto">
        {TABS.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
              tab === key
                ? 'border-red-500 text-red-600 dark:text-red-400'
                : 'border-transparent text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300'
            }`}
          >
            <Icon className="w-4 h-4" />
            {label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === 'overview' && <OverviewTab />}
      {tab === 'operations' && <OperationsTab />}
      {tab === 'operations_rt' && <OperationsRTPage />}
      {tab === 'topology' && <TopologyTab />}
    </div>
  )
}

/* ══════════════════════════════════════════════════════════════
   Overview Tab (formerly the Agents page)
   ══════════════════════════════════════════════════════════════ */

const ALL_WORKSPACES = '__all__'

function resolveAgentType(agent: any): string {
  const ep = (agent.endpoint_name || '').toLowerCase()
  if (ep.startsWith('kie')) return 'information_extraction'
  if (ep.startsWith('mas')) return 'multi_agent_supervisor'
  if (ep.startsWith('ka')) return 'knowledge_assistant'
  return agent.type || 'unknown'
}

function OverviewTab() {
  const [workspaceId, setWorkspaceId] = useState<string>(ALL_WORKSPACES)
  const wsParam = workspaceId === ALL_WORKSPACES ? undefined : workspaceId

  const { data: agents, isLoading } = useAllAgentsMerged(wsParam)
  const { data: discoveryStatus } = useDiscoveryStatus()
  const syncAgents = useSyncAgents()
  const { pinned, togglePin } = usePinnedAgents()

  const [expandedAgent, setExpandedAgent] = useState<string | null>(null)
  const [searchQuery, setSearchQuery] = useState('')
  const [typeFilter, setTypeFilter] = useState<string>('all')
  const [statusFilter, setStatusFilter] = useState<string>('all')
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)

  const agentList = useMemo(
    () => (agents || []).map((a: any) => ({ ...a, type: resolveAgentType(a) })),
    [agents]
  )

  // Search filter — computed before any early returns (hooks rules)
  const q = searchQuery.toLowerCase().trim()
  const filteredAgents = useMemo(() => {
    let filtered = agentList
    if (q) {
      filtered = filtered.filter((a: any) => {
        const haystack = [a.name, a.description, a.endpoint_name, a.type, a.creator, a.model_name, a.workspace_id]
          .filter(Boolean).join(' ').toLowerCase()
        return haystack.includes(q)
      })
    }
    if (typeFilter !== 'all') {
      filtered = filtered.filter((a: any) => a.type === typeFilter)
    }
    if (statusFilter !== 'all') {
      const up = statusFilter.toUpperCase()
      filtered = filtered.filter((a: any) =>
        (a.endpoint_status || '').toUpperCase() === up
      )
    }
    // Sort priority:
    //  0 = manually pinned
    //  1 = workspace-native (source = api | user_api)
    //  2 = everything else (system_table, audit_log, …)
    const tier = (a: any) => {
      if (pinned.has(a.agent_id || a.name)) return 0
      const src = (a.source || '').toLowerCase()
      if (src === 'api' || src === 'user_api') return 1
      return 2
    }
    return [...filtered].sort((a: any, b: any) => tier(a) - tier(b))
  }, [agentList, q, typeFilter, statusFilter, pinned])

  if (isLoading) {
    return <div className="flex items-center justify-center h-64 text-gray-400 dark:text-gray-500">Loading agents…</div>
  }

  // Derive workspace options from agents
  const workspaceSet = new Set<string>()
  agentList.forEach((a: any) => { if (a.workspace_id) workspaceSet.add(a.workspace_id) })
  const workspaces = Array.from(workspaceSet).sort()

  // Derive type options from agents present in the list
  const typeSet = new Set<string>()
  agentList.forEach((a: any) => { if (a.type) typeSet.add(a.type) })
  const typeOptions = Array.from(typeSet).sort()

  // Derive status options
  const statusSet = new Set<string>()
  agentList.forEach((a: any) => { if (a.endpoint_status) statusSet.add(a.endpoint_status.toUpperCase()) })
  const statusOptions = Array.from(statusSet).sort()

  const byType: Record<string, number> = {}
  agentList.forEach((a: any) => {
    byType[a.type || 'unknown'] = (byType[a.type || 'unknown'] || 0) + 1
  })

  const onlineCount = agentList.filter((a: any) =>
    ['ONLINE', 'ACTIVE', 'READY', 'RUNNING'].includes((a.endpoint_status || '').toUpperCase()),
  ).length

  // Pagination
  const totalPages = Math.max(1, Math.ceil(filteredAgents.length / pageSize))
  const safePage = Math.min(page, totalPages - 1)
  const pagedAgents = filteredAgents.slice(safePage * pageSize, (safePage + 1) * pageSize)

  const getPlatformLabel = (t: string) => {
    const map: Record<string, string> = {
      custom_agent: 'Custom Agent (Endpoint)',
      custom_app: 'Custom Agent (App)',
      custom_llm: 'Custom LLM',
      external_agent: 'External Agent',
      genie_space: 'Genie Space',
      information_extraction: 'Information Extraction',
      knowledge_assistant: 'Knowledge Assistant',
      multi_agent_supervisor: 'Multi-Agent Supervisor',
    }
    return map[t] || t
  }

  const getTypeIcon = (t: string) => {
    switch (t) {
      case 'custom_agent':
        return <Server className="w-4 h-4" />
      case 'custom_app':
        return <AppWindow className="w-4 h-4" />
      case 'custom_llm':
        return <Cpu className="w-4 h-4" />
      case 'external_agent':
        return <Globe className="w-4 h-4" />
      case 'genie_space':
        return <Sparkles className="w-4 h-4" />
      case 'information_extraction':
        return <ScanText className="w-4 h-4" />
      case 'knowledge_assistant':
        return <Brain className="w-4 h-4" />
      case 'multi_agent_supervisor':
        return <Network className="w-4 h-4" />
      default:
        return <Bot className="w-4 h-4" />
    }
  }

  const getTypeBgColor = (t: string) => {
    switch (t) {
      case 'custom_agent':
        return 'bg-blue-50 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400'
      case 'custom_app':
        return 'bg-purple-50 text-purple-600 dark:bg-purple-900/30 dark:text-purple-400'
      case 'custom_llm':
        return 'bg-sky-50 text-sky-600 dark:bg-sky-900/30 dark:text-sky-400'
      case 'external_agent':
        return 'bg-amber-50 text-amber-600 dark:bg-amber-900/30 dark:text-amber-400'
      case 'genie_space':
        return 'bg-emerald-50 text-emerald-600 dark:bg-emerald-900/30 dark:text-emerald-400'
      case 'information_extraction':
        return 'bg-orange-50 text-orange-600 dark:bg-orange-900/30 dark:text-orange-400'
      case 'knowledge_assistant':
        return 'bg-indigo-50 text-indigo-600 dark:bg-indigo-900/30 dark:text-indigo-400'
      case 'multi_agent_supervisor':
        return 'bg-rose-50 text-rose-600 dark:bg-rose-900/30 dark:text-rose-400'
      default:
        return 'bg-gray-50 text-gray-600 dark:bg-gray-700/50 dark:text-gray-400'
    }
  }

  const extractTools = (config: any): string[] => {
    if (!config) return []
    const tools: string[] = []
    if (config.tools) {
      if (Array.isArray(config.tools)) tools.push(...config.tools)
      else if (typeof config.tools === 'string') tools.push(config.tools)
    }
    if (config.model_endpoint) tools.push(`LLM: ${config.model_endpoint}`)
    if (config.framework) tools.push(`Framework: ${config.framework}`)
    if (config.pattern) tools.push(`Pattern: ${config.pattern}`)
    if (config.tables && Array.isArray(config.tables)) {
      tools.push(...config.tables.map((t: string) => `Table: ${t}`))
    }
    if (config.warehouse_id) tools.push(`Warehouse: ${config.warehouse_id}`)
    return tools
  }

  const extractSystems = (agent: any): string[] => {
    const systems: string[] = []
    if (agent.type === 'custom_app' && agent.config?.url) {
      systems.push(agent.config.url)
    } else if (agent.endpoint_name) {
      systems.push(agent.endpoint_name)
    }
    if (agent.app_url) systems.push(agent.app_url)
    if (agent.config?.data_sources) {
      if (Array.isArray(agent.config.data_sources)) systems.push(...agent.config.data_sources)
    }
    if (agent.config?.catalog) systems.push(`Catalog: ${agent.config.catalog}`)
    if (agent.config?.source_code_path) systems.push(`Source: ${agent.config.source_code_path}`)
    return systems
  }

  const extractAppResources = (agent: any): { name: string; type: string }[] => {
    if (!agent.config?.resources) return []
    return agent.config.resources
  }

  return (
    <div className="space-y-6">
      {/* Controls */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div className="flex items-center gap-3">
          {/* Search */}
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none" />
            <input
              type="text"
              placeholder="Search agents…"
              value={searchQuery}
              onChange={(e) => {
                setSearchQuery(e.target.value)
                setPage(0)
              }}
              className="pl-8 pr-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-700 dark:text-gray-200 focus:ring-2 focus:ring-db-red/30 focus:border-db-red w-56"
            />
          </div>
          {/* Type filter */}
          <select
            value={typeFilter}
            onChange={(e) => { setTypeFilter(e.target.value); setPage(0) }}
            className="border border-gray-300 dark:border-gray-600 rounded-lg px-3 py-1.5 text-sm bg-white dark:bg-gray-700 dark:text-gray-200 focus:ring-2 focus:ring-db-red/30 focus:border-db-red"
          >
            <option value="all">All Types</option>
            {typeOptions.map((t) => (
              <option key={t} value={t}>{getPlatformLabel(t)}</option>
            ))}
          </select>
          {/* Status filter */}
          <select
            value={statusFilter}
            onChange={(e) => { setStatusFilter(e.target.value); setPage(0) }}
            className="border border-gray-300 dark:border-gray-600 rounded-lg px-3 py-1.5 text-sm bg-white dark:bg-gray-700 dark:text-gray-200 focus:ring-2 focus:ring-db-red/30 focus:border-db-red"
          >
            <option value="all">All Statuses</option>
            {statusOptions.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          {/* Workspace filter */}
          <select
            value={workspaceId}
            onChange={(e) => {
              setWorkspaceId(e.target.value)
              setPage(0)
            }}
            className="border border-gray-300 dark:border-gray-600 rounded-lg px-3 py-1.5 text-sm bg-white dark:bg-gray-700 dark:text-gray-200 focus:ring-2 focus:ring-db-red/30 focus:border-db-red"
          >
            <option value={ALL_WORKSPACES}>All Workspaces</option>
            {workspaces.map((ws) => (
              <option key={ws} value={ws}>{ws}</option>
            ))}
          </select>
          <RefreshButton
            onRefresh={() => syncAgents.mutate()}
            isPending={syncAgents.isPending}
            isRefreshing={discoveryStatus?.is_refreshing}
            lastSynced={discoveryStatus?.last_synced}
            title="Sync agents from Databricks workspace"
          />
        </div>
      </div>

      {/* Discovery status + active filter summary */}
      <div className="flex items-center gap-3 flex-wrap">
        {discoveryStatus && (
          <span className="text-xs text-gray-400 dark:text-gray-500">
            {discoveryStatus.total_discovered} agents discovered
            {discoveryStatus.obo_enabled && (
              <span className="ml-1.5 text-green-600 dark:text-green-400 font-medium"
                    title="Last sync used your user credentials (OBO) — user-owned endpoints are included">
                · OBO active
              </span>
            )}
            {!discoveryStatus.obo_enabled && (
              <span className="ml-1.5 text-amber-500 dark:text-amber-400"
                    title="User authorization not enabled — some user-owned endpoints may be missing. Enable 'User authorization' in the Databricks Apps settings to discover all your endpoints.">
                · SP-only
              </span>
            )}
          </span>
        )}
        {filteredAgents.length !== agentList.length && (
          <span className="text-xs text-db-red font-medium">
            {filteredAgents.length} shown
          </span>
        )}
        {(typeFilter !== 'all' || statusFilter !== 'all' || workspaceId !== ALL_WORKSPACES || searchQuery) && (
          <button
            onClick={() => { setTypeFilter('all'); setStatusFilter('all'); setWorkspaceId(ALL_WORKSPACES); setSearchQuery(''); setPage(0) }}
            className="text-xs text-gray-500 dark:text-gray-400 hover:text-db-red underline"
          >
            Clear filters
          </button>
        )}
      </div>

      {/* KPI Row */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        <KpiCard title="Total Agents" value={agentList.length} format="number" />
        <KpiCard title="Online / Active" value={onlineCount} format="number" />
        <KpiCard title="Workspaces" value={workspaces.length} format="number" />
      </div>

      {/* Distribution by Type */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">By Type</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-2">
            {Object.entries(byType).map(([type, count]) => (
              <button
                key={type}
                onClick={() => { setTypeFilter(t => t === type ? 'all' : type); setPage(0) }}
                className={`flex items-center gap-1.5 rounded-lg px-3 py-2 transition-colors border ${
                  typeFilter === type
                    ? 'border-red-400 bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400'
                    : 'border-transparent bg-gray-50 dark:bg-gray-700/50 hover:border-gray-300 dark:hover:border-gray-500'
                }`}
              >
                {getTypeIcon(type)}
                <span className="text-sm font-medium">{getPlatformLabel(type)}</span>
                <Badge variant="info" className="text-xs ml-1">
                  {count}
                </Badge>
              </button>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Agent Detailed Cards */}
      {filteredAgents.length !== agentList.length && (
        <p className="text-xs text-gray-400 dark:text-gray-500">
          Showing {filteredAgents.length} of {agentList.length} agents
        </p>
      )}
      <div className="space-y-3">
        {pagedAgents.map((agent: any) => {
          const isExpanded = expandedAgent === (agent.agent_id || agent.name)
          const tools = extractTools(agent.config)
          const systems = extractSystems(agent)
          const appResources = extractAppResources(agent)
          const tags = agent.tags || {}

          return (
            <Card key={agent.agent_id || agent.name} className="overflow-hidden">
              {/* Collapsed Row */}
              <button
                onClick={() => setExpandedAgent(isExpanded ? null : agent.agent_id || agent.name)}
                className="w-full text-left px-5 py-4 flex items-center gap-4 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
              >
                <div
                  className={`flex-shrink-0 w-9 h-9 rounded-lg flex items-center justify-center ${getTypeBgColor(agent.type)}`}
                >
                  {getTypeIcon(agent.type)}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="font-semibold text-gray-900 dark:text-gray-100 truncate">{agent.name}</span>
                    <Badge variant="default" className="text-xs">
                      {getPlatformLabel(agent.type || '')}
                    </Badge>
                    {agent.type === 'genie_space' && agent.config?.conversation_count > 0 && (
                      <Badge variant="info" className="text-[10px]">
                        {agent.config.conversation_count} chats · {agent.config.unique_users} users
                      </Badge>
                    )}
                  </div>
                  <div className="text-xs text-gray-400 dark:text-gray-500 mt-0.5 truncate">
                    {agent.description || 'No description'}
                    {agent.workspace_id && agent.workspace_id !== 'current' && (
                      <span className="ml-2 font-mono">ws:{agent.workspace_id}</span>
                    )}
                  </div>
                </div>
                <div className="flex items-center gap-2 flex-shrink-0">
                  <StatusBadge status={agent.endpoint_status || 'UNKNOWN'} />
                  <button
                    onClick={(e) => { e.stopPropagation(); togglePin(agent.agent_id || agent.name) }}
                    className={`p-1 rounded transition-colors ${pinned.has(agent.agent_id || agent.name) ? 'text-db-red' : 'text-gray-300 hover:text-gray-500 dark:text-gray-600 dark:hover:text-gray-400'}`}
                    title={pinned.has(agent.agent_id || agent.name) ? 'Unpin agent' : 'Pin to top'}
                  >
                    {pinned.has(agent.agent_id || agent.name)
                      ? <Pin className="w-3.5 h-3.5 fill-current" />
                      : <Pin className="w-3.5 h-3.5" />
                    }
                  </button>
                  {isExpanded ? (
                    <ChevronDown className="w-4 h-4 text-gray-400" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-gray-400" />
                  )}
                </div>
              </button>

              {/* Expanded Detail */}
              {isExpanded && (
                <div className="border-t dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 px-5 py-4">
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    {/* Info Column */}
                    <div className="space-y-3">
                      <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                        Info
                      </h4>
                      <div className="space-y-1.5 text-sm">
                        <div>
                          <span className="text-gray-400 w-28 inline-block">ID:</span>{' '}
                          <span className="font-mono text-xs">{agent.agent_id}</span>
                        </div>
                        <div>
                          <span className="text-gray-400 w-28 inline-block">Workspace:</span>{' '}
                          <span className="font-mono text-xs">{agent.workspace_id || '—'}</span>
                        </div>
                        <div>
                          <span className="text-gray-400 w-28 inline-block">Model:</span> {agent.model_name || '—'}
                        </div>
                        <div>
                          <span className="text-gray-400 w-28 inline-block">Creator:</span>{' '}
                          {agent.creator || agent.created_by || '—'}
                        </div>
                        <div>
                          <span className="text-gray-400 w-28 inline-block">Endpoint:</span>{' '}
                          {agent.endpoint_name || '—'}
                        </div>
                      </div>
                      {Object.keys(tags).length > 0 && (
                        <div className="pt-1">
                          <div className="text-xs text-gray-400 mb-1">Tags</div>
                          <div className="flex flex-wrap gap-1">
                            {Object.entries(tags).map(([k, v]) => (
                              <Badge key={k} variant="default" className="text-xs">
                                {k}: {String(v)}
                              </Badge>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>

                    {/* Tools & Frameworks Column */}
                    <div className="space-y-3">
                      <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider flex items-center gap-1">
                        <Wrench className="w-3.5 h-3.5" /> Tools & Frameworks
                      </h4>
                      {tools.length > 0 ? (
                        <div className="space-y-1.5">
                          {tools.map((t, i) => (
                            <div key={i} className="flex items-center gap-2 text-sm">
                              <span className="w-1.5 h-1.5 rounded-full bg-blue-400" />
                              {t}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-xs text-gray-400 dark:text-gray-500">No tool metadata available</div>
                      )}
                      {agent.config && (
                        <div className="pt-2">
                          <div className="text-xs text-gray-400 mb-1">Config</div>
                          <pre className="bg-white dark:bg-gray-900 rounded border dark:border-gray-600 p-2 text-xs overflow-auto max-h-32 dark:text-gray-300">
                            {JSON.stringify(agent.config, null, 2)}
                          </pre>
                        </div>
                      )}
                    </div>

                    {/* Systems & Endpoints Column */}
                    <div className="space-y-3">
                      <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider flex items-center gap-1">
                        <Globe className="w-3.5 h-3.5" /> Systems & Endpoints
                      </h4>
                      {systems.length > 0 ? (
                        <div className="space-y-1.5">
                          {systems.map((s, i) => (
                            <div key={i} className="flex items-center gap-2 text-sm">
                              {s.startsWith('http') ? (
                                <>
                                  <Globe className="w-3.5 h-3.5 text-purple-400 flex-shrink-0" />
                                  <a
                                    href={s}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="text-blue-600 hover:text-blue-800 truncate inline-flex items-center gap-1"
                                  >
                                    {s} <ExternalLink className="w-3 h-3 flex-shrink-0" />
                                  </a>
                                </>
                              ) : (
                                <>
                                  <Server className="w-3.5 h-3.5 text-gray-400 flex-shrink-0" />
                                  <span className="truncate">{s}</span>
                                </>
                              )}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-xs text-gray-400 dark:text-gray-500">No system metadata available</div>
                      )}
                      {/* App resources (serving endpoints, experiments, etc.) */}
                      {appResources.length > 0 && (
                        <div className="pt-2">
                          <div className="text-xs text-gray-400 mb-1">App Resources</div>
                          <div className="flex flex-wrap gap-1.5">
                            {appResources.map((r: any, i: number) => (
                              <Badge key={i} variant="default" className="text-xs">
                                {r.name}
                                {r.type ? ` (${r.type})` : ''}
                              </Badge>
                            ))}
                          </div>
                        </div>
                      )}
                      {agent.agent_id && (
                        <Link
                          to={`/agents/detail/${agent.agent_id}`}
                          className="inline-flex items-center gap-1 text-sm text-blue-600 hover:text-blue-800 mt-2"
                        >
                          View Operations Detail →
                        </Link>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </Card>
          )
        })}

        {filteredAgents.length === 0 && (
          <Card>
            <CardContent className="py-12 text-center text-gray-400 dark:text-gray-500">
              {searchQuery
                ? 'No agents match your search.'
                : 'No agents found. Click "Sync" to discover agents from Databricks.'}
            </CardContent>
          </Card>
        )}
      </div>

      {/* Pagination */}
      <TablePagination
        page={safePage}
        totalItems={filteredAgents.length}
        pageSize={pageSize}
        onPageChange={setPage}
        onPageSizeChange={setPageSize}
      />
    </div>
  )
}

/* ══════════════════════════════════════════════════════════════
   Operations Tab — data from system.serving.endpoint_usage
   ══════════════════════════════════════════════════════════════ */

function fmtNum(v: number): string {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}K`
  return String(Math.round(v))
}

function OperationsTab() {
  const [days, setDays] = useState(7)
  const [opsPage, setOpsPage] = useState(0)
  const [opsPageSize, setOpsPageSize] = useState(10)
  const [userPage, setUserPage] = useState(0)
  const [userPageSize, setUserPageSize] = useState(10)
  const opsSort = useSort<string>('total_requests', 'desc')
  const userSort = useSort<string>('total_requests', 'desc')

  const queryClient = useQueryClient()
  const isFetchingOps = useIsFetching({ queryKey: ['gateway'] }) > 0
  const { pinned, togglePin } = usePinnedAgents()

  const { data: allAgents } = useAllAgentsMerged()
  const { data: gatewayPageData } = useGatewayPageData()
  const { data: usageSummary = [], isLoading: summaryLoading } = useGatewayUsageSummary(days)
  const { data: timeseries = [] } = useGatewayUsageTimeseries(days)
  const { data: byUser = [] } = useGatewayUsageByUser(days)

  // Build endpoint → agent lookup from allAgents
  const agentByEndpoint = useMemo(() => {
    const map: Record<string, any> = {}
    for (const a of (allAgents || [])) {
      if (a.endpoint_name) map[a.endpoint_name.toLowerCase()] = a
    }
    return map
  }, [allAgents])

  // Enrich usage rows with agent metadata
  const enriched = useMemo(() =>
    (usageSummary as any[]).map((row: any) => {
      const agent = agentByEndpoint[(row.endpoint_name || '').toLowerCase()]
      const total = row.total_requests || 0
      const errors = row.error_count || 0
      return {
        ...row,
        agent_name: agent?.name || row.endpoint_name,
        agent_type: agent?.type || '—',
        endpoint_status: agent?.endpoint_status || '—',
        total_tokens: (row.total_input_tokens || 0) + (row.total_output_tokens || 0),
        error_rate: total > 0 ? (errors / total) * 100 : 0,
      }
    }), [usageSummary, agentByEndpoint])

  const sortedOps = useMemo(() => {
    const rows = sortRows(enriched, opsSort.sort, (r: any, k) => {
      if (k === 'agent_name') return (r.agent_name || '').toLowerCase()
      if (k === 'agent_type') return (r.agent_type || '').toLowerCase()
      return Number(r[k] || 0)
    })
    // Pinned agents always float to top
    return [...rows].sort((a: any, b: any) => {
      const aPin = pinned.has(a.agent_id || a.endpoint_name) ? 0 : 1
      const bPin = pinned.has(b.agent_id || b.endpoint_name) ? 0 : 1
      return aPin - bPin
    })
  }, [enriched, opsSort.sort, pinned])

  const pagedOps = sortedOps.slice(opsPage * opsPageSize, (opsPage + 1) * opsPageSize)

  // KPI totals
  const totalRequests = enriched.reduce((s, r) => s + (r.total_requests || 0), 0)
  const totalTokens   = enriched.reduce((s, r) => s + (r.total_tokens || 0), 0)
  const totalErrors   = enriched.reduce((s, r) => s + (r.error_count || 0), 0)
  const totalUsers    = enriched.reduce((s, r) => s + (r.unique_users || 0), 0)
  const activeAgents  = enriched.filter(r => (r.total_requests || 0) > 0).length

  // Chart data — hourly request volume
  const requestTrend = (timeseries as any[]).map((r: any) => ({
    timestamp: r.hour,
    value: Number(r.request_count || 0),
    errors: Number(r.error_count || 0),
  }))

  const tokenTrend = (timeseries as any[]).map((r: any) => ({
    timestamp: r.hour,
    value: Number(r.input_tokens || 0),
    output: Number(r.output_tokens || 0),
  }))

  // Top agents bar chart
  const topAgents = [...enriched]
    .sort((a, b) => (b.total_requests || 0) - (a.total_requests || 0))
    .slice(0, 8)
    .map(r => ({
      name: (r.agent_name || r.endpoint_name || '').length > 28
        ? (r.agent_name || r.endpoint_name).slice(0, 28) + '…'
        : (r.agent_name || r.endpoint_name || ''),
      value: r.total_requests || 0,
    }))

  // User table
  const sortedUsers = useMemo(() => sortRows(byUser as any[], userSort.sort, (r: any, k) => {
    if (k === 'requester') return (r.requester || '').toLowerCase()
    return Number(r[k] || 0)
  }), [byUser, userSort.sort])
  const pagedUsers = sortedUsers.slice(userPage * userPageSize, (userPage + 1) * userPageSize)

  return (
    <div className="space-y-6">
      {/* Controls */}
      <div className="flex items-center justify-end gap-3">
        <RefreshButton
          onRefresh={() => queryClient.invalidateQueries({ queryKey: ['gateway'] })}
          isRefreshing={isFetchingOps}
          lastSynced={gatewayPageData?.last_refreshed ?? null}
          title="Refresh from system.serving.endpoint_usage"
        />
        <select
          value={days}
          onChange={(e) => { setDays(Number(e.target.value)); setOpsPage(0) }}
          className="border dark:border-gray-600 rounded-lg px-3 py-2 text-sm bg-white dark:bg-gray-800 dark:text-gray-200"
        >
          <option value={1}>Last 24 hours</option>
          <option value={7}>Last 7 days</option>
          <option value={30}>Last 30 days</option>
          <option value={90}>Last 90 days</option>
        </select>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
        <KpiCard title="Total Requests" value={summaryLoading ? '…' : fmtNum(totalRequests)} format="number" />
        <KpiCard title="Total Tokens" value={summaryLoading ? '…' : fmtNum(totalTokens)} format="number" />
        <KpiCard title="Errors" value={summaryLoading ? '…' : fmtNum(totalErrors)} format="number" />
        <KpiCard title="Unique Users" value={summaryLoading ? '…' : fmtNum(totalUsers)} format="number" />
        <KpiCard title="Active Endpoints" value={summaryLoading ? '…' : String(activeAgents)} format="number" />
      </div>

      {/* Request + Token trend charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Request Volume (hourly)</CardTitle>
          </CardHeader>
          <CardContent>
            {requestTrend.length ? (
              <LineChart data={requestTrend} name="Requests" color={DB_CHART.primary} series={{ errors: 'Errors' }} />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Token Usage (hourly)</CardTitle>
          </CardHeader>
          <CardContent>
            {tokenTrend.length ? (
              <LineChart data={tokenTrend} name="Input Tokens" color={DB_CHART.info} series={{ output: 'Output Tokens' }} />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Top agents bar + user table */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Top Agents by Requests</CardTitle>
          </CardHeader>
          <CardContent>
            {topAgents.length ? (
              <BarChart data={topAgents} dataKey="value" nameKey="name" multiColor height={280} />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Top Users</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b dark:border-gray-700 text-gray-500 dark:text-gray-400">
                    <SortableHeader label="User" sortKey="requester" current={userSort.sort} onToggle={userSort.toggle} />
                    <SortableHeader label="Requests" sortKey="total_requests" current={userSort.sort} onToggle={userSort.toggle} align="right" />
                    <SortableHeader label="Tokens" sortKey="total_tokens" current={userSort.sort} onToggle={userSort.toggle} align="right" />
                    <SortableHeader label="Errors" sortKey="error_count" current={userSort.sort} onToggle={userSort.toggle} align="right" />
                  </tr>
                </thead>
                <tbody>
                  {pagedUsers.map((u: any, i: number) => (
                    <tr key={i} className="border-b border-gray-100 dark:border-gray-700/50">
                      <td className="py-2 text-xs font-mono truncate max-w-[180px]">{u.requester || '—'}</td>
                      <td className="py-2 text-right">{fmtNum(Number(u.total_requests || 0))}</td>
                      <td className="py-2 text-right">{fmtNum(Number(u.total_tokens || 0))}</td>
                      <td className="py-2 text-right">{fmtNum(Number(u.error_count || 0))}</td>
                    </tr>
                  ))}
                  {sortedUsers.length === 0 && (
                    <tr><td colSpan={4} className="py-8 text-center text-gray-400">No user data</td></tr>
                  )}
                </tbody>
              </table>
            </div>
            <TablePagination page={userPage} totalItems={sortedUsers.length} pageSize={userPageSize} onPageChange={setUserPage} onPageSizeChange={setUserPageSize} />
          </CardContent>
        </Card>
      </div>

      {/* Per-agent operations table */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Per-Agent Operations</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b dark:border-gray-700 text-gray-500 dark:text-gray-400">
                  <th className="pb-2 w-6"></th>
                  <SortableHeader label="Agent" sortKey="agent_name" current={opsSort.sort} onToggle={opsSort.toggle} />
                  <SortableHeader label="Type" sortKey="agent_type" current={opsSort.sort} onToggle={opsSort.toggle} />
                  <th className="pb-2 font-medium text-left">Status</th>
                  <SortableHeader label="Requests" sortKey="total_requests" current={opsSort.sort} onToggle={opsSort.toggle} align="right" />
                  <SortableHeader label="Input Tokens" sortKey="total_input_tokens" current={opsSort.sort} onToggle={opsSort.toggle} align="right" />
                  <SortableHeader label="Output Tokens" sortKey="total_output_tokens" current={opsSort.sort} onToggle={opsSort.toggle} align="right" />
                  <SortableHeader label="Error Rate" sortKey="error_rate" current={opsSort.sort} onToggle={opsSort.toggle} align="right" />
                  <SortableHeader label="Unique Users" sortKey="unique_users" current={opsSort.sort} onToggle={opsSort.toggle} align="right" />
                </tr>
              </thead>
              <tbody>
                {pagedOps.map((r: any, i: number) => (
                  <tr key={i} className={`border-b border-gray-100 dark:border-gray-700/50 ${pinned.has(r.agent_id || r.endpoint_name) ? 'bg-db-red/[0.03] dark:bg-db-red/[0.06]' : ''}`}>
                    <td className="py-2.5 pl-1">
                      <button
                        onClick={() => togglePin(r.agent_id || r.endpoint_name)}
                        className={`p-0.5 rounded transition-colors ${pinned.has(r.agent_id || r.endpoint_name) ? 'text-db-red' : 'text-gray-200 hover:text-gray-400 dark:text-gray-700 dark:hover:text-gray-500'}`}
                        title={pinned.has(r.agent_id || r.endpoint_name) ? 'Unpin' : 'Pin to top'}
                      >
                        <Pin className="w-3 h-3" />
                      </button>
                    </td>
                    <td className="py-2.5 font-medium dark:text-gray-200 truncate max-w-[200px]">
                      <div className="truncate">{r.agent_name}</div>
                      {r.agent_name !== r.endpoint_name && (
                        <div className="text-[10px] text-gray-400 font-mono truncate">{r.endpoint_name}</div>
                      )}
                    </td>
                    <td className="py-2.5 text-gray-500 dark:text-gray-400 text-xs">{r.agent_type}</td>
                    <td className="py-2.5">
                      <StatusBadge status={r.endpoint_status} />
                    </td>
                    <td className="py-2.5 text-right font-medium">{fmtNum(r.total_requests || 0)}</td>
                    <td className="py-2.5 text-right text-gray-500">{fmtNum(r.total_input_tokens || 0)}</td>
                    <td className="py-2.5 text-right text-gray-500">{fmtNum(r.total_output_tokens || 0)}</td>
                    <td className="py-2.5 text-right">
                      <span className={r.error_rate > 5 ? 'text-red-500 font-medium' : r.error_rate > 1 ? 'text-yellow-500' : 'text-green-600 dark:text-green-400'}>
                        {r.error_rate.toFixed(1)}%
                      </span>
                    </td>
                    <td className="py-2.5 text-right text-gray-500">{fmtNum(r.unique_users || 0)}</td>
                  </tr>
                ))}
                {enriched.length === 0 && !summaryLoading && (
                  <tr>
                    <td colSpan={8} className="py-8 text-center text-gray-400">
                      No endpoint usage data found in the selected time range
                    </td>
                  </tr>
                )}
                {summaryLoading && (
                  <tr>
                    <td colSpan={8} className="py-8 text-center text-gray-400">Loading…</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination page={opsPage} totalItems={sortedOps.length} pageSize={opsPageSize} onPageChange={setOpsPage} onPageSizeChange={setOpsPageSize} />
        </CardContent>
      </Card>
    </div>
  )
}
