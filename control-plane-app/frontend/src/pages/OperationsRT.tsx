import React, { useState, useMemo } from 'react'
import {
  useOperationsStatus,
  useRefreshOperations,
  OperationsAgent,
} from '@/api/hooks'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { KpiCard } from '@/components/KpiCard'
import { RefreshButton } from '@/components/RefreshButton'
import { SortableHeader, useSort, sortRows } from '@/components/SortableTable'
import { TablePagination } from '@/components/TablePagination'
import {
  Activity,
  CheckCircle2,
  AlertTriangle,
  XCircle,
  Clock,
  Search,
  ChevronDown,
  ChevronRight,
  Server,
  HelpCircle,
} from 'lucide-react'

/* ── health badge ─────────────────────────────────────────────── */

const healthConfig: Record<string, { label: string; color: string; icon: typeof CheckCircle2 }> = {
  healthy:  { label: 'Healthy',  color: 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400',   icon: CheckCircle2 },
  degraded: { label: 'Degraded', color: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400', icon: AlertTriangle },
  down:     { label: 'Down',     color: 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400',           icon: XCircle },
  pending:  { label: 'Pending',  color: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400',       icon: Clock },
  unknown:  { label: 'Unknown',  color: 'bg-gray-100 text-gray-500 dark:bg-gray-700/50 dark:text-gray-500',       icon: HelpCircle },
}

function HealthBadge({ health }: { health: string }) {
  const cfg = healthConfig[health] || healthConfig.unknown
  const Icon = cfg.icon
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cfg.color}`}>
      <Icon className="w-3 h-3" />
      {cfg.label}
    </span>
  )
}

/* ── type labels ──────────────────────────────────────────────── */

const typeLabels: Record<string, string> = {
  knowledge_assistant: 'Knowledge Assistant',
  multi_agent_supervisor: 'Multi-Agent Supervisor',
  custom_llm: 'Custom LLM',
  information_extraction: 'Info Extraction',
  custom_agent: 'Custom Agent',
  custom_app: 'Databricks App',
  external_agent: 'External Model',
  genie_space: 'Genie Space',
}

/* ── state badge variant ──────────────────────────────────────── */

function stateBadgeVariant(state: string): 'success' | 'error' | 'warning' | 'default' {
  const s = state.toUpperCase()
  if (['READY', 'ACTIVE', 'RUNNING'].includes(s)) return 'success'
  if (['NOT_READY', 'ERROR', 'CRASHED', 'FAILED', 'STOPPED'].includes(s)) return 'error'
  if (['STARTING', 'STOPPING', 'DEPLOYING', 'PENDING', 'IN_PROGRESS'].includes(s)) return 'warning'
  return 'default'
}

/* ── detail panel ─────────────────────────────────────────────── */

function AgentDetailRow({ agent }: { agent: OperationsAgent }) {
  return (
    <div className="px-6 py-4 bg-gray-50 dark:bg-gray-800/50 border-t border-gray-100 dark:border-gray-700">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
        <div>
          <span className="text-gray-500 dark:text-gray-400">State</span>
          <p className="font-medium dark:text-gray-200">{agent.state || '—'}</p>
        </div>
        <div>
          <span className="text-gray-500 dark:text-gray-400">Type</span>
          <p className="font-medium dark:text-gray-200">{typeLabels[agent.agent_type] || agent.agent_type || '—'}</p>
        </div>
        <div>
          <span className="text-gray-500 dark:text-gray-400">Model</span>
          <p className="font-medium dark:text-gray-200 truncate">{agent.model_name || '—'}</p>
        </div>
        <div>
          <span className="text-gray-500 dark:text-gray-400">Creator</span>
          <p className="font-medium dark:text-gray-200 truncate">{agent.creator || '—'}</p>
        </div>
        {agent.endpoint_name && agent.endpoint_name !== agent.name && (
          <div>
            <span className="text-gray-500 dark:text-gray-400">Endpoint</span>
            <p className="font-medium dark:text-gray-200 truncate">{agent.endpoint_name}</p>
          </div>
        )}
        {agent.served_entity_count > 0 && (
          <div>
            <span className="text-gray-500 dark:text-gray-400">Served Entities</span>
            <p className="font-medium dark:text-gray-200">{agent.served_entity_count}</p>
          </div>
        )}
        {agent.scale_to_zero !== null && (
          <div>
            <span className="text-gray-500 dark:text-gray-400">Scale to Zero</span>
            <p className="font-medium dark:text-gray-200">{agent.scale_to_zero ? 'Yes' : 'No'}</p>
          </div>
        )}
        {agent.workload_size && (
          <div>
            <span className="text-gray-500 dark:text-gray-400">Workload Size</span>
            <p className="font-medium dark:text-gray-200">{agent.workload_size}</p>
          </div>
        )}
        {agent.has_pending_config && (
          <div>
            <span className="text-gray-500 dark:text-gray-400">Pending Update</span>
            <p className="font-medium text-blue-600 dark:text-blue-400">{agent.pending_reason || 'Yes'}</p>
          </div>
        )}
        {agent.source && (
          <div>
            <span className="text-gray-500 dark:text-gray-400">Source</span>
            <p className="font-medium dark:text-gray-200">{agent.source}</p>
          </div>
        )}
        {agent.error_rate !== null && agent.error_rate > 0 && (
          <div>
            <span className="text-gray-500 dark:text-gray-400">Error Rate</span>
            <p className="font-medium text-red-600 dark:text-red-400">{agent.error_rate}%</p>
          </div>
        )}
        {agent.p95_latency_ms !== null && (
          <div>
            <span className="text-gray-500 dark:text-gray-400">P95 Latency</span>
            <p className="font-medium dark:text-gray-200">{Number(agent.p95_latency_ms).toFixed(0)} ms</p>
          </div>
        )}
        {agent.tags?.app_url && (
          <div>
            <span className="text-gray-500 dark:text-gray-400">App URL</span>
            <p className="font-medium dark:text-gray-200 truncate">
              <a href={agent.tags.app_url} target="_blank" rel="noreferrer" className="text-blue-600 dark:text-blue-400 hover:underline">
                {agent.tags.app_url}
              </a>
            </p>
          </div>
        )}
      </div>
      {agent.description && (
        <p className="mt-3 text-sm text-gray-500 dark:text-gray-400">{agent.description}</p>
      )}
    </div>
  )
}

/* ── main page ────────────────────────────────────────────────── */

export default function OperationsRTPage() {
  const { data: status, isPending: statusLoading } = useOperationsStatus()
  const refreshMutation = useRefreshOperations()

  const [search, setSearch] = useState('')
  const [healthFilter, setHealthFilter] = useState<string>('all')
  const [typeFilter, setTypeFilter] = useState<string>('all')
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(20)
  const sort = useSort<string>('name', 'asc')

  const summary = status?.summary
  const agents = status?.agents ?? []

  // Unique agent types for filter
  const agentTypes = useMemo(() => {
    const types = new Set(agents.map(a => a.agent_type).filter(Boolean))
    return Array.from(types).sort()
  }, [agents])

  // Filter & sort
  const filtered = useMemo(() => {
    let list = agents
    if (healthFilter !== 'all') {
      list = list.filter(a => a.health === healthFilter)
    }
    if (typeFilter !== 'all') list = list.filter(a => a.agent_type === typeFilter)
    if (search) {
      const q = search.toLowerCase()
      list = list.filter(a =>
        a.name.toLowerCase().includes(q) ||
        (a.agent_type || '').toLowerCase().includes(q) ||
        (a.model_name || '').toLowerCase().includes(q) ||
        (a.creator || '').toLowerCase().includes(q) ||
        (a.endpoint_name || '').toLowerCase().includes(q)
      )
    }
    return list
  }, [agents, healthFilter, typeFilter, search])

  const healthOrder: Record<string, number> = { down: 0, degraded: 1, pending: 2, unknown: 3, healthy: 4 }
  const sorted = sortRows(filtered, sort.sort, (row: any, key: string) => {
    if (key === 'health') return healthOrder[row.health] ?? 9
    return row[key]
  })
  const paged = sorted.slice(page * pageSize, (page + 1) * pageSize)

  const toggleExpand = (id: string) => {
    setExpanded(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  return (
    <div className="space-y-6">
      {/* Refresh */}
      <div className="flex justify-end">
        <RefreshButton
          onRefresh={() => refreshMutation.mutate()}
          isPending={refreshMutation.isPending}
          lastSynced={status?.last_refreshed}
          title="Refresh agent status"
        />
      </div>

      {/* Summary KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <KpiCard title="Total Agents" value={summary?.total ?? 0} />
        <Card className="border-green-200 dark:border-green-800">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium text-green-700 dark:text-green-400">Healthy</CardTitle>
            <CheckCircle2 className="w-4 h-4 text-green-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-green-700 dark:text-green-400">{summary?.healthy ?? 0}</div>
          </CardContent>
        </Card>
        <Card className="border-yellow-200 dark:border-yellow-800">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium text-yellow-700 dark:text-yellow-400">Degraded</CardTitle>
            <AlertTriangle className="w-4 h-4 text-yellow-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-yellow-700 dark:text-yellow-400">{summary?.degraded ?? 0}</div>
          </CardContent>
        </Card>
        <Card className="border-red-200 dark:border-red-800">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium text-red-700 dark:text-red-400">Down</CardTitle>
            <XCircle className="w-4 h-4 text-red-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-red-700 dark:text-red-400">{summary?.down ?? 0}</div>
          </CardContent>
        </Card>
        <Card className="border-blue-200 dark:border-blue-800">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium text-blue-700 dark:text-blue-400">Pending</CardTitle>
            <Clock className="w-4 h-4 text-blue-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-blue-700 dark:text-blue-400">{summary?.pending ?? 0}</div>
          </CardContent>
        </Card>
      </div>

      {/* Filters */}
      <Card>
        <CardContent className="pt-4">
          <div className="flex flex-wrap items-center gap-3">
            {/* Search */}
            <div className="relative flex-1 min-w-[200px]">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                type="text"
                placeholder="Search agents..."
                value={search}
                onChange={e => { setSearch(e.target.value); setPage(0) }}
                className="w-full pl-9 pr-3 py-2 border dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-800 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-db-red/30"
              />
            </div>
            {/* Type filter */}
            <select
              value={typeFilter}
              onChange={e => { setTypeFilter(e.target.value); setPage(0) }}
              className="border dark:border-gray-600 rounded-lg px-3 py-2 text-sm bg-white dark:bg-gray-800 dark:text-gray-200"
            >
              <option value="all">All types</option>
              {agentTypes.map(t => (
                <option key={t} value={t}>{typeLabels[t] || t}</option>
              ))}
            </select>
            {/* Health filter pills */}
            <div className="flex gap-1.5 flex-wrap">
              {['all', 'healthy', 'degraded', 'down', 'pending'].map(h => (
                <button
                  key={h}
                  onClick={() => { setHealthFilter(h); setPage(0) }}
                  className={`px-3 py-1.5 rounded-full text-xs font-medium transition-colors ${
                    healthFilter === h
                      ? 'bg-db-red text-white'
                      : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
                  }`}
                >
                  {h === 'all' ? 'All' : (healthConfig[h]?.label ?? h)}
                  {h !== 'all' && summary && (
                    <span className="ml-1 opacity-70">
                      ({summary[h as keyof typeof summary] ?? 0})
                    </span>
                  )}
                </button>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Agent table */}
      <Card>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50">
                <th className="w-8 px-3 py-3" />
                <SortableHeader label="Agent" sortKey="name" current={sort.sort} onToggle={sort.toggle} className="px-4 py-3 text-gray-500 dark:text-gray-400" />
                <SortableHeader label="Health" sortKey="health" current={sort.sort} onToggle={sort.toggle} className="px-4 py-3 text-gray-500 dark:text-gray-400" />
                <SortableHeader label="Type" sortKey="agent_type" current={sort.sort} onToggle={sort.toggle} className="px-4 py-3 text-gray-500 dark:text-gray-400" />
                <SortableHeader label="State" sortKey="state" current={sort.sort} onToggle={sort.toggle} className="px-4 py-3 text-gray-500 dark:text-gray-400" />
                <SortableHeader label="Requests/h" sortKey="request_count" current={sort.sort} onToggle={sort.toggle} className="px-4 py-3 text-gray-500 dark:text-gray-400" />
                <SortableHeader label="Errors/h" sortKey="error_count" current={sort.sort} onToggle={sort.toggle} className="px-4 py-3 text-gray-500 dark:text-gray-400" />
                <SortableHeader label="Avg Latency" sortKey="avg_latency_ms" current={sort.sort} onToggle={sort.toggle} className="px-4 py-3 text-gray-500 dark:text-gray-400" />
              </tr>
            </thead>
            <tbody>
              {statusLoading && (
                <tr>
                  <td colSpan={8} className="px-4 py-12 text-center text-gray-400 dark:text-gray-500">
                    <Activity className="w-5 h-5 mx-auto mb-2 animate-spin" />
                    Loading agent status...
                  </td>
                </tr>
              )}
              {!statusLoading && paged.length === 0 && (
                <tr>
                  <td colSpan={8} className="px-4 py-12 text-center text-gray-400 dark:text-gray-500">
                    <Server className="w-5 h-5 mx-auto mb-2" />
                    No agents found
                  </td>
                </tr>
              )}
              {paged.map(agent => {
                const isExpanded = expanded.has(agent.agent_id)
                return (
                  <React.Fragment key={agent.agent_id}>
                    <tr
                      className="border-b dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800/40 cursor-pointer transition-colors"
                      onClick={() => toggleExpand(agent.agent_id)}
                    >
                      <td className="px-3 py-3">
                        {isExpanded ? (
                          <ChevronDown className="w-4 h-4 text-gray-400" />
                        ) : (
                          <ChevronRight className="w-4 h-4 text-gray-400" />
                        )}
                      </td>
                      <td className="px-4 py-3 font-medium dark:text-gray-200 max-w-[280px] truncate">
                        {agent.name}
                      </td>
                      <td className="px-4 py-3">
                        <HealthBadge health={agent.health} />
                      </td>
                      <td className="px-4 py-3 text-gray-600 dark:text-gray-400 truncate max-w-[160px]">
                        {typeLabels[agent.agent_type] || agent.agent_type || '—'}
                      </td>
                      <td className="px-4 py-3">
                        <Badge variant={stateBadgeVariant(agent.state)}>
                          {agent.state || '—'}
                        </Badge>
                      </td>
                      <td className="px-4 py-3 text-gray-700 dark:text-gray-300 tabular-nums">
                        {agent.request_count !== null ? Number(agent.request_count).toLocaleString() : '—'}
                      </td>
                      <td className="px-4 py-3 tabular-nums">
                        {agent.error_count !== null ? (
                          <span className={Number(agent.error_count) > 0 ? 'text-red-600 dark:text-red-400 font-medium' : 'text-gray-700 dark:text-gray-300'}>
                            {Number(agent.error_count).toLocaleString()}
                          </span>
                        ) : '—'}
                      </td>
                      <td className="px-4 py-3 text-gray-700 dark:text-gray-300 tabular-nums">
                        {agent.avg_latency_ms !== null ? `${Number(agent.avg_latency_ms).toFixed(0)} ms` : '—'}
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={8} className="p-0">
                          <AgentDetailRow agent={agent} />
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
        <TablePagination
          totalItems={filtered.length}
          page={page}
          pageSize={pageSize}
          onPageChange={setPage}
          onPageSizeChange={ps => { setPageSize(ps); setPage(0) }}
        />
      </Card>
    </div>
  )
}
