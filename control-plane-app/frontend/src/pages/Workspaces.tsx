import { useState, useMemo, useCallback } from 'react'
import { useQueryClient, useIsFetching } from '@tanstack/react-query'
import { SortableHeader, useSort, sortRows } from '@/components/SortableTable'
import {
  useWorkspacesPageData,
  useBillingRefresh,
  useBillingCacheStatus,
  type WorkspaceSummary,
  type WorkspacePageData,
} from '@/api/hooks'
import { RefreshButton } from '@/components/RefreshButton'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { KpiCard } from '@/components/KpiCard'
import { TablePagination } from '@/components/TablePagination'
import { BarChart } from '@/components/charts/BarChart'
import { DB_COLORS } from '@/lib/brand'
import {
  Globe,
  DollarSign,
  Bot,
  Zap,
  Server,
  ChevronRight,
  ArrowLeft,
  Building2,
  TrendingUp,
  TrendingDown,
  Minus,
  Search,
} from 'lucide-react'
import {
  LineChart as RechartsLineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { LazyChart } from '@/components/charts/LazyChart'
import { DB_GRID, DB_AXIS_TEXT } from '@/lib/brand'

/* ── helpers ──────────────────────────────────────────────────── */

function fmtCost(v: number): string {
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`
  if (v >= 1000) return `$${(v / 1000).toFixed(1)}k`
  if (v >= 1) return `$${v.toFixed(2)}`
  return `$${v.toFixed(4)}`
}

function fmtNumber(v: number): string {
  if (v >= 1_000_000_000) return `${(v / 1_000_000_000).toFixed(1)}B`
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}K`
  return String(Math.round(v))
}

function shortWs(id: string): string {
  if (!id) return '—'
  return id.length > 10 ? `…${id.slice(-6)}` : id
}

function costDelta(current: number, prev: number): { pct: number; dir: 'up' | 'down' | 'stable' } {
  if (prev <= 0) return { pct: 0, dir: 'stable' }
  const pct = ((current - prev) / prev) * 100
  return { pct: Math.round(pct * 10) / 10, dir: pct > 1 ? 'up' : pct < -1 ? 'down' : 'stable' }
}

/* ── tabs ─────────────────────────────────────────────────────── */

const TABS = [
  { key: 'overview', label: 'Overview', icon: Globe },
  { key: 'costs', label: 'Cost Breakdown', icon: DollarSign },
  { key: 'agents', label: 'Agent Inventory', icon: Bot },
  { key: 'endpoints', label: 'Top Endpoints', icon: Server },
] as const

type TabKey = (typeof TABS)[number]['key']

/* ── main page ───────────────────────────────────────────────── */

export default function WorkspacesPage() {
  const [days, setDays] = useState(30)
  const [tab, setTab] = useState<TabKey>('overview')
  const [selectedWs, setSelectedWs] = useState<string | null>(null)
  const [search, setSearch] = useState('')

  // Pagination for Overview workspace table
  const [wsPage, setWsPage] = useState(0)
  const [wsPageSize, setWsPageSize] = useState(10)

  const { sort: wsSort, toggle: wsToggle } = useSort('serving_cost', 'desc')

  const queryClient = useQueryClient()
  const { data: pageData, isLoading } = useWorkspacesPageData(days)
  const billingRefresh = useBillingRefresh()
  const { data: billingCacheStatus } = useBillingCacheStatus()
  const isFetchingWorkspaces = useIsFetching({ queryKey: ['workspaces'] }) > 0

  // Use the most recent billing cache refresh time (server-reported, not client fetch time)
  const billingLastRefreshed = billingCacheStatus?.caches
    ? Object.values(billingCacheStatus.caches).reduce<string | null>((newest, c: any) => {
        if (!c.last_refreshed) return newest
        if (!newest) return c.last_refreshed
        return c.last_refreshed > newest ? c.last_refreshed : newest
      }, null)
    : null

  /* ── derived data ─────────────────────────────────────── */

  const filteredSummaries = useMemo(() => {
    if (!pageData?.workspace_summaries) return []
    let list = pageData.workspace_summaries
    if (search) {
      const q = search.toLowerCase()
      list = list.filter((ws) => ws.workspace_id.toLowerCase().includes(q))
    }
    return list
  }, [pageData?.workspace_summaries, search])

  const wsAccessor = useCallback((row: WorkspaceSummary, key: string) => {
    switch (key) {
      case 'workspace_id': return (row.workspace_id || '').toLowerCase()
      case 'serving_cost': return Number(row.serving_cost || 0)
      case 'trend': return costDelta(Number(row.serving_cost), Number(row.prev_serving_cost)).pct
      case 'total_cost': return Number(row.total_all_product_cost || 0)
      case 'agents': return Number(row.agent_count || 0)
      case 'endpoints': return Number(row.endpoint_count || 0)
      case 'requests': return Number(row.total_requests || 0)
      case 'tokens': return Number(row.total_input_tokens || 0) + Number(row.total_output_tokens || 0)
      default: return null
    }
  }, [])

  const sortedSummaries = useMemo(
    () => sortRows(filteredSummaries, wsSort, wsAccessor),
    [filteredSummaries, wsSort, wsAccessor]
  )

  const pagedSummaries = sortedSummaries.slice(wsPage * wsPageSize, (wsPage + 1) * wsPageSize)

  const kpis = pageData?.kpis

  // For the detail view
  const detailWs = useMemo(() => {
    if (!selectedWs || !pageData) return null
    const summary = pageData.workspace_summaries.find((ws) => ws.workspace_id === selectedWs)
    if (!summary) return null
    const agents = pageData.all_agents.filter((a) => a.workspace_id === selectedWs)
    const products = pageData.products_by_workspace.filter((p) => p.workspace_id === selectedWs)
    const types = pageData.agent_type_breakdown.filter((t) => t.workspace_id === selectedWs)
    const endpoints = pageData.top_endpoints.filter((e) => e.workspace_id === selectedWs)
    return { summary, agents, products, types, endpoints }
  }, [selectedWs, pageData])

  /* ── cost trend chart data ─────────────────────────── */

  const trendChartData = useMemo(() => {
    if (!pageData?.cost_trend?.length) return []
    const grouped: Record<string, Record<string, number>> = {}
    for (const r of pageData.cost_trend) {
      if (!grouped[r.day]) grouped[r.day] = { day: 0 as any }
      ;(grouped[r.day] as any).day = r.day
      grouped[r.day][r.workspace_id] = Number(r.cost || 0)
    }
    return Object.values(grouped)
  }, [pageData?.cost_trend])

  const trendWsIds = useMemo(() => {
    if (!pageData?.cost_trend?.length) return []
    return [...new Set(pageData.cost_trend.map((r) => r.workspace_id))]
  }, [pageData?.cost_trend])

  /* ── loading state ─────────────────────────────────── */

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        <div className="animate-spin mr-3 h-5 w-5 border-2 border-gray-300 border-t-red-500 rounded-full" />
        Loading workspace data…
      </div>
    )
  }

  /* ── detail view ───────────────────────────────────── */

  if (selectedWs && detailWs) {
    return (
      <WorkspaceDetail
        ws={detailWs}
        wsId={selectedWs}
        currentWsId={pageData?.current_workspace_id ?? null}
        onBack={() => setSelectedWs(null)}
      />
    )
  }

  /* ── main (list) view ──────────────────────────────── */

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold dark:text-gray-100">Workspaces</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Multi-workspace federation — cost, agents, and usage across all workspaces
          </p>
        </div>

        <div className="flex items-center gap-3">
          <RefreshButton
            onRefresh={() => {
              billingRefresh.mutate(90)
              queryClient.invalidateQueries({ queryKey: ['workspaces'] })
            }}
            isPending={billingRefresh.isPending || isFetchingWorkspaces}
            lastSynced={billingLastRefreshed}
            title="Refresh workspace billing data from system tables"
          />
          <select
            value={days}
            onChange={(e) => setDays(Number(e.target.value))}
            className="border rounded-lg px-3 py-2 text-sm dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200"
          >
            {[7, 14, 30, 60, 90].map((d) => (
              <option key={d} value={d}>
                Last {d} days
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Tabs */}
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

      {/* KPI cards */}
      {kpis && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <KpiCard title="Workspaces" value={kpis.total_workspaces} />
          <KpiCard
            title="Serving Cost"
            value={kpis.total_serving_cost}
            format="currency"
            trend={
              kpis.cost_change_pct !== 0
                ? {
                    value: kpis.cost_change_pct,
                    direction: kpis.cost_change_pct > 1 ? 'up' : kpis.cost_change_pct < -1 ? 'down' : 'stable',
                  }
                : undefined
            }
          />
          <KpiCard title="All Product Cost" value={kpis.total_all_product_cost} format="currency" />
          <KpiCard title="Agents" value={kpis.total_agents} />
          <KpiCard title="Endpoints" value={kpis.total_endpoints} />
          <KpiCard title="Requests" value={kpis.total_requests} />
        </div>
      )}

      {/* Tab content */}
      {tab === 'overview' && (
        <div className="space-y-6">
          {/* Cost Trend (multi-workspace line chart) */}
          {trendChartData.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="text-sm font-medium dark:text-gray-300">
                  Daily Serving Cost — Top {trendWsIds.length} Workspaces
                </CardTitle>
              </CardHeader>
              <CardContent>
                <LazyChart height={300}>
                <ResponsiveContainer width="100%" height={300}>
                  <RechartsLineChart data={trendChartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke={DB_GRID} />
                    <XAxis
                      dataKey="day"
                      tick={{ fontSize: 11, fill: DB_AXIS_TEXT }}
                      tickFormatter={(v: string) => v.slice(5)}
                    />
                    <YAxis tick={{ fontSize: 11, fill: DB_AXIS_TEXT }} tickFormatter={(v: number) => fmtCost(v)} />
                    <Tooltip
                      contentStyle={{
                        borderRadius: 8,
                        border: `1px solid ${DB_GRID}`,
                        fontSize: 13,
                        backgroundColor: 'var(--tooltip-bg, #fff)',
                        color: 'var(--tooltip-text, #1f2937)',
                      }}
                      formatter={(value: number) => fmtCost(value)}
                    />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                    {trendWsIds.map((wsId, i) => (
                      <Line
                        key={wsId}
                        type="monotone"
                        dataKey={wsId}
                        stroke={DB_COLORS[i % DB_COLORS.length]}
                        strokeWidth={2}
                        dot={false}
                        name={`WS ${shortWs(wsId)}`}
                      />
                    ))}
                  </RechartsLineChart>
                </ResponsiveContainer>
                </LazyChart>
              </CardContent>
            </Card>
          )}

          {/* Workspace table */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <CardTitle className="text-sm font-medium dark:text-gray-300">All Workspaces</CardTitle>
              <div className="relative w-64">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                <input
                  type="text"
                  value={search}
                  onChange={(e) => { setSearch(e.target.value); setWsPage(0) }}
                  placeholder="Filter by workspace ID…"
                  className="w-full pl-9 pr-3 py-1.5 text-sm border rounded-lg dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200"
                />
              </div>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b dark:border-gray-700 text-left">
                      <SortableHeader label="Workspace ID" sortKey="workspace_id" current={wsSort} onToggle={wsToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Serving Cost" sortKey="serving_cost" current={wsSort} onToggle={wsToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Trend" sortKey="trend" current={wsSort} onToggle={wsToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Total Cost" sortKey="total_cost" current={wsSort} onToggle={wsToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Agents" sortKey="agents" current={wsSort} onToggle={wsToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Endpoints" sortKey="endpoints" current={wsSort} onToggle={wsToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Requests" sortKey="requests" current={wsSort} onToggle={wsToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Tokens (In / Out)" sortKey="tokens" current={wsSort} onToggle={wsToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                      <th className="py-2 px-3 font-medium" />
                    </tr>
                  </thead>
                  <tbody>
                    {pagedSummaries.map((ws) => {
                      const delta = costDelta(Number(ws.serving_cost), Number(ws.prev_serving_cost))
                      const isCurrent = ws.workspace_id === pageData?.current_workspace_id
                      const TrendIcon = delta.dir === 'up' ? TrendingUp : delta.dir === 'down' ? TrendingDown : Minus
                      return (
                        <tr
                          key={ws.workspace_id}
                          className="border-b border-gray-100 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-800/40 cursor-pointer group"
                          onClick={() => setSelectedWs(ws.workspace_id)}
                        >
                          <td className="py-2.5 px-3">
                            <div className="flex items-center gap-2">
                              <span className="font-mono text-xs font-medium dark:text-gray-200">{ws.workspace_id}</span>
                              {isCurrent && <Badge variant="success" className="text-[10px] px-1.5">Current</Badge>}
                            </div>
                          </td>
                          <td className="py-2.5 px-3 text-right font-medium dark:text-gray-200">
                            {fmtCost(Number(ws.serving_cost))}
                          </td>
                          <td className="py-2.5 px-3 text-right">
                            <span className={`inline-flex items-center gap-1 text-xs ${
                              delta.dir === 'up' ? 'text-red-500' : delta.dir === 'down' ? 'text-green-500' : 'text-gray-400'
                            }`}>
                              <TrendIcon className="w-3 h-3" />
                              {delta.pct !== 0 ? `${Math.abs(delta.pct)}%` : '—'}
                            </span>
                          </td>
                          <td className="py-2.5 px-3 text-right dark:text-gray-300">
                            {fmtCost(Number(ws.total_all_product_cost))}
                          </td>
                          <td className="py-2.5 px-3 text-right dark:text-gray-300">{Number(ws.agent_count)}</td>
                          <td className="py-2.5 px-3 text-right dark:text-gray-300">{Number(ws.endpoint_count)}</td>
                          <td className="py-2.5 px-3 text-right dark:text-gray-300">
                            {fmtNumber(Number(ws.total_requests))}
                          </td>
                          <td className="py-2.5 px-3 text-right text-xs text-gray-500 dark:text-gray-400">
                            {fmtNumber(Number(ws.total_input_tokens))} / {fmtNumber(Number(ws.total_output_tokens))}
                          </td>
                          <td className="py-2.5 px-3 text-right">
                            <ChevronRight className="w-4 h-4 text-gray-300 group-hover:text-red-400 transition-colors inline" />
                          </td>
                        </tr>
                      )
                    })}
                    {sortedSummaries.length === 0 && (
                      <tr>
                        <td colSpan={9} className="py-8 text-center text-gray-400 dark:text-gray-500">
                          {search ? 'No workspaces match your search' : 'No workspace data available'}
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
              <TablePagination
                page={wsPage}
                totalItems={sortedSummaries.length}
                pageSize={wsPageSize}
                onPageChange={setWsPage}
                onPageSizeChange={setWsPageSize}
              />
            </CardContent>
          </Card>
        </div>
      )}

      {tab === 'costs' && <CostBreakdownTab data={pageData} />}
      {tab === 'agents' && <AgentInventoryTab data={pageData} />}
      {tab === 'endpoints' && <TopEndpointsTab data={pageData} />}
    </div>
  )
}

/* ── Workspace Detail View ───────────────────────────────────── */

function WorkspaceDetail({
  ws,
  wsId,
  currentWsId,
  onBack,
}: {
  ws: {
    summary: WorkspaceSummary
    agents: WorkspacePageData['all_agents']
    products: WorkspacePageData['products_by_workspace']
    types: WorkspacePageData['agent_type_breakdown']
    endpoints: WorkspacePageData['top_endpoints']
  }
  wsId: string
  currentWsId: string | null
  onBack: () => void
}) {
  const { summary: s, agents: wsAgents, products, types, endpoints } = ws
  const delta = costDelta(Number(s.serving_cost), Number(s.prev_serving_cost))

  const { sort: epSort, toggle: epToggle } = useSort('cost', 'desc')
  const { sort: agentSort, toggle: agentToggle } = useSort('name', 'asc')

  const epAccessor = useCallback((row: { endpoint_name: string; total_cost?: number; total_dbus?: number }, key: string) => {
    switch (key) {
      case 'endpoint': return (row.endpoint_name || '').toLowerCase()
      case 'cost': return Number(row.total_cost || 0)
      case 'dbus': return Number(row.total_dbus || 0)
      default: return null
    }
  }, [])
  const agentAccessor = useCallback((row: { name?: string; type?: string; endpoint_name?: string; model_name?: string; source?: string }, key: string) => {
    switch (key) {
      case 'name': return (row.name || '').toLowerCase()
      case 'type': return (row.type || '').toLowerCase()
      case 'endpoint': return (row.endpoint_name || '').toLowerCase()
      case 'model': return (row.model_name || '').toLowerCase()
      case 'source': return (row.source || '').toLowerCase()
      default: return null
    }
  }, [])

  const sortedEndpoints = useMemo(() => sortRows(endpoints, epSort, epAccessor), [endpoints, epSort, epAccessor])
  const sortedAgents = useMemo(() => sortRows(wsAgents, agentSort, agentAccessor), [wsAgents, agentSort, agentAccessor])

  // Pagination for detail endpoints table
  const [epPage, setEpPage] = useState(0)
  const [epPageSize, setEpPageSize] = useState(10)
  const pagedEndpoints = sortedEndpoints.slice(epPage * epPageSize, (epPage + 1) * epPageSize)

  // Pagination for detail agent table
  const [agentPage, setAgentPage] = useState(0)
  const [agentPageSize, setAgentPageSize] = useState(10)
  const pagedAgents = sortedAgents.slice(agentPage * agentPageSize, (agentPage + 1) * agentPageSize)

  return (
    <div className="space-y-6">
      {/* Back button */}
      <button
        onClick={onBack}
        className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-800 dark:text-gray-400 dark:hover:text-gray-200 transition-colors"
      >
        <ArrowLeft className="w-4 h-4" /> Back to all workspaces
      </button>

      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold dark:text-gray-100 flex items-center gap-2">
          <Building2 className="w-6 h-6 text-red-500" />
          Workspace {wsId}
          {wsId === currentWsId && (
            <Badge variant="success" className="text-xs ml-2">Current</Badge>
          )}
        </h2>
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          title="Serving Cost"
          value={Number(s.serving_cost)}
          format="currency"
          trend={{ value: delta.pct, direction: delta.dir }}
        />
        <KpiCard title="Total Product Cost" value={Number(s.total_all_product_cost)} format="currency" />
        <KpiCard title="Agents" value={Number(s.agent_count)} />
        <KpiCard title="Requests" value={Number(s.total_requests)} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Agent Types */}
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium dark:text-gray-300">Agent Types</CardTitle>
          </CardHeader>
          <CardContent>
            {types.length > 0 ? (
              <BarChart
                data={types.map((t) => ({ name: t.agent_type, count: t.count }))}
                dataKey="count"
                nameKey="name"
                multiColor
                height={220}
              />
            ) : (
              <p className="text-gray-400 text-sm">No agents discovered</p>
            )}
          </CardContent>
        </Card>

        {/* Product Costs */}
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium dark:text-gray-300">Cost by Product</CardTitle>
          </CardHeader>
          <CardContent>
            {products.length > 0 ? (
              <BarChart
                data={products.map((p) => ({
                  name: p.billing_origin_product
                    .replace(/_/g, ' ')
                    .replace(/\b\w/g, (c) => c.toUpperCase()),
                  cost: Number(p.total_cost),
                }))}
                dataKey="cost"
                nameKey="name"
                multiColor
                height={220}
              />
            ) : (
              <p className="text-gray-400 text-sm">No product cost data</p>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Top Endpoints Table */}
      {endpoints.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium dark:text-gray-300">Top Endpoints by Cost ({endpoints.length})</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b dark:border-gray-700">
                    <SortableHeader label="Endpoint" sortKey="endpoint" current={epSort} onToggle={epToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                    <SortableHeader label="Cost" sortKey="cost" current={epSort} onToggle={epToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                    <SortableHeader label="DBUs" sortKey="dbus" current={epSort} onToggle={epToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  </tr>
                </thead>
                <tbody>
                  {pagedEndpoints.map((ep, i) => (
                    <tr key={i} className="border-b dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-800/40">
                      <td className="py-2 px-3 font-mono text-xs dark:text-gray-300">{ep.endpoint_name}</td>
                      <td className="py-2 px-3 text-right dark:text-gray-300">{fmtCost(Number(ep.total_cost))}</td>
                      <td className="py-2 px-3 text-right dark:text-gray-300">{fmtNumber(Number(ep.total_dbus))}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <TablePagination
              page={epPage}
              totalItems={sortedEndpoints.length}
              pageSize={epPageSize}
              onPageChange={setEpPage}
              onPageSizeChange={setEpPageSize}
            />
          </CardContent>
        </Card>
      )}

      {/* Agent Inventory */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium dark:text-gray-300">
            Agent Inventory ({wsAgents.length})
          </CardTitle>
        </CardHeader>
        <CardContent>
          {wsAgents.length > 0 ? (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                <thead>
                  <tr className="border-b dark:border-gray-700">
                    <SortableHeader label="Name" sortKey="name" current={agentSort} onToggle={agentToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                    <SortableHeader label="Type" sortKey="type" current={agentSort} onToggle={agentToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                    <SortableHeader label="Endpoint" sortKey="endpoint" current={agentSort} onToggle={agentToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                    <SortableHeader label="Model" sortKey="model" current={agentSort} onToggle={agentToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                    <SortableHeader label="Source" sortKey="source" current={agentSort} onToggle={agentToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  </tr>
                </thead>
                  <tbody>
                    {pagedAgents.map((a, i) => (
                      <tr key={i} className="border-b dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-800/40">
                        <td className="py-2 px-3 font-medium dark:text-gray-200">{a.name}</td>
                        <td className="py-2 px-3">
                          <Badge variant="default" className="text-xs">{a.type?.replace(/_/g, ' ')}</Badge>
                        </td>
                        <td className="py-2 px-3 font-mono text-xs dark:text-gray-400">{a.endpoint_name || '—'}</td>
                        <td className="py-2 px-3 font-mono text-xs dark:text-gray-400">{a.model_name || '—'}</td>
                        <td className="py-2 px-3">
                          <Badge variant={a.source === 'api' ? 'success' : 'default'} className="text-xs">{a.source}</Badge>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <TablePagination
                page={agentPage}
                totalItems={sortedAgents.length}
                pageSize={agentPageSize}
                onPageChange={setAgentPage}
                onPageSizeChange={setAgentPageSize}
              />
            </>
          ) : (
            <p className="text-gray-400 text-sm">No agents discovered in this workspace</p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Cost Breakdown Tab ──────────────────────────────────────── */

function CostBreakdownTab({ data }: { data: WorkspacePageData | undefined }) {
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort: costSort, toggle: costToggle } = useSort('serving', 'desc')

  const wsCosts = useMemo(() => {
    if (!data?.workspace_summaries) return []
    return data.workspace_summaries.map((ws) => ({
      workspace: shortWs(ws.workspace_id),
      full_id: ws.workspace_id,
      serving: Number(ws.serving_cost),
      total: Number(ws.total_all_product_cost),
      dbus: Number(ws.serving_dbus),
    }))
  }, [data?.workspace_summaries])

  const costAccessor = useCallback((row: { full_id: string; serving: number; total: number; dbus: number }, key: string) => {
    switch (key) {
      case 'workspace_id': return (row.full_id || '').toLowerCase()
      case 'serving': return Number(row.serving || 0)
      case 'total': return Number(row.total || 0)
      case 'dbus': return Number(row.dbus || 0)
      default: return null
    }
  }, [])
  const sortedCosts = useMemo(() => sortRows(wsCosts, costSort, costAccessor), [wsCosts, costSort, costAccessor])

  if (!data) return null

  const pagedCosts = sortedCosts.slice(page * pageSize, (page + 1) * pageSize)

  // Product cost aggregated
  const productTotals = useMemo(() => {
    const map: Record<string, number> = {}
    for (const r of data.products_by_workspace) {
      const name = r.billing_origin_product
        .replace(/_/g, ' ')
        .replace(/\b\w/g, (c: string) => c.toUpperCase())
      map[name] = (map[name] || 0) + Number(r.total_cost)
    }
    return Object.entries(map)
      .map(([name, cost]) => ({ name, cost }))
      .sort((a, b) => b.cost - a.cost)
  }, [data.products_by_workspace])

  return (
    <div className="space-y-6">
      {/* Bar: serving cost per workspace */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium dark:text-gray-300">Serving Cost by Workspace</CardTitle>
        </CardHeader>
        <CardContent>
          {wsCosts.length > 0 ? (
            <BarChart data={wsCosts.slice(0, 15)} dataKey="serving" nameKey="workspace" multiColor height={280} />
          ) : (
            <p className="text-gray-400 text-sm">No cost data</p>
          )}
        </CardContent>
      </Card>

      {/* Bar: product cost totals */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium dark:text-gray-300">Cost by Product (All Workspaces)</CardTitle>
        </CardHeader>
        <CardContent>
          {productTotals.length > 0 ? (
            <BarChart data={productTotals.slice(0, 12)} dataKey="cost" nameKey="name" multiColor height={280} />
          ) : (
            <p className="text-gray-400 text-sm">No product data</p>
          )}
        </CardContent>
      </Card>

      {/* Table: detailed workspace costs */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium dark:text-gray-300">Workspace Cost Details</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b dark:border-gray-700">
                  <SortableHeader label="Workspace ID" sortKey="workspace_id" current={costSort} onToggle={costToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="Serving Cost" sortKey="serving" current={costSort} onToggle={costToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="Total Cost" sortKey="total" current={costSort} onToggle={costToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="DBUs" sortKey="dbus" current={costSort} onToggle={costToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                </tr>
              </thead>
              <tbody>
                {pagedCosts.map((r, i) => (
                  <tr key={i} className="border-b dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-800/40">
                    <td className="py-2 px-3 font-mono text-xs dark:text-gray-300">{r.full_id}</td>
                    <td className="py-2 px-3 text-right dark:text-gray-300">{fmtCost(r.serving)}</td>
                    <td className="py-2 px-3 text-right dark:text-gray-300">{fmtCost(r.total)}</td>
                    <td className="py-2 px-3 text-right dark:text-gray-300">{fmtNumber(r.dbus)}</td>
                  </tr>
                ))}
                {sortedCosts.length === 0 && (
                  <tr>
                    <td colSpan={4} className="py-8 text-center text-gray-400">No cost data</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination
            page={page}
            totalItems={sortedCosts.length}
            pageSize={pageSize}
            onPageChange={setPage}
            onPageSizeChange={setPageSize}
          />
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Agent Inventory Tab ─────────────────────────────────────── */

function AgentInventoryTab({ data }: { data: WorkspacePageData | undefined }) {
  const [agentSearch, setAgentSearch] = useState('')
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort: agentInvSort, toggle: agentInvToggle } = useSort('name', 'asc')

  const agents = useMemo(() => {
    if (!data?.all_agents) return []
    let list = data.all_agents
    if (agentSearch) {
      const q = agentSearch.toLowerCase()
      list = list.filter(
        (a) =>
          a.name.toLowerCase().includes(q) ||
          a.type.toLowerCase().includes(q) ||
          a.workspace_id.toLowerCase().includes(q),
      )
    }
    return list
  }, [data?.all_agents, agentSearch])

  const agentInvAccessor = useCallback((row: { workspace_id?: string; name?: string; type?: string; endpoint_name?: string; model_name?: string; source?: string }, key: string) => {
    switch (key) {
      case 'workspace': return (row.workspace_id || '').toLowerCase()
      case 'name': return (row.name || '').toLowerCase()
      case 'type': return (row.type || '').toLowerCase()
      case 'endpoint': return (row.endpoint_name || '').toLowerCase()
      case 'model': return (row.model_name || '').toLowerCase()
      case 'source': return (row.source || '').toLowerCase()
      default: return null
    }
  }, [])
  const sortedAgentsInv = useMemo(() => sortRows(agents, agentInvSort, agentInvAccessor), [agents, agentInvSort, agentInvAccessor])

  const pagedAgents = sortedAgentsInv.slice(page * pageSize, (page + 1) * pageSize)

  // Type summary
  const typeSummary = useMemo(() => {
    if (!data?.agent_type_breakdown) return []
    const map: Record<string, number> = {}
    for (const r of data.agent_type_breakdown) {
      map[r.agent_type] = (map[r.agent_type] || 0) + r.count
    }
    return Object.entries(map)
      .map(([type, count]) => ({ type, count }))
      .sort((a, b) => b.count - a.count)
  }, [data?.agent_type_breakdown])

  if (!data) return null

  return (
    <div className="space-y-6">
      {/* Summary bar */}
      <div className="flex flex-wrap gap-2">
        {typeSummary.map(({ type, count }) => (
          <Badge key={type} variant="default" className="text-sm px-3 py-1">
            {type.replace(/_/g, ' ')} · {count}
          </Badge>
        ))}
      </div>

      {/* Agent type chart */}
      {typeSummary.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium dark:text-gray-300">Agents by Type (All Workspaces)</CardTitle>
          </CardHeader>
          <CardContent>
            <BarChart
              data={typeSummary.map((t) => ({ name: t.type.replace(/_/g, ' '), count: t.count }))}
              dataKey="count"
              nameKey="name"
              multiColor
              height={220}
            />
          </CardContent>
        </Card>
      )}

      {/* Agent table */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-sm font-medium dark:text-gray-300">All Agents ({agents.length})</CardTitle>
          <div className="relative w-64">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={agentSearch}
              onChange={(e) => { setAgentSearch(e.target.value); setPage(0) }}
              placeholder="Search agents…"
              className="w-full pl-9 pr-3 py-1.5 text-sm border rounded-lg dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200"
            />
          </div>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b dark:border-gray-700">
                  <SortableHeader label="Workspace" sortKey="workspace" current={agentInvSort} onToggle={agentInvToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="Name" sortKey="name" current={agentInvSort} onToggle={agentInvToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="Type" sortKey="type" current={agentInvSort} onToggle={agentInvToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="Endpoint" sortKey="endpoint" current={agentInvSort} onToggle={agentInvToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="Model" sortKey="model" current={agentInvSort} onToggle={agentInvToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="Source" sortKey="source" current={agentInvSort} onToggle={agentInvToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                </tr>
              </thead>
              <tbody>
                {pagedAgents.map((a, i) => (
                  <tr key={i} className="border-b dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-800/40">
                    <td className="py-2 px-3 font-mono text-xs dark:text-gray-400">{shortWs(a.workspace_id)}</td>
                    <td className="py-2 px-3 font-medium dark:text-gray-200">{a.name}</td>
                    <td className="py-2 px-3">
                      <Badge variant="default" className="text-xs">{a.type?.replace(/_/g, ' ')}</Badge>
                    </td>
                    <td className="py-2 px-3 font-mono text-xs text-gray-500 dark:text-gray-400">{a.endpoint_name || '—'}</td>
                    <td className="py-2 px-3 font-mono text-xs text-gray-500 dark:text-gray-400">{a.model_name || '—'}</td>
                    <td className="py-2 px-3">
                      <Badge variant={a.source === 'api' ? 'success' : 'default'} className="text-xs">{a.source}</Badge>
                    </td>
                  </tr>
                ))}
                {sortedAgentsInv.length === 0 && (
                  <tr>
                    <td colSpan={6} className="py-8 text-center text-gray-400">
                      {agentSearch ? 'No agents match your search' : 'No agents found'}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination
            page={page}
            totalItems={sortedAgentsInv.length}
            pageSize={pageSize}
            onPageChange={setPage}
            onPageSizeChange={setPageSize}
          />
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Top Endpoints Tab ───────────────────────────────────────── */

function TopEndpointsTab({ data }: { data: WorkspacePageData | undefined }) {
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort: epTabSort, toggle: epTabToggle } = useSort('cost', 'desc')

  const topEndpoints = data?.top_endpoints ?? []
  const epTabAccessor = useCallback((row: { workspace_id?: string; endpoint_name?: string; total_cost?: number; total_dbus?: number }, key: string) => {
    switch (key) {
      case 'workspace': return (row.workspace_id || '').toLowerCase()
      case 'endpoint': return (row.endpoint_name || '').toLowerCase()
      case 'cost': return Number(row.total_cost || 0)
      case 'dbus': return Number(row.total_dbus || 0)
      default: return null
    }
  }, [])
  const sortedTopEndpoints = useMemo(() => sortRows(topEndpoints, epTabSort, epTabAccessor), [topEndpoints, epTabSort, epTabAccessor])
  const pagedEndpoints = sortedTopEndpoints.slice(page * pageSize, (page + 1) * pageSize)

  if (!data) return null

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium dark:text-gray-300">
            Top 20 Endpoints by Cost (All Workspaces)
          </CardTitle>
        </CardHeader>
        <CardContent>
          {data.top_endpoints.length > 0 ? (
            <BarChart
              data={data.top_endpoints.slice(0, 10).map((ep) => ({
                name: ep.endpoint_name.length > 30 ? ep.endpoint_name.slice(0, 27) + '…' : ep.endpoint_name,
                cost: Number(ep.total_cost),
              }))}
              dataKey="cost"
              nameKey="name"
              multiColor
              height={300}
            />
          ) : (
            <p className="text-gray-400 text-sm">No endpoint data</p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium dark:text-gray-300">Endpoint Details</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b dark:border-gray-700">
                  <SortableHeader label="Workspace" sortKey="workspace" current={epTabSort} onToggle={epTabToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="Endpoint" sortKey="endpoint" current={epTabSort} onToggle={epTabToggle} className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="Cost" sortKey="cost" current={epTabSort} onToggle={epTabToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                  <SortableHeader label="DBUs" sortKey="dbus" current={epTabSort} onToggle={epTabToggle} align="right" className="py-2 px-3 text-gray-500 dark:text-gray-400" />
                </tr>
              </thead>
              <tbody>
                {pagedEndpoints.map((ep, i) => (
                  <tr key={i} className="border-b dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-800/40">
                    <td className="py-2 px-3 font-mono text-xs dark:text-gray-400">{shortWs(ep.workspace_id)}</td>
                    <td className="py-2 px-3 font-mono text-xs font-medium dark:text-gray-200">{ep.endpoint_name}</td>
                    <td className="py-2 px-3 text-right dark:text-gray-300">{fmtCost(Number(ep.total_cost))}</td>
                    <td className="py-2 px-3 text-right dark:text-gray-300">{fmtNumber(Number(ep.total_dbus))}</td>
                  </tr>
                ))}
                {sortedTopEndpoints.length === 0 && (
                  <tr>
                    <td colSpan={4} className="py-8 text-center text-gray-400">No endpoint data</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination
            page={page}
            totalItems={sortedTopEndpoints.length}
            pageSize={pageSize}
            onPageChange={setPage}
            onPageSizeChange={setPageSize}
          />
        </CardContent>
      </Card>
    </div>
  )
}
