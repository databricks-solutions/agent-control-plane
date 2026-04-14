import { useState, useMemo } from 'react'
import { useQueryClient, useIsFetching } from '@tanstack/react-query'
import {
  useVectorSearchOverview,
  useVectorSearchEndpoints,
  useVectorSearchIndexes,
  useVectorSearchIndexDetails,
  useVectorSearchHealthHistory,
  useVectorSearchPageData,
  useKnowledgeBasesOverview,
  useKnowledgeBasesCostTrend,
  useLakebaseInstances,
  useLakebaseCostSummary,
  useLakebaseCostTrend,
  useLakebaseCostByWorkspace,
  useLakebaseCostByType,
  useKBTopWorkspacesDaily,
  useVSTopWorkspacesDaily,
  useLBTopWorkspacesDaily,
} from '@/api/hooks'
import { apiClient } from '@/api/client'
import { RefreshButton } from '@/components/RefreshButton'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { KpiCard } from '@/components/KpiCard'
import { TablePagination } from '@/components/TablePagination'
import { LazyChart } from '@/components/charts/LazyChart'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import {
  Search,
  Database,
  LayoutDashboard,
  Server,
  CheckCircle2,
  XCircle,
  Loader2,
  ChevronUp,
  ChevronDown,
  Layers,
} from 'lucide-react'
import { DB_GRID, DB_AXIS_TEXT } from '@/lib/brand'

const WS_COLORS = ['#E53935', '#3B82F6', '#10B981', '#F59E0B', '#8B5CF6']

/* ── helpers ─────────────────────────────────────────────────── */

function fmtCost(v: number): string {
  if (v >= 1000) return `$${(v / 1000).toFixed(1)}k`
  if (v >= 1) return `$${v.toFixed(2)}`
  return `$${v.toFixed(4)}`
}

function fmtNumber(v: number): string {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}K`
  return String(Math.round(v))
}

/* ── tabs ─────────────────────────────────────────────────────── */

const TABS = [
  { id: 'overview', label: 'Overview', icon: LayoutDashboard },
  { id: 'vector-search', label: 'Vector Search', icon: Search },
  { id: 'lakebase', label: 'Lakebase', icon: Database },
] as const

type TabId = (typeof TABS)[number]['id']

/* ── main component ──────────────────────────────────────────── */

export default function VectorSearchPage() {
  const [tab, setTab] = useState<TabId>('overview')
  const [days, setDays] = useState(30)
  const [selectedWs, setSelectedWs] = useState<string | null>(null)

  const queryClient = useQueryClient()
  const isFetching = useIsFetching({ queryKey: ['vector-search'] }) > 0
  const updatedAt = queryClient.getQueryState(['vector-search', 'overview'])?.dataUpdatedAt
  const lastSynced = updatedAt ? new Date(updatedAt).toISOString() : null

  // Fetch overview for workspace list in filter dropdown
  const { data: overviewForFilter } = useKnowledgeBasesOverview(days)

  const handleRefresh = async () => {
    try {
      await apiClient.post('/vector-search/refresh')
    } catch {
      // best-effort
    }
    queryClient.invalidateQueries({ queryKey: ['vector-search'] })
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Knowledge Bases</h2>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Vector Search, Lakebase &mdash; performance &amp; cost monitoring
          </p>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={selectedWs || ''}
            onChange={(e) => setSelectedWs(e.target.value || null)}
            className="text-xs border border-gray-300 dark:border-gray-600 rounded-md px-2.5 py-1.5 bg-white dark:bg-gray-800"
          >
            <option value="">All Workspaces</option>
            {(overviewForFilter?.top_workspaces || []).slice(0, 20).map((ws: any) => (
              <option key={ws.workspace_id} value={ws.workspace_id}>WS {String(ws.workspace_id).substring(0, 12)}...</option>
            ))}
          </select>
          <RefreshButton
            onRefresh={handleRefresh}
            isRefreshing={isFetching}
            lastSynced={lastSynced}
            title="Refresh Knowledge Bases data"
          />
        </div>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-gray-200 dark:border-gray-700">
        {TABS.map((t) => {
          const Icon = t.icon
          const active = tab === t.id
          return (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={`flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                active
                  ? 'border-db-red text-db-red'
                  : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300 hover:border-gray-300 dark:hover:border-gray-600'
              }`}
            >
              <Icon className="w-4 h-4" />
              {t.label}
            </button>
          )
        })}
      </div>

      {/* Tab content */}
      {tab === 'overview' && <OverviewTab days={days} setDays={setDays} selectedWs={selectedWs} />}
      {tab === 'vector-search' && <VectorSearchTab days={days} setDays={setDays} selectedWs={selectedWs} />}
      {tab === 'lakebase' && <LakebaseTab days={days} setDays={setDays} selectedWs={selectedWs} />}
    </div>
  )
}

/* ── tooltip style helper ───────────────────────────────────── */

const TOOLTIP_STYLE = {
  borderRadius: 8,
  border: `1px solid ${DB_GRID}`,
  fontSize: 13,
  backgroundColor: 'var(--tooltip-bg, #fff)',
  color: 'var(--tooltip-text, #1f2937)',
}

/* ── Sort helpers ────────────────────────────────────────────── */

function SortIcon({ sortCol, sortDir, col }: { sortCol: string; sortDir: 'asc' | 'desc'; col: string }) {
  if (sortCol !== col) return null
  return sortDir === 'asc' ? <ChevronUp className="w-3 h-3 inline ml-0.5" /> : <ChevronDown className="w-3 h-3 inline ml-0.5" />
}

/* ── Overview Tab ────────────────────────────────────────────── */

function OverviewTab({ days, setDays, selectedWs }: { days: number; setDays: (d: number) => void; selectedWs: string | null }) {
  const { data: overview, isLoading: overviewLoading } = useKnowledgeBasesOverview(days)
  const { data: costTrend, isLoading: trendLoading } = useKnowledgeBasesCostTrend(days)
  const { data: kbTopWsDaily } = useKBTopWorkspacesDaily(days)

  const [owSortCol, setOwSortCol] = useState<string>('total_cost')
  const [owSortDir, setOwSortDir] = useState<'asc' | 'desc'>('desc')
  const [owPage, setOwPage] = useState(0)
  const [owPageSize, setOwPageSize] = useState(10)

  const handleOwSort = (col: string) => {
    if (owSortCol === col) setOwSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setOwSortCol(col); setOwSortDir('desc') }
    setOwPage(0)
  }

  const isLoading = overviewLoading || trendLoading

  const vs = overview?.vector_search ?? { total_dbus: 0, total_cost_usd: 0, endpoint_count: 0, workspace_count: 0 }
  const lb = overview?.lakebase ?? { total_dbus: 0, total_cost_usd: 0, workspace_count: 0 }
  const totalCost = Number(vs.total_cost_usd || 0) + Number(lb.total_cost_usd || 0)

  // Collect unique workspaces from overview data
  const vsWsCount = Number(vs.workspace_count || 0)
  const lbWsCount = Number(lb.workspace_count || 0)
  const totalWorkspaces = Math.max(vsWsCount, lbWsCount, vsWsCount + lbWsCount > 0 ? vsWsCount + lbWsCount : 0)

  // Pivot combined cost trend by date with one key per product
  const pivotedTrend = useMemo(() => {
    const safeTrend = Array.isArray(costTrend) ? costTrend : []
    const map: Record<string, Record<string, number> & { date: string }> = {}
    for (const row of safeTrend) {
      const date = row.usage_date || row.date || ''
      if (!map[date]) map[date] = { date } as any
      const product = (row.product || row.billing_origin_product || 'UNKNOWN').toUpperCase()
      const cost = Number(row.total_cost_usd || row.cost || 0)
      if (product.includes('VECTOR_SEARCH') || product.includes('VECTOR SEARCH')) {
        map[date]['VECTOR_SEARCH'] = (map[date]['VECTOR_SEARCH'] || 0) + cost
      } else if (product.includes('LAKEBASE') || product.includes('DATABASE')) {
        // Keep DATABASE as separate if present
        const key = product.includes('DATABASE') ? 'DATABASE' : 'LAKEBASE'
        map[date][key] = (map[date][key] || 0) + cost
      } else {
        map[date][product] = (map[date][product] || 0) + cost
      }
    }
    return Object.values(map).sort((a, b) => a.date.localeCompare(b.date))
  }, [costTrend])

  // Determine which product keys exist in the data
  const productKeys = useMemo(() => {
    const keys = new Set<string>()
    for (const row of pivotedTrend) {
      for (const k of Object.keys(row)) {
        if (k !== 'date') keys.add(k)
      }
    }
    return keys
  }, [pivotedTrend])

  // Top workspaces from overview
  const topWorkspaces = useMemo(() => {
    const ws = Array.isArray(overview?.top_workspaces) ? overview.top_workspaces : []
    return ws.map((w: any) => ({
      workspace_id: String(w.workspace_id || ''),
      vs_cost: Number(w.vs_cost || 0),
      lb_cost: Number(w.lb_cost || 0),
      total_cost: Number(w.total_cost || 0),
    })).slice(0, 20)
  }, [overview])

  // Filter workspaces by selected workspace
  const filteredWorkspaces = useMemo(() => {
    if (!selectedWs) return topWorkspaces
    return topWorkspaces.filter((w: any) => w.workspace_id === selectedWs)
  }, [topWorkspaces, selectedWs])

  // Sorted + paginated workspaces
  const sortedOwWorkspaces = useMemo(() => {
    const rows = [...filteredWorkspaces]
    rows.sort((a: any, b: any) => {
      const av = owSortCol === 'workspace_id' ? String(a[owSortCol] || '') : Number(a[owSortCol] || 0)
      const bv = owSortCol === 'workspace_id' ? String(b[owSortCol] || '') : Number(b[owSortCol] || 0)
      const cmp = av < bv ? -1 : av > bv ? 1 : 0
      return owSortDir === 'asc' ? cmp : -cmp
    })
    return rows
  }, [filteredWorkspaces, owSortCol, owSortDir])

  const pagedOwWorkspaces = useMemo(() => {
    const start = owPage * owPageSize
    return sortedOwWorkspaces.slice(start, start + owPageSize)
  }, [sortedOwWorkspaces, owPage, owPageSize])

  // Pivot top workspace daily data
  const topWsDailyData = useMemo(() => {
    const raw = kbTopWsDaily || []
    const byDate: Record<string, Record<string, number>> = {}
    const wsIds = new Set<string>()
    for (const row of raw) {
      const d = row.usage_date || ''
      const ws = String(row.workspace_id || '').substring(0, 12)
      wsIds.add(ws)
      if (!byDate[d]) byDate[d] = { date: d } as any
      byDate[d][ws] = Number(row.total_cost_usd || 0)
    }
    return { data: Object.values(byDate).sort((a: any, b: any) => (a.date > b.date ? 1 : -1)), workspaces: Array.from(wsIds) }
  }, [kbTopWsDaily])

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12 text-gray-400">
        <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading overview data...
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Days selector */}
      <div className="flex justify-end">
        <select
          value={days}
          onChange={(e) => setDays(Number(e.target.value))}
          className="border rounded-lg px-3 py-2 text-sm dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200"
        >
          <option value={7}>Last 7 days</option>
          <option value={14}>Last 14 days</option>
          <option value={30}>Last 30 days</option>
          <option value={90}>Last 90 days</option>
        </select>
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
        <KpiCard title="Total Cost (USD)" value={totalCost} format="currency" />
        <KpiCard title="Vector Search Cost" value={Number(vs.total_cost_usd || 0)} format="currency" />
        <KpiCard title="Lakebase Cost" value={Number(lb.total_cost_usd || 0)} format="currency" />
        <KpiCard title="Total Workspaces" value={totalWorkspaces} format="number" />
      </div>

      {/* Combined Daily Cost Trend */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Combined Daily Cost Trend</CardTitle>
        </CardHeader>
        <CardContent>
          {pivotedTrend.length === 0 ? (
            <div className="text-center py-12 text-gray-400 dark:text-gray-500">
              No billing data for the selected period.
            </div>
          ) : (
            <LazyChart height={300}>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={pivotedTrend}>
                  <CartesianGrid strokeDasharray="3 3" stroke={DB_GRID} />
                  <XAxis dataKey="date" tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} />
                  <YAxis tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} tickFormatter={(v: number) => `$${v.toFixed(0)}`} />
                  <Tooltip
                    formatter={(v: number, name: string) => [`$${v.toFixed(2)}`, name]}
                    contentStyle={TOOLTIP_STYLE}
                  />
                  <Legend wrapperStyle={{ fontSize: 13 }} />
                  {productKeys.has('VECTOR_SEARCH') && (
                    <Line type="monotone" dataKey="VECTOR_SEARCH" stroke="#E53935" strokeWidth={2} dot={false} name="Vector Search" />
                  )}
                  {productKeys.has('LAKEBASE') && (
                    <Line type="monotone" dataKey="LAKEBASE" stroke="#3B82F6" strokeWidth={2} dot={false} name="Lakebase" />
                  )}
                  {productKeys.has('DATABASE') && (
                    <Line type="monotone" dataKey="DATABASE" stroke="#8B5CF6" strokeWidth={2} dot={false} name="Database" />
                  )}
                </LineChart>
              </ResponsiveContainer>
            </LazyChart>
          )}
        </CardContent>
      </Card>

      {/* Daily Cost — Top 5 Workspaces */}
      <Card>
        <CardHeader><CardTitle className="text-base">Daily Cost — Top 5 Workspaces</CardTitle></CardHeader>
        <CardContent>
          {topWsDailyData.data.length === 0 ? (
            <div className="text-center py-12 text-gray-400">No data</div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={topWsDailyData.data}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={(v) => `$${v >= 1000 ? (v/1000).toFixed(0) + 'k' : v.toFixed(0)}`} />
                <Tooltip formatter={(v: number) => [`$${v.toFixed(2)}`, '']} />
                <Legend />
                {topWsDailyData.workspaces.map((ws, i) => (
                  <Line key={ws} type="monotone" dataKey={ws} stroke={WS_COLORS[i % WS_COLORS.length]} strokeWidth={2} dot={false} name={ws} />
                ))}
              </LineChart>
            </ResponsiveContainer>
          )}
        </CardContent>
      </Card>

      {/* Top Workspaces table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Top Workspaces by Spend</CardTitle>
        </CardHeader>
        <CardContent>
          {filteredWorkspaces.length === 0 ? (
            <div className="text-center py-8 text-gray-400 dark:text-gray-500">No workspace cost data.</div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                      <th className="pb-2 pr-4 font-medium cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200" onClick={() => handleOwSort('workspace_id')}>
                        Workspace ID <SortIcon sortCol={owSortCol} sortDir={owSortDir} col="workspace_id" />
                      </th>
                      <th className="pb-2 pr-4 font-medium text-right cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200" onClick={() => handleOwSort('vs_cost')}>
                        Vector Search <SortIcon sortCol={owSortCol} sortDir={owSortDir} col="vs_cost" />
                      </th>
                      <th className="pb-2 pr-4 font-medium text-right cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200" onClick={() => handleOwSort('lb_cost')}>
                        Lakebase <SortIcon sortCol={owSortCol} sortDir={owSortDir} col="lb_cost" />
                      </th>
                      <th className="pb-2 font-medium text-right cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200" onClick={() => handleOwSort('total_cost')}>
                        Total Cost <SortIcon sortCol={owSortCol} sortDir={owSortDir} col="total_cost" />
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                    {pagedOwWorkspaces.map((ws: any, i: number) => (
                      <tr key={ws.workspace_id || i} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                        <td className="py-2 pr-4 font-medium text-gray-900 dark:text-gray-100">{ws.workspace_id || '\u2014'}</td>
                        <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtCost(ws.vs_cost)}</td>
                        <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtCost(ws.lb_cost)}</td>
                        <td className="py-2 text-right tabular-nums font-medium text-gray-900 dark:text-gray-100">{fmtCost(ws.total_cost)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <TablePagination
                page={owPage}
                totalItems={sortedOwWorkspaces.length}
                pageSize={owPageSize}
                onPageChange={setOwPage}
                onPageSizeChange={setOwPageSize}
              />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Vector Search Tab ──────────────────────────────────────── */

function VectorSearchTab({ days, setDays, selectedWs }: { days: number; setDays: (d: number) => void; selectedWs: string | null }) {
  const { data, isLoading: pageLoading } = useVectorSearchPageData(days)
  const { data: indexDetails, isLoading: idxLoading } = useVectorSearchIndexDetails()
  const { data: healthHistory } = useVectorSearchHealthHistory(days)
  const { data: vsTopWsDaily } = useVSTopWorkspacesDaily(days)

  const [sortCol, setSortCol] = useState<string>('total_cost_usd')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [wsPage, setWsPage] = useState(0)
  const [wsPageSize, setWsPageSize] = useState(10)
  const [healthPage, setHealthPage] = useState(0)
  const [healthPageSize, setHealthPageSize] = useState(10)

  const costSummary = data?.cost_summary
  const costByWorkspace = Array.isArray(data?.cost_by_workspace) ? data.cost_by_workspace : []
  const costTrendByWorkload = Array.isArray(data?.cost_trend_by_workload) ? data.cost_trend_by_workload : []

  // Pivot cost_trend_by_workload by date
  const trendByDate = useMemo(() => {
    const map: Record<string, { date: string; ingest: number; serving: number; storage: number }> = {}
    for (const row of costTrendByWorkload) {
      const date = row.usage_date || ''
      if (!map[date]) map[date] = { date, ingest: 0, serving: 0, storage: 0 }
      const wt = (row.workload_type || 'ingest').toLowerCase() as 'ingest' | 'serving' | 'storage'
      const cost = Number(row.total_cost_usd || 0)
      map[date][wt] = cost
    }
    return Object.values(map).sort((a, b) => a.date.localeCompare(b.date))
  }, [costTrendByWorkload])

  // Sorted workspaces
  const sortedWorkspaces = useMemo(() => {
    const rows = costByWorkspace.map((r: any) => ({
      workspace_id: String(r.workspace_id || ''),
      total_dbus: Number(r.total_dbus || 0),
      total_cost_usd: Number(r.total_cost_usd || 0),
      endpoint_count: Number(r.endpoint_count || 0),
    }))
    rows.sort((a: any, b: any) => {
      const av = a[sortCol] ?? 0
      const bv = b[sortCol] ?? 0
      if (typeof av === 'string') return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return sortDir === 'asc' ? av - bv : bv - av
    })
    return rows
  }, [costByWorkspace, sortCol, sortDir])

  const pagedWorkspaces = sortedWorkspaces.slice(wsPage * wsPageSize, (wsPage + 1) * wsPageSize)

  const handleSort = (col: string) => {
    if (sortCol === col) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortCol(col)
      setSortDir('desc')
    }
    setWsPage(0)
  }

  // Pivot VS top workspace daily data
  const vsTopWsDailyData = useMemo(() => {
    const raw = vsTopWsDaily || []
    const byDate: Record<string, Record<string, number>> = {}
    const wsIds = new Set<string>()
    for (const row of raw) {
      const d = row.usage_date || ''
      const ws = String(row.workspace_id || '').substring(0, 12)
      wsIds.add(ws)
      if (!byDate[d]) byDate[d] = { date: d } as any
      byDate[d][ws] = Number(row.total_cost_usd || 0)
    }
    return { data: Object.values(byDate).sort((a: any, b: any) => (a.date > b.date ? 1 : -1)), workspaces: Array.from(wsIds) }
  }, [vsTopWsDaily])

  // Filter workspaces by selected workspace
  const filteredVsWorkspaces = useMemo(() => {
    if (!selectedWs) return pagedWorkspaces
    return sortedWorkspaces.filter((w: any) => w.workspace_id === selectedWs)
  }, [sortedWorkspaces, pagedWorkspaces, selectedWs])

  // Health KPIs
  const safeIndexDetails = Array.isArray(indexDetails) ? indexDetails : []
  const safeHealthHistory = Array.isArray(healthHistory) ? healthHistory : []
  const totalRows = safeIndexDetails.reduce((s: number, i: any) => s + (Number(i.indexed_row_count) || 0), 0)
  const readyCount = safeIndexDetails.filter((i: any) => i.ready).length
  const totalSnapshots = safeHealthHistory.length
  const onlineSnapshots = safeHealthHistory.filter((h: any) => h.status === 'ONLINE').length
  const uptimePct = totalSnapshots > 0 ? Math.round((onlineSnapshots / totalSnapshots) * 100) : 100

  const pagedHealth = safeHealthHistory.slice(healthPage * healthPageSize, (healthPage + 1) * healthPageSize)

  if (pageLoading) {
    return (
      <div className="flex items-center justify-center py-12 text-gray-400">
        <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading Vector Search data...
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Days selector */}
      <div className="flex justify-end">
        <select
          value={days}
          onChange={(e) => setDays(Number(e.target.value))}
          className="border rounded-lg px-3 py-2 text-sm dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200"
        >
          <option value={7}>Last 7 days</option>
          <option value={14}>Last 14 days</option>
          <option value={30}>Last 30 days</option>
          <option value={90}>Last 90 days</option>
        </select>
      </div>

      {/* Cost KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
        <KpiCard title="Total VS Cost (USD)" value={costSummary?.total_cost_usd ?? 0} format="currency" />
        <KpiCard title="Total DBUs" value={costSummary?.total_dbus ?? 0} format="number" />
        <KpiCard title="Endpoints" value={costSummary?.endpoint_count ?? 0} format="number" />
        <KpiCard title="Workspaces" value={costSummary?.workspace_count ?? 0} format="number" />
      </div>

      {/* Daily Cost by Workload Type */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Daily Cost by Workload Type</CardTitle>
        </CardHeader>
        <CardContent>
          {trendByDate.length === 0 ? (
            <div className="text-center py-12 text-gray-400 dark:text-gray-500">
              No billing data for the selected period.
            </div>
          ) : (
            <LazyChart height={300}>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={trendByDate}>
                  <CartesianGrid strokeDasharray="3 3" stroke={DB_GRID} />
                  <XAxis dataKey="date" tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} />
                  <YAxis tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} tickFormatter={(v: number) => `$${v.toFixed(0)}`} />
                  <Tooltip
                    formatter={(v: number, name: string) => [`$${v.toFixed(2)}`, name]}
                    contentStyle={TOOLTIP_STYLE}
                  />
                  <Legend wrapperStyle={{ fontSize: 13 }} />
                  <Line type="monotone" dataKey="ingest" stroke="#3B82F6" strokeWidth={2} dot={false} name="Ingest" />
                  <Line type="monotone" dataKey="serving" stroke="#10B981" strokeWidth={2} dot={false} name="Serving" />
                  <Line type="monotone" dataKey="storage" stroke="#F59E0B" strokeWidth={2} dot={false} name="Storage" />
                </LineChart>
              </ResponsiveContainer>
            </LazyChart>
          )}
        </CardContent>
      </Card>

      {/* Daily VS Cost — Top 5 Workspaces */}
      <Card>
        <CardHeader><CardTitle className="text-base">Daily VS Cost — Top 5 Workspaces</CardTitle></CardHeader>
        <CardContent>
          {vsTopWsDailyData.data.length === 0 ? (
            <div className="text-center py-12 text-gray-400">No data</div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={vsTopWsDailyData.data}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={(v) => `$${v >= 1000 ? (v/1000).toFixed(0) + 'k' : v.toFixed(0)}`} />
                <Tooltip formatter={(v: number) => [`$${v.toFixed(2)}`, '']} />
                <Legend />
                {vsTopWsDailyData.workspaces.map((ws, i) => (
                  <Line key={ws} type="monotone" dataKey={ws} stroke={WS_COLORS[i % WS_COLORS.length]} strokeWidth={2} dot={false} name={ws} />
                ))}
              </LineChart>
            </ResponsiveContainer>
          )}
        </CardContent>
      </Card>

      {/* All Workspaces table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">All Workspaces</CardTitle>
        </CardHeader>
        <CardContent>
          {filteredVsWorkspaces.length === 0 ? (
            <div className="text-center py-8 text-gray-400 dark:text-gray-500">No workspace data.</div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                      <th
                        className="pb-2 pr-4 font-medium cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200"
                        onClick={() => handleSort('workspace_id')}
                      >
                        Workspace ID <SortIcon sortCol={sortCol} sortDir={sortDir} col="workspace_id" />
                      </th>
                      <th
                        className="pb-2 pr-4 font-medium text-right cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200"
                        onClick={() => handleSort('total_dbus')}
                      >
                        Total DBUs <SortIcon sortCol={sortCol} sortDir={sortDir} col="total_dbus" />
                      </th>
                      <th
                        className="pb-2 pr-4 font-medium text-right cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200"
                        onClick={() => handleSort('total_cost_usd')}
                      >
                        Total Cost (USD) <SortIcon sortCol={sortCol} sortDir={sortDir} col="total_cost_usd" />
                      </th>
                      <th
                        className="pb-2 font-medium text-right cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200"
                        onClick={() => handleSort('endpoint_count')}
                      >
                        Endpoint Count <SortIcon sortCol={sortCol} sortDir={sortDir} col="endpoint_count" />
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                    {filteredVsWorkspaces.map((row: any, i: number) => (
                      <tr key={row.workspace_id || i} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                        <td className="py-2 pr-4 font-medium text-gray-900 dark:text-gray-100">{row.workspace_id || '\u2014'}</td>
                        <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtNumber(row.total_dbus)}</td>
                        <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtCost(row.total_cost_usd)}</td>
                        <td className="py-2 text-right tabular-nums text-gray-700 dark:text-gray-300">{row.endpoint_count}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!selectedWs && (
                <TablePagination
                  page={wsPage}
                  totalItems={sortedWorkspaces.length}
                  pageSize={wsPageSize}
                  onPageChange={setWsPage}
                  onPageSizeChange={setWsPageSize}
                />
              )}
            </>
          )}
        </CardContent>
      </Card>

      {/* Section divider */}
      <div className="flex items-center gap-3 pt-4">
        <Layers className="w-5 h-5 text-gray-400" />
        <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Index Inventory &amp; Health</h3>
        <div className="flex-1 border-t border-gray-200 dark:border-gray-700" />
      </div>

      {/* Health KPIs */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <KpiCard title="Total Indexed Rows" value={totalRows} format="number" />
        <KpiCard title="Ready Indexes" value={readyCount} format="number" />
        <KpiCard title="Endpoint Uptime" value={uptimePct} format="percentage" />
        <KpiCard title="Health Snapshots" value={totalSnapshots} format="number" />
      </div>

      {/* Index Sync Status table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Index Sync Status</CardTitle>
        </CardHeader>
        <CardContent>
          {idxLoading ? (
            <div className="flex items-center justify-center py-12 text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading...
            </div>
          ) : safeIndexDetails.length === 0 ? (
            <div className="text-center py-12 text-gray-400 dark:text-gray-500">
              No index data. Click refresh to discover indexes.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                    <th className="pb-2 pr-4 font-medium">Index</th>
                    <th className="pb-2 pr-4 font-medium">State</th>
                    <th className="pb-2 pr-4 font-medium text-right">Rows</th>
                    <th className="pb-2 pr-4 font-medium">Source Table</th>
                    <th className="pb-2 pr-4 font-medium">Embedding Model</th>
                    <th className="pb-2 font-medium">Pipeline</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                  {safeIndexDetails.map((idx: any) => (
                    <tr key={`${idx.endpoint_name}-${idx.index_name}`} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                      <td className="py-2 pr-4">
                        <div className="font-medium text-gray-900 dark:text-gray-100 text-xs truncate max-w-[200px]" title={idx.index_name}>
                          {idx.index_name?.split('.').pop() || idx.index_name}
                        </div>
                        <div className="text-[11px] text-gray-400 truncate max-w-[200px]">{idx.endpoint_name}</div>
                      </td>
                      <td className="py-2 pr-4">
                        <Badge variant={idx.ready ? 'success' : 'default'} className="text-[10px]">
                          {idx.detailed_state || idx.index_type || '\u2014'}
                        </Badge>
                      </td>
                      <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">
                        {Number(idx.indexed_row_count || 0).toLocaleString()}
                      </td>
                      <td className="py-2 pr-4 text-xs text-gray-500 truncate max-w-[180px]" title={idx.source_table}>
                        {idx.source_table?.split('.').pop() || '\u2014'}
                      </td>
                      <td className="py-2 pr-4 text-xs text-gray-500">
                        {idx.embedding_model || '\u2014'}
                      </td>
                      <td className="py-2 text-xs text-gray-500">
                        {idx.pipeline_type || '\u2014'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Endpoint Health History table */}
      {safeHealthHistory.length > 0 && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">Endpoint Health History</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                    <th className="pb-2 pr-4 font-medium">Endpoint</th>
                    <th className="pb-2 pr-4 font-medium">Status</th>
                    <th className="pb-2 pr-4 font-medium text-right">Indexes</th>
                    <th className="pb-2 font-medium">Recorded</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                  {pagedHealth.map((h: any, i: number) => (
                    <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                      <td className="py-2 pr-4 text-gray-900 dark:text-gray-100 text-xs">{h.endpoint_name}</td>
                      <td className="py-2 pr-4">
                        <Badge variant={h.status === 'ONLINE' ? 'success' : 'error'} className="text-[10px]">
                          {h.status}
                        </Badge>
                      </td>
                      <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{h.num_indexes}</td>
                      <td className="py-2 text-xs text-gray-500">{h.recorded_at ? new Date(h.recorded_at).toLocaleString() : '\u2014'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <TablePagination
              page={healthPage}
              totalItems={safeHealthHistory.length}
              pageSize={healthPageSize}
              onPageChange={setHealthPage}
              onPageSizeChange={setHealthPageSize}
            />
          </CardContent>
        </Card>
      )}
    </div>
  )
}

/* ── Lakebase Tab ────────────────────────────────────────────── */

function LakebaseTab({ days, setDays, selectedWs }: { days: number; setDays: (d: number) => void; selectedWs: string | null }) {
  const { data: costSummary, isLoading: summaryLoading } = useLakebaseCostSummary(days)
  const { data: costTrend, isLoading: trendLoading } = useLakebaseCostTrend(days)
  const { data: instances, isLoading: instancesLoading } = useLakebaseInstances()
  const { data: costByWorkspace, isLoading: wsByWsLoading } = useLakebaseCostByWorkspace(days)
  const { data: costByType } = useLakebaseCostByType(days)
  const { data: lbTopWsDaily } = useLBTopWorkspacesDaily(days)

  const [sortCol, setSortCol] = useState<string>('total_cost_usd')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [wsPage, setWsPage] = useState(0)
  const [wsPageSize, setWsPageSize] = useState(10)

  const isLoading = summaryLoading || trendLoading || instancesLoading

  const safeInstances = Array.isArray(instances) ? instances : []
  const safeCostTrend = Array.isArray(costTrend) ? costTrend : []
  const safeCostByWorkspace = Array.isArray(costByWorkspace) ? costByWorkspace : []
  const safeCostByType = Array.isArray(costByType) ? costByType : []

  // Cost type breakdown
  const computeCost = useMemo(() => {
    return safeCostByType
      .filter((r: any) => (r.cost_type || r.sku_name || '').toLowerCase().includes('compute'))
      .reduce((s: number, r: any) => s + Number(r.total_cost_usd || 0), 0)
  }, [safeCostByType])

  const storageCost = useMemo(() => {
    return safeCostByType
      .filter((r: any) => (r.cost_type || r.sku_name || '').toLowerCase().includes('storage'))
      .reduce((s: number, r: any) => s + Number(r.total_cost_usd || 0), 0)
  }, [safeCostByType])

  // Trend data for chart
  const trendData = useMemo(() => {
    return safeCostTrend.map((r: any) => ({
      date: r.usage_date || r.date || '',
      cost: Number(r.total_cost_usd || r.cost || 0),
    })).sort((a: any, b: any) => a.date.localeCompare(b.date))
  }, [safeCostTrend])

  // Sorted workspaces
  const sortedWorkspaces = useMemo(() => {
    const rows = safeCostByWorkspace.map((r: any) => ({
      workspace_id: String(r.workspace_id || ''),
      total_dbus: Number(r.total_dbus || 0),
      total_cost_usd: Number(r.total_cost_usd || 0),
    }))
    rows.sort((a: any, b: any) => {
      const av = a[sortCol] ?? 0
      const bv = b[sortCol] ?? 0
      if (typeof av === 'string') return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return sortDir === 'asc' ? av - bv : bv - av
    })
    return rows
  }, [safeCostByWorkspace, sortCol, sortDir])

  const pagedWorkspaces = sortedWorkspaces.slice(wsPage * wsPageSize, (wsPage + 1) * wsPageSize)

  const handleSort = (col: string) => {
    if (sortCol === col) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortCol(col)
      setSortDir('desc')
    }
    setWsPage(0)
  }

  // Pivot LB top workspace daily data
  const lbTopWsDailyData = useMemo(() => {
    const raw = lbTopWsDaily || []
    const byDate: Record<string, Record<string, number>> = {}
    const wsIds = new Set<string>()
    for (const row of raw) {
      const d = row.usage_date || ''
      const ws = String(row.workspace_id || '').substring(0, 12)
      wsIds.add(ws)
      if (!byDate[d]) byDate[d] = { date: d } as any
      byDate[d][ws] = Number(row.total_cost_usd || 0)
    }
    return { data: Object.values(byDate).sort((a: any, b: any) => (a.date > b.date ? 1 : -1)), workspaces: Array.from(wsIds) }
  }, [lbTopWsDaily])

  // Filter workspaces by selected workspace
  const filteredLbWorkspaces = useMemo(() => {
    if (!selectedWs) return pagedWorkspaces
    return sortedWorkspaces.filter((w: any) => w.workspace_id === selectedWs)
  }, [sortedWorkspaces, pagedWorkspaces, selectedWs])

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12 text-gray-400">
        <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading Lakebase data...
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Days selector */}
      <div className="flex justify-end">
        <select
          value={days}
          onChange={(e) => setDays(Number(e.target.value))}
          className="border rounded-lg px-3 py-2 text-sm dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200"
        >
          <option value={7}>Last 7 days</option>
          <option value={14}>Last 14 days</option>
          <option value={30}>Last 30 days</option>
          <option value={90}>Last 90 days</option>
        </select>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
        <KpiCard title="Total Lakebase Cost" value={costSummary?.total_cost_usd ?? 0} format="currency" />
        <KpiCard title="Total DBUs" value={costSummary?.total_dbus ?? 0} format="number" />
        <KpiCard title="Instances" value={safeInstances.length} format="number" />
        <KpiCard title="Workspaces" value={costSummary?.workspace_count ?? 0} format="number" />
      </div>

      {/* Cost Type Summary — compute vs storage */}
      {safeCostByType.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <KpiCard title="Compute Cost" value={computeCost} format="currency" />
          <KpiCard title="Storage Cost" value={storageCost} format="currency" />
        </div>
      )}

      {/* Daily Cost Trend */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Daily Cost Trend</CardTitle>
        </CardHeader>
        <CardContent>
          {trendData.length === 0 ? (
            <div className="text-center py-12 text-gray-400 dark:text-gray-500">
              No billing data for the selected period.
            </div>
          ) : (
            <LazyChart height={300}>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={trendData}>
                  <CartesianGrid strokeDasharray="3 3" stroke={DB_GRID} />
                  <XAxis dataKey="date" tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} />
                  <YAxis tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} tickFormatter={(v: number) => `$${v.toFixed(0)}`} />
                  <Tooltip
                    formatter={(v: number) => [`$${v.toFixed(2)}`, 'Cost']}
                    contentStyle={TOOLTIP_STYLE}
                  />
                  <Line type="monotone" dataKey="cost" stroke="#3B82F6" strokeWidth={2} dot={false} name="Lakebase Cost" />
                </LineChart>
              </ResponsiveContainer>
            </LazyChart>
          )}
        </CardContent>
      </Card>

      {/* Daily Lakebase Cost — Top 5 Workspaces */}
      <Card>
        <CardHeader><CardTitle className="text-base">Daily Lakebase Cost — Top 5 Workspaces</CardTitle></CardHeader>
        <CardContent>
          {lbTopWsDailyData.data.length === 0 ? (
            <div className="text-center py-12 text-gray-400">No data</div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={lbTopWsDailyData.data}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={(v) => `$${v >= 1000 ? (v/1000).toFixed(0) + 'k' : v.toFixed(0)}`} />
                <Tooltip formatter={(v: number) => [`$${v.toFixed(2)}`, '']} />
                <Legend />
                {lbTopWsDailyData.workspaces.map((ws, i) => (
                  <Line key={ws} type="monotone" dataKey={ws} stroke={WS_COLORS[i % WS_COLORS.length]} strokeWidth={2} dot={false} name={ws} />
                ))}
              </LineChart>
            </ResponsiveContainer>
          )}
        </CardContent>
      </Card>

      {/* Instances table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Lakebase Instances</CardTitle>
        </CardHeader>
        <CardContent>
          {safeInstances.length === 0 ? (
            <div className="text-center py-8 text-gray-400 dark:text-gray-500">No instances found.</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                    <th className="pb-2 pr-4 font-medium">Name</th>
                    <th className="pb-2 pr-4 font-medium">State</th>
                    <th className="pb-2 pr-4 font-medium">Capacity</th>
                    <th className="pb-2 font-medium">PG Version</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                  {safeInstances.map((inst: any, i: number) => {
                    const state = (inst.state || inst.status || 'UNKNOWN').toUpperCase()
                    const isAvailable = state === 'AVAILABLE' || state === 'RUNNING' || state === 'ACTIVE'
                    return (
                      <tr key={inst.name || i} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                        <td className="py-2 pr-4 font-medium text-gray-900 dark:text-gray-100">{inst.name || '\u2014'}</td>
                        <td className="py-2 pr-4">
                          <Badge
                            variant={isAvailable ? 'success' : 'error'}
                            className="text-[10px]"
                          >
                            {state}
                          </Badge>
                        </td>
                        <td className="py-2 pr-4 text-gray-700 dark:text-gray-300">{inst.capacity || inst.size || '\u2014'}</td>
                        <td className="py-2 text-gray-700 dark:text-gray-300">{inst.pg_version || inst.engine_version || '\u2014'}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Cost by Workspace table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Cost by Workspace</CardTitle>
        </CardHeader>
        <CardContent>
          {filteredLbWorkspaces.length === 0 ? (
            <div className="text-center py-8 text-gray-400 dark:text-gray-500">No workspace cost data.</div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                      <th
                        className="pb-2 pr-4 font-medium cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200"
                        onClick={() => handleSort('workspace_id')}
                      >
                        Workspace ID <SortIcon sortCol={sortCol} sortDir={sortDir} col="workspace_id" />
                      </th>
                      <th
                        className="pb-2 pr-4 font-medium text-right cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200"
                        onClick={() => handleSort('total_dbus')}
                      >
                        Total DBUs <SortIcon sortCol={sortCol} sortDir={sortDir} col="total_dbus" />
                      </th>
                      <th
                        className="pb-2 font-medium text-right cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200"
                        onClick={() => handleSort('total_cost_usd')}
                      >
                        Total Cost (USD) <SortIcon sortCol={sortCol} sortDir={sortDir} col="total_cost_usd" />
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                    {filteredLbWorkspaces.map((row: any, i: number) => (
                      <tr key={row.workspace_id || i} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                        <td className="py-2 pr-4 font-medium text-gray-900 dark:text-gray-100">{row.workspace_id || '\u2014'}</td>
                        <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtNumber(row.total_dbus)}</td>
                        <td className="py-2 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtCost(row.total_cost_usd)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!selectedWs && (
                <TablePagination
                  page={wsPage}
                  totalItems={sortedWorkspaces.length}
                  pageSize={wsPageSize}
                  onPageChange={setWsPage}
                  onPageSizeChange={setWsPageSize}
                />
              )}
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
