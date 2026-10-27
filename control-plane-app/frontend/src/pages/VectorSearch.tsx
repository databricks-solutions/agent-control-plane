import { useState, useMemo } from 'react'
import { useQueryClient, useIsFetching } from '@tanstack/react-query'
import {
  useVectorSearchOverview,
  useVectorSearchEndpoints,
  useVectorSearchIndexes,
  useVectorSearchIndexDetails,
  useVectorSearchHealthHistory,
  useVectorSearchPageData,
  useVectorSearchUsageTrend,
  useVectorSearchIndexUsage,
  useVectorSearchIndexCallers,
  useKnowledgeBasesOverview,
  useKnowledgeBasesCostTrend,
  useLakebaseInstances,
  useLakebaseCostSummary,
  useLakebaseCostTrend,
  useLakebaseCostByWorkspace,
  useLakebaseCostByType,
  useLakebaseHealth,
  useLakebaseWaitEvents,
  useLakebaseCostByProject,
  useKBTopWorkspacesDaily,
  useVSTopWorkspacesDaily,
  useLBTopWorkspacesDaily,
  useWorkspaceDirectory,
  useSeesAccountWide,
  type VsIndexUsageRow,
  type VsIndexCallerRow,
  type LakebaseHealthRow,
  type LakebaseWaitEventRow,
  type LakebaseProjectCostRow,
} from '@/api/hooks'
import { apiClient } from '@/api/client'
import { RefreshButton } from '@/components/RefreshButton'
import { WorkspaceSelect, workspaceOption } from '@/components/WorkspaceSelect'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { KpiCard } from '@/components/KpiCard'
import { TablePagination } from '@/components/TablePagination'
import { SortableHeader, useSort, sortRows } from '@/components/SortableTable'
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
  Activity,
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
  const [selectedWsIds, setSelectedWsIds] = useState<string[]>([])

  const queryClient = useQueryClient()
  const isFetching = useIsFetching({ queryKey: ['vector-search'] }) > 0
  const updatedAt = queryClient.getQueryState(['vector-search', 'overview'])?.dataUpdatedAt
  const lastSynced = updatedAt ? new Date(updatedAt).toISOString() : null

  // Fetch overview for workspace list in filter dropdown
  const { data: overviewForFilter } = useKnowledgeBasesOverview(days)
  const { data: wsDir } = useWorkspaceDirectory()

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
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Retrieval &amp; Memory</h2>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Vector Search usage, Lakebase health &amp; cost
          </p>
        </div>
        <div className="flex items-center gap-3">
          <WorkspaceSelect
            multiple
            values={selectedWsIds}
            onChangeMulti={setSelectedWsIds}
            options={(overviewForFilter?.top_workspaces || []).map((ws: any) =>
              workspaceOption(String(ws.workspace_id), wsDir))}
            allValue=""
            className="text-xs border border-gray-300 dark:border-gray-600 rounded-md px-2.5 py-1.5 bg-white dark:bg-gray-800 dark:text-gray-200"
          />
          <RefreshButton
            onRefresh={handleRefresh}
            isRefreshing={isFetching}
            lastSynced={lastSynced}
            title="Updates the shared cache, not only the selected workspace"
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
      {tab === 'overview' && <OverviewTab days={days} setDays={setDays} selectedWsIds={selectedWsIds} />}
      {tab === 'vector-search' && <VectorSearchTab days={days} setDays={setDays} selectedWsIds={selectedWsIds} />}
      {tab === 'lakebase' && <LakebaseTab days={days} setDays={setDays} selectedWsIds={selectedWsIds} />}
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

function OverviewTab({ days, setDays, selectedWsIds }: { days: number; setDays: (d: number) => void; selectedWsIds: string[] }) {
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
    if (!selectedWsIds.length) return topWorkspaces
    return topWorkspaces.filter((w: any) => selectedWsIds.includes(String(w.workspace_id)))
  }, [topWorkspaces, selectedWsIds])

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

/* ── Retrieval & Memory helpers ─────────────────────────────── */

function formatBytes(v: number): string {
  if (v >= 1e12) return `${(v / 1e12).toFixed(2)} TB`
  if (v >= 1e9) return `${(v / 1e9).toFixed(2)} GB`
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)} MB`
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)} KB`
  return `${Math.round(v)} B`
}

function shortIndexName(full: string): string {
  return full?.split('.').pop() || full || '—'
}

/** Service principals show up as bare UUIDs in audit identities. */
function callerLabel(caller: string): string {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-/i.test(caller) ? `Service principal ${caller.slice(0, 8)}` : caller
}

/** Agent Bricks label — derived server-side from the endpoint / index name. */
function ManagedByBadge({ managedBy }: { managedBy: VsIndexUsageRow['managed_by'] }) {
  if (!managedBy) return <span className="text-xs text-gray-400">{'—'}</span>
  const label = managedBy === 'knowledge_assistant' ? 'Knowledge Assistant' : 'Supervisor'
  return (
    <Badge variant="info" className="text-[10px] whitespace-nowrap" title="Managed by Agent Bricks">
      Agent Bricks · {label}
    </Badge>
  )
}

/** Honest empty state: scoped callers only see their workspaces, account-wide needs admin. */
function UsageEmpty({ seesAccountWide, days, what }: { seesAccountWide: boolean | undefined; days: number; what: string }) {
  return (
    <div className="text-center py-10 text-gray-400 dark:text-gray-500 text-sm">
      {seesAccountWide === false
        ? `No ${what} for your workspaces in the last ${days} days. Account-wide view requires account admin.`
        : `No ${what} in the last ${days} days. Usage refreshes with the discovery workflow.`}
    </div>
  )
}

/** Pivot "one series per label" rows by day; keep the topN series by peak value. */
function pivotByDay<T extends { usage_date: string }>(
  rows: T[],
  label: (r: T) => string,
  value: (r: T) => number,
  topN = 5,
) {
  const peaks: Record<string, number> = {}
  const byDate: Record<string, Record<string, number | string>> = {}
  for (const r of rows) {
    const s = label(r)
    const v = value(r)
    peaks[s] = Math.max(peaks[s] ?? 0, v)
    if (!byDate[r.usage_date]) byDate[r.usage_date] = { date: r.usage_date }
    byDate[r.usage_date][s] = ((byDate[r.usage_date][s] as number | undefined) ?? 0) + v
  }
  const series = Object.entries(peaks)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([k]) => k)
  const data = Object.values(byDate).sort((a, b) => String(a.date).localeCompare(String(b.date)))
  return { data, series }
}

function projectSeriesLabel(r: { project_name: string; project_id: string; branch_name: string }): string {
  const base = r.project_name || r.project_id || 'unknown'
  return r.branch_name ? `${base} · ${r.branch_name}` : base
}

/* ── Vector Search: index usage (Retrieval & Memory) ───────── */

type IndexUsageRow = VsIndexUsageRow & { name: string; total: number; error_rate: number }

function indexUsageAccessor(r: IndexUsageRow, key: string): string | number | null {
  switch (key) {
    case 'name': return r.name
    case 'managed': return r.managed_by ?? ''
    case 'query_count': return r.query_count
    case 'error_rate': return r.error_rate
    case 'peak_daily_callers': return r.peak_daily_callers
    case 'last_active_date': return r.last_active_date
    default: return null
  }
}

function VectorSearchUsageSection({ days, wsIds }: { days: number; wsIds: string[] }) {
  const { data: trendRaw, isLoading: trendLoading } = useVectorSearchUsageTrend(days, wsIds)
  const { data: indexesRaw, isLoading: indexesLoading } = useVectorSearchIndexUsage(days, wsIds)
  const seesAccountWide = useSeesAccountWide()

  const { sort, toggle } = useSort<string>('query_count', 'desc')
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const [selected, setSelected] = useState<{ index_name: string; workspace_id: string } | null>(null)
  const { data: callersRaw, isLoading: callersLoading } = useVectorSearchIndexCallers(
    selected?.index_name ?? null,
    selected?.workspace_id ?? null,
  )

  // Daily reads (queryVectorIndex) and errors (all actions) for the trend chart.
  const trend = useMemo(() => {
    const byDate: Record<string, { date: string; queries: number; errors: number }> = {}
    for (const r of trendRaw ?? []) {
      const d = r.usage_date
      if (!byDate[d]) byDate[d] = { date: d, queries: 0, errors: 0 }
      if (r.action === 'queryVectorIndex') byDate[d].queries += Number(r.request_count || 0)
      byDate[d].errors += Number(r.error_count || 0)
    }
    return Object.values(byDate).sort((a, b) => a.date.localeCompare(b.date))
  }, [trendRaw])

  const indexRows: IndexUsageRow[] = useMemo(() => (indexesRaw ?? []).map((r) => {
    const total = Number(r.query_count || 0) + Number(r.scan_count || 0) + Number(r.write_count || 0)
    return {
      ...r,
      name: shortIndexName(r.index_name),
      total,
      error_rate: total > 0 ? Number(r.error_count || 0) / total : 0,
    }
  }), [indexesRaw])

  const sortedIdx = useMemo(() => sortRows(indexRows, sort, indexUsageAccessor), [indexRows, sort])
  const pagedIdx = sortedIdx.slice(page * pageSize, (page + 1) * pageSize)

  const totalQueries = indexRows.reduce((s, r) => s + Number(r.query_count || 0), 0)
  const totalAll = indexRows.reduce((s, r) => s + r.total, 0)
  const totalErrors = indexRows.reduce((s, r) => s + Number(r.error_count || 0), 0)
  const activeIndexes = indexRows.filter((r) => r.total > 0).length
  const agentBricksIndexes = indexRows.filter((r) => r.managed_by).length

  const callers: VsIndexCallerRow[] = Array.isArray(callersRaw) ? callersRaw : []
  const selectedName = selected ? shortIndexName(selected.index_name) : ''

  if (trendLoading || indexesLoading) {
    return (
      <div className="flex items-center justify-center py-12 text-gray-400">
        <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading index usage...
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 pt-1">
        <Activity className="w-5 h-5 text-gray-400" />
        <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Index Usage</h3>
        <div className="flex-1 border-t border-gray-200 dark:border-gray-700" />
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <KpiCard title={`Queries (${days}d)`} value={totalQueries} format="number" />
        <KpiCard
          title="Error Rate"
          value={totalAll > 0 ? (totalErrors / totalAll) * 100 : 0}
          format="percentage"
        />
        <KpiCard title="Indexes Queried" value={activeIndexes} format="number" />
        <KpiCard title="Agent Bricks Indexes" value={agentBricksIndexes} format="number" />
      </div>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Daily Queries &amp; Errors</CardTitle>
        </CardHeader>
        <CardContent>
          {trend.length === 0 ? (
            <UsageEmpty seesAccountWide={seesAccountWide} days={days} what="Vector Search audit events" />
          ) : (
            <LazyChart height={260}>
              <ResponsiveContainer width="100%" height={260}>
                <LineChart data={trend}>
                  <CartesianGrid strokeDasharray="3 3" stroke={DB_GRID} />
                  <XAxis dataKey="date" tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} />
                  <YAxis tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} tickFormatter={(v: number) => fmtNumber(v)} />
                  <Tooltip formatter={(v: number, name: string) => [fmtNumber(v), name]} contentStyle={TOOLTIP_STYLE} />
                  <Legend wrapperStyle={{ fontSize: 13 }} />
                  <Line type="monotone" dataKey="queries" stroke="#E53935" strokeWidth={2} dot={false} name="Queries" />
                  <Line type="monotone" dataKey="errors" stroke="#F59E0B" strokeWidth={2} dot={false} name="Errors" />
                </LineChart>
              </ResponsiveContainer>
            </LazyChart>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Top Indexes by Queries</CardTitle>
        </CardHeader>
        <CardContent>
          {indexRows.length === 0 ? (
            <UsageEmpty seesAccountWide={seesAccountWide} days={days} what="Vector Search audit events" />
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-gray-700">
                      <SortableHeader label="Index" sortKey="name" current={sort} onToggle={toggle} className="py-2 pr-4 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Managed by" sortKey="managed" current={sort} onToggle={toggle} className="py-2 pr-4 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Queries" sortKey="query_count" current={sort} onToggle={toggle} align="right" className="py-2 pr-4 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Error Rate" sortKey="error_rate" current={sort} onToggle={toggle} align="right" className="py-2 pr-4 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Peak Daily Callers" sortKey="peak_daily_callers" current={sort} onToggle={toggle} align="right" className="py-2 pr-4 text-gray-500 dark:text-gray-400" />
                      <SortableHeader label="Last Active" sortKey="last_active_date" current={sort} onToggle={toggle} className="py-2 text-gray-500 dark:text-gray-400" />
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                    {pagedIdx.map((r) => {
                      const isSel = selected?.index_name === r.index_name && selected?.workspace_id === r.workspace_id
                      return (
                        <tr
                          key={`${r.workspace_id}-${r.index_name}`}
                          onClick={() => setSelected({ index_name: r.index_name, workspace_id: r.workspace_id })}
                          className={`cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800/50 ${isSel ? 'bg-gray-50 dark:bg-gray-800/70' : ''}`}
                        >
                          <td className="py-2 pr-4">
                            <div className="font-medium text-gray-900 dark:text-gray-100 text-xs truncate max-w-[220px]" title={r.index_name}>{r.name}</div>
                            <div className="text-[11px] text-gray-400 truncate max-w-[220px]">{r.endpoint_name || r.workspace_id}</div>
                          </td>
                          <td className="py-2 pr-4"><ManagedByBadge managedBy={r.managed_by} /></td>
                          <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtNumber(Number(r.query_count || 0))}</td>
                          <td className="py-2 pr-4 text-right tabular-nums">
                            <span className={r.error_rate > 0.05 ? 'text-red-600 dark:text-red-400' : 'text-gray-700 dark:text-gray-300'}>
                              {(r.error_rate * 100).toFixed(1)}%
                            </span>
                          </td>
                          <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{r.peak_daily_callers}</td>
                          <td className="py-2 text-xs text-gray-500">{r.last_active_date || '—'}</td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
              <TablePagination
                page={page}
                totalItems={sortedIdx.length}
                pageSize={pageSize}
                onPageChange={setPage}
                onPageSizeChange={setPageSize}
              />
            </>
          )}
        </CardContent>
      </Card>

      {selected && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base flex items-center gap-2">
              Top Callers
              <span className="font-mono text-sm font-normal text-gray-500 dark:text-gray-400 truncate" title={selected.index_name}>{selectedName}</span>
              <button
                type="button"
                onClick={() => setSelected(null)}
                className="ml-auto text-xs font-normal text-gray-400 hover:text-gray-700 dark:hover:text-gray-200"
              >
                Clear
              </button>
            </CardTitle>
          </CardHeader>
          <CardContent>
            {callersLoading ? (
              <div className="flex items-center justify-center py-8 text-gray-400">
                <Loader2 className="w-4 h-4 animate-spin mr-2" /> Loading callers...
              </div>
            ) : callers.length === 0 ? (
              <UsageEmpty seesAccountWide={seesAccountWide} days={days} what="caller activity for this index" />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                      <th className="pb-2 pr-4 font-medium">Caller</th>
                      <th className="pb-2 pr-4 font-medium text-right">Requests</th>
                      <th className="pb-2 pr-4 font-medium text-right">Errors</th>
                      <th className="pb-2 font-medium">Last Seen</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                    {callers.map((c, i) => (
                      <tr key={`${c.caller}-${i}`} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                        <td className="py-2 pr-4 text-gray-900 dark:text-gray-100 text-xs" title={c.caller}>{callerLabel(c.caller)}</td>
                        <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtNumber(Number(c.request_count || 0))}</td>
                        <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{Number(c.error_count || 0)}</td>
                        <td className="py-2 text-xs text-gray-500">{c.last_seen ? new Date(c.last_seen).toLocaleString() : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}

/* ── Lakebase: operational health + per-project cost (Retrieval & Memory) ── */

function LakebaseHealthSection({ days, wsIds }: { days: number; wsIds: string[] }) {
  const { data: healthRaw, isLoading: healthLoading } = useLakebaseHealth(days, wsIds)
  const { data: waitsRaw, isLoading: waitsLoading } = useLakebaseWaitEvents(25, wsIds)
  const seesAccountWide = useSeesAccountWide()

  const health: LakebaseHealthRow[] = Array.isArray(healthRaw) ? healthRaw : []
  const waits: LakebaseWaitEventRow[] = Array.isArray(waitsRaw) ? waitsRaw : []

  const projectCount = new Set(health.map((r) => r.project_id)).size
  const peakConnections = health.reduce((m, r) => Math.max(m, Number(r.max_connections || 0)), 0)
  const largestDb = health.reduce((m, r) => Math.max(m, Number(r.max_db_size_bytes || 0)), 0)
  const topWait = waits[0]

  const connPivot = useMemo(
    () => pivotByDay(health, projectSeriesLabel, (r) => Number(r.max_connections || 0)),
    [health],
  )
  const sizePivot = useMemo(
    () => pivotByDay(health, projectSeriesLabel, (r) => Number(r.max_db_size_bytes || 0)),
    [health],
  )

  if (healthLoading || waitsLoading) {
    return (
      <div className="flex items-center justify-center py-12 text-gray-400">
        <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading Lakebase health...
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 pt-1">
        <Activity className="w-5 h-5 text-gray-400" />
        <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Operational Health</h3>
        <div className="flex-1 border-t border-gray-200 dark:border-gray-700" />
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <KpiCard title="Projects Monitored" value={projectCount} format="number" />
        <KpiCard title="Peak Connections" value={peakConnections} format="number" />
        <KpiCard title="Largest Database" value={formatBytes(largestDb)} />
        <KpiCard title="Top Wait Event" value={topWait ? topWait.wait_event_name : '—'} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">Peak Connections per Day</CardTitle>
          </CardHeader>
          <CardContent>
            {connPivot.data.length === 0 ? (
              <UsageEmpty seesAccountWide={seesAccountWide} days={days} what="Lakebase gauge samples" />
            ) : (
              <LazyChart height={260}>
                <ResponsiveContainer width="100%" height={260}>
                  <LineChart data={connPivot.data}>
                    <CartesianGrid strokeDasharray="3 3" stroke={DB_GRID} />
                    <XAxis dataKey="date" tick={{ fontSize: 11, fill: DB_AXIS_TEXT }} />
                    <YAxis tick={{ fontSize: 11, fill: DB_AXIS_TEXT }} />
                    <Tooltip contentStyle={TOOLTIP_STYLE} />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                    {connPivot.series.map((s, i) => (
                      <Line key={s} type="monotone" dataKey={s} stroke={WS_COLORS[i % WS_COLORS.length]} strokeWidth={2} dot={false} name={s} />
                    ))}
                  </LineChart>
                </ResponsiveContainer>
              </LazyChart>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">Database Size per Day</CardTitle>
          </CardHeader>
          <CardContent>
            {sizePivot.data.length === 0 ? (
              <UsageEmpty seesAccountWide={seesAccountWide} days={days} what="Lakebase gauge samples" />
            ) : (
              <LazyChart height={260}>
                <ResponsiveContainer width="100%" height={260}>
                  <LineChart data={sizePivot.data}>
                    <CartesianGrid strokeDasharray="3 3" stroke={DB_GRID} />
                    <XAxis dataKey="date" tick={{ fontSize: 11, fill: DB_AXIS_TEXT }} />
                    <YAxis tick={{ fontSize: 11, fill: DB_AXIS_TEXT }} tickFormatter={(v: number) => formatBytes(v)} width={80} />
                    <Tooltip formatter={(v: number, name: string) => [formatBytes(v), name]} contentStyle={TOOLTIP_STYLE} />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                    {sizePivot.series.map((s, i) => (
                      <Line key={s} type="monotone" dataKey={s} stroke={WS_COLORS[i % WS_COLORS.length]} strokeWidth={2} dot={false} name={s} />
                    ))}
                  </LineChart>
                </ResponsiveContainer>
              </LazyChart>
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Top Wait Events</CardTitle>
        </CardHeader>
        <CardContent>
          {waits.length === 0 ? (
            <UsageEmpty seesAccountWide={seesAccountWide} days={7} what="Lakebase wait-event samples" />
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                    <th className="pb-2 pr-4 font-medium">Wait Event</th>
                    <th className="pb-2 pr-4 font-medium">Class</th>
                    <th className="pb-2 pr-4 font-medium">Project · Branch</th>
                    <th className="pb-2 pr-4 font-medium text-right">Count</th>
                    <th className="pb-2 font-medium text-right">Total Wait Time</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                  {waits.map((w, i) => (
                    <tr key={`${w.wait_event_name}-${w.project_name}-${i}`} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                      <td className="py-2 pr-4 text-xs font-medium text-gray-900 dark:text-gray-100">{w.wait_event_name}</td>
                      <td className="py-2 pr-4 text-xs text-gray-500">{w.wait_class_name}</td>
                      <td className="py-2 pr-4 text-xs text-gray-600 dark:text-gray-300">{projectSeriesLabel({ project_name: w.project_name, project_id: '', branch_name: w.branch_name })}</td>
                      <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtNumber(Number(w.wait_event_count || 0))}</td>
                      <td className="py-2 text-right tabular-nums text-gray-700 dark:text-gray-300">{Math.round(Number(w.wait_event_time || 0)).toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

const projectCostAccessor = (r: LakebaseProjectCostRow, key: string): string | number | null => {
  switch (key) {
    case 'name': return r.project_name
    case 'total_dbus': return r.total_dbus
    case 'total_cost_usd': return r.total_cost_usd
    default: return null
  }
}

function LakebaseProjectCostCard({ days, wsIds }: { days: number; wsIds: string[] }) {
  const { data: raw, isLoading } = useLakebaseCostByProject(days, wsIds)
  const seesAccountWide = useSeesAccountWide()
  const { sort, toggle } = useSort<string>('total_cost_usd', 'desc')
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)

  const sorted = useMemo(
    () => sortRows(Array.isArray(raw) ? raw : [], sort, projectCostAccessor),
    [raw, sort],
  )
  const paged = sorted.slice(page * pageSize, (page + 1) * pageSize)

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Cost by Project</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex items-center justify-center py-8 text-gray-400">
            <Loader2 className="w-4 h-4 animate-spin mr-2" /> Loading...
          </div>
        ) : sorted.length === 0 ? (
          <UsageEmpty seesAccountWide={seesAccountWide} days={days} what="project-tagged Lakebase usage" />
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700">
                    <SortableHeader label="Project · Branch" sortKey="name" current={sort} onToggle={toggle} className="py-2 pr-4 text-gray-500 dark:text-gray-400" />
                    <SortableHeader label="DBUs" sortKey="total_dbus" current={sort} onToggle={toggle} align="right" className="py-2 pr-4 text-gray-500 dark:text-gray-400" />
                    <SortableHeader label="Cost (USD)" sortKey="total_cost_usd" current={sort} onToggle={toggle} align="right" className="py-2 text-gray-500 dark:text-gray-400" />
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                  {paged.map((r, i) => (
                    <tr key={`${r.project_id}-${r.branch_id}-${i}`} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                      <td className="py-2 pr-4 text-gray-900 dark:text-gray-100 text-xs">{projectSeriesLabel({ project_name: r.project_name, project_id: r.project_id, branch_name: r.branch_name })}</td>
                      <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtNumber(Number(r.total_dbus || 0))}</td>
                      <td className="py-2 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtCost(Number(r.total_cost_usd || 0))}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <TablePagination
              page={page}
              totalItems={sorted.length}
              pageSize={pageSize}
              onPageChange={setPage}
              onPageSizeChange={setPageSize}
            />
          </>
        )}
      </CardContent>
    </Card>
  )
}

/* ── Vector Search Tab ──────────────────────────────────────── */

function VectorSearchTab({ days, setDays, selectedWsIds }: { days: number; setDays: (d: number) => void; selectedWsIds: string[] }) {
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
    if (!selectedWsIds.length) return pagedWorkspaces
    return sortedWorkspaces.filter((w: any) => selectedWsIds.includes(String(w.workspace_id)))
  }, [sortedWorkspaces, pagedWorkspaces, selectedWsIds])

  // Health KPIs
  const safeIndexDetails = Array.isArray(indexDetails) ? indexDetails : []
  const safeHealthHistory = Array.isArray(healthHistory) ? healthHistory : []
  const totalRows = safeIndexDetails.reduce((s: number, i: any) => s + (Number(i.indexed_row_count) || 0), 0)
  const readyCount = safeIndexDetails.filter((i: any) => i.ready).length
  const totalSnapshots = safeHealthHistory.length
  const onlineSnapshots = safeHealthHistory.filter((h: any) => h.status === 'ONLINE').length
  // No snapshots → unknown, not a reassuring 100%.
  const uptimePct: number | null = totalSnapshots > 0 ? Math.round((onlineSnapshots / totalSnapshots) * 100) : null

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

      {/* Usage leads: query volume, errors, callers, Agent Bricks ownership */}
      <VectorSearchUsageSection days={days} wsIds={selectedWsIds} />

      {/* Section divider — cost & inventory follow usage */}
      <div className="flex items-center gap-3 pt-4">
        <Database className="w-5 h-5 text-gray-400" />
        <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Cost</h3>
        <div className="flex-1 border-t border-gray-200 dark:border-gray-700" />
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
              {selectedWsIds.length === 0 && (
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
        <KpiCard title="Endpoint Uptime" value={uptimePct ?? "—"} format={uptimePct === null ? undefined : "percentage"} />
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

function LakebaseTab({ days, setDays, selectedWsIds }: { days: number; setDays: (d: number) => void; selectedWsIds: string[] }) {
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
    if (!selectedWsIds.length) return pagedWorkspaces
    return sortedWorkspaces.filter((w: any) => selectedWsIds.includes(String(w.workspace_id)))
  }, [sortedWorkspaces, pagedWorkspaces, selectedWsIds])

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

      {/* Operational health: connections, DB size, wait events */}
      <LakebaseHealthSection days={days} wsIds={selectedWsIds} />

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

      {/* Per-project cost (LAKEBASE usage tagged with project_id) */}
      <LakebaseProjectCostCard days={days} wsIds={selectedWsIds} />

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
              {selectedWsIds.length === 0 && (
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
