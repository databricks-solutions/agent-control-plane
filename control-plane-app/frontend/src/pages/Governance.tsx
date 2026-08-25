import { useState, useEffect, useRef, useMemo } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  useBillingPageData,
  useBillingRefresh,
  useBillingCacheStatus,
  type BillingPageData,
  type CostByTagRow,
  type ExternalModelSpendRow,
} from '@/api/hooks'
import { usePersistedWorkspaceFilter } from '@/lib/usePersistedWorkspaceFilter'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { KpiCard } from '@/components/KpiCard'
import { TablePagination } from '@/components/TablePagination'
import { SortableHeader, useSort, sortRows } from '@/components/SortableTable'
import { LineChart } from '@/components/charts/LineChart'
import { BarChart } from '@/components/charts/BarChart'
import { PieChart } from '@/components/charts/PieChart'
import { DB_CHART } from '@/lib/brand'
import { LayoutDashboard, Zap, Server, ChevronDown, ChevronRight, Layers, Globe, RefreshCw, Users, Info, Tag, Boxes } from 'lucide-react'

/* ── helpers ──────────────────────────────────────────────────── */

/** Prettify a SKU name for display. */
function prettySku(sku: string): string {
  return sku
    .replace(/^ENTERPRISE_/, '')
    .replace(/^PREMIUM_/, '')
    .replace(/_US_EAST_N_VIRGINIA$/, '')
    .replace(/_US_WEST_OREGON$/, '')
    .replace(/_US_EAST_OHIO$/, '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .replace('Model Serving', 'Serving')
    .replace('Serverless Real Time Inference', 'Serverless RT')
}

function fmtCost(v: number): string {
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

/* ── tabs ─────────────────────────────────────────────────────── */

const TABS = [
  { key: 'overview', label: 'Cost Overview', icon: LayoutDashboard },
  { key: 'endpoints', label: 'Endpoint Costs', icon: Server },
  { key: 'tokens', label: 'Token Usage', icon: Zap },
  { key: 'products', label: 'All Products', icon: Layers },
] as const

type TabKey = (typeof TABS)[number]['key']

/* ── Workspace Selector ──────────────────────────────────────── */

const ALL_WORKSPACES = '__all__'
// Build v2 – default "All Workspaces" (no auto-select)

function WorkspaceSelector({
  value,
  onChange,
  workspaces,
  isLoading,
}: {
  value: string
  onChange: (ws: string) => void
  workspaces: BillingPageData['workspaces']
  isLoading: boolean
}) {
  return (
    <div className="flex items-center gap-2">
      <Globe className="w-4 h-4 text-gray-400 dark:text-gray-500" />
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        disabled={isLoading}
        className="border rounded-lg px-3 py-2 text-sm min-w-[220px] dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200"
      >
        <option value={ALL_WORKSPACES}>All Workspaces</option>
        {(workspaces || []).map((ws) => (
          <option key={ws.workspace_id} value={ws.workspace_id}>
            WS {ws.workspace_id}
            {Number(ws.endpoint_count) > 0 ? ` · ${ws.endpoint_count} endpoints` : ''}
          </option>
        ))}
      </select>
    </div>
  )
}

/* ── main page ───────────────────────────────────────────────── */

export default function GovernancePage() {
  const [days, setDays] = useState(30)
  const [tab, setTab] = useState<TabKey>('overview')
  const [workspaceId, setWorkspaceId] = usePersistedWorkspaceFilter('ws-filter:governance', ALL_WORKSPACES)
  // Track local refresh completion time so the age display updates immediately
  const [localRefreshTime, setLocalRefreshTime] = useState<Date | null>(null)

  // Convert selector value to the param we pass to the composite hook
  const wsParam = workspaceId === ALL_WORKSPACES ? undefined : workspaceId

  // ★ Single composite fetch: ALL billing data in one request (~0.8 s)
  const { data: pageData, isLoading } = useBillingPageData(days, wsParam)

  // Refresh mutation (still separate — it's a POST, not a read)
  const refreshMutation = useBillingRefresh()

  // Poll cache status (10 s) — used to detect when background refresh finishes
  const { data: liveCacheStatus } = useBillingCacheStatus()
  const queryClient = useQueryClient()
  const wasRefreshing = useRef(false)

  // When liveCacheStatus.is_refreshing goes from true → false, reload billing data
  useEffect(() => {
    if (liveCacheStatus?.is_refreshing) {
      wasRefreshing.current = true
    } else if (wasRefreshing.current) {
      wasRefreshing.current = false
      setLocalRefreshTime(new Date()) // record completion time locally
      queryClient.invalidateQueries({ queryKey: ['billing'] })
    }
  }, [liveCacheStatus?.is_refreshing, queryClient])

  // Prefer live cache status (polled) over the composite snapshot
  const cacheStatus = liveCacheStatus ?? pageData?.cache_status
  const cacheSource = cacheStatus?.caches ?? (pageData?.cache_status?.caches as any)
  // Use the MOST RECENT refresh time (max) — the oldest would show a stale cache that may never be refreshed
  const newestRefresh = cacheSource
    ? Object.values(cacheSource).reduce<string | null>((newest: string | null, c: any) => {
        if (!c.last_refreshed) return newest
        if (!newest) return c.last_refreshed
        return c.last_refreshed > newest ? c.last_refreshed : newest
      }, null)
    : null

  // Use local refresh timestamp if it's newer than what the backend reports
  const backendMs = newestRefresh ? new Date(newestRefresh).getTime() : 0
  const localMs = localRefreshTime?.getTime() ?? 0
  const effectiveMs = Math.max(backendMs, localMs)

  const cacheAge = effectiveMs > 0
    ? Math.round((Date.now() - effectiveMs) / 60_000)
    : null

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Governance</h2>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Cost attribution from Databricks system tables, token usage & guardrails
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Cache freshness + refresh */}
          <div className="flex items-center gap-2 text-xs text-gray-400 dark:text-gray-500">
            {cacheStatus?.is_refreshing ? (
              <span className="flex items-center gap-1 text-blue-500">
                <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                Refreshing…
              </span>
            ) : cacheAge !== null ? (
              <span>{cacheAge < 60 ? `${cacheAge}m ago` : `${Math.round(cacheAge / 60)}h ago`}</span>
            ) : (
              <span>No cache</span>
            )}
            <button
              onClick={() => refreshMutation.mutate(90)}
              disabled={refreshMutation.isPending || cacheStatus?.is_refreshing}
              className="p-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-40 transition-colors"
              title="Refresh billing data from system tables"
            >
              <RefreshCw className={`w-4 h-4 ${refreshMutation.isPending ? 'animate-spin text-blue-500' : 'text-gray-500 dark:text-gray-400'}`} />
            </button>
          </div>

          <WorkspaceSelector
            value={workspaceId}
            onChange={setWorkspaceId}
            workspaces={pageData?.workspaces || []}
            isLoading={isLoading}
          />
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
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-gray-200 dark:border-gray-700">
        {TABS.map((t) => {
          const Icon = t.icon
          const active = tab === t.key
          return (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
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

      {/* Tab content — all data from pageData, zero extra fetches */}
      {tab === 'overview' && <OverviewTab data={pageData} loading={isLoading} />}
      {tab === 'endpoints' && <EndpointCostsTab data={pageData} />}
      {tab === 'tokens' && <TokenUsageTab data={pageData} />}
      {tab === 'products' && <AllProductsTab data={pageData} />}
    </div>
  )
}

/* ── Overview Tab ─────────────────────────────────────────────── */

function OverviewTab({ data, loading }: { data?: BillingPageData; loading: boolean }) {
  const summary = data?.summary
  const trend = data?.trend || []
  const skus = data?.by_sku || []

  const costTrend = trend.map((r: any) => ({
    timestamp: r.day,
    value: Number(r.total_cost_usd || 0),
  }))

  const dbuTrend = trend.map((r: any) => ({
    timestamp: r.day,
    value: Number(r.total_dbus || 0),
  }))

  // Aggregate SKUs into provider-level groupings
  const providerCosts: Record<string, number> = {}
  for (const s of skus) {
    const name = prettySku(s.sku_name)
    providerCosts[name] = (providerCosts[name] || 0) + Number(s.total_cost_usd || 0)
  }

  const providerPieData = Object.entries(providerCosts)
    .map(([name, value]) => ({ name, value: Math.round(value * 100) / 100 }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 8)

  const avgDailyCost =
    costTrend.length > 0 ? costTrend.reduce((s: number, p: any) => s + p.value, 0) / costTrend.length : 0

  return (
    <div className="space-y-6">
      {/* KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          title="Total Serving Cost"
          value={loading ? '…' : fmtCost(summary?.total_cost_usd || 0)}
        />
        <KpiCard
          title="Total DBUs"
          value={loading ? '…' : fmtNumber(summary?.total_dbus || 0)}
        />
        <KpiCard
          title="Endpoints Billed"
          value={loading ? '…' : String(summary?.endpoint_count || 0)}
          format="number"
        />
        <KpiCard
          title="Avg Daily Cost"
          value={fmtCost(avgDailyCost)}
        />
      </div>

      {/* Cost trend + DBU trend */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Daily Serving Cost ($)</CardTitle>
          </CardHeader>
          <CardContent>
            {costTrend.length ? (
              <LineChart data={costTrend} name="Cost ($)" color={DB_CHART.success} />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Daily DBU Consumption</CardTitle>
          </CardHeader>
          <CardContent>
            {dbuTrend.length ? (
              <LineChart data={dbuTrend} name="DBUs" color={DB_CHART.primary} />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Cost by provider (SKU) */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Cost by SKU / Provider</CardTitle>
          </CardHeader>
          <CardContent>
            {providerPieData.length ? (
              <PieChart data={providerPieData} />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Top SKUs by Cost ($)</CardTitle>
          </CardHeader>
          <CardContent>
            {providerPieData.length ? (
              <BarChart
                data={providerPieData.slice(0, 6)}
                dataKey="value"
                nameKey="name"
                multiColor
              />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Cost by user */}
      <CostByUserSection costByUser={data?.cost_by_user || []} source={data?.cost_by_user_source || 'estimate'} />
    </div>
  )
}

/* ── Cost by User (used in Overview tab) ─────────────────────── */

function CostByUserSection({ costByUser, source = 'estimate' }: { costByUser: any[]; source?: 'actual' | 'estimate' }) {
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort<string>('total_cost_usd')

  const sorted = useMemo(() => sortRows(costByUser, sort, (r: any, k) => {
    if (k === 'user_identity') return (r.user_identity || 'unknown').toLowerCase()
    return Number(r[k] || 0)
  }), [costByUser, sort])

  const barData = costByUser.slice(0, 10).map((u: any) => ({
    name: (u.user_identity || 'unknown').length > 25 ? (u.user_identity || 'unknown').slice(0, 25) + '…' : (u.user_identity || 'unknown'),
    value: Math.round(Number(u.total_cost_usd || 0) * 100) / 100,
  }))

  const pagedUsers = sorted.slice(page * pageSize, (page + 1) * pageSize)

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Users className="w-4 h-4 text-blue-600" /> Top Users by Serving Cost
            <span className="group relative ml-1 inline-flex">
              <span
                tabIndex={0}
                aria-label="How this is calculated"
                className={`px-1.5 py-0.5 rounded text-[10px] font-medium cursor-help inline-flex items-center gap-0.5 outline-none ${
                  source === 'actual'
                    ? 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300'
                    : 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300'
                }`}
              >
                {source === 'actual' ? 'Actual' : 'Estimated'}
                <Info className="w-2.5 h-2.5 opacity-70" />
              </span>
              <span
                role="tooltip"
                className="pointer-events-none absolute left-0 top-full z-50 mt-1.5 hidden w-64 rounded-lg border border-gray-200 bg-white p-3 text-left text-[11px] font-normal leading-relaxed text-gray-600 shadow-lg group-hover:block group-focus-within:block dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300"
              >
                <span className="mb-1 block font-semibold text-gray-900 dark:text-gray-100">How per-user cost is calculated</span>
                <span className={`block ${source === 'actual' ? 'text-green-700 dark:text-green-300' : 'opacity-70'}`}>
                  <strong>Actual</strong> — real per-user dollars attributed by Unity Gateway (<code>system.billing.usage</code>).
                </span>
                <span className={`mt-1.5 block ${source === 'actual' ? 'opacity-70' : 'text-amber-700 dark:text-amber-300'}`}>
                  <strong>Estimated</strong> — each endpoint’s total cost split across users by their share of tokens. Shown when Unity Gateway attribution isn’t available yet.
                </span>
              </span>
            </span>
          </CardTitle>
        </CardHeader>
        <CardContent>
          {barData.length ? (
            <BarChart data={barData} dataKey="value" nameKey="name" multiColor height={300} />
          ) : (
            <div className="text-gray-400 dark:text-gray-500 text-center py-12">No user-level cost data</div>
          )}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Users className="w-4 h-4 text-blue-600" /> Cost by User Detail
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-gray-500 dark:text-gray-400 dark:border-gray-700">
                  <SortableHeader label="User" sortKey="user_identity" current={sort} onToggle={toggle} />
                  <SortableHeader label="DBUs" sortKey="total_dbus" current={sort} onToggle={toggle} align="right" />
                  <SortableHeader label="Cost ($)" sortKey="total_cost_usd" current={sort} onToggle={toggle} align="right" />
                </tr>
              </thead>
              <tbody>
                {pagedUsers.map((u: any) => (
                  <tr key={u.user_identity} className="border-b border-gray-100 dark:border-gray-700/50">
                    <td className="py-2 text-xs font-mono truncate max-w-[200px]">{u.user_identity || 'unknown'}</td>
                    <td className="py-2 text-right">{fmtNumber(Number(u.total_dbus || 0))}</td>
                    <td className="py-2 text-right font-medium">{fmtCost(Number(u.total_cost_usd || 0))}</td>
                  </tr>
                ))}
                {costByUser.length === 0 && (
                  <tr><td colSpan={3} className="py-8 text-center text-gray-400">No user cost data</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination page={page} totalItems={sorted.length} pageSize={pageSize} onPageChange={setPage} onPageSizeChange={setPageSize} />
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Cost by Tag (section of the Endpoint Costs tab) ──────────── */

// Human labels for the allowlisted tag keys emitted by workflow 09.
const TAG_KEY_LABELS: Record<string, string> = {
  project: 'Project',
  team: 'Team',
  environment: 'Environment',
  app: 'App',
  agent: 'Agent',
  owner: 'Owner',
  cost_center: 'Cost Center',
  business_unit: 'Business Unit',
  use_case: 'Use Case',
  source: 'Source',
}

function prettyTagKey(k: string): string {
  return TAG_KEY_LABELS[k] || k.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

function CostByTagSection({ data }: { data?: BillingPageData }) {
  const rows = useMemo<CostByTagRow[]>(() => data?.cost_by_tag || [], [data])

  // Distinct tag keys present, ordered by total cost so the busiest is first.
  const tagKeys = useMemo(() => {
    const totals: Record<string, number> = {}
    for (const r of rows) totals[r.tag_key] = (totals[r.tag_key] || 0) + Number(r.total_cost_usd || 0)
    return Object.keys(totals).sort((a, b) => totals[b] - totals[a])
  }, [rows])

  const [selectedKey, setSelectedKey] = useState<string>('')
  // Default to the top tag key once data arrives; keep a valid selection if the
  // key set changes across refreshes.
  useEffect(() => {
    if (tagKeys.length && !tagKeys.includes(selectedKey)) setSelectedKey(tagKeys[0])
  }, [tagKeys, selectedKey])

  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort<string>('total_cost_usd')

  // Reset to the first page whenever the tag key changes.
  useEffect(() => setPage(0), [selectedKey])

  const keyRows = useMemo(() => rows.filter((r) => r.tag_key === selectedKey), [rows, selectedKey])

  const sorted = useMemo(() => sortRows(keyRows, sort, (r: CostByTagRow, k) => {
    if (k === 'tag_value') return (r.tag_value || '').toLowerCase()
    // "% of Key" is monotonic with cost within a single key, so sort by cost.
    if (k === 'pct') return Number(r.total_cost_usd || 0)
    return Number((r as any)[k] || 0)
  }), [keyRows, sort])

  const paged = sorted.slice(page * pageSize, (page + 1) * pageSize)

  const keyTotal = useMemo(() => keyRows.reduce((s, r) => s + Number(r.total_cost_usd || 0), 0), [keyRows])

  const barData = useMemo(() =>
    [...keyRows]
      .sort((a, b) => Number(b.total_cost_usd || 0) - Number(a.total_cost_usd || 0))
      .slice(0, 10)
      .map((r) => ({
        name: (r.tag_value || '—').length > 25 ? (r.tag_value || '—').slice(0, 25) + '…' : (r.tag_value || '—'),
        value: Math.round(Number(r.total_cost_usd || 0) * 100) / 100,
      })),
    [keyRows])

  // Secondary section of the Endpoint Costs tab — render nothing when there is
  // no tagged usage rather than a large empty-state card.
  if (rows.length === 0) return null

  return (
    <div className="space-y-4 pt-2">
      <div className="border-t border-gray-100 dark:border-gray-700/50 pt-6">
        <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 flex items-center gap-2">
          <Tag className="w-4 h-4 text-blue-600" /> Cost Attribution by Tag
        </h3>
      </div>
      {/* Key selector + explainer */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <Tag className="w-4 h-4 text-gray-400 dark:text-gray-500" />
          <select
            value={selectedKey}
            onChange={(e) => setSelectedKey(e.target.value)}
            className="border rounded-lg px-3 py-2 text-sm min-w-[200px] dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200"
          >
            {tagKeys.map((k) => (
              <option key={k} value={k}>{prettyTagKey(k)}</option>
            ))}
          </select>
        </div>
        <span className="text-xs text-gray-400 dark:text-gray-500">
          Model-serving cost attributed by custom tag. Windowed total owned by the discovery workflow.
        </span>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Tag className="w-4 h-4 text-blue-600" /> Top {prettyTagKey(selectedKey)} values by cost
            </CardTitle>
          </CardHeader>
          <CardContent>
            {barData.length ? (
              <BarChart data={barData} dataKey="value" nameKey="name" multiColor height={300} />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Tag className="w-4 h-4 text-blue-600" /> {prettyTagKey(selectedKey)} Detail
              <span className="ml-1 text-xs font-normal text-gray-400">· {fmtCost(keyTotal)} total</span>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-gray-500 dark:text-gray-400 dark:border-gray-700">
                    <SortableHeader label={prettyTagKey(selectedKey)} sortKey="tag_value" current={sort} onToggle={toggle} />
                    <SortableHeader label="Cost ($)" sortKey="total_cost_usd" current={sort} onToggle={toggle} align="right" />
                    <SortableHeader label="% of Key" sortKey="pct" current={sort} onToggle={toggle} align="right" />
                  </tr>
                </thead>
                <tbody>
                  {paged.map((r) => (
                    <tr key={r.tag_value} className="border-b border-gray-100 dark:border-gray-700/50">
                      <td className="py-2 text-xs font-mono truncate max-w-[240px]">{r.tag_value || '—'}</td>
                      <td className="py-2 text-right font-medium">{fmtCost(Number(r.total_cost_usd || 0))}</td>
                      <td className="py-2 text-right text-gray-500">
                        {keyTotal > 0 ? ((Number(r.total_cost_usd || 0) / keyTotal) * 100).toFixed(1) : '0.0'}%
                      </td>
                    </tr>
                  ))}
                  {keyRows.length === 0 && (
                    <tr><td colSpan={3} className="py-8 text-center text-gray-400">No data for this tag</td></tr>
                  )}
                </tbody>
              </table>
            </div>
            <TablePagination page={page} totalItems={sorted.length} pageSize={pageSize} onPageChange={setPage} onPageSizeChange={setPageSize} />
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

/* ── Token Usage by User (used in Token Usage tab) ───────────── */

function TokensByUserSection({ tokensByUser }: { tokensByUser: any[] }) {
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort<string>('total_tokens')

  const sorted = useMemo(() => sortRows(tokensByUser, sort, (r: any, k) => {
    if (k === 'user_identity') return (r.user_identity || 'unknown').toLowerCase()
    return Number(r[k] || 0)
  }), [tokensByUser, sort])

  const barData = tokensByUser.slice(0, 10).map((u: any) => ({
    name: (u.user_identity || 'unknown').length > 25 ? (u.user_identity || 'unknown').slice(0, 25) + '…' : (u.user_identity || 'unknown'),
    value: Number(u.total_tokens || 0),
  }))

  const pagedUsers = sorted.slice(page * pageSize, (page + 1) * pageSize)

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Users className="w-4 h-4 text-purple-600" /> Top Users by Token Usage
          </CardTitle>
        </CardHeader>
        <CardContent>
          {barData.length ? (
            <BarChart data={barData} dataKey="value" nameKey="name" multiColor height={300} />
          ) : (
            <div className="text-gray-400 dark:text-gray-500 text-center py-12">No user-level token data</div>
          )}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Users className="w-4 h-4 text-purple-600" /> Token Usage by User Detail
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-gray-500 dark:text-gray-400 dark:border-gray-700">
                  <SortableHeader label="User" sortKey="user_identity" current={sort} onToggle={toggle} />
                  <SortableHeader label="Requests" sortKey="request_count" current={sort} onToggle={toggle} align="right" />
                  <SortableHeader label="Input Tokens" sortKey="total_input_tokens" current={sort} onToggle={toggle} align="right" />
                  <SortableHeader label="Output Tokens" sortKey="total_output_tokens" current={sort} onToggle={toggle} align="right" />
                  <SortableHeader label="Total Tokens" sortKey="total_tokens" current={sort} onToggle={toggle} align="right" />
                </tr>
              </thead>
              <tbody>
                {pagedUsers.map((u: any) => (
                  <tr key={u.user_identity} className="border-b border-gray-100 dark:border-gray-700/50">
                    <td className="py-2 text-xs font-mono truncate max-w-[200px]">{u.user_identity || 'unknown'}</td>
                    <td className="py-2 text-right">{fmtNumber(Number(u.request_count || 0))}</td>
                    <td className="py-2 text-right">{fmtNumber(Number(u.total_input_tokens || 0))}</td>
                    <td className="py-2 text-right">{fmtNumber(Number(u.total_output_tokens || 0))}</td>
                    <td className="py-2 text-right font-medium">{fmtNumber(Number(u.total_tokens || 0))}</td>
                  </tr>
                ))}
                {tokensByUser.length === 0 && (
                  <tr><td colSpan={5} className="py-8 text-center text-gray-400">No user token data</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination page={page} totalItems={sorted.length} pageSize={pageSize} onPageChange={setPage} onPageSizeChange={setPageSize} />
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Endpoint Costs Tab ───────────────────────────────────────── */

function EndpointCostsTab({ data }: { data?: BillingPageData }) {
  const summary = data?.summary
  const [expanded, setExpanded] = useState<string | null>(null)
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort<string>('cost')

  const totalCost = summary?.total_cost_usd || 1

  const endpoints = useMemo(() => {
    const raw = Object.entries(summary?.cost_by_endpoint || {})
      .map(([name, cost]) => ({
        name,
        cost: Number(cost),
        dbus: Number(summary?.dbus_by_endpoint?.[name] || 0),
        pct: totalCost > 0 ? (Number(cost) / totalCost) * 100 : 0,
      }))
    return sortRows(raw, sort ?? { key: 'cost', dir: 'desc' }, (r, k) => {
      if (k === 'name') return r.name.toLowerCase()
      return (r as any)[k]
    })
  }, [summary, sort, totalCost])

  const pagedEndpoints = endpoints.slice(page * pageSize, (page + 1) * pageSize)

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Server className="w-4 h-4" />
              Cost Attribution by Endpoint ({endpoints.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-gray-500 dark:text-gray-400 dark:border-gray-700">
                    <th className="pb-2 font-medium pl-1 w-6"></th>
                    <SortableHeader label="Endpoint" sortKey="name" current={sort} onToggle={toggle} />
                    <SortableHeader label="Cost ($)" sortKey="cost" current={sort} onToggle={toggle} align="right" />
                    <SortableHeader label="DBUs" sortKey="dbus" current={sort} onToggle={toggle} align="right" />
                    <SortableHeader label="% of Total" sortKey="pct" current={sort} onToggle={toggle} align="right" />
                  </tr>
                </thead>
                <tbody>
                  {pagedEndpoints.map((ep) => {
                    const isExpanded = expanded === ep.name
                    return (
                      <tr
                        key={ep.name}
                        className="border-b border-gray-100 dark:border-gray-700/50 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
                        onClick={() => setExpanded(isExpanded ? null : ep.name)}
                      >
                        <td className="py-2 pl-1">
                          {isExpanded ? (
                            <ChevronDown className="w-3.5 h-3.5 text-gray-400" />
                          ) : (
                            <ChevronRight className="w-3.5 h-3.5 text-gray-400" />
                          )}
                        </td>
                        <td className="py-2 font-medium font-mono text-xs truncate max-w-[180px]">{ep.name}</td>
                        <td className="py-2 text-right">{fmtCost(ep.cost)}</td>
                        <td className="py-2 text-right">{fmtNumber(ep.dbus)}</td>
                        <td className="py-2 text-right">
                          <div className="flex items-center justify-end gap-2">
                            <div className="w-16 h-2 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
                              <div
                                className="h-full rounded-full"
                                style={{
                                  width: `${Math.min(ep.pct, 100)}%`,
                                  backgroundColor: DB_CHART.primary,
                                }}
                              />
                            </div>
                            <span className="text-gray-500 dark:text-gray-400 w-12 text-right text-xs">{ep.pct.toFixed(1)}%</span>
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                  {endpoints.length === 0 && (
                    <tr>
                      <td colSpan={5} className="py-8 text-center text-gray-400">
                        No cost data available
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
            <TablePagination page={page} totalItems={endpoints.length} pageSize={pageSize} onPageChange={setPage} onPageSizeChange={setPageSize} />
          </CardContent>
        </Card>

        {/* Top 10 cost pie chart */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Top 10 Most Expensive Endpoints</CardTitle>
          </CardHeader>
          <CardContent>
            {endpoints.length > 0 ? (
              <PieChart
                data={endpoints.slice(0, 10).map((ep) => ({
                  name: ep.name.length > 30 ? ep.name.slice(0, 30) + '…' : ep.name,
                  value: Math.round(ep.cost * 100) / 100,
                }))}
              />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Cost attribution by custom tag (moved here from its own tab) */}
      <CostByTagSection data={data} />
    </div>
  )
}

/* ── Token Usage Tab ──────────────────────────────────────────── */

function TokenUsageTab({ data }: { data?: BillingPageData }) {
  const tokens = data?.tokens || []
  const dailyTokens = data?.daily_tokens || []

  const totalInputTokens = tokens.reduce((s: number, r: any) => s + Number(r.total_input_tokens || 0), 0)
  const totalOutputTokens = tokens.reduce((s: number, r: any) => s + Number(r.total_output_tokens || 0), 0)
  const totalRequests = tokens.reduce((s: number, r: any) => s + Number(r.request_count || 0), 0)

  const inputTrend = dailyTokens.map((r: any) => ({
    timestamp: r.day,
    value: Number(r.total_input_tokens || 0),
    output: Number(r.total_output_tokens || 0),
  }))

  const requestTrend = dailyTokens.map((r: any) => ({
    timestamp: r.day,
    value: Number(r.request_count || 0),
  }))

  // Top endpoints by token usage
  const topEndpoints = tokens
    .slice(0, 10)
    .map((r: any) => ({
      name: r.endpoint_name?.length > 35 ? r.endpoint_name.slice(0, 35) + '…' : r.endpoint_name,
      value: Number(r.total_tokens || 0),
    }))

  return (
    <div className="space-y-6">
      {/* KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard title="Total Input Tokens" value={fmtNumber(totalInputTokens)} />
        <KpiCard title="Total Output Tokens" value={fmtNumber(totalOutputTokens)} />
        <KpiCard title="Total Requests" value={fmtNumber(totalRequests)} />
        <KpiCard
          title="Avg Tokens / Request"
          value={totalRequests > 0 ? fmtNumber((totalInputTokens + totalOutputTokens) / totalRequests) : '—'}
        />
      </div>

      {/* Daily trends */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Daily Token Volume</CardTitle>
          </CardHeader>
          <CardContent>
            {inputTrend.length ? (
              <LineChart
                data={inputTrend}
                name="Input Tokens"
                color={DB_CHART.info}
                series={{ output: 'Output Tokens' }}
              />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Daily Request Count</CardTitle>
          </CardHeader>
          <CardContent>
            {requestTrend.length ? (
              <LineChart data={requestTrend} name="Requests" color={DB_CHART.primary} />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Top endpoints by token usage bar */}
      {topEndpoints.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Top Endpoints by Total Tokens</CardTitle>
          </CardHeader>
          <CardContent>
            <BarChart data={topEndpoints} dataKey="value" nameKey="name" multiColor height={350} />
          </CardContent>
        </Card>
      )}

      {/* Detailed table */}
      <TokenBreakdownTable tokens={tokens} />
    

      {/* Token usage by user */}
      <TokensByUserSection tokensByUser={data?.tokens_by_user || []} />
    </div>
  )
}

/* ── Token Breakdown Table (paginated) ────────────────────────── */

function TokenBreakdownTable({ tokens }: { tokens: any[] }) {
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort<string>('total_tokens')

  const sorted = useMemo(() => sortRows(tokens, sort, (r: any, k) => {
    if (k === 'endpoint_name') return (r.endpoint_name || '').toLowerCase()
    return Number(r[k] || 0)
  }), [tokens, sort])

  const pagedTokens = sorted.slice(page * pageSize, (page + 1) * pageSize)

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-2">
          <Zap className="w-4 h-4" />
          Per-Endpoint Token Breakdown
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-gray-500 dark:text-gray-400 dark:border-gray-700">
                <SortableHeader label="Endpoint" sortKey="endpoint_name" current={sort} onToggle={toggle} />
                <SortableHeader label="Requests" sortKey="request_count" current={sort} onToggle={toggle} align="right" />
                <SortableHeader label="Input Tokens" sortKey="total_input_tokens" current={sort} onToggle={toggle} align="right" />
                <SortableHeader label="Output Tokens" sortKey="total_output_tokens" current={sort} onToggle={toggle} align="right" />
                <SortableHeader label="Total Tokens" sortKey="total_tokens" current={sort} onToggle={toggle} align="right" />
                <SortableHeader label="Avg In" sortKey="avg_input_tokens" current={sort} onToggle={toggle} align="right" />
                <SortableHeader label="Avg Out" sortKey="avg_output_tokens" current={sort} onToggle={toggle} align="right" />
              </tr>
            </thead>
            <tbody>
              {pagedTokens.map((r: any) => (
                <tr key={r.endpoint_name} className="border-b border-gray-100 dark:border-gray-700/50">
                  <td className="py-2 font-mono text-xs">{r.endpoint_name}</td>
                  <td className="py-2 text-right">{fmtNumber(Number(r.request_count || 0))}</td>
                  <td className="py-2 text-right">{fmtNumber(Number(r.total_input_tokens || 0))}</td>
                  <td className="py-2 text-right">{fmtNumber(Number(r.total_output_tokens || 0))}</td>
                  <td className="py-2 text-right font-medium">{fmtNumber(Number(r.total_tokens || 0))}</td>
                  <td className="py-2 text-right text-gray-500">{fmtNumber(Number(r.avg_input_tokens || 0))}</td>
                  <td className="py-2 text-right text-gray-500">{fmtNumber(Number(r.avg_output_tokens || 0))}</td>
                </tr>
              ))}
              {tokens.length === 0 && (
                <tr>
                  <td colSpan={7} className="py-8 text-center text-gray-400">
                    No token usage data
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <TablePagination page={page} totalItems={sorted.length} pageSize={pageSize} onPageChange={setPage} onPageSizeChange={setPageSize} />
      </CardContent>
    </Card>
  )
}

/* ── All Products Tab ─────────────────────────────────────────── */

function AllProductsTab({ data }: { data?: BillingPageData }) {
  const products = data?.products || []
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort<string>('total_cost_usd')

  const total = products.reduce((s: number, r: any) => s + Number(r.total_cost_usd || 0), 0)

  const sorted = useMemo(() => {
    const enriched = products.map((r: any) => ({
      ...r,
      _cost: Number(r.total_cost_usd || 0),
      _dbus: Number(r.total_dbus || 0),
      _pct: total > 0 ? (Number(r.total_cost_usd || 0) / total) * 100 : 0,
    }))
    return sortRows(enriched, sort, (r: any, k) => {
      if (k === 'billing_origin_product') return (r.billing_origin_product || '').toLowerCase()
      if (k === 'total_cost_usd') return r._cost
      if (k === 'total_dbus') return r._dbus
      if (k === 'pct') return r._pct
      return Number(r[k] || 0)
    })
  }, [products, sort, total])

  const pieData = products
    .filter((r: any) => Number(r.total_cost_usd || 0) > 0)
    .map((r: any) => ({
      name: r.billing_origin_product,
      value: Math.round(Number(r.total_cost_usd) * 100) / 100,
    }))
    .slice(0, 10)

  return (
    <div className="space-y-6">
      {/* KPI */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        <KpiCard title="Total Platform Cost" value={fmtCost(total)} />
        <KpiCard title="Products Billed" value={String(products.length)} format="number" />
        <KpiCard
          title="Serving % of Total"
          value={
            total > 0
              ? `${((Number(products.find((p: any) => p.billing_origin_product === 'MODEL_SERVING')?.total_cost_usd || 0) / total) * 100).toFixed(1)}%`
              : '—'
          }
        />
      </div>

      {/* Pie + Table */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Cost by Product</CardTitle>
          </CardHeader>
          <CardContent>
            {pieData.length ? (
              <PieChart data={pieData} />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Product Cost Breakdown</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-gray-500 dark:text-gray-400 dark:border-gray-700">
                    <SortableHeader label="Product" sortKey="billing_origin_product" current={sort} onToggle={toggle} />
                    <SortableHeader label="DBUs" sortKey="total_dbus" current={sort} onToggle={toggle} align="right" />
                    <SortableHeader label="Cost ($)" sortKey="total_cost_usd" current={sort} onToggle={toggle} align="right" />
                    <SortableHeader label="% of Total" sortKey="pct" current={sort} onToggle={toggle} align="right" />
                  </tr>
                </thead>
                <tbody>
                  {sorted.slice(page * pageSize, (page + 1) * pageSize).map((r: any) => (
                    <tr key={r.billing_origin_product} className="border-b border-gray-100 dark:border-gray-700/50">
                      <td className="py-2 font-medium text-xs">
                        <Badge variant="default" className="text-xs font-mono">
                          {r.billing_origin_product}
                        </Badge>
                      </td>
                      <td className="py-2 text-right">{fmtNumber(r._dbus)}</td>
                      <td className="py-2 text-right">{fmtCost(r._cost)}</td>
                      <td className="py-2 text-right">
                        <div className="flex items-center justify-end gap-2">
                          <div className="w-20 h-2 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
                            <div
                              className="h-full rounded-full"
                              style={{
                                width: `${Math.min(r._pct, 100)}%`,
                                backgroundColor: DB_CHART.primary,
                              }}
                            />
                          </div>
                          <span className="text-gray-500 dark:text-gray-400 w-12 text-right text-xs">{r._pct.toFixed(1)}%</span>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {products.length === 0 && (
                    <tr>
                      <td colSpan={4} className="py-8 text-center text-gray-400">
                        No billing data
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
            <TablePagination page={page} totalItems={sorted.length} pageSize={pageSize} onPageChange={setPage} onPageSizeChange={setPageSize} />
          </CardContent>
        </Card>
      </div>

      {/* External-model spend (actual $ for external LLMs via the AI Gateway) */}
      <ExternalModelSpendSection data={data} />
    </div>
  )
}

/* ── External-model spend (section of the All Products tab) ───── */

function ExternalModelSpendSection({ data }: { data?: BillingPageData }) {
  const rows = useMemo<ExternalModelSpendRow[]>(() => data?.external_model_spend || [], [data])
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort<string>('total_cost_usd')

  const sorted = useMemo(() => sortRows(rows, sort, (r: ExternalModelSpendRow, k) => {
    if (k === 'call_count' || k === 'total_cost_usd') return Number((r as any)[k] || 0)
    return ((r as any)[k] || '').toString().toLowerCase()
  }), [rows, sort])

  const total = useMemo(() => rows.reduce((s, r) => s + Number(r.total_cost_usd || 0), 0), [rows])

  // By-provider rollup for the bar chart.
  const byProvider = useMemo(() => {
    const m: Record<string, number> = {}
    for (const r of rows) m[r.provider || '—'] = (m[r.provider || '—'] || 0) + Number(r.total_cost_usd || 0)
    return Object.entries(m)
      .map(([name, value]) => ({ name, value: Math.round(value * 1e6) / 1e6 }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 10)
  }, [rows])

  const paged = sorted.slice(page * pageSize, (page + 1) * pageSize)

  // Secondary section — render nothing when there's no external-model routing.
  if (rows.length === 0) return null

  return (
    <div className="space-y-4 pt-2">
      <div className="border-t border-gray-100 dark:border-gray-700/50 pt-6">
        <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 flex items-center gap-2">
          <Boxes className="w-4 h-4 text-blue-600" /> External Model Spend
          <span className="ml-1 text-xs font-normal text-gray-400">· {fmtCost(total)} total</span>
        </h3>
        <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">
          Actual billed cost for external models (OpenAI, Microsoft Foundry, …) routed through the Unity Gateway.
          Real dollars from <code>system.ai_gateway.external_model_spend</code> — not estimated.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Boxes className="w-4 h-4 text-blue-600" /> Spend by Provider
            </CardTitle>
          </CardHeader>
          <CardContent>
            {byProvider.length ? (
              <BarChart data={byProvider} dataKey="value" nameKey="name" multiColor height={280} />
            ) : (
              <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Boxes className="w-4 h-4 text-blue-600" /> External Model Detail
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-gray-500 dark:text-gray-400 dark:border-gray-700">
                    <SortableHeader label="Provider" sortKey="provider" current={sort} onToggle={toggle} />
                    <SortableHeader label="Model" sortKey="model" current={sort} onToggle={toggle} />
                    <SortableHeader label="Calls" sortKey="call_count" current={sort} onToggle={toggle} align="right" />
                    <SortableHeader label="Cost ($)" sortKey="total_cost_usd" current={sort} onToggle={toggle} align="right" />
                  </tr>
                </thead>
                <tbody>
                  {paged.map((r, i) => (
                    <tr key={`${r.provider}-${r.model}-${r.endpoint_name}-${i}`} className="border-b border-gray-100 dark:border-gray-700/50">
                      <td className="py-2 text-xs"><Badge variant="default" className="text-xs">{r.provider || '—'}</Badge></td>
                      <td className="py-2 text-xs font-mono truncate max-w-[200px]" title={`${r.model} · ${r.endpoint_name}`}>{r.model || '—'}</td>
                      <td className="py-2 text-right tabular-nums">{Number(r.call_count || 0).toLocaleString()}</td>
                      <td className="py-2 text-right font-medium">{fmtCost(Number(r.total_cost_usd || 0))}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <TablePagination page={page} totalItems={sorted.length} pageSize={pageSize} onPageChange={setPage} onPageSizeChange={setPageSize} />
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

