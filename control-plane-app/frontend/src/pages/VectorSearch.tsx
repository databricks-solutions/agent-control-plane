import { useState } from 'react'
import { useQueryClient, useIsFetching } from '@tanstack/react-query'
import {
  useVectorSearchOverview,
  useVectorSearchEndpoints,
  useVectorSearchIndexes,
  useVectorSearchCostSummary,
  useVectorSearchCostTrend,
  useVectorSearchCostByEndpoint,
  useVectorSearchCostByWorkload,
} from '@/api/hooks'
import { apiClient } from '@/api/client'
import { RefreshButton } from '@/components/RefreshButton'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { KpiCard } from '@/components/KpiCard'
import { TablePagination } from '@/components/TablePagination'
import {
  Search,
  Database,
  DollarSign,
  Server,
  Activity,
  CheckCircle2,
  XCircle,
  Loader2,
} from 'lucide-react'

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
  { key: 'indexes', label: 'Indexes', icon: Database },
  { key: 'performance', label: 'Performance', icon: Activity },
  { key: 'cost', label: 'Cost', icon: DollarSign },
] as const

type TabKey = (typeof TABS)[number]['key']

/* ── main component ──────────────────────────────────────────── */

export default function VectorSearchPage() {
  const [tab, setTab] = useState<TabKey>('indexes')
  const queryClient = useQueryClient()
  const isFetching = useIsFetching({ queryKey: ['vector-search'] }) > 0
  const updatedAt = queryClient.getQueryState(['vector-search', 'overview'])?.dataUpdatedAt
  const lastSynced = updatedAt ? new Date(updatedAt).toISOString() : null

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
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Vector Search</h2>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Endpoints, indexes, performance & cost monitoring
          </p>
        </div>
        <div className="flex items-center gap-3">
          <RefreshButton
            onRefresh={handleRefresh}
            isRefreshing={isFetching}
            lastSynced={lastSynced}
            title="Refresh Vector Search data"
          />
        </div>
      </div>

      {/* Tab bar */}
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

      {/* Tab content */}
      {tab === 'indexes' && <IndexesTab />}
      {tab === 'performance' && <PerformanceTab />}
      {tab === 'cost' && <CostTab />}
    </div>
  )
}

/* ── Indexes Tab ─────────────────────────────────────────────── */

function IndexesTab() {
  const { data: overview, isLoading: overviewLoading } = useVectorSearchOverview()
  const { data: endpoints } = useVectorSearchEndpoints()
  const { data: indexes, isLoading: indexesLoading } = useVectorSearchIndexes()

  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const [filterEndpoint, setFilterEndpoint] = useState<string>('')

  // Join indexes with endpoint info
  const enrichedIndexes = (indexes || []).map((idx: any) => {
    const ep = (endpoints || []).find((e: any) => e.name === idx.endpoint_name)
    return { ...idx, endpoint_status: ep?.endpoint_status || ep?.status || 'UNKNOWN' }
  })

  const filtered = filterEndpoint
    ? enrichedIndexes.filter((idx: any) => idx.endpoint_name === filterEndpoint)
    : enrichedIndexes

  const paged = filtered.slice(page * pageSize, (page + 1) * pageSize)

  const isLoading = overviewLoading || indexesLoading

  return (
    <div className="space-y-4">
      {/* KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
        <KpiCard title="Total Endpoints" value={overview?.total_endpoints ?? 0} format="number" />
        <KpiCard title="Online Endpoints" value={overview?.online_endpoints ?? 0} format="number" />
        <KpiCard title="Total Indexes" value={overview?.total_indexes ?? 0} format="number" />
        <KpiCard
          title="Index Types"
          value={
            overview?.by_index_type
              ? Object.entries(overview.by_index_type)
                  .map(([k, v]) => `${k}: ${v}`)
                  .join(', ')
              : '—'
          }
        />
      </div>

      {/* Filter */}
      <div className="flex items-center gap-3">
        <Search className="w-4 h-4 text-gray-400 dark:text-gray-500" />
        <select
          value={filterEndpoint}
          onChange={(e) => { setFilterEndpoint(e.target.value); setPage(0) }}
          className="border rounded-lg px-3 py-2 text-sm dark:bg-gray-700 dark:border-gray-600 dark:text-gray-200"
        >
          <option value="">All Endpoints</option>
          {(endpoints || []).map((ep: any) => (
            <option key={ep.name} value={ep.name}>{ep.name}</option>
          ))}
        </select>
      </div>

      {/* Table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Vector Search Indexes</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="flex items-center justify-center py-12 text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading indexes…
            </div>
          ) : filtered.length === 0 ? (
            <div className="text-center py-12 text-gray-400 dark:text-gray-500">
              No indexes found.
            </div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                      <th className="pb-2 pr-4 font-medium">Endpoint</th>
                      <th className="pb-2 pr-4 font-medium">Status</th>
                      <th className="pb-2 pr-4 font-medium">Index Name</th>
                      <th className="pb-2 pr-4 font-medium">Type</th>
                      <th className="pb-2 font-medium">Creator</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                    {paged.map((idx: any, i: number) => (
                      <tr key={`${idx.endpoint_name}-${idx.name}-${i}`} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                        <td className="py-2.5 pr-4">
                          <div className="flex items-center gap-2">
                            <Server className="w-3.5 h-3.5 text-gray-400" />
                            <span className="font-medium text-gray-900 dark:text-gray-100">{idx.endpoint_name || '—'}</span>
                          </div>
                        </td>
                        <td className="py-2.5 pr-4">
                          <EndpointStatusBadge status={idx.endpoint_status} />
                        </td>
                        <td className="py-2.5 pr-4">
                          <span className="text-gray-700 dark:text-gray-300">{idx.name || '—'}</span>
                        </td>
                        <td className="py-2.5 pr-4">
                          <IndexTypeBadge type={idx.index_type} />
                        </td>
                        <td className="py-2.5 text-gray-600 dark:text-gray-400">
                          {idx.creator || '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <TablePagination
                page={page}
                totalItems={filtered.length}
                pageSize={pageSize}
                onPageChange={setPage}
                onPageSizeChange={setPageSize}
              />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function EndpointStatusBadge({ status }: { status: string }) {
  const s = (status || '').toUpperCase()
  if (s === 'ONLINE') {
    return (
      <Badge variant="default" className="text-xs bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400 hover:bg-green-100 dark:hover:bg-green-900/30">
        <CheckCircle2 className="w-3 h-3 mr-1" /> ONLINE
      </Badge>
    )
  }
  if (s === 'OFFLINE' || s === 'PROVISIONING_FAILED') {
    return (
      <Badge variant="error" className="text-xs">
        <XCircle className="w-3 h-3 mr-1" /> {s}
      </Badge>
    )
  }
  return (
    <Badge variant="default" className="text-xs">
      {status || 'UNKNOWN'}
    </Badge>
  )
}

function IndexTypeBadge({ type }: { type: string }) {
  const t = (type || '').toUpperCase()
  if (t === 'DELTA_SYNC') {
    return (
      <Badge variant="default" className="text-xs bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400 hover:bg-blue-100 dark:hover:bg-blue-900/30">
        DELTA_SYNC
      </Badge>
    )
  }
  if (t === 'DIRECT_ACCESS') {
    return (
      <Badge variant="default" className="text-xs bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400 hover:bg-purple-100 dark:hover:bg-purple-900/30">
        DIRECT_ACCESS
      </Badge>
    )
  }
  return <Badge variant="default" className="text-xs">{type || '—'}</Badge>
}

/* ── Performance Tab ─────────────────────────────────────────── */

function PerformanceTab() {
  const [days, setDays] = useState(30)
  const { data: trend, isLoading } = useVectorSearchCostTrend(days)

  return (
    <div className="space-y-4">
      {/* Note */}
      <Card>
        <CardContent className="py-4">
          <div className="flex items-start gap-3">
            <Activity className="w-5 h-5 text-amber-500 flex-shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-medium text-gray-900 dark:text-gray-100">
                Performance metrics derived from billing data
              </p>
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                Per-query metrics are not yet available via the Vector Search API. The table below shows daily DBU consumption as a proxy for usage intensity.
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Days selector */}
      <div className="flex items-center gap-3 justify-end">
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

      {/* Usage trend table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Daily Usage Trend</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="flex items-center justify-center py-12 text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading trend data…
            </div>
          ) : !trend || trend.length === 0 ? (
            <div className="text-center py-12 text-gray-400 dark:text-gray-500">
              No usage data available for the selected period.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                    <th className="pb-2 pr-4 font-medium">Date</th>
                    <th className="pb-2 pr-4 font-medium text-right">DBUs</th>
                    <th className="pb-2 font-medium text-right">Cost (USD)</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                  {trend.map((row: any, i: number) => (
                    <tr key={row.day || i} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                      <td className="py-2 pr-4 text-gray-900 dark:text-gray-100">{row.day || '—'}</td>
                      <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtNumber(Number(row.total_dbus || 0))}</td>
                      <td className="py-2 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtCost(Number(row.total_cost_usd || 0))}</td>
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

/* ── Cost Tab ────────────────────────────────────────────────── */

function CostTab() {
  const [days, setDays] = useState(30)
  const { data: summary, isLoading: summaryLoading } = useVectorSearchCostSummary(days)
  const { data: trend, isLoading: trendLoading } = useVectorSearchCostTrend(days)
  const { data: byEndpoint, isLoading: epLoading } = useVectorSearchCostByEndpoint(days)
  const { data: byWorkload, isLoading: wlLoading } = useVectorSearchCostByWorkload(days)

  const [trendPage, setTrendPage] = useState(0)
  const [trendPageSize, setTrendPageSize] = useState(10)
  const [epPage, setEpPage] = useState(0)
  const [epPageSize, setEpPageSize] = useState(10)

  const isLoading = summaryLoading || trendLoading || epLoading || wlLoading

  const trendList = trend || []
  const epList = byEndpoint || []
  const wlList = byWorkload || []

  const pagedTrend = trendList.slice(trendPage * trendPageSize, (trendPage + 1) * trendPageSize)
  const pagedEp = epList.slice(epPage * epPageSize, (epPage + 1) * epPageSize)

  return (
    <div className="space-y-4">
      {/* Days selector */}
      <div className="flex items-center gap-3 justify-end">
        <select
          value={days}
          onChange={(e) => { setDays(Number(e.target.value)); setTrendPage(0); setEpPage(0) }}
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
        <KpiCard title="Total DBUs" value={summary?.total_dbus ?? 0} format="number" />
        <KpiCard title="Total Cost (USD)" value={summary?.total_cost_usd ?? 0} format="currency" />
        <KpiCard title="Endpoints" value={summary?.endpoint_count ?? 0} format="number" />
        <KpiCard title="Workspaces" value={summary?.workspace_count ?? 0} format="number" />
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-8 text-gray-400">
          <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading cost data…
        </div>
      )}

      {/* Cost trend */}
      {!isLoading && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">Cost Trend</CardTitle>
          </CardHeader>
          <CardContent>
            {trendList.length === 0 ? (
              <div className="text-center py-8 text-gray-400 dark:text-gray-500">No cost data available.</div>
            ) : (
              <>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                        <th className="pb-2 pr-4 font-medium">Date</th>
                        <th className="pb-2 pr-4 font-medium text-right">DBUs</th>
                        <th className="pb-2 font-medium text-right">Cost (USD)</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                      {pagedTrend.map((row: any, i: number) => (
                        <tr key={row.day || i} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                          <td className="py-2 pr-4 text-gray-900 dark:text-gray-100">{row.day || '—'}</td>
                          <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtNumber(Number(row.total_dbus || 0))}</td>
                          <td className="py-2 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtCost(Number(row.total_cost_usd || 0))}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <TablePagination
                  page={trendPage}
                  totalItems={trendList.length}
                  pageSize={trendPageSize}
                  onPageChange={setTrendPage}
                  onPageSizeChange={setTrendPageSize}
                />
              </>
            )}
          </CardContent>
        </Card>
      )}

      {/* Cost by endpoint */}
      {!isLoading && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">Cost by Endpoint</CardTitle>
          </CardHeader>
          <CardContent>
            {epList.length === 0 ? (
              <div className="text-center py-8 text-gray-400 dark:text-gray-500">No endpoint cost data.</div>
            ) : (
              <>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                        <th className="pb-2 pr-4 font-medium">Endpoint</th>
                        <th className="pb-2 pr-4 font-medium">Workspace</th>
                        <th className="pb-2 pr-4 font-medium text-right">DBUs</th>
                        <th className="pb-2 font-medium text-right">Cost (USD)</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                      {pagedEp.map((row: any, i: number) => (
                        <tr key={`${row.endpoint_name}-${i}`} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                          <td className="py-2 pr-4 font-medium text-gray-900 dark:text-gray-100">{row.endpoint_name || '—'}</td>
                          <td className="py-2 pr-4 text-gray-600 dark:text-gray-400">{row.workspace_id || '—'}</td>
                          <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtNumber(Number(row.total_dbus || 0))}</td>
                          <td className="py-2 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtCost(Number(row.total_cost_usd || 0))}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <TablePagination
                  page={epPage}
                  totalItems={epList.length}
                  pageSize={epPageSize}
                  onPageChange={setEpPage}
                  onPageSizeChange={setEpPageSize}
                />
              </>
            )}
          </CardContent>
        </Card>
      )}

      {/* Cost by workload type */}
      {!isLoading && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">Cost by Workload Type</CardTitle>
          </CardHeader>
          <CardContent>
            {wlList.length === 0 ? (
              <div className="text-center py-8 text-gray-400 dark:text-gray-500">No workload cost data.</div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                      <th className="pb-2 pr-4 font-medium">Workload Type</th>
                      <th className="pb-2 pr-4 font-medium text-right">DBUs</th>
                      <th className="pb-2 font-medium text-right">Cost (USD)</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                    {wlList.map((row: any, i: number) => (
                      <tr key={row.workload_type || i} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                        <td className="py-2 pr-4 font-medium text-gray-900 dark:text-gray-100">
                          <Badge variant="default" className="text-xs">
                            {row.workload_type || '—'}
                          </Badge>
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtNumber(Number(row.total_dbus || 0))}</td>
                        <td className="py-2 text-right tabular-nums text-gray-700 dark:text-gray-300">{fmtCost(Number(row.total_cost_usd || 0))}</td>
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
