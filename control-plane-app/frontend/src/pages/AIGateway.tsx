import { useState, useMemo, useCallback, Fragment } from 'react'
import { useIsFetching } from '@tanstack/react-query'
import {
  useGatewayPageData,
  useGatewayPermissions,
  useEndpointsWithPermissions,
  useUpdateEndpointPermission,
  useRemoveEndpointPermission,
  useGatewayRateLimits,
  useGatewayGuardrails,
  useGatewayUsageSummary,
  useGatewayUsageTimeseries,
  useGatewayUsageByUser,
  useGatewayInferenceLogs,
  useGatewayMetrics,
  useAppConfig,
  useRefreshGateway,
} from '@/api/hooks'
import { RefreshButton } from '@/components/RefreshButton'
import { SortableHeader, useSort, sortRows } from '@/components/SortableTable'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { KpiCard } from '@/components/KpiCard'
import { TablePagination } from '@/components/TablePagination'
import { LineChart } from '@/components/charts/LineChart'
import { BarChart } from '@/components/charts/BarChart'
import { PrincipalAutocomplete } from '@/components/PrincipalAutocomplete'
import { DB_CHART } from '@/lib/brand'
import { format } from 'date-fns'
import {
  Shield,
  BarChart3,
  ScrollText,
  LayoutDashboard,
  ShieldAlert,
  ExternalLink,
  Cpu,
  Users,
  ShieldCheck,
  Search,
  Plus,
  Trash2,
  X,
  ChevronDown,
  ChevronRight,
  CheckCircle,
  AlertCircle,
  Pencil,
} from 'lucide-react'

/* ── tab definitions ─────────────────────────────────────────── */
const tabs = [
  { id: 'overview', label: 'Overview', icon: LayoutDashboard },
  { id: 'metrics', label: 'Metrics', icon: Cpu },
  { id: 'permissions', label: 'Permissions', icon: Shield },
  { id: 'rate-limits', label: 'Rate Limits & Guardrails', icon: ShieldAlert },
] as const

type TabId = (typeof tabs)[number]['id']

export default function AIGatewayPage() {
  const [activeTab, setActiveTab] = useState<TabId>('overview')
  const [days, setDays] = useState(7)
  const [searchQuery, setSearchQuery] = useState('')

  const { data: pageData, isLoading: pageLoading } = useGatewayPageData()
  const overview = pageData?.overview
  const endpoints = pageData?.endpoints
  const { data: config } = useAppConfig()
  const rawHost = (config?.databricks_host || '').replace(/\/$/, '')
  const workspaceUrl = rawHost && !rawHost.startsWith('http') ? `https://${rawHost}` : rawHost
  const refreshGateway = useRefreshGateway()
  const isFetchingGateway = useIsFetching({ queryKey: ['gateway'] }) > 0

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">AI Gateway</h2>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Manage, monitor and govern serving endpoints across your workspace&nbsp;
            <a
              href="https://docs.databricks.com/aws/en/ai-gateway/overview-beta"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:underline inline-flex items-center gap-0.5"
            >
              (Beta docs <ExternalLink className="w-3 h-3" />)
            </a>
          </p>
        </div>
        <div className="flex items-center gap-3">
          <RefreshButton
            onRefresh={() => refreshGateway.mutate()}
            isPending={refreshGateway.isPending || isFetchingGateway}
            lastSynced={pageData?.last_refreshed ?? null}
            title="Refresh gateway data from Databricks APIs"
          />
          {/* Search */}
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none" />
            <input
              type="text"
              placeholder="Search endpoints…"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-8 pr-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-800 dark:text-gray-200 focus:ring-2 focus:ring-db-red/30 focus:border-db-red w-56"
            />
          </div>
          <select
            value={days}
            onChange={(e) => setDays(Number(e.target.value))}
            className="border dark:border-gray-600 rounded-lg px-3 py-2 text-sm dark:bg-gray-800 dark:text-gray-200"
          >
            <option value={1}>Last 24h</option>
            <option value={7}>Last 7 days</option>
            <option value={30}>Last 30 days</option>
          </select>
        </div>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-gray-200 dark:border-gray-700 overflow-x-auto">
        {tabs.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setActiveTab(id)}
            className={`flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
              activeTab === id
                ? 'border-red-500 text-red-600 dark:text-red-400'
                : 'border-transparent text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300'
            }`}
          >
            <Icon className="w-4 h-4" />
            {label}
          </button>
        ))}
      </div>

      {/* KPI Row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-3">
        <KpiCard title="Total Endpoints" value={overview?.total_endpoints ?? 0} format="number" />
        <KpiCard title="Ready" value={overview?.ready_endpoints ?? 0} format="number" />
        <KpiCard title="Gateway Enabled" value={overview?.gateway_enabled ?? 0} format="number" />
        <KpiCard title="Requests (24h)" value={overview?.total_requests_24h ?? 0} format="number" />
        <KpiCard title="Unique Users (24h)" value={overview?.unique_users_24h ?? 0} format="number" />
        <KpiCard title="Error Rate (24h)" value={overview?.error_rate_24h ?? 0} format="percentage" />
      </div>

      {/* Tab content */}
      {activeTab === 'overview' && <OverviewSection endpoints={endpoints} overview={overview} workspaceUrl={workspaceUrl} loading={pageLoading} searchQuery={searchQuery} days={days} />}
      {activeTab === 'metrics' && <MetricsSection />}
      {activeTab === 'permissions' && <PermissionsSection />}
      {activeTab === 'rate-limits' && <RateLimitsAndGuardrailsSection />}
    </div>
  )
}

/* ── Overview Section ─────────────────────────────────────────── */

function OverviewSection({ endpoints, overview, workspaceUrl, loading, searchQuery, days }: { endpoints: any; overview: any; workspaceUrl: string; loading: boolean; searchQuery: string; days: number }) {
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort('name', 'asc')

  // Usage data
  const { data: summary, isLoading: usageLoading } = useGatewayUsageSummary(days)
  const { data: timeseries } = useGatewayUsageTimeseries(days)
  const { data: users, isLoading: usersLoading } = useGatewayUsageByUser(days)
  const [usagePage, setUsagePage] = useState(0)
  const [usagePageSize, setUsagePageSize] = useState(10)
  const usageSort = useSort('total_requests', 'desc')
  const [userPage, setUserPage] = useState(0)
  const [userPageSize, setUserPageSize] = useState(10)
  const userSort = useSort('total_requests', 'desc')

  const chartData = timeseries?.map((t: any) => ({ timestamp: t.hour, value: Number(t.request_count) })) || []
  const tokenData = timeseries?.map((t: any) => ({ timestamp: t.hour, value: Number(t.input_tokens) + Number(t.output_tokens) })) || []

  const allSummary = summary || []
  const sortedUsage = useMemo(() => sortRows(allSummary, usageSort.sort, (s: any, key) => {
    if (key === 'endpoint_name') return (s.endpoint_name || '').toLowerCase()
    if (key === 'total_requests') return Number(s.total_requests || 0)
    if (key === 'total_input_tokens') return Number(s.total_input_tokens || 0)
    if (key === 'total_output_tokens') return Number(s.total_output_tokens || 0)
    if (key === 'error_count') return Number(s.error_count || 0)
    if (key === 'unique_users') return Number(s.unique_users || 0)
    return 0
  }), [allSummary, usageSort.sort])
  const usageTotalPages = Math.max(1, Math.ceil(sortedUsage.length / usagePageSize))
  const usageSafePage = Math.min(usagePage, usageTotalPages - 1)
  const pagedUsage = sortedUsage.slice(usageSafePage * usagePageSize, (usageSafePage + 1) * usagePageSize)

  const allUsers = users || []
  const sortedUsers = useMemo(() => sortRows(allUsers, userSort.sort, (u: any, key) => {
    if (key === 'requester') return (u.requester || '').toLowerCase()
    if (key === 'total_requests') return Number(u.total_requests || 0)
    if (key === 'total_input_tokens') return Number(u.total_input_tokens || 0)
    if (key === 'total_output_tokens') return Number(u.total_output_tokens || 0)
    if (key === 'error_count') return Number(u.error_count || 0)
    return 0
  }), [allUsers, userSort.sort])
  const userTotalPages = Math.max(1, Math.ceil(sortedUsers.length / userPageSize))
  const userSafePage = Math.min(userPage, userTotalPages - 1)
  const pagedUsers = sortedUsers.slice(userSafePage * userPageSize, (userSafePage + 1) * userPageSize)

  const q = searchQuery.toLowerCase().trim()
  const allEndpoints: any[] = endpoints || []
  const filtered = q
    ? allEndpoints.filter((ep: any) => {
        const haystack = [ep.name, ep.task, ep.state, ep.creator]
          .filter(Boolean)
          .join(' ')
          .toLowerCase()
        return haystack.includes(q)
      })
    : allEndpoints

  const sorted = useMemo(
    () =>
      sortRows(filtered, sort, (ep: any, key: string) => {
        if (key === 'name') return (ep.name || '').toLowerCase()
        if (key === 'task') return (ep.task || '').toLowerCase()
        if (key === 'state') return (ep.state || '').toLowerCase()
        if (key === 'ai_gateway') return ep.ai_gateway ? 'enabled' : ''
        if (key === 'creator') return (ep.creator || '').toLowerCase()
        if (key === 'served_entities') return (ep.served_entities?.map((se: any) => se.entity_name || se.name).filter(Boolean).join(', ') || '').toLowerCase()
        return ''
      }),
    [filtered, sort],
  )

  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize))
  const safePage = Math.min(page, totalPages - 1)
  const paged = sorted.slice(safePage * pageSize, (safePage + 1) * pageSize)

  return (
    <div className="space-y-6">
      {/* Task distribution */}
      {overview?.tasks && Object.keys(overview.tasks).length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card>
            <CardHeader><CardTitle className="text-base">Endpoints by Task</CardTitle></CardHeader>
            <CardContent>
              <BarChart
                data={Object.entries(overview.tasks).map(([name, value]) => ({ name, value: Number(value) }))}
                dataKey="value"
                nameKey="name"
                multiColor
              />
            </CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle className="text-base">Token Usage (24h)</CardTitle></CardHeader>
            <CardContent>
              <BarChart
                data={[
                  { name: 'Input Tokens', value: Number(overview?.total_input_tokens_24h || 0) },
                  { name: 'Output Tokens', value: Number(overview?.total_output_tokens_24h || 0) },
                ]}
                dataKey="value"
                nameKey="name"
                multiColor
              />
            </CardContent>
          </Card>
        </div>
      )}

      {/* Endpoint table */}
      <Card>
        <CardHeader><CardTitle className="text-base">Serving Endpoints ({filtered.length}{filtered.length !== allEndpoints.length ? ` of ${allEndpoints.length}` : ''})</CardTitle></CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                  <SortableHeader label="Name" sortKey="name" current={sort} onToggle={toggle} />
                  <SortableHeader label="Task" sortKey="task" current={sort} onToggle={toggle} />
                  <SortableHeader label="Status" sortKey="state" current={sort} onToggle={toggle} />
                  <SortableHeader label="AI Gateway" sortKey="ai_gateway" current={sort} onToggle={toggle} />
                  <SortableHeader label="Creator" sortKey="creator" current={sort} onToggle={toggle} />
                  <SortableHeader label="Served Entities" sortKey="served_entities" current={sort} onToggle={toggle} />
                </tr>
              </thead>
              <tbody>
                {paged.map((ep: any) => (
                  <tr key={ep.endpoint_id || ep.name} className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700/50">
                    <td className="py-2.5 font-medium">
                      {workspaceUrl ? (
                        <a
                          href={`${workspaceUrl}/ml/endpoints/${ep.name}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-blue-600 hover:underline flex items-center gap-1"
                        >
                          {ep.name}
                          <ExternalLink className="w-3 h-3 opacity-50" />
                        </a>
                      ) : ep.name}
                    </td>
                    <td className="py-2.5">
                      <Badge variant="default" className="text-xs font-mono">{ep.task || '—'}</Badge>
                    </td>
                    <td className="py-2.5">
                      <Badge
                        variant={ep.state === 'READY' ? 'success' : 'warning'}
                        className="text-xs"
                      >
                        {ep.state}
                      </Badge>
                    </td>
                    <td className="py-2.5">
                      {ep.ai_gateway ? (
                        <div className="flex gap-1 flex-wrap">
                          {ep.ai_gateway.rate_limits?.length > 0 && (
                            <Badge variant="info" className="text-[10px]">Rate Limits</Badge>
                          )}
                          {ep.ai_gateway.guardrails && (
                            <Badge variant="warning" className="text-[10px]">Guardrails</Badge>
                          )}
                          {ep.ai_gateway.inference_table?.enabled && (
                            <Badge variant="default" className="text-[10px]">Inference Table</Badge>
                          )}
                          {ep.ai_gateway.usage_tracking?.enabled && (
                            <Badge variant="default" className="text-[10px]">Usage Tracking</Badge>
                          )}
                          {!ep.ai_gateway.rate_limits?.length && !ep.ai_gateway.guardrails && !ep.ai_gateway.inference_table?.enabled && !ep.ai_gateway.usage_tracking?.enabled && (
                            <Badge variant="default" className="text-[10px]">Enabled</Badge>
                          )}
                        </div>
                      ) : (
                        <span className="text-gray-400 text-xs">—</span>
                      )}
                    </td>
                    <td className="py-2.5 text-xs text-gray-500 dark:text-gray-400 max-w-[150px] truncate">{ep.creator || '—'}</td>
                    <td className="py-2.5 text-xs text-gray-500 dark:text-gray-400">
                      {ep.served_entities?.map((se: any) => se.entity_name || se.name).filter(Boolean).join(', ') || '—'}
                    </td>
                  </tr>
                ))}
                {sorted.length === 0 && (
                  <tr><td colSpan={6} className="py-8 text-center text-gray-400 dark:text-gray-500">
                    {loading ? 'Loading endpoints…' : searchQuery ? 'No endpoints match your search' : 'No serving endpoints found'}
                  </td></tr>
                )}
              </tbody>
            </table>
          </div>
          {/* Pagination */}
          <TablePagination
            page={safePage}
            totalItems={sorted.length}
            pageSize={pageSize}
            onPageChange={setPage}
            onPageSizeChange={setPageSize}
          />
        </CardContent>
      </Card>

      {/* ── Usage Charts ──────────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader><CardTitle className="text-base">Request Volume</CardTitle></CardHeader>
          <CardContent>
            {chartData.length ? <LineChart data={chartData} name="Requests" color={DB_CHART.primary} /> : <div className="text-gray-400 dark:text-gray-500 text-center py-12">{usageLoading ? 'Loading…' : 'No data'}</div>}
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">Token Volume</CardTitle></CardHeader>
          <CardContent>
            {tokenData.length ? <LineChart data={tokenData} name="Total Tokens" color={DB_CHART.success} /> : <div className="text-gray-400 dark:text-gray-500 text-center py-12">{usageLoading ? 'Loading…' : 'No data'}</div>}
          </CardContent>
        </Card>
      </div>

      {/* ── Usage by Endpoint ─────────────────────────────────── */}
      <Card>
        <CardHeader><CardTitle className="text-base">Usage by Endpoint</CardTitle></CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                  <SortableHeader label="Endpoint" sortKey="endpoint_name" current={usageSort.sort} onToggle={usageSort.toggle} />
                  <SortableHeader label="Requests" sortKey="total_requests" current={usageSort.sort} onToggle={usageSort.toggle} align="right" />
                  <SortableHeader label="Input Tokens" sortKey="total_input_tokens" current={usageSort.sort} onToggle={usageSort.toggle} align="right" />
                  <SortableHeader label="Output Tokens" sortKey="total_output_tokens" current={usageSort.sort} onToggle={usageSort.toggle} align="right" />
                  <SortableHeader label="Errors" sortKey="error_count" current={usageSort.sort} onToggle={usageSort.toggle} align="right" />
                  <SortableHeader label="Users" sortKey="unique_users" current={usageSort.sort} onToggle={usageSort.toggle} align="right" />
                </tr>
              </thead>
              <tbody>
                {pagedUsage.map((s: any) => (
                  <tr key={s.endpoint_name} className="border-b border-gray-100 dark:border-gray-700">
                    <td className="py-2 font-medium">{s.endpoint_name}</td>
                    <td className="py-2 text-right">{Number(s.total_requests).toLocaleString()}</td>
                    <td className="py-2 text-right">{Number(s.total_input_tokens).toLocaleString()}</td>
                    <td className="py-2 text-right">{Number(s.total_output_tokens).toLocaleString()}</td>
                    <td className="py-2 text-right text-red-600">{Number(s.error_count).toLocaleString()}</td>
                    <td className="py-2 text-right">{Number(s.unique_users).toLocaleString()}</td>
                  </tr>
                ))}
                {sortedUsage.length === 0 && (
                  <tr><td colSpan={6} className="py-8 text-center text-gray-400 dark:text-gray-500">{usageLoading ? 'Loading…' : 'No usage data'}</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination page={usageSafePage} totalItems={sortedUsage.length} pageSize={usagePageSize} onPageChange={setUsagePage} onPageSizeChange={setUsagePageSize} />
        </CardContent>
      </Card>

      {/* ── Usage by User ─────────────────────────────────────── */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Users className="w-4 h-4 text-blue-600" /> Usage by User (last {days} days)
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                  <SortableHeader label="User" sortKey="requester" current={userSort.sort} onToggle={userSort.toggle} />
                  <SortableHeader label="Requests" sortKey="total_requests" current={userSort.sort} onToggle={userSort.toggle} align="right" />
                  <SortableHeader label="Input Tokens" sortKey="total_input_tokens" current={userSort.sort} onToggle={userSort.toggle} align="right" />
                  <SortableHeader label="Output Tokens" sortKey="total_output_tokens" current={userSort.sort} onToggle={userSort.toggle} align="right" />
                  <SortableHeader label="Errors" sortKey="error_count" current={userSort.sort} onToggle={userSort.toggle} align="right" />
                </tr>
              </thead>
              <tbody>
                {pagedUsers.map((u: any) => (
                  <tr key={u.requester} className="border-b border-gray-100 dark:border-gray-700">
                    <td className="py-2 font-medium text-sm">{u.requester || '—'}</td>
                    <td className="py-2 text-right">{Number(u.total_requests).toLocaleString()}</td>
                    <td className="py-2 text-right">{Number(u.total_input_tokens).toLocaleString()}</td>
                    <td className="py-2 text-right">{Number(u.total_output_tokens).toLocaleString()}</td>
                    <td className="py-2 text-right text-red-600">{Number(u.error_count).toLocaleString()}</td>
                  </tr>
                ))}
                {sortedUsers.length === 0 && (
                  <tr><td colSpan={5} className="py-8 text-center text-gray-400 dark:text-gray-500">{usersLoading ? 'Loading…' : 'No user data'}</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination page={userSafePage} totalItems={sortedUsers.length} pageSize={userPageSize} onPageChange={setUserPage} onPageSizeChange={setUserPageSize} />
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Permissions Section ──────────────────────────────────────── */

function PermissionsSection() {
  const { data: endpoints, isLoading } = useEndpointsWithPermissions()
  const updatePerm = useUpdateEndpointPermission()
  const removePerm = useRemoveEndpointPermission()

  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort('endpoint_name', 'asc')
  const [expanded, setExpanded] = useState<string | null>(null)
  const [showGrant, setShowGrant] = useState<string | null>(null)
  const [grantPrincipal, setGrantPrincipal] = useState('')
  const [grantType, setGrantType] = useState<'user' | 'group' | 'service_principal'>('group')
  const [grantLevel, setGrantLevel] = useState('CAN_QUERY')

  // Notification state
  const [notification, setNotification] = useState<{ type: 'success' | 'error'; message: string } | null>(null)

  // Per-endpoint ACL sort/pagination
  const [aclPage, setAclPage] = useState(0)
  const [aclPageSize] = useState(5)
  const aclSort = useSort('principal', 'asc')

  // Edit state
  const [editingAcl, setEditingAcl] = useState<{ principal: string; principal_type: string; level: string } | null>(null)

  // Auto-dismiss notification after 4s
  const showNotification = useCallback((type: 'success' | 'error', message: string) => {
    setNotification({ type, message })
    setTimeout(() => setNotification(null), 4000)
  }, [])

  const sorted = useMemo(() => {
    if (!endpoints) return []
    return sortRows(endpoints, sort, (row: any, key: string) => {
      if (key === 'endpoint_name') return (row.endpoint_name || '').toLowerCase()
      if (key === 'state') return (row.state || '').toLowerCase()
      if (key === 'served_models') return (row.served_models || '').toLowerCase()
      if (key === 'task') return (row.task || '').toLowerCase()
      if (key === 'acl_count') return (row.acl || []).length
      return ''
    })
  }, [endpoints, sort])

  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize))
  const safePage = Math.min(page, totalPages - 1)
  const pagedRows = sorted.slice(safePage * pageSize, (safePage + 1) * pageSize)

  const toggleExpanded = useCallback((name: string) => {
    setExpanded((prev) => (prev === name ? null : name))
    setShowGrant(null)
    setEditingAcl(null)
    setAclPage(0)
  }, [])

  const handleGrant = useCallback((endpointName: string) => {
    if (!grantPrincipal.trim()) return
    updatePerm.mutate({
      endpoint_name: endpointName,
      principal: grantPrincipal.trim(),
      principal_type: grantType,
      permission_level: grantLevel,
    }, {
      onSuccess: (data: any) => {
        setGrantPrincipal('')
        setShowGrant(null)
        if (data?.error) {
          showNotification('error', `Grant failed: ${data.error}`)
        } else {
          showNotification('success', `Granted ${grantLevel} to ${grantPrincipal.trim()} on ${endpointName}`)
        }
      },
      onError: () => {
        showNotification('error', `Failed to grant permission on ${endpointName}`)
      },
    })
  }, [grantPrincipal, grantType, grantLevel, updatePerm, showNotification])

  const handleEdit = useCallback((endpointName: string) => {
    if (!editingAcl) return
    updatePerm.mutate({
      endpoint_name: endpointName,
      principal: editingAcl.principal,
      principal_type: editingAcl.principal_type,
      permission_level: editingAcl.level,
    }, {
      onSuccess: (data: any) => {
        setEditingAcl(null)
        if (data?.error) {
          showNotification('error', `Update failed: ${data.error}`)
        } else {
          showNotification('success', `Updated ${editingAcl.principal} to ${editingAcl.level} on ${endpointName}`)
        }
      },
      onError: () => {
        showNotification('error', `Failed to update permission on ${endpointName}`)
      },
    })
  }, [editingAcl, updatePerm, showNotification])

  const handleRevoke = useCallback((endpointName: string, principal: string, principalType: string) => {
    if (!confirm(`Remove all permissions for "${principal}" on ${endpointName}?`)) return
    removePerm.mutate({ endpoint_name: endpointName, principal, principal_type: principalType }, {
      onSuccess: (data: any) => {
        if (data?.error) {
          showNotification('error', `Revoke failed: ${data.error}`)
        } else {
          showNotification('success', `Removed permissions for ${principal} on ${endpointName}`)
        }
      },
      onError: () => {
        showNotification('error', `Failed to remove permissions for ${principal} on ${endpointName}`)
      },
    })
  }, [removePerm, showNotification])

  if (isLoading) return <div className="text-sm text-gray-400 dark:text-gray-500">Loading endpoint permissions…</div>

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-2">
          <Shield className="w-4 h-4 text-db-red" /> Endpoint Permissions
        </CardTitle>
      </CardHeader>
      <CardContent>
        {/* Toast notification — fixed top-right */}
        {notification && (
          <div className={`fixed top-4 right-4 z-[100] flex items-center gap-2 px-4 py-3 rounded-lg text-sm shadow-lg animate-in slide-in-from-top-2 fade-in duration-200 max-w-sm ${
            notification.type === 'success'
              ? 'bg-green-50 dark:bg-green-900/80 text-green-700 dark:text-green-300 border border-green-200 dark:border-green-700'
              : 'bg-red-50 dark:bg-red-900/80 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-700'
          }`}>
            {notification.type === 'success'
              ? <CheckCircle className="w-4 h-4 flex-shrink-0" />
              : <AlertCircle className="w-4 h-4 flex-shrink-0" />}
            <span className="flex-1">{notification.message}</span>
            <button onClick={() => setNotification(null)} className="p-0.5 hover:opacity-70"><X className="w-3.5 h-3.5" /></button>
          </div>
        )}

        <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">
          All serving endpoints and their access control lists. Click an endpoint to view and edit permissions.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                <th className="pb-2 w-6" />
                <SortableHeader label="Endpoint" sortKey="endpoint_name" current={sort} onToggle={toggle} />
                <SortableHeader label="Status" sortKey="state" current={sort} onToggle={toggle} />
                <SortableHeader label="Task" sortKey="task" current={sort} onToggle={toggle} />
                <SortableHeader label="Served Models" sortKey="served_models" current={sort} onToggle={toggle} />
                <SortableHeader label="Grants" sortKey="acl_count" current={sort} onToggle={toggle} align="right" />
              </tr>
            </thead>
            <tbody>
              {pagedRows.map((ep: any) => {
                const isExpanded = expanded === ep.endpoint_name
                const directAcl = (ep.acl || []).flatMap((a: any) =>
                  (a.permissions || []).map((p: any) => ({
                    ...p,
                    principal: a.principal,
                    principal_type: a.principal_type,
                  })),
                )
                // Sort and paginate ACL rows
                const sortedAcl = sortRows(directAcl, aclSort.sort, (row: any, key: string) => {
                  if (key === 'principal') return (row.principal || '').toLowerCase()
                  if (key === 'principal_type') return (row.principal_type || '').toLowerCase()
                  if (key === 'permission_level') return (row.permission_level || '').toLowerCase()
                  return ''
                })
                const aclTotalPages = Math.max(1, Math.ceil(sortedAcl.length / aclPageSize))
                const aclSafePage = Math.min(aclPage, aclTotalPages - 1)
                const pagedAcl = sortedAcl.slice(aclSafePage * aclPageSize, (aclSafePage + 1) * aclPageSize)

                return (
                  <Fragment key={ep.endpoint_name}>
                    <tr
                      className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800/40 cursor-pointer"
                      onClick={() => toggleExpanded(ep.endpoint_name)}
                    >
                      <td className="py-2.5 pl-1">
                        {isExpanded
                          ? <ChevronDown className="w-4 h-4 text-gray-400" />
                          : <ChevronRight className="w-4 h-4 text-gray-400" />}
                      </td>
                      <td className="py-2.5 font-medium dark:text-gray-200">
                        <span className="flex items-center gap-1.5">
                          {ep.endpoint_name}
                          {ep.is_foundation_model && (
                            <Badge variant="warning" className="text-[10px]">Foundation Model</Badge>
                          )}
                        </span>
                      </td>
                      <td className="py-2.5">
                        <Badge variant={ep.state === 'READY' ? 'success' : ep.state === 'NOT_READY' ? 'error' : 'default'} className="text-xs">
                          {ep.state}
                        </Badge>
                      </td>
                      <td className="py-2.5 text-gray-500 dark:text-gray-400 text-xs">{ep.task || '—'}</td>
                      <td className="py-2.5 text-xs text-gray-500 dark:text-gray-400 max-w-xs truncate" title={ep.served_models}>
                        {ep.served_models}
                      </td>
                      <td className="py-2.5 text-right font-medium dark:text-gray-300">{directAcl.length}</td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={6} className="bg-gray-50 dark:bg-gray-800/60 px-4 py-3">
                          <div className="space-y-3">
                            {ep.is_foundation_model && ep.uc_model_name && (
                              <p className="text-xs text-amber-600 dark:text-amber-400">
                                UC Model: <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">{ep.uc_model_name}</code> — Permissions managed via Unity Catalog grants
                              </p>
                            )}

                            {/* Permissions table — sortable & paginated */}
                            {directAcl.length > 0 ? (
                              <>
                                <table className="w-full text-xs">
                                  <thead>
                                    <tr className="text-left text-gray-500 dark:text-gray-400 border-b dark:border-gray-600">
                                      <SortableHeader label="Principal" sortKey="principal" current={aclSort.sort} onToggle={aclSort.toggle} className="pb-1.5 font-medium text-xs" />
                                      <SortableHeader label="Type" sortKey="principal_type" current={aclSort.sort} onToggle={aclSort.toggle} className="pb-1.5 font-medium text-xs" />
                                      <SortableHeader label="Permission" sortKey="permission_level" current={aclSort.sort} onToggle={aclSort.toggle} className="pb-1.5 font-medium text-xs" />
                                      <th className="pb-1.5 font-medium">Source</th>
                                      <th className="pb-1.5 font-medium w-20 text-right">Actions</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {pagedAcl.map((acl: any, j: number) => {
                                      const isEditing = editingAcl?.principal === acl.principal && editingAcl?.principal_type === acl.principal_type
                                      return (
                                        <tr key={j} className="border-b border-gray-200 dark:border-gray-700/50">
                                          <td className="py-1.5 font-medium dark:text-gray-200">{acl.principal}</td>
                                          <td className="py-1.5">
                                            <Badge variant={acl.principal_type === 'group' ? 'info' : acl.principal_type === 'service_principal' ? 'warning' : 'default'} className="text-[10px]">
                                              {acl.principal_type}
                                            </Badge>
                                          </td>
                                          <td className="py-1.5">
                                            {isEditing ? (
                                              <select
                                                value={editingAcl!.level}
                                                onChange={(e) => setEditingAcl({ principal: editingAcl!.principal, principal_type: editingAcl!.principal_type, level: e.target.value })}
                                                onClick={(e) => e.stopPropagation()}
                                                className="px-1.5 py-0.5 text-[10px] border rounded dark:bg-gray-800 dark:border-gray-600 dark:text-gray-200"
                                              >
                                                {ep.is_foundation_model ? (
                                                  <option value="EXECUTE">EXECUTE</option>
                                                ) : (
                                                  <>
                                                    <option value="CAN_QUERY">CAN_QUERY</option>
                                                    <option value="CAN_MANAGE">CAN_MANAGE</option>
                                                    <option value="CAN_VIEW">CAN_VIEW</option>
                                                  </>
                                                )}
                                              </select>
                                            ) : (
                                              <Badge variant={acl.permission_level === 'CAN_MANAGE' ? 'error' : 'success'} className="text-[10px]">
                                                {acl.permission_level}
                                              </Badge>
                                            )}
                                          </td>
                                          <td className="py-1.5 text-gray-500 dark:text-gray-400">
                                            {acl.inherited ? `inherited from ${acl.inherited_from_object || '…'}` : 'direct'}
                                          </td>
                                          <td className="py-1.5 text-right">
                                            {!acl.inherited && (
                                              <div className="flex items-center justify-end gap-1.5">
                                                {isEditing ? (
                                                  <>
                                                    <button
                                                      onClick={(e) => { e.stopPropagation(); handleEdit(ep.endpoint_name) }}
                                                      disabled={updatePerm.isPending}
                                                      className="text-green-600 hover:text-green-700 dark:text-green-400 disabled:opacity-50"
                                                      title="Save"
                                                    >
                                                      <CheckCircle className="w-3.5 h-3.5" />
                                                    </button>
                                                    <button
                                                      onClick={(e) => { e.stopPropagation(); setEditingAcl(null) }}
                                                      className="text-gray-400 hover:text-gray-600"
                                                      title="Cancel"
                                                    >
                                                      <X className="w-3.5 h-3.5" />
                                                    </button>
                                                  </>
                                                ) : (
                                                  <>
                                                    {!ep.is_foundation_model && (
                                                      <button
                                                        onClick={(e) => { e.stopPropagation(); setEditingAcl({ principal: acl.principal, principal_type: acl.principal_type, level: acl.permission_level }) }}
                                                        className="text-blue-500 hover:text-blue-700 dark:text-blue-400 disabled:opacity-50"
                                                        title="Edit permission"
                                                      >
                                                        <Pencil className="w-3.5 h-3.5" />
                                                      </button>
                                                    )}
                                                    <button
                                                      onClick={(e) => { e.stopPropagation(); handleRevoke(ep.endpoint_name, acl.principal, acl.principal_type) }}
                                                      disabled={removePerm.isPending}
                                                      className="text-red-500 hover:text-red-700 dark:text-red-400 disabled:opacity-50"
                                                      title="Remove permission"
                                                    >
                                                      <Trash2 className="w-3.5 h-3.5" />
                                                    </button>
                                                  </>
                                                )}
                                              </div>
                                            )}
                                          </td>
                                        </tr>
                                      )
                                    })}
                                  </tbody>
                                </table>
                                {sortedAcl.length > aclPageSize && (
                                  <TablePagination page={aclSafePage} totalItems={sortedAcl.length} pageSize={aclPageSize} onPageChange={setAclPage} onPageSizeChange={() => {}} />
                                )}
                              </>
                            ) : (
                              <p className="text-xs text-gray-400 dark:text-gray-500 italic">
                                No explicit grants — using default workspace permissions.
                              </p>
                            )}

                            {/* Add grant */}
                            {showGrant === ep.endpoint_name ? (
                              <div className="flex items-end gap-2 bg-white dark:bg-gray-900 border dark:border-gray-700 rounded-lg p-3">
                                <div className="flex-1 min-w-0">
                                  <label className="block text-[10px] font-medium text-gray-500 dark:text-gray-400 mb-1">Principal</label>
                                  <PrincipalAutocomplete
                                    value={grantPrincipal}
                                    onChange={setGrantPrincipal}
                                    onSelect={(p) => {
                                      setGrantPrincipal(p.email || p.display_name)
                                      setGrantType(p.type as any)
                                    }}
                                    placeholder="Search users, groups, or service principals..."
                                  />
                                </div>
                                {!ep.is_foundation_model && (
                                  <div>
                                    <label className="block text-[10px] font-medium text-gray-500 dark:text-gray-400 mb-1">Type</label>
                                    <select
                                      value={grantType}
                                      onChange={(e) => setGrantType(e.target.value as any)}
                                      className="px-2 py-1 text-xs border rounded dark:bg-gray-800 dark:border-gray-600 dark:text-gray-200"
                                      onClick={(e) => e.stopPropagation()}
                                    >
                                      <option value="group">Group</option>
                                      <option value="user">User</option>
                                      <option value="service_principal">Service Principal</option>
                                    </select>
                                  </div>
                                )}
                                {!ep.is_foundation_model && (
                                  <div>
                                    <label className="block text-[10px] font-medium text-gray-500 dark:text-gray-400 mb-1">Permission</label>
                                    <select
                                      value={grantLevel}
                                      onChange={(e) => setGrantLevel(e.target.value)}
                                      className="px-2 py-1 text-xs border rounded dark:bg-gray-800 dark:border-gray-600 dark:text-gray-200"
                                      onClick={(e) => e.stopPropagation()}
                                    >
                                      <option value="CAN_QUERY">CAN_QUERY</option>
                                      <option value="CAN_MANAGE">CAN_MANAGE</option>
                                      <option value="CAN_VIEW">CAN_VIEW</option>
                                    </select>
                                  </div>
                                )}
                                <button
                                  onClick={(e) => { e.stopPropagation(); handleGrant(ep.endpoint_name) }}
                                  disabled={updatePerm.isPending || !grantPrincipal.trim()}
                                  className="px-3 py-1 text-xs font-medium bg-db-red text-white rounded hover:bg-db-red/90 disabled:opacity-50"
                                >
                                  {updatePerm.isPending ? '…' : 'Grant'}
                                </button>
                                <button
                                  onClick={(e) => { e.stopPropagation(); setShowGrant(null) }}
                                  className="p-1 text-gray-400 hover:text-gray-600"
                                >
                                  <X className="w-4 h-4" />
                                </button>
                              </div>
                            ) : (
                              <button
                                onClick={(e) => { e.stopPropagation(); setShowGrant(ep.endpoint_name); setGrantPrincipal(''); setGrantLevel(ep.is_foundation_model ? 'EXECUTE' : 'CAN_QUERY') }}
                                className="inline-flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400 hover:underline"
                              >
                                <Plus className="w-3.5 h-3.5" /> Add permission
                              </button>
                            )}
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                )
              })}
              {sorted.length === 0 && (
                <tr><td colSpan={6} className="py-8 text-center text-gray-400 dark:text-gray-500">No serving endpoints found</td></tr>
              )}
            </tbody>
          </table>
        </div>
        <TablePagination page={safePage} totalItems={sorted.length} pageSize={pageSize} onPageChange={setPage} onPageSizeChange={setPageSize} />
      </CardContent>
    </Card>
  )
}

/* ── Metrics Section (metrics + request logs) ────────────────── */

function MetricsSection() {
  const { data: metrics, isLoading } = useGatewayMetrics(24)
  const { data: timeseries } = useGatewayUsageTimeseries(1) // last 24h
  const { data: logs, isLoading: logsLoading } = useGatewayInferenceLogs(100)
  const [logPage, setLogPage] = useState(0)
  const [logPageSize, setLogPageSize] = useState(10)
  const logSort = useSort('request_time', 'desc')

  const requestData = timeseries?.map((t: any) => ({
    timestamp: t.hour,
    value: Number(t.request_count || 0),
  })) || []

  const errorData = timeseries?.map((t: any) => ({
    timestamp: t.hour,
    value: t.request_count > 0 ? (Number(t.error_count) * 100 / Number(t.request_count)) : 0,
  })) || []

  const allLogs = logs || []
  const sortedLogs = useMemo(
    () =>
      sortRows(allLogs, logSort.sort, (l: any, key) => {
        if (key === 'request_time') return l.request_time ? new Date(l.request_time).getTime() : 0
        if (key === 'endpoint_name') return (l.endpoint_name || '').toLowerCase()
        if (key === 'requester') return (l.requester || '').toLowerCase()
        if (key === 'input_tokens') return Number(l.input_tokens || 0)
        if (key === 'output_tokens') return Number(l.output_tokens || 0)
        if (key === 'status_code') return Number(l.status_code || 0)
        if (key === 'streaming') return l.streaming ? 'yes' : 'no'
        return ''
      }),
    [allLogs, logSort.sort],
  )
  const logTotalPages = Math.max(1, Math.ceil(sortedLogs.length / logPageSize))
  const logSafePage = Math.min(logPage, logTotalPages - 1)
  const pagedLogs = sortedLogs.slice(logSafePage * logPageSize, (logSafePage + 1) * logPageSize)

  return (
    <div className="space-y-6">
      {/* KPIs */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
        <KpiCard title="Total Requests" value={metrics?.total_requests ?? 0} format="number" />
        <KpiCard title="Total Errors" value={metrics?.total_errors ?? 0} format="number" />
        <KpiCard title="Error Rate" value={metrics?.error_rate ?? 0} format="percentage" />
        <KpiCard title="Input Tokens" value={metrics?.total_input_tokens ?? 0} format="number" />
        <KpiCard title="Output Tokens" value={metrics?.total_output_tokens ?? 0} format="number" />
        <KpiCard title="Unique Users" value={metrics?.unique_users ?? 0} format="number" />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader><CardTitle className="text-base">Requests (24h)</CardTitle></CardHeader>
          <CardContent>
            {requestData.length ? <LineChart data={requestData} name="Requests/hr" color={DB_CHART.primary} /> : <div className="text-gray-400 dark:text-gray-500 text-center py-12">{isLoading ? 'Loading…' : 'No data'}</div>}
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">Error Rate (24h)</CardTitle></CardHeader>
          <CardContent>
            {errorData.length ? <LineChart data={errorData} name="Error Rate (%)" color={DB_CHART.error} /> : <div className="text-gray-400 dark:text-gray-500 text-center py-12">{isLoading ? 'Loading…' : 'No data'}</div>}
          </CardContent>
        </Card>
      </div>

      {/* Per-task breakdown */}
      {metrics?.by_task && metrics.by_task.length > 0 && (
        <TaskMetricsTable tasks={metrics.by_task} />
      )}

      {/* Request Logs */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <ScrollText className="w-4 h-4 text-db-orange" /> Recent Request Logs
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-xs text-gray-500 mb-4">
            Individual requests from <code className="text-xs bg-gray-100 dark:bg-gray-700 px-1 rounded">system.serving.endpoint_usage</code>.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                  <SortableHeader label="Time" sortKey="request_time" current={logSort.sort} onToggle={logSort.toggle} />
                  <SortableHeader label="Endpoint" sortKey="endpoint_name" current={logSort.sort} onToggle={logSort.toggle} />
                  <SortableHeader label="User" sortKey="requester" current={logSort.sort} onToggle={logSort.toggle} />
                  <SortableHeader label="Input Tokens" sortKey="input_tokens" current={logSort.sort} onToggle={logSort.toggle} align="right" />
                  <SortableHeader label="Output Tokens" sortKey="output_tokens" current={logSort.sort} onToggle={logSort.toggle} align="right" />
                  <SortableHeader label="Status" sortKey="status_code" current={logSort.sort} onToggle={logSort.toggle} />
                  <SortableHeader label="Streaming" sortKey="streaming" current={logSort.sort} onToggle={logSort.toggle} />
                </tr>
              </thead>
              <tbody>
                {pagedLogs.map((l: any, i: number) => (
                  <tr key={l.request_id || i} className="border-b border-gray-100 dark:border-gray-700">
                    <td className="py-2 text-xs text-gray-500">
                      {l.request_time ? format(new Date(l.request_time), 'MMM dd HH:mm:ss') : '—'}
                    </td>
                    <td className="py-2 font-medium text-sm">{l.endpoint_name || '—'}</td>
                    <td className="py-2 text-gray-500 text-xs max-w-[200px] truncate">{l.requester || '—'}</td>
                    <td className="py-2 text-right text-xs">{Number(l.input_tokens || 0).toLocaleString()}</td>
                    <td className="py-2 text-right text-xs">{Number(l.output_tokens || 0).toLocaleString()}</td>
                    <td className="py-2 text-center">
                      <span className={`inline-block w-2 h-2 rounded-full ${
                        (l.status_code || 0) >= 400 ? 'bg-red-500' : 'bg-green-500'
                      }`} />
                      <span className="ml-1 text-xs text-gray-500">{l.status_code}</span>
                    </td>
                    <td className="py-2 text-center text-xs text-gray-400">{l.streaming ? 'Yes' : 'No'}</td>
                  </tr>
                ))}
                {sortedLogs.length === 0 && (
                  <tr><td colSpan={7} className="py-8 text-center text-gray-400 dark:text-gray-500">{logsLoading ? 'Loading…' : 'No request logs'}</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination page={logSafePage} totalItems={sortedLogs.length} pageSize={logPageSize} onPageChange={setLogPage} onPageSizeChange={setLogPageSize} />
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Task Metrics Table (paginated) ──────────────────────────── */

function TaskMetricsTable({ tasks }: { tasks: any[] }) {
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort('requests', 'desc')

  const sorted = useMemo(
    () =>
      sortRows(tasks, sort, (t: any, key) => {
        if (key === 'task') return (t.task || '').toLowerCase()
        if (key === 'requests') return Number(t.requests || 0)
        if (key === 'input_tokens') return Number(t.input_tokens || 0)
        if (key === 'output_tokens') return Number(t.output_tokens || 0)
        if (key === 'errors') return Number(t.errors || 0)
        return 0
      }),
    [tasks, sort],
  )

  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize))
  const safePage = Math.min(page, totalPages - 1)
  const pagedTasks = sorted.slice(safePage * pageSize, (safePage + 1) * pageSize)

  return (
    <Card>
      <CardHeader><CardTitle className="text-base">Metrics by Task Type (24h)</CardTitle></CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                <SortableHeader label="Task" sortKey="task" current={sort} onToggle={toggle} />
                <SortableHeader label="Requests" sortKey="requests" current={sort} onToggle={toggle} align="right" />
                <SortableHeader label="Input Tokens" sortKey="input_tokens" current={sort} onToggle={toggle} align="right" />
                <SortableHeader label="Output Tokens" sortKey="output_tokens" current={sort} onToggle={toggle} align="right" />
                <SortableHeader label="Errors" sortKey="errors" current={sort} onToggle={toggle} align="right" />
              </tr>
            </thead>
            <tbody>
              {pagedTasks.map((t: any) => (
                <tr key={t.task} className="border-b border-gray-100 dark:border-gray-700">
                  <td className="py-2 font-mono text-sm">{t.task}</td>
                  <td className="py-2 text-right">{Number(t.requests).toLocaleString()}</td>
                  <td className="py-2 text-right">{Number(t.input_tokens).toLocaleString()}</td>
                  <td className="py-2 text-right">{Number(t.output_tokens).toLocaleString()}</td>
                  <td className="py-2 text-right text-red-600">{Number(t.errors).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <TablePagination page={safePage} totalItems={sorted.length} pageSize={pageSize} onPageChange={setPage} onPageSizeChange={setPageSize} />
      </CardContent>
    </Card>
  )
}

/* ── Rate Limits & Guardrails Section ────────────────────────── */

function RateLimitsAndGuardrailsSection() {
  const { data: limits, isLoading: limitsLoading } = useGatewayRateLimits()
  const { data: guardrails, isLoading: guardrailsLoading } = useGatewayGuardrails()
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(10)
  const { sort, toggle } = useSort('endpoint_name', 'asc')

  const allLimits = limits || []
  const sorted = useMemo(
    () =>
      sortRows(allLimits, sort, (l: any, key) => {
        if (key === 'endpoint_name') return (l.endpoint_name || '').toLowerCase()
        if (key === 'calls') return Number(l.calls || 0)
        if (key === 'renewal_period') return (l.renewal_period || '').toLowerCase()
        if (key === 'key') return (l.key || '').toLowerCase()
        return ''
      }),
    [allLimits, sort],
  )

  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize))
  const safePage = Math.min(page, totalPages - 1)
  const pagedLimits = sorted.slice(safePage * pageSize, (safePage + 1) * pageSize)

  if (limitsLoading && guardrailsLoading) return <div className="text-sm text-gray-400 dark:text-gray-500">Loading…</div>

  return (
    <div className="space-y-6">
      {/* Rate Limits */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <ShieldAlert className="w-4 h-4 text-yellow-600" /> Rate Limits
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">
            Rate limits configured via AI Gateway on serving endpoints.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                  <SortableHeader label="Endpoint" sortKey="endpoint_name" current={sort} onToggle={toggle} />
                  <SortableHeader label="Calls" sortKey="calls" current={sort} onToggle={toggle} align="right" />
                  <SortableHeader label="Renewal Period" sortKey="renewal_period" current={sort} onToggle={toggle} />
                  <SortableHeader label="Key" sortKey="key" current={sort} onToggle={toggle} />
                </tr>
              </thead>
              <tbody>
                {pagedLimits.map((l: any, i: number) => (
                  <tr key={i} className="border-b border-gray-100 dark:border-gray-700">
                    <td className="py-2 font-medium">{l.endpoint_name}</td>
                    <td className="py-2 text-right">{l.calls?.toLocaleString() ?? '—'}</td>
                    <td className="py-2">
                      <Badge variant="info" className="text-xs">{l.renewal_period || '—'}</Badge>
                    </td>
                    <td className="py-2 text-gray-500">{l.key || '—'}</td>
                  </tr>
                ))}
                {sorted.length === 0 && (
                  <tr><td colSpan={4} className="py-8 text-center text-gray-400 dark:text-gray-500">No rate limits configured on any endpoint</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination page={safePage} totalItems={sorted.length} pageSize={pageSize} onPageChange={setPage} onPageSizeChange={setPageSize} />
        </CardContent>
      </Card>

      {/* Guardrails */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <ShieldCheck className="w-4 h-4 text-green-600" /> Guardrails
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">
            Safety guardrails configured via AI Gateway to protect inputs and outputs.
          </p>
          {guardrails && guardrails.length > 0 ? (
            <div className="space-y-4">
              {guardrails.map((g: any, i: number) => (
                <div key={i} className="border dark:border-gray-700 rounded-lg p-4">
                  <div className="font-medium dark:text-gray-200 mb-2">{g.endpoint_name}</div>
                  {g.guardrails?.input && (
                    <div className="mb-2">
                      <span className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase">Input Guardrails</span>
                      <div className="mt-1 flex gap-2 flex-wrap">
                        {g.guardrails.input.pii && (
                          <Badge variant="warning" className="text-xs">PII: {g.guardrails.input.pii.behavior}</Badge>
                        )}
                        {g.guardrails.input.safety && (
                          <Badge variant="error" className="text-xs">Safety: ON</Badge>
                        )}
                      </div>
                    </div>
                  )}
                  {g.guardrails?.output && (
                    <div>
                      <span className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase">Output Guardrails</span>
                      <div className="mt-1 flex gap-2 flex-wrap">
                        {g.guardrails.output.pii && (
                          <Badge variant="warning" className="text-xs">PII: {g.guardrails.output.pii.behavior}</Badge>
                        )}
                        {g.guardrails.output.safety && (
                          <Badge variant="error" className="text-xs">Safety: ON</Badge>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          ) : (
            <div className="text-gray-400 dark:text-gray-500 text-center py-8">No guardrails configured on any endpoint</div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
