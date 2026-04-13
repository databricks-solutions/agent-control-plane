import { useState, useMemo, Fragment } from 'react'
import { useQueryClient, useIsFetching } from '@tanstack/react-query'
import {
  useAgentsFull,
  useRecentRequests,
  useAllPrincipals,
  useResourcePermissions,
  useGrantPermission,
  useRevokePermission,
  useAllAgentsMerged,
  useUserAnalyticsPageData,
  useDiscoveryStatus,
  useEndpointsWithPermissions,
  useAgentsWithPermissions,
  useUpdateEndpointPermission,
  useRemoveEndpointPermission,
  useCurrentUser,
  useSyncAgents,
} from '@/api/hooks'
import { RefreshButton } from '@/components/RefreshButton'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { KpiCard } from '@/components/KpiCard'
import { TablePagination } from '@/components/TablePagination'
import { SortableHeader, useSort, sortRows } from '@/components/SortableTable'
import { BarChart } from '@/components/charts/BarChart'
import { LineChart } from '@/components/charts/LineChart'
import { PieChart } from '@/components/charts/PieChart'
import { DB_CHART, DB_RED_SHADES } from '@/lib/brand'
import { PrincipalAutocomplete } from '@/components/PrincipalAutocomplete'
import {
  UserCog,
  Shield,
  Users,
  Plus,
  Trash2,
  Search,
  Lock,
  Unlock,
  TrendingUp,
  Clock,
  Grid3X3,
  Network,
  Activity,
  Bot,
  ChevronDown,
  ChevronRight,
  X,
  CheckCircle,
  AlertCircle,
  RefreshCw,
  Filter,
} from 'lucide-react'

type TabKey = 'principals' | 'permissions' | 'builders' | 'top-users' | 'heatmap' | 'rbac' | 'user-agent'

const DOW_LABELS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
const HOUR_LABELS = Array.from({ length: 24 }, (_, i) =>
  i === 0 ? '12a' : i < 12 ? `${i}a` : i === 12 ? '12p' : `${i - 12}p`
)

function fmtNumber(v: number): string {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}K`
  return String(Math.round(v))
}

function fmtCost(v: number): string {
  if (v >= 1000) return `$${(v / 1000).toFixed(1)}k`
  if (v >= 1) return `$${v.toFixed(2)}`
  return `$${v.toFixed(4)}`
}

function heatColor(value: number, max: number): string {
  if (max === 0 || value === 0) return 'rgba(255, 54, 33, 0.03)'
  const ratio = value / max
  if (ratio > 0.75) return DB_RED_SHADES.p25
  if (ratio > 0.5) return DB_RED_SHADES.p50
  if (ratio > 0.25) return DB_RED_SHADES.p75
  return DB_RED_SHADES.max
}

function textColor(value: number, max: number): string {
  if (max === 0 || value === 0) return '#9CA3AF'
  return (value / max) > 0.5 ? '#FFFFFF' : '#1F2937'
}

export default function AdminPage() {
  const [tab, setTab] = useState<TabKey>('principals')
  const [analyticsDays, setAnalyticsDays] = useState(30)
  const queryClient = useQueryClient()
  const fetchingAdmin = useIsFetching({ queryKey: ['admin'] })
  const fetchingAgents = useIsFetching({ queryKey: ['agents'] })
  const isFetchingAdmin = fetchingAdmin > 0 || fetchingAgents > 0

  /* ── existing admin hooks ───────────────────────────────────── */
  const { data: agents } = useAgentsFull()
  const { data: allAgents } = useAllAgentsMerged()
  const { data: discoveryStatus } = useDiscoveryStatus()
  const { data: gatewayEndpoints } = useEndpointsWithPermissions()
  const { data: recentData } = useRecentRequests(100)
  const { data: principals, isLoading: principalsLoading } = useAllPrincipals(analyticsDays)
  const grantMutation = useGrantPermission()
  const revokeMutation = useRevokePermission()
  const { data: agentsWithPerms, isLoading: agentPermsLoading } = useAgentsWithPermissions()
  const updateAgentPerm = useUpdateEndpointPermission()
  const removeAgentPerm = useRemoveEndpointPermission()
  const { data: currentUser } = useCurrentUser()
  const isAccountAdmin = currentUser?.is_account_admin ?? false
  const syncAgents = useSyncAgents()

  const requests = recentData?.data || []

  /* ── user analytics hook ────────────────────────────────────── */
  const { data: analyticsData } = useUserAnalyticsPageData(analyticsDays)
  const uaKpis = analyticsData?.kpis
  const topUsers = analyticsData?.top_users || []
  const heatmap = analyticsData?.heatmap || []
  const dauTrend = analyticsData?.daily_active_users || []
  const userAgentMatrix = analyticsData?.user_agent_matrix || []
  const distribution = analyticsData?.distribution || []

  /* ── pagination state ───────────────────────────────────────── */
  const [principalPage, setPrincipalPage] = useState(0)
  const [principalPageSize, setPrincipalPageSize] = useState(10)
  const [grantPage, setGrantPage] = useState(0)
  const [grantPageSize, setGrantPageSize] = useState(10)
  const [builderPage, setBuilderPage] = useState(0)
  const [builderPageSize, setBuilderPageSize] = useState(10)
  const [userPage, setUserPage] = useState(0)
  const [userPageSize, setUserPageSize] = useState(10)
  const [uaUserPage, setUaUserPage] = useState(0)
  const [uaUserPageSize, setUaUserPageSize] = useState(10)
  const [rbacPage, setRbacPage] = useState(0)
  const [rbacPageSize, setRbacPageSize] = useState(10)
  const [matrixPage, setMatrixPage] = useState(0)
  const [matrixPageSize, setMatrixPageSize] = useState(10)
  const [agentPermPage, setAgentPermPage] = useState(0)
  const [agentPermPageSize, setAgentPermPageSize] = useState(10)

  /* ── agent permissions state ─────────────────────────────── */
  const [agentExpanded, setAgentExpanded] = useState<string | null>(null)
  const [agentShowGrant, setAgentShowGrant] = useState<string | null>(null)
  const [agentGrantPrincipal, setAgentGrantPrincipal] = useState('')
  const [agentGrantType, setAgentGrantType] = useState<'user' | 'group' | 'service_principal'>('user')
  const [agentGrantLevel, setAgentGrantLevel] = useState('CAN_QUERY')
  const [agentNotification, setAgentNotification] = useState<{ type: 'success' | 'error'; message: string } | null>(null)
  const [agentAclPage, setAgentAclPage] = useState(0)
  const [agentAclPageSize] = useState(5)

  /* ── agent permission filters ───────────────────────────────── */
  const [agentPermSearch, setAgentPermSearch] = useState('')
  const [agentPermTypeFilter, setAgentPermTypeFilter] = useState<string>('all')
  const [agentPermWsFilter, setAgentPermWsFilter] = useState<string>('all') // 'all' | 'online' | 'offline' | 'local'

  const agentPermTypes = useMemo(() => {
    if (!agentsWithPerms) return []
    const types = new Set(agentsWithPerms.map((a: any) => a.type || '').filter(Boolean))
    return Array.from(types).sort()
  }, [agentsWithPerms])

  const filteredAgentsWithPerms = useMemo(() => {
    if (!agentsWithPerms) return []
    return agentsWithPerms.filter((a: any) => {
      // search
      if (agentPermSearch) {
        const q = agentPermSearch.toLowerCase()
        const match = (a.name || '').toLowerCase().includes(q)
          || (a.endpoint_name || '').toLowerCase().includes(q)
          || (a.type || '').toLowerCase().includes(q)
          || (a.workspace_id || '').toLowerCase().includes(q)
        if (!match) return false
      }
      // type filter
      if (agentPermTypeFilter !== 'all' && (a.type || '') !== agentPermTypeFilter) return false
      // workspace status filter
      if (agentPermWsFilter === 'online' && (a.workspace_active === false)) return false
      if (agentPermWsFilter === 'offline' && (a.workspace_active !== false)) return false
      if (agentPermWsFilter === 'local' && a.is_cross_workspace) return false
      return true
    })
  }, [agentsWithPerms, agentPermSearch, agentPermTypeFilter, agentPermWsFilter])

  /* ── sort state ───────────────────────────────────────────── */
  const principalSort = useSort<string>('principal', 'asc')
  const grantSort = useSort<string>('principal', 'asc')
  const builderSort = useSort<string>('agentCount', 'desc')
  const userSort = useSort<string>('requestCount', 'desc')
  const uaUserSort = useSort<string>('request_count', 'desc')
  const rbacSort = useSort<string>('principal', 'asc')
  const matrixSort = useSort<string>('total', 'desc')
  const agentPermSort = useSort<string>('name', 'asc')
  const agentAclSort = useSort<string>('principal', 'asc')

  /* ── permissions panel state ────────────────────────────────── */
  const [selectedResource, setSelectedResource] = useState<{ type: string; name: string } | null>(null)
  const { data: resourcePerms, isLoading: permsLoading } = useResourcePermissions(
    selectedResource?.type || '',
    selectedResource?.name || ''
  )

  /* ── grant form state ───────────────────────────────────────── */
  const [showGrantForm, setShowGrantForm] = useState(false)
  const [grantPrincipal, setGrantPrincipal] = useState('')
  const [grantPrivilege, setGrantPrivilege] = useState('CAN_QUERY')

  /* ── search state ───────────────────────────────────────────── */
  const [uaUserSearch, setUaUserSearch] = useState('')
  const [rbacSearch, setRbacSearch] = useState('')

  /* ── derive builders ────────────────────────────────────────── */
  const builders = useMemo(() => {
    if (!agents) return []
    const map = new Map<string, { name: string; agentCount: number; agents: string[] }>()
    agents.forEach((a: any) => {
      const creator = a.created_by || 'unknown'
      const entry = map.get(creator) || { name: creator, agentCount: 0, agents: [] as string[] }
      entry.agentCount++
      entry.agents.push(String(a.name))
      map.set(creator, entry)
    })
    return Array.from(map.values())
  }, [agents])

  /* ── derive users (from recent requests) ────────────────────── */
  const users = useMemo(() => {
    const map = new Map<string, { userId: string; requestCount: number; agents: Set<string>; lastSeen: string }>()
    requests.forEach((r: any) => {
      if (!r.user_id) return
      const entry = map.get(r.user_id) || { userId: r.user_id, requestCount: 0, agents: new Set<string>(), lastSeen: '' }
      entry.requestCount++
      if (r.agent_id) entry.agents.add(r.agent_id)
      if (!entry.lastSeen || r.timestamp > entry.lastSeen) entry.lastSeen = r.timestamp
      map.set(r.user_id, entry)
    })
    return Array.from(map.values())
      .map(u => ({ ...u, agents: Array.from(u.agents) }))
      .sort((a, b) => b.requestCount - a.requestCount)
  }, [requests])

  /* ── resource options for dropdown ──────────────────────────── */
  const resourceOptions = useMemo(() => {
    const options: Array<{ type: string; name: string; label: string; isFoundationModel?: boolean }> = []
    ;(allAgents || []).forEach((a: any) => {
      if (a.endpoint_name) {
        options.push({ type: 'serving_endpoint', name: a.endpoint_name, label: `Endpoint: ${a.endpoint_name}` })
      }
    })
    // Add FMAPI endpoints from gateway data
    ;(gatewayEndpoints || []).forEach((ep: any) => {
      if (ep.is_foundation_model && ep.uc_model_name) {
        options.push({
          type: 'function',
          name: ep.uc_model_name,
          label: `Foundation Model: ${ep.endpoint_name}`,
          isFoundationModel: true,
        })
      }
    })
    // Add schema-level option
    options.push({ type: 'schema', name: 'system.ai', label: 'Schema: system.ai (all foundation models)' })
    const seen = new Set<string>()
    return options.filter((o) => {
      const key = `${o.type}:${o.name}`
      if (seen.has(key)) return false
      seen.add(key)
      return true
    })
  }, [allAgents, gatewayEndpoints])

  /* ── heatmap grid (7 × 24) ─────────────────────────────────── */
  const { grid: heatGrid, maxVal } = useMemo(() => {
    const grid: number[][] = Array.from({ length: 7 }, () => Array(24).fill(0))
    let maxVal = 0
    heatmap.forEach(({ dow, hour, count }) => {
      grid[dow][hour] = count
      if (count > maxVal) maxVal = count
    })
    return { grid, maxVal }
  }, [heatmap])

  /* ── filtered analytics users ───────────────────────────────── */
  const filteredUaUsers = useMemo(() => {
    if (!uaUserSearch) return topUsers
    const q = uaUserSearch.toLowerCase()
    return topUsers.filter(
      (u) => u.user_id.toLowerCase().includes(q) || u.agent_list.some((a) => a.toLowerCase().includes(q))
    )
  }, [topUsers, uaUserSearch])

  /* ── RBAC matrix data ───────────────────────────────────────── */
  const analyticsPrincipals = analyticsData?.principals || []
  const { rbacResources, filteredPrincipals, rbacMap } = useMemo(() => {
    const resSet = new Map<string, string>()
    const rbacMap = new Map<string, Map<string, string[]>>()
    analyticsPrincipals.forEach((p) => {
      const permMap = new Map<string, string[]>()
      p.resources.forEach((r) => {
        const key = `${r.resource_type}:${r.resource_name}`
        resSet.set(key, r.resource_name)
        const existing = permMap.get(key) || []
        existing.push(r.permission)
        permMap.set(key, existing)
      })
      rbacMap.set(p.principal, permMap)
    })
    const resources = Array.from(resSet.entries()).map(([key, name]) => ({ key, name }))
    let filtered = analyticsPrincipals
    if (rbacSearch) {
      const q = rbacSearch.toLowerCase()
      filtered = analyticsPrincipals.filter((p) => p.principal.toLowerCase().includes(q))
    }
    return { rbacResources: resources, filteredPrincipals: filtered, rbacMap }
  }, [analyticsPrincipals, rbacSearch])

  /* ── user-agent grouped ─────────────────────────────────────── */
  const userAgentGrouped = useMemo(() => {
    const map = new Map<string, Array<{ agent_id: string; request_count: number }>>()
    userAgentMatrix.forEach(({ user_id, agent_id, request_count }) => {
      const arr = map.get(user_id) || []
      arr.push({ agent_id, request_count })
      map.set(user_id, arr)
    })
    return Array.from(map.entries())
      .map(([user_id, agents]) => ({
        user_id,
        agents: agents.sort((a, b) => b.request_count - a.request_count),
        total: agents.reduce((s, a) => s + a.request_count, 0),
      }))
      .sort((a, b) => b.total - a.total)
  }, [userAgentMatrix])

  /* ── sorted arrays ────────────────────────────────────────── */
  const sortedPrincipals = useMemo(() => sortRows(principals || [], principalSort.sort, (r: any, k) => {
    if (k === 'principal') return (r.principal || '').toLowerCase()
    if (k === 'type') return (r.principal_type || '').toLowerCase()
    if (k === 'resources') return (r.resources || []).length
    return ''
  }), [principals, principalSort.sort])

  const sortedGrants = useMemo(() => sortRows(resourcePerms || [], grantSort.sort, (r: any, k) => {
    if (k === 'principal') return (r.principal || '').toLowerCase()
    if (k === 'type') return (r.principal_type || '').toLowerCase()
    if (k === 'permission') return (r.permission || '').toLowerCase()
    if (k === 'inherited') return r.inherited ? 1 : 0
    return ''
  }), [resourcePerms, grantSort.sort])

  const sortedBuilders = useMemo(() => sortRows(builders, builderSort.sort, (r: any, k) => {
    if (k === 'name') return (r.name || '').toLowerCase()
    if (k === 'agentCount') return Number(r.agentCount || 0)
    return ''
  }), [builders, builderSort.sort])

  const sortedUsers = useMemo(() => sortRows(users, userSort.sort, (r: any, k) => {
    if (k === 'userId') return (r.userId || '').toLowerCase()
    if (k === 'requestCount') return Number(r.requestCount || 0)
    if (k === 'agents') return (r.agents || []).length
    if (k === 'lastSeen') return r.lastSeen || ''
    return ''
  }), [users, userSort.sort])

  const sortedUaUsers = useMemo(() => sortRows(filteredUaUsers, uaUserSort.sort, (r: any, k) => {
    if (k === 'user_id') return (r.user_id || '').toLowerCase()
    if (k === 'request_count') return Number(r.request_count || 0)
    if (k === 'agents_used') return Number(r.agents_used || 0)
    if (k === 'total_tokens') return Number(r.total_tokens || 0)
    if (k === 'total_cost') return Number(r.total_cost || 0)
    if (k === 'avg_latency_ms') return Number(r.avg_latency_ms || 0)
    if (k === 'last_active') return r.last_active || ''
    return ''
  }), [filteredUaUsers, uaUserSort.sort])

  const sortedRbacPrincipals = useMemo(() => sortRows(filteredPrincipals, rbacSort.sort, (r: any, k) => {
    if (k === 'principal') return (r.principal || '').toLowerCase()
    if (k === 'type') return (r.principal_type || '').toLowerCase()
    return ''
  }), [filteredPrincipals, rbacSort.sort])

  const sortedUserAgentGrouped = useMemo(() => sortRows(userAgentGrouped, matrixSort.sort, (r: any, k) => {
    if (k === 'user_id') return (r.user_id || '').toLowerCase()
    if (k === 'total') return Number(r.total || 0)
    return ''
  }), [userAgentGrouped, matrixSort.sort])

  /* ── chart data ─────────────────────────────────────────────── */
  const barData = topUsers.slice(0, 15).map((u) => ({
    name: u.user_id.length > 18 ? u.user_id.slice(0, 16) + '…' : u.user_id,
    requests: u.request_count,
  }))
  const dauChartData = dauTrend.map((d) => ({ timestamp: d.day, value: d.active_users, requests: d.requests }))
  const distPieData = distribution.map((d) => ({ name: `${d.bucket} reqs`, value: d.user_count }))

  const handleGrant = () => {
    if (!selectedResource || !grantPrincipal) return
    grantMutation.mutate(
      { resource_type: selectedResource.type, resource_name: selectedResource.name, principal: grantPrincipal, privileges: [grantPrivilege] },
      { onSuccess: () => { setShowGrantForm(false); setGrantPrincipal('') } }
    )
  }

  const handleRevoke = (principal: string, permission: string) => {
    if (!selectedResource) return
    revokeMutation.mutate({ resource_type: selectedResource.type, resource_name: selectedResource.name, principal, privileges: [permission] })
  }

  const uniqueUsers = users.length
  const totalPrincipals = principals?.length || 0

  const tabs: { key: TabKey; label: string; icon: any }[] = [
    { key: 'principals', label: 'Principals', icon: Users },
    { key: 'permissions', label: 'Permissions', icon: Shield },
    { key: 'builders', label: 'Builders & Users', icon: UserCog },
    { key: 'top-users', label: 'Top Users', icon: TrendingUp },
    { key: 'heatmap', label: 'Activity Heatmap', icon: Clock },
    { key: 'rbac', label: 'RBAC Matrix', icon: Grid3X3 },
    { key: 'user-agent', label: 'User–Agent Map', icon: Network },
  ]

  const avgReqPerUser = uaKpis && uaKpis.active_users_period > 0
    ? (uaKpis.total_requests / uaKpis.active_users_period).toFixed(1) : '0'

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Admin</h2>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            Access management, user analytics, and RBAC controls
          </p>
        </div>
        <div className="flex items-center gap-3">
          <RefreshButton
            onRefresh={() => {
              queryClient.invalidateQueries({ queryKey: ['admin'] })
              queryClient.invalidateQueries({ queryKey: ['agents'] })
              queryClient.invalidateQueries({ queryKey: ['user-analytics'] })
            }}
            isRefreshing={isFetchingAdmin}
            lastSynced={discoveryStatus?.last_synced ?? null}
            title="Refresh admin data"
          />
          <select
            value={analyticsDays}
            onChange={(e) => setAnalyticsDays(Number(e.target.value))}
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
      <div className="border-b dark:border-gray-700 flex gap-1 overflow-x-auto">
        {tabs.map(({ key, label, icon: Icon }) => (
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

      {/* KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard title="Principals" value={totalPrincipals} format="number" />
        <KpiCard title={`Active Users (${analyticsDays}d)`} value={uaKpis?.active_users_period ?? uaKpis?.active_users_24h ?? uniqueUsers} format="number" />
        <KpiCard title="Total Requests" value={uaKpis?.total_requests ?? 0} format="number" />
        <KpiCard title="Avg Requests / User" value={avgReqPerUser} />
      </div>

      {/* ═══════ TAB: Principals ═══════ */}
      {tab === 'principals' && (
        <Card>
          <CardHeader><CardTitle className="text-base">All Principals with Access</CardTitle></CardHeader>
          <CardContent>
            <p className="text-xs text-gray-400 dark:text-gray-500 mb-4">Aggregated view of users, groups, and service principals that have UC grants on AI resources</p>
            {principalsLoading ? (
              <div className="py-8 text-center text-gray-400">Loading principals…</div>
            ) : sortedPrincipals.length === 0 ? (
              <div className="py-8 text-center text-gray-400">No principal data available</div>
            ) : (
              <>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead><tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                    <SortableHeader label="Principal" sortKey="principal" current={principalSort.sort} onToggle={principalSort.toggle} />
                    <SortableHeader label="Type" sortKey="type" current={principalSort.sort} onToggle={principalSort.toggle} />
                    <SortableHeader label="Resources" sortKey="resources" current={principalSort.sort} onToggle={principalSort.toggle} />
                  </tr></thead>
                  <tbody>
                    {(() => { const all = sortedPrincipals; const tp = Math.max(1, Math.ceil(all.length / principalPageSize)); const sp = Math.min(principalPage, tp - 1); return all.slice(sp * principalPageSize, (sp + 1) * principalPageSize) })().map((p: any) => (
                      <tr key={p.principal} className="border-b border-gray-100 dark:border-gray-700">
                        <td className="py-2.5"><div className="flex items-center gap-2"><div className="w-7 h-7 bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-400 rounded-full flex items-center justify-center text-xs font-bold">{p.principal.charAt(0).toUpperCase()}</div><span className="font-medium">{p.principal}</span></div></td>
                        <td className="py-2.5"><Badge variant="default" className="text-xs">{p.principal_type || 'USER'}</Badge></td>
                        <td className="py-2.5"><div className="flex flex-wrap gap-1">{(p.resources || []).slice(0, 5).map((r: any, i: number) => (<Badge key={i} variant="info" className="text-xs">{r.resource_name}: {r.permission}</Badge>))}{(p.resources || []).length > 5 && (<Badge variant="default" className="text-xs">+{(p.resources || []).length - 5} more</Badge>)}</div></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <TablePagination page={principalPage} totalItems={sortedPrincipals.length} pageSize={principalPageSize} onPageChange={setPrincipalPage} onPageSizeChange={setPrincipalPageSize} />
              </>
            )}
          </CardContent>
        </Card>
      )}

      {/* ═══════ TAB: Permissions ═══════ */}
      {tab === 'permissions' && (
        <div className="space-y-4">
          {/* ── Agent Permissions ────────────────────────────────── */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <CardTitle className="text-base flex items-center gap-2">
                <Bot className="w-4 h-4 text-db-red" /> Agent Permissions
              </CardTitle>
              <button
                onClick={() => syncAgents.mutate(undefined, {
                  onSuccess: () => {
                    queryClient.invalidateQueries({ queryKey: ['agents', 'with-permissions'] })
                    setAgentNotification({ type: 'success', message: 'Sync complete — permissions refreshed' })
                  },
                  onError: (err: any) => {
                    setAgentNotification({ type: 'error', message: `Sync failed: ${err?.message || 'unknown'}` })
                  },
                })}
                disabled={syncAgents.isPending}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md bg-db-red/10 text-db-red hover:bg-db-red/20 disabled:opacity-50 transition-colors"
              >
                <RefreshCw className={`w-3.5 h-3.5 ${syncAgents.isPending ? 'animate-spin' : ''}`} />
                {syncAgents.isPending ? 'Syncing…' : 'Sync'}
              </button>
            </CardHeader>
            <CardContent>
              {/* Toast notification */}
              {agentNotification && (
                <div className={`fixed top-4 right-4 z-[100] flex items-center gap-2 px-4 py-3 rounded-lg text-sm shadow-lg max-w-sm ${
                  agentNotification.type === 'success'
                    ? 'bg-green-50 dark:bg-green-900/80 text-green-700 dark:text-green-300 border border-green-200 dark:border-green-700'
                    : 'bg-red-50 dark:bg-red-900/80 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-700'
                }`}>
                  {agentNotification.type === 'success'
                    ? <CheckCircle className="w-4 h-4 flex-shrink-0" />
                    : <AlertCircle className="w-4 h-4 flex-shrink-0" />}
                  <span className="flex-1">{agentNotification.message}</span>
                  <button onClick={() => setAgentNotification(null)} className="p-0.5 hover:opacity-70"><X className="w-3.5 h-3.5" /></button>
                </div>
              )}
              <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">
                All registered agents and their serving endpoint access control lists. Click an agent to view and manage permissions.
              </p>
              {/* ── Filter bar ─────────────────────────────────────── */}
              {agentsWithPerms && agentsWithPerms.length > 0 && (
                <div className="flex flex-wrap items-center gap-2 mb-3">
                  <div className="relative flex-1 min-w-[200px] max-w-xs">
                    <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" />
                    <input
                      type="text"
                      value={agentPermSearch}
                      onChange={e => { setAgentPermSearch(e.target.value); setAgentPermPage(0) }}
                      placeholder="Search agents…"
                      className="w-full pl-8 pr-3 py-1.5 text-xs border border-gray-200 dark:border-gray-700 rounded-md bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200 focus:outline-none focus:ring-1 focus:ring-db-red/40"
                    />
                    {agentPermSearch && (
                      <button onClick={() => setAgentPermSearch('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"><X className="w-3 h-3" /></button>
                    )}
                  </div>
                  <select
                    value={agentPermTypeFilter}
                    onChange={e => { setAgentPermTypeFilter(e.target.value); setAgentPermPage(0) }}
                    className="text-xs border border-gray-200 dark:border-gray-700 rounded-md px-2 py-1.5 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200"
                  >
                    <option value="all">All types</option>
                    {agentPermTypes.map((t: string) => <option key={t} value={t}>{t.replace('genie_space', 'genie').replace('custom_agent', 'custom').replace('external_agent', 'external')}</option>)}
                  </select>
                  <select
                    value={agentPermWsFilter}
                    onChange={e => { setAgentPermWsFilter(e.target.value); setAgentPermPage(0) }}
                    className="text-xs border border-gray-200 dark:border-gray-700 rounded-md px-2 py-1.5 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200"
                  >
                    <option value="all">All workspaces</option>
                    <option value="online">Online only</option>
                    <option value="offline">Offline only</option>
                    <option value="local">Local only</option>
                  </select>
                  {(agentPermSearch || agentPermTypeFilter !== 'all' || agentPermWsFilter !== 'all') && (
                    <span className="text-[10px] text-gray-400">
                      {filteredAgentsWithPerms.length} of {agentsWithPerms.length} agents
                    </span>
                  )}
                </div>
              )}
              {agentPermsLoading ? (
                <div className="py-8 text-center text-gray-400">Loading agent permissions…</div>
              ) : !agentsWithPerms || agentsWithPerms.length === 0 ? (
                <div className="py-8 text-center text-gray-400">No agents found</div>
              ) : filteredAgentsWithPerms.length === 0 ? (
                <div className="py-8 text-center text-gray-400">No agents match the current filters</div>
              ) : (() => {
                const sortedAgents = sortRows(filteredAgentsWithPerms, agentPermSort.sort, (row: any, key: string) => {
                  if (key === 'name') return (row.name || '').toLowerCase()
                  if (key === 'type') return (row.type || '').toLowerCase()
                  if (key === 'endpoint_name') return (row.endpoint_name || '').toLowerCase()
                  if (key === 'acl_count') return (row.acl || []).length
                  return ''
                })
                const totalPages = Math.max(1, Math.ceil(sortedAgents.length / agentPermPageSize))
                const safePage = Math.min(agentPermPage, totalPages - 1)
                const pagedAgents = sortedAgents.slice(safePage * agentPermPageSize, (safePage + 1) * agentPermPageSize)
                return (
                  <>
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                            <th className="pb-2 w-6" />
                            <SortableHeader label="Agent" sortKey="name" current={agentPermSort.sort} onToggle={agentPermSort.toggle} />
                            <SortableHeader label="Type" sortKey="type" current={agentPermSort.sort} onToggle={agentPermSort.toggle} />
                            <SortableHeader label="Endpoint" sortKey="endpoint_name" current={agentPermSort.sort} onToggle={agentPermSort.toggle} />
                            <SortableHeader label="Grants" sortKey="acl_count" current={agentPermSort.sort} onToggle={agentPermSort.toggle} align="right" />
                          </tr>
                        </thead>
                        <tbody>
                          {pagedAgents.map((agent: any) => {
                            const isExpanded = agentExpanded === agent.agent_id
                            const directAcl = (agent.acl || []).flatMap((a: any) =>
                              (a.permissions || []).map((p: any) => ({
                                ...p,
                                principal: a.principal,
                                principal_type: a.principal_type,
                              })),
                            )
                            const sortedAcl = sortRows(directAcl, agentAclSort.sort, (row: any, key: string) => {
                              if (key === 'principal') return (row.principal || '').toLowerCase()
                              if (key === 'principal_type') return (row.principal_type || '').toLowerCase()
                              if (key === 'permission_level') return (row.permission_level || '').toLowerCase()
                              return ''
                            })
                            const aclTotalPages = Math.max(1, Math.ceil(sortedAcl.length / agentAclPageSize))
                            const aclSafePage = Math.min(agentAclPage, aclTotalPages - 1)
                            const pagedAcl = sortedAcl.slice(aclSafePage * agentAclPageSize, (aclSafePage + 1) * agentAclPageSize)
                            return (
                              <Fragment key={agent.agent_id}>
                                <tr
                                  className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800/40 cursor-pointer"
                                  onClick={() => { setAgentExpanded(isExpanded ? null : agent.agent_id); setAgentShowGrant(null); setAgentAclPage(0) }}
                                >
                                  <td className="py-2.5 w-6">{isExpanded ? <ChevronDown className="w-4 h-4 text-gray-400" /> : <ChevronRight className="w-4 h-4 text-gray-400" />}</td>
                                  <td className="py-2.5">
                                    <div className="flex items-center gap-2">
                                      <Bot className="w-4 h-4 text-blue-500 flex-shrink-0" />
                                      <span className="font-medium dark:text-gray-200">{agent.name}</span>
                                    </div>
                                  </td>
                                  <td className="py-2.5"><Badge variant="default" className="text-[10px]">{(agent.type || '—').replace('genie_space', 'genie').replace('custom_agent', 'custom').replace('external_agent', 'external')}</Badge></td>
                                  <td className="py-2.5 text-gray-500 dark:text-gray-400 text-xs font-mono">
                                    {agent.endpoint_name || '—'}
                                    {agent.resource_type === 'app' && <Badge variant="info" className="text-[10px] ml-1.5">app</Badge>}
                                    {agent.resource_type === 'serving_endpoint' && <Badge variant="default" className="text-[10px] ml-1.5">endpoint</Badge>}
                                    {agent.resource_type === 'genie_space' && <Badge variant="warning" className="text-[10px] ml-1.5">genie</Badge>}
                                    {agent.is_cross_workspace && <Badge variant="error" className="text-[10px] ml-1.5">cross-workspace</Badge>}
                                    {agent.is_cross_workspace && agent.workspace_active === false && <Badge variant="error" className="text-[10px] ml-1.5 opacity-60">ws offline</Badge>}
                                  </td>
                                  <td className="py-2.5 text-right font-medium dark:text-gray-300">{directAcl.length}</td>
                                </tr>
                                {isExpanded && (
                                  <tr>
                                    <td colSpan={5} className="bg-gray-50 dark:bg-gray-800/60 px-4 py-3">
                                      {agent.is_cross_workspace && agent.workspace_active === false ? (
                                        <div className="text-xs text-gray-400">
                                          Workspace {agent.workspace_id} is no longer active — this agent cannot be managed
                                        </div>
                                      ) : !agent.has_endpoint && !(agent.is_cross_workspace && isAccountAdmin && agent.endpoint_name) ? (
                                        <div className="text-xs text-gray-400">
                                          {agent.is_cross_workspace && !isAccountAdmin
                                            ? `Cross-workspace agent (workspace ${agent.workspace_id}) — account admin required to manage permissions`
                                            : agent.is_cross_workspace
                                            ? `Cross-workspace agent (workspace ${agent.workspace_id}) — run Sync to refresh permissions cache`
                                            : `No serving endpoint associated with this agent${agent.endpoint_name ? ` (${agent.endpoint_name})` : ''}`}
                                        </div>
                                      ) : agent.is_cross_workspace && !isAccountAdmin ? (
                                        <div className="text-xs text-gray-400">Cross-workspace agent — account admin access required to manage permissions</div>
                                      ) : (
                                        <>
                                          {directAcl.length > 0 ? (
                                            <>
                                              <table className="w-full text-xs mb-2">
                                                <thead>
                                                  <tr className="text-left text-gray-500 dark:text-gray-400">
                                                    <SortableHeader label="Principal" sortKey="principal" current={agentAclSort.sort} onToggle={agentAclSort.toggle} className="pb-1.5 font-medium text-xs" />
                                                    <SortableHeader label="Type" sortKey="principal_type" current={agentAclSort.sort} onToggle={agentAclSort.toggle} className="pb-1.5 font-medium text-xs" />
                                                    <SortableHeader label="Permission" sortKey="permission_level" current={agentAclSort.sort} onToggle={agentAclSort.toggle} className="pb-1.5 font-medium text-xs" />
                                                    <th className="pb-1.5 font-medium">Source</th>
                                                    <th className="pb-1.5 font-medium w-20 text-right">Actions</th>
                                                  </tr>
                                                </thead>
                                                <tbody>
                                                  {pagedAcl.map((acl: any, j: number) => (
                                                    <tr key={j} className="border-b border-gray-200 dark:border-gray-700/50">
                                                      <td className="py-1.5 font-medium dark:text-gray-200">{acl.principal}</td>
                                                      <td className="py-1.5">
                                                        <Badge variant={acl.principal_type === 'group' ? 'info' : acl.principal_type === 'service_principal' ? 'warning' : 'default'} className="text-[10px]">
                                                          {acl.principal_type}
                                                        </Badge>
                                                      </td>
                                                      <td className="py-1.5">
                                                        <Badge variant={acl.permission_level === 'CAN_MANAGE' ? 'error' : 'success'} className="text-[10px]">
                                                          {acl.permission_level}
                                                        </Badge>
                                                      </td>
                                                      <td className="py-1.5 text-gray-500 dark:text-gray-400">
                                                        {acl.inherited ? `inherited` : 'direct'}
                                                      </td>
                                                      <td className="py-1.5 text-right">
                                                        {!acl.inherited && (
                                                          <button
                                                            onClick={(e) => {
                                                              e.stopPropagation()
                                                              if (!confirm(`Remove ${acl.permission_level} for "${acl.principal}" on ${agent.endpoint_name}?`)) return
                                                              removeAgentPerm.mutate({ endpoint_name: agent.endpoint_name, principal: acl.principal, principal_type: acl.principal_type, resource_type: agent.resource_type || undefined, workspace_id: agent.is_cross_workspace ? agent.workspace_id : undefined }, {
                                                                onSuccess: (data: any) => {
                                                                  if (data?.error) {
                                                                    setAgentNotification({ type: 'error', message: `Revoke failed: ${data.error}` })
                                                                  } else {
                                                                    setAgentNotification({ type: 'success', message: `Removed ${acl.principal} from ${agent.name}` })
                                                                  }
                                                                  setTimeout(() => setAgentNotification(null), 4000)
                                                                },
                                                                onError: () => {
                                                                  setAgentNotification({ type: 'error', message: `Failed to remove permission` })
                                                                  setTimeout(() => setAgentNotification(null), 4000)
                                                                },
                                                              })
                                                            }}
                                                            disabled={removeAgentPerm.isPending}
                                                            className="text-red-500 hover:text-red-700 dark:text-red-400 disabled:opacity-50"
                                                            title="Remove permission"
                                                          >
                                                            <Trash2 className="w-3.5 h-3.5" />
                                                          </button>
                                                        )}
                                                      </td>
                                                    </tr>
                                                  ))}
                                                </tbody>
                                              </table>
                                              {sortedAcl.length > agentAclPageSize && (
                                                <TablePagination page={aclSafePage} totalItems={sortedAcl.length} pageSize={agentAclPageSize} onPageChange={setAgentAclPage} onPageSizeChange={() => {}} />
                                              )}
                                            </>
                                          ) : (
                                            <div className="text-xs text-gray-400 mb-2">No permissions granted on this agent's endpoint</div>
                                          )}
                                          <div className="mt-2">
                                            {agentShowGrant === agent.agent_id ? (
                                              <div className="flex items-end gap-2 bg-white dark:bg-gray-900 border dark:border-gray-700 rounded-lg p-3">
                                                <div className="flex-1 min-w-0">
                                                  <label className="block text-[10px] font-medium text-gray-500 dark:text-gray-400 mb-1">Principal</label>
                                                  <PrincipalAutocomplete
                                                    value={agentGrantPrincipal}
                                                    onChange={setAgentGrantPrincipal}
                                                    onSelect={(p) => {
                                                      setAgentGrantPrincipal(p.email || p.display_name)
                                                      setAgentGrantType(p.type as any)
                                                    }}
                                                    placeholder="Search users, groups, or service principals..."
                                                  />
                                                </div>
                                                <div>
                                                  <label className="block text-[10px] font-medium text-gray-500 dark:text-gray-400 mb-1">Type</label>
                                                  <select
                                                    value={agentGrantType}
                                                    onChange={(e) => setAgentGrantType(e.target.value as any)}
                                                    className="px-2 py-1 text-xs border rounded dark:bg-gray-800 dark:border-gray-600 dark:text-gray-200"
                                                    onClick={(e) => e.stopPropagation()}
                                                  >
                                                    <option value="user">User</option>
                                                    <option value="group">Group</option>
                                                    <option value="service_principal">Service Principal</option>
                                                  </select>
                                                </div>
                                                <div>
                                                  <label className="block text-[10px] font-medium text-gray-500 dark:text-gray-400 mb-1">Permission</label>
                                                  <select
                                                    value={agentGrantLevel}
                                                    onChange={(e) => setAgentGrantLevel(e.target.value)}
                                                    className="px-2 py-1 text-xs border rounded dark:bg-gray-800 dark:border-gray-600 dark:text-gray-200"
                                                    onClick={(e) => e.stopPropagation()}
                                                  >
                                                    {agent.resource_type === 'genie_space' ? (
                                                      <>
                                                        <option value="CAN_RUN">CAN_RUN</option>
                                                        <option value="CAN_MANAGE">CAN_MANAGE</option>
                                                        <option value="CAN_VIEW">CAN_VIEW</option>
                                                        <option value="CAN_EDIT">CAN_EDIT</option>
                                                      </>
                                                    ) : agent.resource_type === 'app' ? (
                                                      <>
                                                        <option value="CAN_USE">CAN_USE</option>
                                                        <option value="CAN_MANAGE">CAN_MANAGE</option>
                                                      </>
                                                    ) : (
                                                      <>
                                                        <option value="CAN_QUERY">CAN_QUERY</option>
                                                        <option value="CAN_MANAGE">CAN_MANAGE</option>
                                                        <option value="CAN_VIEW">CAN_VIEW</option>
                                                      </>
                                                    )}
                                                  </select>
                                                </div>
                                                <button
                                                  onClick={(e) => {
                                                    e.stopPropagation()
                                                    if (!agentGrantPrincipal.trim()) return
                                                    updateAgentPerm.mutate({
                                                      endpoint_name: agent.endpoint_name,
                                                      principal: agentGrantPrincipal.trim(),
                                                      principal_type: agentGrantType,
                                                      permission_level: agentGrantLevel,
                                                      resource_type: agent.resource_type || undefined,
                                                      workspace_id: agent.is_cross_workspace ? agent.workspace_id : undefined,
                                                    }, {
                                                      onSuccess: (data: any) => {
                                                        setAgentGrantPrincipal('')
                                                        setAgentShowGrant(null)
                                                        if (data?.error) {
                                                          setAgentNotification({ type: 'error', message: `Grant failed: ${data.error}` })
                                                        } else {
                                                          setAgentNotification({ type: 'success', message: `Granted ${agentGrantLevel} to ${agentGrantPrincipal.trim()} on ${agent.name}` })
                                                        }
                                                        setTimeout(() => setAgentNotification(null), 4000)
                                                      },
                                                      onError: () => {
                                                        setAgentNotification({ type: 'error', message: `Failed to grant permission` })
                                                        setTimeout(() => setAgentNotification(null), 4000)
                                                      },
                                                    })
                                                  }}
                                                  disabled={updateAgentPerm.isPending || !agentGrantPrincipal.trim()}
                                                  className="px-3 py-1 text-xs font-medium bg-db-red text-white rounded hover:bg-db-red/90 disabled:opacity-50"
                                                >
                                                  {updateAgentPerm.isPending ? '…' : 'Grant'}
                                                </button>
                                                <button
                                                  onClick={(e) => { e.stopPropagation(); setAgentShowGrant(null) }}
                                                  className="p-1 text-gray-400 hover:text-gray-600"
                                                >
                                                  <X className="w-4 h-4" />
                                                </button>
                                              </div>
                                            ) : (
                                              <button
                                                onClick={(e) => { e.stopPropagation(); setAgentShowGrant(agent.agent_id); setAgentGrantPrincipal(''); setAgentGrantLevel(agent.resource_type === 'genie_space' ? 'CAN_RUN' : agent.resource_type === 'app' ? 'CAN_USE' : 'CAN_QUERY') }}
                                                className="inline-flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400 hover:underline"
                                              >
                                                <Plus className="w-3.5 h-3.5" /> Add permission
                                              </button>
                                            )}
                                          </div>
                                        </>
                                      )}
                                    </td>
                                  </tr>
                                )}
                              </Fragment>
                            )
                          })}
                          {sortedAgents.length === 0 && (
                            <tr><td colSpan={5} className="py-8 text-center text-gray-400 dark:text-gray-500">No agents found</td></tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                    <TablePagination page={safePage} totalItems={sortedAgents.length} pageSize={agentPermPageSize} onPageChange={setAgentPermPage} onPageSizeChange={setAgentPermPageSize} />
                  </>
                )
              })()}
            </CardContent>
          </Card>
        </div>
      )}

      {/* ═══════ TAB: Builders & Users ═══════ */}
      {tab === 'builders' && (
        <div className="space-y-6">
          <Card>
            <CardHeader><CardTitle className="text-base">Builders / Creators</CardTitle></CardHeader>
            <CardContent>
              <p className="text-xs text-gray-400 dark:text-gray-500 mb-3">Principals who registered and deployed agents</p>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead><tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                    <SortableHeader label="Principal" sortKey="name" current={builderSort.sort} onToggle={builderSort.toggle} />
                    <SortableHeader label="Agents Built" sortKey="agentCount" current={builderSort.sort} onToggle={builderSort.toggle} align="right" />
                    <th className="pb-2 font-medium">Agents</th>
                  </tr></thead>
                  <tbody>
                    {(() => { const tp = Math.max(1, Math.ceil(sortedBuilders.length / builderPageSize)); const sp = Math.min(builderPage, tp - 1); return sortedBuilders.slice(sp * builderPageSize, (sp + 1) * builderPageSize) })().map((b) => (
                      <tr key={b.name} className="border-b border-gray-100 dark:border-gray-700">
                        <td className="py-2.5"><div className="flex items-center gap-2"><div className="w-7 h-7 bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-400 rounded-full flex items-center justify-center text-xs font-bold">{b.name.charAt(0).toUpperCase()}</div><span className="font-medium">{b.name}</span></div></td>
                        <td className="py-2.5 text-right font-medium">{b.agentCount}</td>
                        <td className="py-2.5"><div className="flex flex-wrap gap-1">{b.agents.map((a: string) => (<Badge key={a} variant="default" className="text-xs">{a}</Badge>))}</div></td>
                      </tr>
                    ))}
                    {builders.length === 0 && (<tr><td colSpan={3} className="py-8 text-center text-gray-400">No builder data available</td></tr>)}
                  </tbody>
                </table>
              </div>
              <TablePagination page={builderPage} totalItems={sortedBuilders.length} pageSize={builderPageSize} onPageChange={setBuilderPage} onPageSizeChange={setBuilderPageSize} />
            </CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle className="text-base">Users / Consumers</CardTitle></CardHeader>
            <CardContent>
              <p className="text-xs text-gray-400 dark:text-gray-500 mb-3">Identities that have interacted with agents (from recent requests)</p>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead><tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                    <SortableHeader label="User ID" sortKey="userId" current={userSort.sort} onToggle={userSort.toggle} />
                    <SortableHeader label="Requests" sortKey="requestCount" current={userSort.sort} onToggle={userSort.toggle} align="right" />
                    <SortableHeader label="Agents Used" sortKey="agents" current={userSort.sort} onToggle={userSort.toggle} />
                    <SortableHeader label="Last Active" sortKey="lastSeen" current={userSort.sort} onToggle={userSort.toggle} />
                  </tr></thead>
                  <tbody>
                    {(() => { const tp = Math.max(1, Math.ceil(sortedUsers.length / userPageSize)); const sp = Math.min(userPage, tp - 1); return sortedUsers.slice(sp * userPageSize, (sp + 1) * userPageSize) })().map((u) => (
                      <tr key={u.userId} className="border-b border-gray-100 dark:border-gray-700">
                        <td className="py-2.5"><div className="flex items-center gap-2"><div className="w-7 h-7 bg-green-100 dark:bg-green-900/40 text-green-700 dark:text-green-400 rounded-full flex items-center justify-center text-xs font-bold">{u.userId.charAt(0).toUpperCase()}</div><span className="font-medium">{u.userId}</span></div></td>
                        <td className="py-2.5 text-right font-medium">{u.requestCount}</td>
                        <td className="py-2.5"><div className="flex flex-wrap gap-1">{u.agents.map((a: string) => (<Badge key={a} variant="default" className="text-xs">{a}</Badge>))}</div></td>
                        <td className="py-2.5 text-xs text-gray-500 dark:text-gray-400">{u.lastSeen ? new Date(u.lastSeen).toLocaleString() : '—'}</td>
                      </tr>
                    ))}
                    {users.length === 0 && (<tr><td colSpan={4} className="py-8 text-center text-gray-400">No user interaction data yet</td></tr>)}
                  </tbody>
                </table>
              </div>
              <TablePagination page={userPage} totalItems={sortedUsers.length} pageSize={userPageSize} onPageChange={setUserPage} onPageSizeChange={setUserPageSize} />
            </CardContent>
          </Card>
        </div>
      )}

      {/* ═══════ TAB: Top Users ═══════ */}
      {tab === 'top-users' && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <Card className="lg:col-span-2">
              <CardHeader><CardTitle className="text-base">Top Users by Request Volume</CardTitle></CardHeader>
              <CardContent>{barData.length > 0 ? <BarChart data={barData} dataKey="requests" nameKey="name" multiColor height={320} /> : <div className="py-12 text-center text-gray-400">No user activity data</div>}</CardContent>
            </Card>
            <Card>
              <CardHeader><CardTitle className="text-base">User Distribution</CardTitle></CardHeader>
              <CardContent>{distPieData.length > 0 ? <PieChart data={distPieData} height={320} /> : <div className="py-12 text-center text-gray-400">No data</div>}</CardContent>
            </Card>
          </div>
          <Card>
            <CardHeader><CardTitle className="text-base">Daily Active Users</CardTitle></CardHeader>
            <CardContent>{dauChartData.length > 0 ? <LineChart data={dauChartData} dataKey="value" name="Active Users" series={{ requests: 'Requests' }} color={DB_CHART.primary} height={260} /> : <div className="py-12 text-center text-gray-400">No trend data</div>}</CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0">
              <CardTitle className="text-base">All Users</CardTitle>
              <div className="relative"><Search className="absolute left-2.5 top-2.5 w-4 h-4 text-gray-400" /><input type="text" placeholder="Search users or agents…" value={uaUserSearch} onChange={(e) => { setUaUserSearch(e.target.value); setUaUserPage(0) }} className="pl-8 pr-3 py-2 border rounded-lg text-sm w-64 dark:bg-gray-800 dark:border-gray-600 dark:text-gray-200 focus:ring-2 focus:ring-db-red/30 focus:border-db-red" /></div>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead><tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                    <SortableHeader label="User" sortKey="user_id" current={uaUserSort.sort} onToggle={uaUserSort.toggle} />
                    <SortableHeader label="Requests" sortKey="request_count" current={uaUserSort.sort} onToggle={uaUserSort.toggle} align="right" />
                    <SortableHeader label="Agents" sortKey="agents_used" current={uaUserSort.sort} onToggle={uaUserSort.toggle} align="right" />
                    <SortableHeader label="Tokens" sortKey="total_tokens" current={uaUserSort.sort} onToggle={uaUserSort.toggle} align="right" />
                    <SortableHeader label="Cost" sortKey="total_cost" current={uaUserSort.sort} onToggle={uaUserSort.toggle} align="right" />
                    <SortableHeader label="Avg Latency" sortKey="avg_latency_ms" current={uaUserSort.sort} onToggle={uaUserSort.toggle} align="right" />
                    <SortableHeader label="Last Active" sortKey="last_active" current={uaUserSort.sort} onToggle={uaUserSort.toggle} />
                    <th className="pb-2 font-medium">Agent List</th>
                  </tr></thead>
                  <tbody>
                    {(() => { const t = sortedUaUsers.length; const tp = Math.max(1, Math.ceil(t / uaUserPageSize)); const sp = Math.min(uaUserPage, tp - 1); return sortedUaUsers.slice(sp * uaUserPageSize, (sp + 1) * uaUserPageSize) })().map((u) => (
                      <tr key={u.user_id} className="border-b border-gray-100 dark:border-gray-700">
                        <td className="py-2.5"><div className="flex items-center gap-2"><div className="w-7 h-7 bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-400 rounded-full flex items-center justify-center text-xs font-bold">{u.user_id.charAt(0).toUpperCase()}</div><span className="font-medium truncate max-w-[180px]" title={u.user_id}>{u.user_id}</span></div></td>
                        <td className="py-2.5 text-right font-medium">{fmtNumber(u.request_count)}</td>
                        <td className="py-2.5 text-right">{u.agents_used}</td>
                        <td className="py-2.5 text-right">{fmtNumber(u.total_tokens)}</td>
                        <td className="py-2.5 text-right">{fmtCost(u.total_cost)}</td>
                        <td className="py-2.5 text-right">{u.avg_latency_ms ? `${Math.round(u.avg_latency_ms)}ms` : '—'}</td>
                        <td className="py-2.5 text-xs text-gray-500 dark:text-gray-400">{u.last_active ? new Date(u.last_active).toLocaleDateString() : '—'}</td>
                        <td className="py-2.5"><div className="flex flex-wrap gap-1">{u.agent_list.slice(0, 3).map((a: string) => (<Badge key={a} variant="default" className="text-xs">{a}</Badge>))}{u.agent_list.length > 3 && (<Badge variant="info" className="text-xs">+{u.agent_list.length - 3}</Badge>)}</div></td>
                      </tr>
                    ))}
                    {sortedUaUsers.length === 0 && (<tr><td colSpan={8} className="py-8 text-center text-gray-400">No user data available</td></tr>)}
                  </tbody>
                </table>
              </div>
              <TablePagination page={uaUserPage} totalItems={sortedUaUsers.length} pageSize={uaUserPageSize} onPageChange={setUaUserPage} onPageSizeChange={setUaUserPageSize} />
            </CardContent>
          </Card>
        </div>
      )}

      {/* ═══════ TAB: Activity Heatmap ═══════ */}
      {tab === 'heatmap' && (
        <div className="space-y-6">
          <Card>
            <CardHeader><CardTitle className="text-base flex items-center gap-2"><Activity className="w-4 h-4" />Request Activity — Day of Week × Hour of Day</CardTitle></CardHeader>
            <CardContent>
              {heatmap.length > 0 ? (
                <div className="overflow-x-auto"><div className="min-w-[700px]">
                  <div className="flex"><div className="w-14 flex-shrink-0" />{HOUR_LABELS.map((h) => (<div key={h} className="flex-1 text-center text-[10px] text-gray-400 dark:text-gray-500 pb-1">{h}</div>))}</div>
                  {heatGrid.map((row, dow) => (
                    <div key={dow} className="flex items-center">
                      <div className="w-14 flex-shrink-0 text-xs text-gray-500 dark:text-gray-400 font-medium pr-2 text-right">{DOW_LABELS[dow]}</div>
                      {row.map((count, hour) => (
                        <div key={hour} className="flex-1 aspect-square m-[1px] rounded-sm flex items-center justify-center text-[9px] font-medium cursor-default transition-transform hover:scale-110" style={{ backgroundColor: heatColor(count, maxVal), color: textColor(count, maxVal) }} title={`${DOW_LABELS[dow]} ${HOUR_LABELS[hour]}: ${count} requests`}>
                          {count > 0 ? fmtNumber(count) : ''}
                        </div>
                      ))}
                    </div>
                  ))}
                  <div className="flex items-center justify-end gap-2 mt-3 text-[10px] text-gray-400 dark:text-gray-500">
                    <span>Less</span>
                    {[0.03, 0.25, 0.5, 0.75, 1.0].map((r, i) => (<div key={i} className="w-4 h-4 rounded-sm" style={{ backgroundColor: r < 0.1 ? 'rgba(255, 54, 33, 0.03)' : r <= 0.25 ? DB_RED_SHADES.max : r <= 0.5 ? DB_RED_SHADES.p75 : r <= 0.75 ? DB_RED_SHADES.p50 : DB_RED_SHADES.p25 }} />))}
                    <span>More</span>
                  </div>
                </div></div>
              ) : (<div className="py-12 text-center text-gray-400">No heatmap data available</div>)}
            </CardContent>
          </Card>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <Card>
              <CardHeader><CardTitle className="text-base">Busiest Hours</CardTitle></CardHeader>
              <CardContent>{(() => { const ht = Array(24).fill(0); heatGrid.forEach((row) => row.forEach((v, h) => { ht[h] += v })); const sorted = ht.map((count, h) => ({ name: HOUR_LABELS[h], count })).sort((a, b) => b.count - a.count).slice(0, 8); return sorted.length > 0 ? <BarChart data={sorted} dataKey="count" nameKey="name" color={DB_CHART.primary} height={240} /> : <div className="py-12 text-center text-gray-400">No data</div> })()}</CardContent>
            </Card>
            <Card>
              <CardHeader><CardTitle className="text-base">Busiest Days</CardTitle></CardHeader>
              <CardContent>{(() => { const dt = heatGrid.map((row) => row.reduce((s, v) => s + v, 0)); const data = DOW_LABELS.map((name, i) => ({ name, count: dt[i] })); return data.some((d) => d.count > 0) ? <BarChart data={data} dataKey="count" nameKey="name" multiColor height={240} /> : <div className="py-12 text-center text-gray-400">No data</div> })()}</CardContent>
            </Card>
          </div>
        </div>
      )}

      {/* ═══════ TAB: RBAC Matrix ═══════ */}
      {tab === 'rbac' && (
        <div className="space-y-6">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0">
              <CardTitle className="text-base flex items-center gap-2"><Grid3X3 className="w-4 h-4" />RBAC Matrix — Principals × Resources</CardTitle>
              <div className="relative"><Search className="absolute left-2.5 top-2.5 w-4 h-4 text-gray-400" /><input type="text" placeholder="Filter principals…" value={rbacSearch} onChange={(e) => { setRbacSearch(e.target.value); setRbacPage(0) }} className="pl-8 pr-3 py-2 border rounded-lg text-sm w-56 dark:bg-gray-800 dark:border-gray-600 dark:text-gray-200 focus:ring-2 focus:ring-db-red/30 focus:border-db-red" /></div>
            </CardHeader>
            <CardContent>
              <p className="text-xs text-gray-400 dark:text-gray-500 mb-4">Each cell shows the permission a principal holds on a resource. Derived from Unity Catalog grants on serving endpoints.</p>
              {sortedRbacPrincipals.length === 0 ? (<div className="py-8 text-center text-gray-400">No principals with resource access</div>) : rbacResources.length === 0 ? (<div className="py-8 text-center text-gray-400">No resources found</div>) : (
                <>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead><tr className="border-b dark:border-gray-700 text-left">
                      <SortableHeader label="Principal" sortKey="principal" current={rbacSort.sort} onToggle={rbacSort.toggle} className="pr-4 sticky left-0 bg-white dark:bg-gray-800 z-10" />
                      <SortableHeader label="Type" sortKey="type" current={rbacSort.sort} onToggle={rbacSort.toggle} className="pr-2" />
                      {rbacResources.map((r) => (<th key={r.key} className="pb-2 px-2 font-medium text-gray-500 dark:text-gray-400 text-center whitespace-nowrap" title={r.key}><span className="inline-block max-w-[120px] truncate">{r.name}</span></th>))}
                    </tr></thead>
                    <tbody>
                      {(() => { const t = sortedRbacPrincipals.length; const tp = Math.max(1, Math.ceil(t / rbacPageSize)); const sp = Math.min(rbacPage, tp - 1); return sortedRbacPrincipals.slice(sp * rbacPageSize, (sp + 1) * rbacPageSize) })().map((p) => {
                        const permMap = rbacMap.get(p.principal)
                        return (
                          <tr key={p.principal} className="border-b border-gray-100 dark:border-gray-700">
                            <td className="py-2 pr-4 sticky left-0 bg-white dark:bg-gray-800 z-10"><div className="flex items-center gap-2"><div className={`w-6 h-6 rounded-full flex items-center justify-center text-[10px] font-bold ${p.principal_type === 'group' ? 'bg-purple-100 dark:bg-purple-900/40 text-purple-700 dark:text-purple-400' : p.principal_type === 'service_principal' ? 'bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-400' : 'bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-400'}`}>{p.principal.charAt(0).toUpperCase()}</div><span className="font-medium truncate max-w-[180px]" title={p.principal}>{p.principal}</span></div></td>
                            <td className="py-2 pr-2"><Badge variant={p.principal_type === 'group' ? 'info' : p.principal_type === 'service_principal' ? 'warning' : 'default'} className="text-[10px]">{p.principal_type || 'user'}</Badge></td>
                            {rbacResources.map((r) => { const perms = permMap?.get(r.key) || []; return (<td key={r.key} className="py-2 px-2 text-center">{perms.length > 0 ? (<div className="flex flex-wrap justify-center gap-0.5">{perms.map((perm, i) => (<Badge key={i} variant={perm === 'CAN_MANAGE' ? 'error' : perm === 'CAN_QUERY' ? 'success' : 'default'} className="text-[10px]">{perm}</Badge>))}</div>) : (<span className="text-gray-300 dark:text-gray-600">—</span>)}</td>) })}
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
                <TablePagination page={rbacPage} totalItems={sortedRbacPrincipals.length} pageSize={rbacPageSize} onPageChange={setRbacPage} onPageSizeChange={setRbacPageSize} />
                </>
              )}
            </CardContent>
          </Card>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <Card><CardContent className="pt-4"><div className="text-sm text-gray-500 dark:text-gray-400">Total Principals</div><div className="text-2xl font-bold dark:text-gray-100 mt-1">{analyticsPrincipals.length}</div><div className="flex gap-2 mt-2"><Badge variant="default" className="text-xs">{analyticsPrincipals.filter((p) => p.principal_type === 'user').length} users</Badge><Badge variant="info" className="text-xs">{analyticsPrincipals.filter((p) => p.principal_type === 'group').length} groups</Badge><Badge variant="warning" className="text-xs">{analyticsPrincipals.filter((p) => p.principal_type === 'service_principal').length} SPs</Badge></div></CardContent></Card>
            <Card><CardContent className="pt-4"><div className="text-sm text-gray-500 dark:text-gray-400">Resources Protected</div><div className="text-2xl font-bold dark:text-gray-100 mt-1">{rbacResources.length}</div></CardContent></Card>
            <Card><CardContent className="pt-4"><div className="text-sm text-gray-500 dark:text-gray-400">Total Grants</div><div className="text-2xl font-bold dark:text-gray-100 mt-1">{analyticsPrincipals.reduce((s, p) => s + p.resources.length, 0)}</div></CardContent></Card>
          </div>
        </div>
      )}

      {/* ═══════ TAB: User-Agent Map ═══════ */}
      {tab === 'user-agent' && (
        <div className="space-y-6">
          <Card>
            <CardHeader><CardTitle className="text-base flex items-center gap-2"><Network className="w-4 h-4" />Agent Usage by User</CardTitle></CardHeader>
            <CardContent>{(() => { const at = new Map<string, number>(); userAgentMatrix.forEach(({ agent_id, request_count }) => { at.set(agent_id, (at.get(agent_id) || 0) + request_count) }); const pd = Array.from(at.entries()).map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value).slice(0, 10); return pd.length > 0 ? <PieChart data={pd} height={300} /> : <div className="py-12 text-center text-gray-400">No user-agent mapping data</div> })()}</CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle className="text-base">User–Agent Request Breakdown</CardTitle></CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead><tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                    <SortableHeader label="User" sortKey="user_id" current={matrixSort.sort} onToggle={matrixSort.toggle} />
                    <SortableHeader label="Total Requests" sortKey="total" current={matrixSort.sort} onToggle={matrixSort.toggle} align="right" />
                    <th className="pb-2 font-medium">Agents & Request Counts</th>
                  </tr></thead>
                  <tbody>
                    {(() => { const t = sortedUserAgentGrouped.length; const tp = Math.max(1, Math.ceil(t / matrixPageSize)); const sp = Math.min(matrixPage, tp - 1); return sortedUserAgentGrouped.slice(sp * matrixPageSize, (sp + 1) * matrixPageSize) })().map((u) => (
                      <tr key={u.user_id} className="border-b border-gray-100 dark:border-gray-700">
                        <td className="py-2.5"><div className="flex items-center gap-2"><div className="w-7 h-7 bg-green-100 dark:bg-green-900/40 text-green-700 dark:text-green-400 rounded-full flex items-center justify-center text-xs font-bold">{u.user_id.charAt(0).toUpperCase()}</div><span className="font-medium truncate max-w-[180px]" title={u.user_id}>{u.user_id}</span></div></td>
                        <td className="py-2.5 text-right font-medium">{fmtNumber(u.total)}</td>
                        <td className="py-2.5"><div className="flex flex-wrap gap-1.5">{u.agents.slice(0, 6).map((a: { agent_id: string; request_count: number }) => (<span key={a.agent_id} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300">{a.agent_id}<span className="font-bold text-db-red">{fmtNumber(a.request_count)}</span></span>))}{u.agents.length > 6 && (<Badge variant="default" className="text-xs">+{u.agents.length - 6}</Badge>)}</div></td>
                      </tr>
                    ))}
                    {userAgentGrouped.length === 0 && (<tr><td colSpan={3} className="py-8 text-center text-gray-400">No mapping data</td></tr>)}
                  </tbody>
                </table>
              </div>
              <TablePagination page={matrixPage} totalItems={sortedUserAgentGrouped.length} pageSize={matrixPageSize} onPageChange={setMatrixPage} onPageSizeChange={setMatrixPageSize} />
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  )
}
