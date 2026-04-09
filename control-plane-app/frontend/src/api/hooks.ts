import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiClient } from './client'

// Config
export function useAppConfig() {
  return useQuery({
    queryKey: ['app-config'],
    queryFn: async () => {
      const { data } = await apiClient.get('/config')
      return data as { databricks_host: string }
    },
    staleTime: Infinity, // Never refetch — it won't change at runtime
  })
}

// Current user (OBO identity)
export interface CurrentUser {
  username: string
  display_name: string
  user_id: string
  is_admin: boolean
  is_account_admin: boolean
  groups: string[]
}

export function useCurrentUser() {
  return useQuery({
    queryKey: ['current-user'],
    queryFn: async () => {
      const { data } = await apiClient.get('/me')
      return data as CurrentUser
    },
    staleTime: Infinity,
    retry: false,
  })
}

// Agents
export function useAgents(activeOnly = false) {
  return useQuery({
    queryKey: ['agents', activeOnly],
    queryFn: async () => {
      const { data } = await apiClient.get('/agents', { params: { active_only: activeOnly } })
      return data
    },
  })
}

export function useAgentsFull(activeOnly = false) {
  return useQuery({
    queryKey: ['agents-full', activeOnly],
    queryFn: async () => {
      const { data } = await apiClient.get('/agents/full', { params: { active_only: activeOnly } })
      return data
    },
  })
}

export function useAgent(agentId: string) {
  return useQuery({
    queryKey: ['agent', agentId],
    queryFn: async () => {
      const { data } = await apiClient.get(`/agents/${agentId}`)
      return data
    },
    enabled: !!agentId,
  })
}

export function useAgentMetrics(agentId: string, hours = 24) {
  return useQuery({
    queryKey: ['agent-metrics', agentId, hours],
    queryFn: async () => {
      const { data } = await apiClient.get(`/agents/${agentId}/metrics`, { params: { hours } })
      return data
    },
    enabled: !!agentId,
  })
}

export function useAgentsWithPermissions() {
  return useQuery({
    queryKey: ['agents', 'with-permissions'],
    queryFn: async () => {
      const { data } = await apiClient.get('/agents/with-permissions')
      return data as Array<{
        agent_id: string
        name: string
        type: string
        endpoint_name: string
        endpoint_status: string
        created_by: string
        is_active: boolean
        has_endpoint: boolean
        resource_type: string
        workspace_id: string
        is_cross_workspace: boolean
        workspace_active: boolean
        acl: Array<{
          principal: string
          principal_type: string
          permissions: Array<{
            permission_level: string
            inherited: boolean
            inherited_from_object?: string
          }>
        }>
      }>
    },
  })
}

export function useUpdateAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async ({ agentId, update }: { agentId: string; update: any }) => {
      const { data } = await apiClient.put(`/agents/${agentId}`, update)
      return data
    },
    onSuccess: (_, { agentId }) => {
      queryClient.invalidateQueries({ queryKey: ['agent', agentId] })
      queryClient.invalidateQueries({ queryKey: ['agents'] })
    },
  })
}

// Requests
export function useRecentRequests(limit = 20) {
  return useQuery({
    queryKey: ['requests', 'recent', limit],
    queryFn: async () => {
      const { data } = await apiClient.get('/requests/recent', { params: { limit } })
      return data
    },
    refetchInterval: 5000, // Refetch every 5 seconds
  })
}

export function useRequests(filters: any) {
  return useQuery({
    queryKey: ['requests', filters],
    queryFn: async () => {
      const { data } = await apiClient.get('/requests', { params: filters })
      return data
    },
  })
}

// KPIs
export function useKPIs() {
  return useQuery({
    queryKey: ['kpis'],
    queryFn: async () => {
      const { data } = await apiClient.get('/kpis/overview')
      return data
    },
    refetchInterval: 10000, // Refetch every 10 seconds
  })
}

// Analytics
export function usePerformanceMetrics(days = 30) {
  return useQuery({
    queryKey: ['analytics', 'performance', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/analytics/performance', { params: { days } })
      return data
    },
  })
}

export function useUsageMetrics(days = 30) {
  return useQuery({
    queryKey: ['analytics', 'usage', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/analytics/usage', { params: { days } })
      return data
    },
  })
}

export function useCostMetrics(days = 30) {
  return useQuery({
    queryKey: ['analytics', 'cost', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/analytics/cost', { params: { days } })
      return data
    },
  })
}

export function useHealthMetrics() {
  return useQuery({
    queryKey: ['analytics', 'health'],
    queryFn: async () => {
      const { data } = await apiClient.get('/analytics/health')
      return data
    },
    refetchInterval: 10000,
  })
}

// Health
export function useHealthStatus() {
  return useQuery({
    queryKey: ['health', 'status'],
    queryFn: async () => {
      const { data } = await apiClient.get('/health/status')
      return data
    },
    refetchInterval: 30000,
  })
}

// ── MLflow / Observability ──────────────────────────────────────

export function useMlflowExperiments(workspaceId?: string | null) {
  return useQuery({
    queryKey: ['mlflow', 'experiments', workspaceId],
    queryFn: async () => {
      const params: Record<string, string> = {}
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/mlflow/experiments', { params })
      return data as any[]
    },
  })
}

export function useMlflowRuns(experimentIds?: string, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['mlflow', 'runs', experimentIds, workspaceId],
    queryFn: async () => {
      const params: Record<string, string> = {}
      if (experimentIds) params.experiment_ids = experimentIds
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/mlflow/runs', { params })
      return data as any[]
    },
  })
}

export function useMlflowTraces(workspaceId?: string | null) {
  return useQuery({
    queryKey: ['mlflow', 'traces', workspaceId],
    queryFn: async () => {
      const params: Record<string, string> = {}
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/mlflow/traces', { params })
      return data as any[]
    },
  })
}

export function useMlflowTraceDetail(requestId: string | null, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['mlflow', 'trace-detail', requestId, workspaceId],
    queryFn: async () => {
      const params: Record<string, string> = {}
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get(`/mlflow/traces/${requestId}`, { params })
      return data
    },
    enabled: !!requestId,
  })
}

export function useMlflowModels(workspaceId?: string | null) {
  return useQuery({
    queryKey: ['mlflow', 'models', workspaceId],
    queryFn: async () => {
      const params: Record<string, string> = {}
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/mlflow/models', { params })
      return data as any[]
    },
  })
}

export function useMlflowModelVersions(name: string) {
  return useQuery({
    queryKey: ['mlflow', 'model-versions', name],
    queryFn: async () => {
      const { data } = await apiClient.get(`/mlflow/models/${name}/versions`)
      return data as any[]
    },
    enabled: !!name,
  })
}

export function useMlflowObservabilityWorkspaces() {
  return useQuery({
    queryKey: ['mlflow', 'workspaces'],
    queryFn: async () => {
      const { data } = await apiClient.get('/mlflow/workspaces')
      return data as Array<{ workspace_id: string; trace_count: number; last_synced: string }>
    },
    staleTime: 60_000,
  })
}

// ── AI Gateway (real Databricks data) ───────────────────────────
// Backend caches for 10 min; match that on the client so React Query
// never refetches while the server-side cache is still fresh.
const GW_STALE = 10 * 60 * 1000 // 10 minutes

/** Composite hook: overview + endpoints in a single request (avoids waterfall). */
export function useGatewayPageData() {
  return useQuery({
    queryKey: ['gateway', 'page-data'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/page-data')
      return {
        overview: {},
        endpoints: [],
        last_refreshed: null,
        ...data,
      } as { overview: any; endpoints: any[]; last_refreshed: string | null }
    },
    staleTime: GW_STALE,
  })
}

export function useGatewayOverview() {
  return useQuery({
    queryKey: ['gateway', 'overview'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/overview')
      return data
    },
    staleTime: GW_STALE,
  })
}

export function useGatewayEndpoints() {
  return useQuery({
    queryKey: ['gateway', 'endpoints'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/endpoints')
      return data
    },
    staleTime: GW_STALE,
  })
}

export function useGatewayPermissions(endpointName?: string) {
  return useQuery({
    queryKey: ['gateway', 'permissions', endpointName],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/permissions', {
        params: endpointName ? { endpoint_name: endpointName } : {},
      })
      return data
    },
    staleTime: GW_STALE,
  })
}

export function useEndpointsWithPermissions() {
  return useQuery({
    queryKey: ['gateway', 'endpoints-permissions'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/endpoints-permissions')
      return data as Array<{
        endpoint_id: string
        endpoint_name: string
        state: string
        task: string
        endpoint_type: string
        served_models: string
        is_foundation_model?: boolean
        uc_model_name?: string | null
        acl: Array<{
          principal: string
          principal_type: string
          permissions: Array<{
            permission_level: string
            inherited: boolean
            inherited_from_object?: string
          }>
        }>
      }>
    },
    staleTime: GW_STALE,
  })
}

export function useUpdateEndpointPermission() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async (body: {
      endpoint_name: string
      principal: string
      principal_type: string
      permission_level: string
      resource_type?: string
      workspace_id?: string
    }) => {
      const { data } = await apiClient.post('/gateway/permissions/update', body)
      return data
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['gateway', 'endpoints-permissions'] })
      qc.invalidateQueries({ queryKey: ['gateway', 'permissions'] })
      qc.invalidateQueries({ queryKey: ['agents', 'with-permissions'] })
    },
  })
}

export function useRemoveEndpointPermission() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async (body: {
      endpoint_name: string
      principal: string
      principal_type: string
      resource_type?: string
      workspace_id?: string
    }) => {
      const { data } = await apiClient.post('/gateway/permissions/remove', body)
      return data
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['gateway', 'endpoints-permissions'] })
      qc.invalidateQueries({ queryKey: ['gateway', 'permissions'] })
      qc.invalidateQueries({ queryKey: ['agents', 'with-permissions'] })
    },
  })
}

export function useGatewayRateLimits(endpointName?: string) {
  return useQuery({
    queryKey: ['gateway', 'rate-limits', endpointName],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/rate-limits', {
        params: endpointName ? { endpoint_name: endpointName } : {},
      })
      return data
    },
    staleTime: GW_STALE,
  })
}

export function useGatewayGuardrails(endpointName?: string) {
  return useQuery({
    queryKey: ['gateway', 'guardrails', endpointName],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/guardrails', {
        params: endpointName ? { endpoint_name: endpointName } : {},
      })
      return data
    },
    staleTime: GW_STALE,
  })
}

export function useGatewayUsageSummary(days = 7) {
  return useQuery({
    queryKey: ['gateway', 'usage-summary', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/usage/summary', { params: { days } })
      return Array.isArray(data) ? data : []
    },
    staleTime: GW_STALE,
  })
}

export function useGatewayUsageTimeseries(days = 7, endpointName?: string) {
  return useQuery({
    queryKey: ['gateway', 'usage-ts', days, endpointName],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/usage/timeseries', {
        params: { days, ...(endpointName ? { endpoint_name: endpointName } : {}) },
      })
      return Array.isArray(data) ? data : []
    },
    staleTime: GW_STALE,
  })
}

export function useGatewayUsageByUser(days = 7) {
  return useQuery({
    queryKey: ['gateway', 'usage-by-user', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/usage/by-user', { params: { days } })
      return Array.isArray(data) ? data : []
    },
    staleTime: GW_STALE,
  })
}

export function useGatewayInferenceLogs(limit = 50, endpointName?: string) {
  return useQuery({
    queryKey: ['gateway', 'inference-logs', limit, endpointName],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/inference-logs', {
        params: { limit, ...(endpointName ? { endpoint_name: endpointName } : {}) },
      })
      return data
    },
    staleTime: GW_STALE,
  })
}

export function useGatewayMetrics(hours = 24) {
  return useQuery({
    queryKey: ['gateway', 'metrics', hours],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/metrics', { params: { hours } })
      return data
    },
    staleTime: GW_STALE,
  })
}

// ── Discovered Agents (cross-workspace) ─────────────────────────

export function useDiscoveredAgents(workspaceId?: string | null) {
  return useQuery({
    queryKey: ['agents', 'discovered', workspaceId],
    queryFn: async () => {
      const params: any = {}
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/agents/discovered', { params })
      return data as any[]
    },
  })
}

export function useAllAgentsMerged(workspaceId?: string | null) {
  return useQuery({
    queryKey: ['agents', 'all-merged', workspaceId],
    queryFn: async () => {
      const params: any = {}
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/agents/all', { params })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useDiscoveryStatus() {
  return useQuery({
    queryKey: ['agents', 'discovery-status'],
    queryFn: async () => {
      const { data } = await apiClient.get('/agents/discovery/status')
      return data as { total_discovered: number; last_synced: string | null; is_refreshing: boolean; obo_enabled: boolean }
    },
  })
}

export function useSyncAgents() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async () => {
      const { data } = await apiClient.post('/agents/sync')
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['agents'] })
    },
  })
}

// ── Tools ────────────────────────────────────────────────────────

export function useToolsOverview() {
  return useQuery({
    queryKey: ['tools', 'overview'],
    queryFn: async () => {
      const { data } = await apiClient.get('/tools/overview')
      return data as {
        total_tools: number
        mcp_servers: number
        uc_functions: number
        managed_count: number
        custom_app_count: number
        is_refreshing: boolean
        last_refreshed: string | null
      }
    },
  })
}

export function useMcpServers() {
  return useQuery({
    queryKey: ['tools', 'mcp-servers'],
    queryFn: async () => {
      const { data } = await apiClient.get('/tools/mcp-servers')
      return data as any[]
    },
  })
}

export function useUcFunctions() {
  return useQuery({
    queryKey: ['tools', 'uc-functions'],
    queryFn: async () => {
      const { data } = await apiClient.get('/tools/functions')
      return data as any[]
    },
  })
}

export function useToolUsage(days = 7) {
  return useQuery({
    queryKey: ['tools', 'usage', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/tools/usage', { params: { days } })
      return data as any[]
    },
  })
}

export function useSyncTools() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async () => {
      const { data } = await apiClient.post('/tools/sync')
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['tools'] })
    },
  })
}

// ── Admin / Access Management ───────────────────────────────────

export function useResourcePermissions(resourceType: string, resourceName: string) {
  return useQuery({
    queryKey: ['admin', 'permissions', resourceType, resourceName],
    queryFn: async () => {
      const { data } = await apiClient.get('/admin/permissions', {
        params: { resource_type: resourceType, resource_name: resourceName },
      })
      return data as Array<{
        principal: string
        principal_type: string
        permission: string
        inherited: boolean
      }>
    },
    enabled: !!resourceType && !!resourceName,
  })
}

export function useGrantPermission() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (body: {
      resource_type: string
      resource_name: string
      principal: string
      privileges: string[]
      principal_type?: string
    }) => {
      const { data } = await apiClient.post('/admin/permissions/grant', body)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin'] })
    },
  })
}

export function useRevokePermission() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (body: {
      resource_type: string
      resource_name: string
      principal: string
      privileges: string[]
    }) => {
      const { data } = await apiClient.post('/admin/permissions/revoke', body)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin'] })
    },
  })
}

export function useAllPrincipals(days = 30) {
  return useQuery({
    queryKey: ['admin', 'principals', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/admin/principals', { params: { days } })
      return data as Array<{
        principal: string
        principal_type: string
        last_active?: string
        request_count?: number
        resources: Array<{
          resource_type: string
          resource_name: string
          permission: string
        }>
      }>
    },
  })
}

export function useSearchPrincipals(query: string, type?: string) {
  return useQuery({
    queryKey: ['admin', 'search-principals', query, type],
    queryFn: async () => {
      const params: any = { q: query }
      if (type) params.type = type
      const { data } = await apiClient.get('/admin/search-principals', { params })
      return data as Array<{
        display_name: string
        id: string
        type: 'user' | 'group' | 'service_principal'
        email?: string
      }>
    },
    enabled: query.length >= 2,
    staleTime: 60_000, // match backend 60s cache
  })
}

// ── Billing / Cost (cached in Lakebase) ─────────────────────────

/** Shape returned by /billing/page-data composite endpoint */
export interface BillingPageData {
  current_workspace_id: string | null
  cache_status: { is_refreshing: boolean; caches: Record<string, { last_refreshed: string | null; rows_loaded: number }> }
  workspaces: Array<{ workspace_id: string; total_dbus: string; endpoint_count: string }>
  summary: any
  trend: any[]
  by_sku: any[]
  tokens: any[]
  daily_tokens: any[]
  products: any[]
  cost_by_user: any[]
  tokens_by_user: any[]
}

/**
 * Fetch ALL billing data the Governance page needs in a SINGLE request.
 * Runs 8 queries on 1 DB connection (~0.8 s) instead of 7+ parallel
 * requests that each open a new connection (~14 s from local dev).
 */
export function useBillingPageData(days = 30, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['billing', 'page-data', days, workspaceId],
    queryFn: async () => {
      const params: any = { days }
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/billing/page-data', { params })
      // Ensure all array fields have defaults to prevent .map() crashes
      return {
        current_workspace_id: null,
        cache_status: { is_refreshing: false, caches: {} },
        workspaces: [],
        summary: {},
        trend: [],
        by_sku: [],
        tokens: [],
        daily_tokens: [],
        products: [],
        cost_by_user: [],
        tokens_by_user: [],
        ...data,
      } as BillingPageData
    },
  })
}

export function useBillingCacheStatus() {
  return useQuery({
    queryKey: ['billing', 'cache-status'],
    queryFn: async () => {
      const { data } = await apiClient.get('/billing/cache/status')
      return data as { is_refreshing: boolean; caches: Record<string, { last_refreshed: string | null; rows_loaded: number }> }
    },
    refetchInterval: 10_000, // poll while refresh running
  })
}

export function useBillingRefresh() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (days: number = 90) => {
      const { data } = await apiClient.post('/billing/cache/refresh', null, { params: { days } })
      return data
    },
    onSuccess: () => {
      // Immediately re-poll cache status so the UI shows "Refreshing…"
      queryClient.invalidateQueries({ queryKey: ['billing', 'cache-status'] })
    },
  })
}

export function useBillingCurrentWorkspace() {
  return useQuery({
    queryKey: ['billing', 'current-workspace'],
    queryFn: async () => {
      const { data } = await apiClient.get('/billing/current-workspace')
      return data as { workspace_id: string | null }
    },
    staleTime: Infinity,
  })
}

export function useBillingWorkspaces(days = 30) {
  return useQuery({
    queryKey: ['billing', 'workspaces', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/billing/workspaces', { params: { days } })
      return data as Array<{ workspace_id: string; total_dbus: string; endpoint_count: string }>
    },
  })
}

export function useBillingServingSummary(days = 30, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['billing', 'serving-summary', days, workspaceId],
    queryFn: async () => {
      const params: any = { days }
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/billing/serving/summary', { params })
      return data
    },
  })
}

export function useBillingServingTrend(days = 30, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['billing', 'serving-trend', days, workspaceId],
    queryFn: async () => {
      const params: any = { days }
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/billing/serving/trend', { params })
      return data as any[]
    },
  })
}

export function useBillingServingBySku(days = 30, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['billing', 'serving-by-sku', days, workspaceId],
    queryFn: async () => {
      const params: any = { days }
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/billing/serving/by-sku', { params })
      return data as any[]
    },
  })
}

export function useBillingServingTokens(days = 30, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['billing', 'serving-tokens', days, workspaceId],
    queryFn: async () => {
      const params: any = { days }
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/billing/serving/tokens', { params })
      return data as any[]
    },
  })
}

export function useBillingDailyTokens(days = 30, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['billing', 'daily-tokens', days, workspaceId],
    queryFn: async () => {
      const params: any = { days }
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/billing/serving/daily-tokens', { params })
      return data as any[]
    },
  })
}

export function useBillingProductCosts(days = 30, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['billing', 'products', days, workspaceId],
    queryFn: async () => {
      const params: any = { days }
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/billing/products', { params })
      return data as any[]
    },
  })
}

// ── Playground ──────────────────────────────────────────────────

export interface PlaygroundSession {
  session_id: string
  endpoint_name: string
  agent_name: string | null
  title: string | null
  created_at: string
  updated_at: string
}

export interface PlaygroundMessage {
  message_id: string
  session_id: string
  role: 'user' | 'assistant' | 'error'
  content: string
  input_tokens: number | null
  output_tokens: number | null
  total_tokens: number | null
  latency_ms: number | null
  model: string | null
  created_at: string
}

export interface PlaygroundSessionDetail extends PlaygroundSession {
  messages: PlaygroundMessage[]
}

export interface ChatResponse {
  session_id: string
  response: string
  input_tokens: number | null
  output_tokens: number | null
  total_tokens: number | null
  latency_ms: number | null
  model: string | null
  error: string | null
}

export interface PlaygroundEndpoint {
  endpoint_name: string
  agent_name: string
  type: string
  kind: string            // "serving_endpoint" | "app"
  status: string
  model_name: string
  task: string
  creator: string
  app_url?: string        // set for Databricks App agents
}

export function usePlaygroundEndpoints() {
  return useQuery({
    queryKey: ['playground', 'endpoints'],
    queryFn: async () => {
      const { data } = await apiClient.get('/playground/endpoints')
      return data as PlaygroundEndpoint[]
    },
    staleTime: 120_000, // matches backend 2-min cache
  })
}

export function usePlaygroundSessions() {
  return useQuery({
    queryKey: ['playground', 'sessions'],
    queryFn: async () => {
      const { data } = await apiClient.get('/playground/sessions')
      return data as PlaygroundSession[]
    },
  })
}

export function usePlaygroundMessages(sessionId: string | null) {
  return useQuery({
    queryKey: ['playground', 'session', sessionId],
    queryFn: async () => {
      const { data } = await apiClient.get(`/playground/sessions/${sessionId}`)
      return data as PlaygroundSessionDetail
    },
    enabled: !!sessionId,
  })
}

export function useSendPlaygroundMessage() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (body: {
      endpoint_name: string
      agent_name?: string | null
      session_id?: string | null
      message: string
      max_tokens?: number
      temperature?: number
      app_url?: string | null
    }) => {
      const { data } = await apiClient.post('/playground/chat', body)
      return data as ChatResponse
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['playground', 'sessions'] })
      queryClient.invalidateQueries({ queryKey: ['playground', 'session', data.session_id] })
    },
  })
}

export function useDeletePlaygroundSession() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (sessionId: string) => {
      const { data } = await apiClient.delete(`/playground/sessions/${sessionId}`)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['playground'] })
    },
  })
}

// ── User Analytics & RBAC Dashboard ─────────────────────────────

export interface UserAnalyticsPageData {
  kpis: {
    active_users_24h: number
    active_users_7d: number
    active_users_period: number
    total_requests: number
    unique_agents: number
    total_tokens: number
    total_cost: number
  }
  top_users: Array<{
    user_id: string
    request_count: number
    agents_used: number
    total_tokens: number
    total_cost: number
    avg_latency_ms: number
    last_active: string
    agent_list: string[]
  }>
  heatmap: Array<{ dow: number; hour: number; count: number }>
  daily_active_users: Array<{ day: string; active_users: number; requests: number }>
  user_agent_matrix: Array<{ user_id: string; agent_id: string; request_count: number }>
  distribution: Array<{ bucket: string; user_count: number }>
  principals: Array<{
    principal: string
    principal_type: string
    resources: Array<{ resource_type: string; resource_name: string; permission: string }>
  }>
}

export function useUserAnalyticsPageData(days = 30) {
  return useQuery({
    queryKey: ['user-analytics', 'page-data', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/user-analytics/page-data', { params: { days } })
      return data as UserAnalyticsPageData
    },
  })
}

// ── Workspaces (Multi-Workspace Federation) ─────────────────────

export interface WorkspaceKpis {
  total_workspaces: number
  total_serving_cost: number
  total_all_product_cost: number
  total_agents: number
  total_requests: number
  total_endpoints: number
  cost_change_pct: number
}

export interface WorkspaceSummary {
  workspace_id: string
  serving_cost: number
  serving_dbus: number
  endpoint_count: number
  total_requests: number
  total_input_tokens: number
  total_output_tokens: number
  agent_count: number
  agent_type_count: number
  total_all_product_cost: number
  prev_serving_cost: number
}

export interface WorkspacePageData {
  current_workspace_id: string | null
  kpis: WorkspaceKpis
  workspace_summaries: WorkspaceSummary[]
  cost_trend: Array<{ day: string; workspace_id: string; cost: number }>
  agent_type_breakdown: Array<{ workspace_id: string; agent_type: string; count: number }>
  top_endpoints: Array<{ workspace_id: string; endpoint_name: string; total_cost: number; total_dbus: number }>
  products_by_workspace: Array<{ workspace_id: string; billing_origin_product: string; total_cost: number }>
  all_agents: Array<{
    workspace_id: string
    name: string
    type: string
    endpoint_name: string
    endpoint_status: string
    model_name: string
    creator: string
    source: string
  }>
}

export function useWorkspacesPageData(days = 30) {
  return useQuery({
    queryKey: ['workspaces', 'page-data', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/workspaces/page-data', { params: { days } })
      return data as WorkspacePageData
    },
  })
}

// Topology
// refreshKey increments on every manual Refresh — a new key always triggers a fresh fetch.
// force=true is sent to the backend whenever refreshKey > 0 (any user-triggered rebuild).
export function useTopology(refreshKey = 0) {
  return useQuery({
    queryKey: ['topology', refreshKey],
    queryFn: async () => {
      const { data } = await apiClient.get('/topology', { params: { force: refreshKey > 0 } })
      return data
    },
    staleTime: 5 * 60 * 1000,
  })
}

// ── Real-time Operations ─────────────────────────────────────────

export interface OperationsAgent {
  agent_id: string
  name: string
  endpoint_name: string
  state: string
  health: 'healthy' | 'degraded' | 'down' | 'pending' | 'unknown'
  agent_type: string
  model_name: string
  creator: string
  description: string
  source: string
  has_pending_config: boolean
  pending_reason: string
  scale_to_zero: boolean | null
  workload_size: string
  served_entity_count: number
  tags: Record<string, string>
  created_at: number | string | null
  updated_at: number | string | null
  request_count: number | null
  error_count: number | null
  avg_latency_ms: number | null
  p95_latency_ms: number | null
  error_rate: number | null
}

export interface OperationsStatus {
  agents: OperationsAgent[]
  summary: {
    total: number
    healthy: number
    degraded: number
    down: number
    pending: number
  }
  last_refreshed: string
}

export interface OperationsUsage {
  usage: Array<{
    endpoint_name: string
    request_count: number
    error_count: number
    avg_latency_ms: number
    p95_latency_ms: number
    p99_latency_ms: number
    total_tokens: number
  }>
  hours: number
  last_refreshed: string
}

export function useOperationsStatus() {
  return useQuery({
    queryKey: ['operations', 'status'],
    queryFn: async () => {
      const { data } = await apiClient.get('/operations/status')
      return data as OperationsStatus
    },
    refetchInterval: 30_000, // match 30s backend cache TTL
  })
}

export function useOperationsEndpointDetail(endpointName: string | null) {
  return useQuery({
    queryKey: ['operations', 'endpoint', endpointName],
    queryFn: async () => {
      const { data } = await apiClient.get(`/operations/endpoints/${endpointName}`)
      return data
    },
    enabled: !!endpointName,
  })
}

export function useOperationsUsage(hours = 1) {
  return useQuery({
    queryKey: ['operations', 'usage', hours],
    queryFn: async () => {
      const { data } = await apiClient.get('/operations/usage', { params: { hours } })
      return data as OperationsUsage
    },
    refetchInterval: 30_000,
  })
}

export function useRefreshOperations() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async () => {
      const { data } = await apiClient.post('/operations/cache/refresh')
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['operations'] })
    },
  })
}

// Gateway cache refresh
export function useRefreshGateway() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async () => {
      const { data } = await apiClient.post('/gateway/cache/refresh')
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['gateway'] })
    },
  })
}

// ── Vector Search ──────────────────────────────────────────────
export function useVectorSearchOverview() {
  return useQuery({
    queryKey: ['vector-search', 'overview'],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/overview')
      return data as { total_endpoints: number; online_endpoints: number; offline_endpoints: number; total_indexes: number; by_status: Record<string, number>; by_index_type: Record<string, number> }
    },
  })
}

export function useVectorSearchEndpoints() {
  return useQuery({
    queryKey: ['vector-search', 'endpoints'],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/endpoints')
      return Array.isArray(data) ? data : []
    },
  })
}

export function useVectorSearchIndexes(endpointName?: string) {
  return useQuery({
    queryKey: ['vector-search', 'indexes', endpointName],
    queryFn: async () => {
      const params: any = {}
      if (endpointName) params.endpoint_name = endpointName
      const { data } = await apiClient.get('/vector-search/indexes', { params })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useVectorSearchCostSummary(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'cost-summary', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/cost/summary', { params: { days } })
      return data as { total_dbus: number; total_cost_usd: number; endpoint_count: number; workspace_count: number; days: number }
    },
  })
}

export function useVectorSearchCostTrend(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'cost-trend', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/cost/trend', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useVectorSearchCostByEndpoint(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'cost-by-endpoint', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/cost/by-endpoint', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useVectorSearchCostByWorkload(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'cost-by-workload', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/cost/by-workload', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}
