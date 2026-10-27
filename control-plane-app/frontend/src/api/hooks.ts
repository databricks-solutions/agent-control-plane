import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiClient } from './client'

// Config
export interface AppConfig {
  databricks_host: string
  features?: { genie_enabled?: boolean }
}

export function useAppConfig() {
  return useQuery({
    queryKey: ['app-config'],
    queryFn: async () => {
      const { data } = await apiClient.get('/config')
      return data as AppConfig
    },
    staleTime: 300_000, // 5 min — host won't change often
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
  // Mirrors the server-side access scope (backend/utils/access_scope.py):
  // true for account admins and workspace admins, false for everyone else.
  // The API enforces this on every data endpoint regardless — this flag is
  // only used to render a "no access" state instead of empty charts/tables.
  has_workspace_access: boolean
  allowed_workspace_count: number | null
  // True when the caller may see live APIs that only exist on the app's
  // home (deploy) workspace. Account admin is always true; workspace admin
  // only when that home workspace is one they administer.
  sees_deploy_workspace: boolean
  current_workspace_id: string | null
}

/** True when the server serves this user account-wide data (allowed scope = all).
 *  Account-wide views (system.ai_gateway.*, budgets, serving usage) are suppressed
 *  server-side for everyone else, so an empty view for a scoped user means
 *  "restricted", not "no traffic". Mirrors backend/utils/access_scope.py. */
export function useSeesAccountWide(): boolean | undefined {
  const { data } = useCurrentUser()
  return data ? data.allowed_workspace_count === null : undefined
}

export function useCurrentUser() {
  return useQuery({
    queryKey: ['current-user'],
    queryFn: async () => {
      const { data } = await apiClient.get('/me')
      return data as CurrentUser
    },
    staleTime: 300_000, // 5 min — user identity rarely changes
    retry: false,
  })
}

// App settings (admin-toggleable access mode)
export type AccessMode = 'open' | 'strict'

export interface AppSettings {
  access_mode: AccessMode
  valid_access_modes: AccessMode[]
  can_edit: boolean
}

export function useAppSettings() {
  return useQuery({
    queryKey: ['settings'],
    queryFn: async () => {
      const { data } = await apiClient.get('/settings')
      return data as AppSettings
    },
    staleTime: 60_000,
  })
}

export function useSetAccessMode() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (accessMode: AccessMode) => {
      const { data } = await apiClient.put('/settings', { access_mode: accessMode })
      return data as AppSettings
    },
    onSuccess: () => {
      // Access mode changes what every scoped query returns, so refresh
      // the settings, the caller's identity/scope, and all cached data.
      queryClient.invalidateQueries({ queryKey: ['settings'] })
      queryClient.invalidateQueries({ queryKey: ['current-user'] })
      queryClient.invalidateQueries()
    },
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

// KPIs
// Analytics
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
      // Default to account-wide cache when no specific workspace is selected,
      // otherwise the local-MLflow REST path returns 0 rows even though the
      // Lakebase cache is populated by the discovery workflow. A workspaceId of
      // one id or a comma-joined multi-selection is sent as workspace_ids.
      const params: Record<string, string> =
        workspaceId && workspaceId !== 'all' ? { workspace_ids: workspaceId } : { workspace_id: 'all' }
      const { data } = await apiClient.get('/mlflow/experiments', { params })
      return data as any[]
    },
  })
}

export function useMlflowRuns(experimentIds?: string, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['mlflow', 'runs', experimentIds, workspaceId],
    queryFn: async () => {
      const params: Record<string, string> =
        workspaceId && workspaceId !== 'all' ? { workspace_ids: workspaceId } : { workspace_id: 'all' }
      if (experimentIds) params.experiment_ids = experimentIds
      const { data } = await apiClient.get('/mlflow/runs', { params })
      return data as any[]
    },
  })
}

export function useMlflowTraces(workspaceId?: string | null, windowDays?: number | null) {
  return useQuery({
    queryKey: ['mlflow', 'traces', workspaceId, windowDays],
    queryFn: async () => {
      const params: Record<string, string> = {}
      // One id or a comma-joined multi-selection → workspace_ids (backend IN-filter).
      if (workspaceId && workspaceId !== 'all') params.workspace_ids = workspaceId
      if (windowDays) params.window_days = String(windowDays)
      const { data } = await apiClient.get('/mlflow/traces', { params })
      return data as any[]
    },
  })
}

export interface AgentToolUsage {
  totals: Partial<{ experiments: number; tools: number; retrievers: number; calls: number }>
  rows: Array<{
    experiment_id: string
    experiment_name: string
    asset: string
    span_type: string
    call_count: number
    trace_count: number
    last_seen: string
  }>
}

/** TOOL/RETRIEVER span usage per experiment — what UC functions / vector indexes agents touch. */
export function useAgentToolUsage() {
  return useQuery({
    queryKey: ['mlflow', 'agent-tool-usage'],
    queryFn: async () => {
      const { data } = await apiClient.get('/mlflow/agent-tool-usage')
      return data as AgentToolUsage
    },
  })
}

export interface AgentEvalScores {
  totals: Partial<{ experiments: number; scorers: number; assessments: number; pass_rate: number | null }>
  rows: Array<{
    experiment_id: string
    experiment_name: string
    scorer_name: string
    source_type: string
    assessment_count: number
    pass_count: number
    fail_count: number
    pass_rate: number | null
    last_seen: string
  }>
}

/** MLflow-3 eval / assessment scores per experiment — LLM-judge & human quality signals. */
export function useAgentEvalScores() {
  return useQuery({
    queryKey: ['mlflow', 'agent-eval-scores'],
    queryFn: async () => {
      const { data } = await apiClient.get('/mlflow/agent-eval-scores')
      return data as AgentEvalScores
    },
  })
}

export interface AiAudit {
  totals: Partial<{ services: number; actions: number; events: number; errors: number }>
  summary: Array<{
    service_name: string
    action_name: string
    event_count: number
    actor_count: number
    error_count: number
    workspace_count: number
    last_seen: string
  }>
  recent: Array<{
    event_time: string
    service_name: string
    action_name: string
    actor: string | null
    status_code: number | null
    workspace_id: string | null
  }>
}

/** Governed AI audit trail from system.access.audit (AI services only). */
export function useAiAudit() {
  return useQuery({
    queryKey: ['mlflow', 'ai-audit'],
    queryFn: async () => {
      const { data } = await apiClient.get('/mlflow/ai-audit')
      return data as AiAudit
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

// ── Gateway inference logs (Tier 2a) ──────────────────────────────

export function useMlflowModels(workspaceId?: string | null) {
  return useQuery({
    queryKey: ['mlflow', 'models', workspaceId],
    queryFn: async () => {
      const params: Record<string, string> = { workspace_id: workspaceId || 'all' }
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

/** Map of workspace_id → host URL (e.g., 'https://my-ws.cloud.databricks.com').
 *  Used to build "Open in MLflow" links that route to each record's owning
 *  workspace rather than the deploy workspace. */
export function useWorkspaceHosts() {
  return useQuery({
    queryKey: ['mlflow', 'workspace-hosts'],
    queryFn: async () => {
      const { data } = await apiClient.get('/mlflow/workspace-hosts')
      return (data || {}) as Record<string, string>
    },
    staleTime: 5 * 60_000,
  })
}

export interface WorkspaceMeta { host: string; name: string; deployment_name: string }

/** workspace_id → {host, name, deployment_name} from the Lakebase-cached registry.
 * Used to label the workspace picker with human-readable names instead of numeric
 * ids. Small and static-ish, so cache generously. */
export function useWorkspaceDirectory() {
  return useQuery({
    queryKey: ['mlflow', 'workspace-directory'],
    queryFn: async () => {
      const { data } = await apiClient.get('/mlflow/workspace-directory')
      return (data || {}) as Record<string, WorkspaceMeta>
    },
    staleTime: 10 * 60_000,
  })
}

// ── Unity Gateway (real Databricks data) ───────────────────────────
// Backend caches for 10 min; match that on the client so React Query
// never refetches while the server-side cache is still fresh.
const GW_STALE = 10 * 60 * 1000 // 10 minutes

/** Composite hook: overview + endpoints in a single request (avoids waterfall). */
export function useGatewayPageData(options?: { enabled?: boolean }) {
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
    enabled: options?.enabled ?? true,
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

export interface UnityUsage {
  as_of: string | null
  totals: {
    request_count?: number
    input_tokens?: number
    output_tokens?: number
    cached_tokens?: number
    cache_read_pct?: number
    endpoints?: number
  }
  endpoints: Array<{
    endpoint_name: string
    request_count: number
    input_tokens: number
    output_tokens: number
    cache_read_tokens: number
    cache_creation_tokens: number
    p50_latency_ms: number
    p90_latency_ms: number
    p95_latency_ms: number
    p99_latency_ms: number
    p95_ttfb_ms: number
    error_count: number
    unique_users: number
  }>
  breakdowns?: {
    requester_type?: UagBreakdownRow[]
    destination_model?: UagBreakdownRow[]
    api_type?: UagBreakdownRow[]
    source?: UagBreakdownRow[]
    service_type?: UagBreakdownRow[]
    route_action?: UagBreakdownRow[]
    status_code?: UagBreakdownRow[]
    workspace_id?: UagBreakdownRow[]
  }
}

export interface UnityTimeseries {
  series: Array<{
    usage_date: string
    request_count: number
    input_tokens: number
    output_tokens: number
  }>
}

export interface UagMcpTools {
  as_of: string | null
  totals: Partial<{
    request_count: number
    error_count: number
    services: number
    tools: number
  }>
  tools: Array<{
    service_name: string
    tool_name: string
    server_type: string
    request_count: number
    error_count: number
    unique_users: number
  }>
}

export interface UagBreakdownRow {
  key: string
  request_count: number
  input_tokens: number
  output_tokens: number
  cached_tokens: number
}

/** Unity Gateway usage — gateway-routed traffic only, ~20-min fresh. */
export function useUnityUsage() {
  return useQuery({
    queryKey: ['gateway', 'unity-usage'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/unity-usage')
      return data as UnityUsage
    },
    staleTime: GW_STALE,
  })
}

/** Read-only budget config inventory from the account Budgets API. */
export interface UagBudgetStatus {
  as_of: string | null
  totals: {
    budget_count?: number
    enforcing_count?: number
    alerting_count?: number
    ai_budget_count?: number
    over_cap_count?: number
    near_cap_count?: number
  }
  budgets: Array<{
    budget_id: string
    display_name: string
    enforce: boolean
    alerting: boolean
    min_threshold_usd: number
    max_threshold_usd: number
    time_period: string
    filter_summary: string
    is_ai: boolean
    spent_usd: number | null
    pct_used: number | null
  }>
}

export function useUagBudgetStatus() {
  return useQuery({
    queryKey: ['gateway', 'uag-budget-status'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/uag-budget-status')
      return data as UagBudgetStatus
    },
    staleTime: GW_STALE,
  })
}

/** Budget create/update payload — passed through to the account Budgets API. */
export interface BudgetWritePayload {
  display_name: string
  alert_configurations: Array<Record<string, unknown>>
  filter: Record<string, unknown>
}

/** Fetch one budget's full (unflattened) object for the edit form (account admin). */
export async function fetchBudgetRaw(budgetId: string): Promise<any> {
  const { data } = await apiClient.get(`/gateway/uag-budgets/${budgetId}`)
  return data
}

export function useCreateBudget() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async (body: BudgetWritePayload) => {
      const { data } = await apiClient.post('/gateway/uag-budgets', body)
      return data
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ['gateway', 'uag-budget-status'] }),
  })
}

export function useUpdateBudget() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async ({ budgetId, body }: { budgetId: string; body: BudgetWritePayload }) => {
      const { data } = await apiClient.put(`/gateway/uag-budgets/${budgetId}`, body)
      return data
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ['gateway', 'uag-budget-status'] }),
  })
}

export function useDeleteBudget() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async (budgetId: string) => {
      const { data } = await apiClient.delete(`/gateway/uag-budgets/${budgetId}`)
      return data
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ['gateway', 'uag-budget-status'] }),
  })
}

/** Account-wide served-entity inventory (read-only) from system.serving.served_entities. */
export interface EndpointInventory {
  as_of: string | null
  totals: {
    served_entity_count?: number
    endpoint_count?: number
    workspace_count?: number
    foundation_count?: number
    custom_count?: number
    external_count?: number
  }
  endpoints: Array<{
    endpoint_name: string
    workspace_id: string
    entity_type: string
    entity_name: string
    entity_version: string
    provider: string
    task: string
    created_by: string
    change_time: string | null
  }>
}

export function useEndpointInventory() {
  return useQuery({
    queryKey: ['gateway', 'endpoint-inventory'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/endpoint-inventory')
      return data as EndpointInventory
    },
    staleTime: GW_STALE,
  })
}

/** v3 Unity Gateway UC model services + their UC grants (OBO, UC-enforced). */
export interface ModelService {
  full_name: string
  owner: string
  supported_api_types: string[]
  create_time: string | null
}
export interface ModelServiceGrantRow {
  principal: string
  privileges: string[]
}

export function useModelServices() {
  return useQuery({
    queryKey: ['gateway', 'model-services'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/model-services')
      return data as { services: ModelService[] }
    },
    staleTime: GW_STALE,
  })
}

export type GatewayServiceType = 'MODEL_SERVICE' | 'MCP_SERVICE' | 'MODEL_PROVIDER_SERVICE' | 'AGENT_SERVICE'

export interface GatewayRateLimit {
  key?: string            // RATE_LIMIT_KEY_SERVICE | RATE_LIMIT_KEY_USER_DEFAULT | …
  principal?: string
  renewal_period?: string // RATE_LIMIT_RENEWAL_PERIOD_MINUTE
  requests?: string | number
  tokens?: string | number
}

/** One Unity Gateway UC service, merged with its 7-day traffic. Traffic fields are
 *  null when there's no usage (or the caller can't see account-wide traffic). */
export interface GatewayService {
  full_name: string
  service_type: GatewayServiceType | null
  owner: string
  comment: string
  supported_api_types: string[]
  provider_type: string
  source_connection: string
  destinations: string[]
  rate_limits: GatewayRateLimit[]
  inference_table: string
  inference_table_enabled: boolean | null
  update_time: string | null
  in_inventory: boolean
  request_count: number | null
  error_count: number | null
  throttled_count: number | null
  unique_users: number | null
  input_tokens: number | null
  output_tokens: number | null
  p50_latency_ms: number | null
  p95_latency_ms: number | null
  error_rate: number | null
  last_seen: string | null
}

export interface GatewayServicesResponse {
  as_of: string | null
  traffic_visible: boolean
  totals: {
    services?: number
    by_type?: Record<string, number>
    with_rate_limits?: number
    with_inference_table?: number
    with_traffic?: number
    throttled?: number
  }
  services: GatewayService[]
}

/** Unity Gateway services hub (all UC service kinds + config + traffic). */
export function useGatewayServices() {
  return useQuery({
    queryKey: ['gateway', 'services'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/services')
      return data as GatewayServicesResponse
    },
    staleTime: GW_STALE,
  })
}

/** UC grants on any Unity Gateway service (model / MCP / provider / agent). */
export function useServiceGrants(fullName: string | null, serviceType: GatewayServiceType | null) {
  return useQuery({
    queryKey: ['gateway', 'service-grants', serviceType, fullName],
    enabled: !!fullName,
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/services/grants', {
        params: { full_name: fullName, service_type: serviceType || 'MODEL_SERVICE' },
      })
      return data as { grants: ModelServiceGrantRow[]; error?: string }
    },
  })
}

export function useModelServiceGrants(fullName: string | null) {
  return useQuery({
    queryKey: ['gateway', 'model-service-grants', fullName],
    enabled: !!fullName,
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/model-services/grants', { params: { full_name: fullName } })
      return data as { grants: ModelServiceGrantRow[]; error?: string }
    },
  })
}

export function useSetModelServiceGrant() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async (body: { full_name: string; principal: string; add?: string[]; remove?: string[] }) => {
      const { data } = await apiClient.post('/gateway/model-services/grant', body)
      return data as { ok: boolean; error?: string }
    },
    onSuccess: (_d, vars) => {
      qc.invalidateQueries({ queryKey: ['gateway', 'model-service-grants', vars.full_name] })
    },
  })
}

/** Per-tool MCP activity from system.ai_gateway.usage (service_type = MCP_SERVICE). */
export function useUagMcpTools() {
  return useQuery({
    queryKey: ['gateway', 'uag-mcp-tools'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/uag-mcp-tools')
      return data as UagMcpTools
    },
    staleTime: GW_STALE,
  })
}

export interface GuardrailCoverage {
  as_of: string | null
  totals: Partial<{ guarded_endpoints: number; checked_requests: number }>
  endpoints: Array<{
    endpoint_name: string
    checked_requests: number
    unique_users: number
    judge_models: string
  }>
}

/** Guardrail coverage/activity per endpoint from Unity Gateway v2 (not outcomes). */
export function useGuardrailCoverage() {
  return useQuery({
    queryKey: ['gateway', 'guardrail-coverage'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/guardrail-coverage')
      return data as GuardrailCoverage
    },
    staleTime: GW_STALE,
  })
}

export interface Throttling {
  as_of: string | null
  totals: Partial<{ endpoints: number; total_requests: number; throttled_count: number; server_error_count: number }>
  endpoints: Array<{
    endpoint_name: string
    total_requests: number
    throttled_count: number
    server_error_count: number
    throttle_rate: number | null
  }>
}

/** Per-endpoint throttling (HTTP 429) + server errors (5xx) from Unity Gateway usage. */
export function useThrottling() {
  return useQuery({
    queryKey: ['gateway', 'throttling'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/throttling')
      return data as Throttling
    },
    staleTime: GW_STALE,
  })
}

export interface FallbackRouting {
  as_of: string | null
  totals: Partial<{ endpoints: number; fallback_requests: number; fallback_recovered: number }>
  endpoints: Array<{
    endpoint_name: string
    total_requests: number
    fallback_requests: number
    fallback_recovered: number
    fallback_destinations: string
    recovery_rate: number | null
  }>
}

/** Per-endpoint smart-routing fallback (backup-model routing) from Unity Gateway usage. */
export function useFallbackRouting() {
  return useQuery({
    queryKey: ['gateway', 'fallback-routing'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/fallback-routing')
      return data as FallbackRouting
    },
    staleTime: GW_STALE,
  })
}

export interface UagCodingAgents {
  as_of: string | null
  agents: Array<{
    coding_agent: string
    request_count: number
    unique_users: number
    active_days: number
    total_tokens: number
  }>
}

/** Coding-agent activity (Claude Code / Codex / Cursor / Gemini CLI) from UAG usage. */
export function useUagCodingAgents() {
  return useQuery({
    queryKey: ['gateway', 'uag-coding-agents'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/uag-coding-agents')
      return data as UagCodingAgents
    },
    staleTime: GW_STALE,
  })
}

/** Daily Unity Gateway usage series (requests + tokens) for trend charts. */
export function useUnityTimeseries() {
  return useQuery({
    queryKey: ['gateway', 'unity-timeseries'],
    queryFn: async () => {
      const { data } = await apiClient.get('/gateway/unity-timeseries')
      return data as UnityTimeseries
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

export function useToolsOverview(options?: { enabled?: boolean }) {
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
    enabled: options?.enabled ?? true,
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

export interface McpActivity {
  as_of: string | null
  totals: Partial<{ services: number; managed: number; request_count: number }>
  servers: Array<{
    service_name: string
    managed: boolean
    request_count: number
    error_count: number
    tool_count: number
    tools: Array<{ tool_name: string; request_count: number; error_count: number; unique_users: number }>
  }>
}

/** Server-grouped MCP tool activity from Unity Gateway v2. */
export function useMcpActivity() {
  return useQuery({
    queryKey: ['tools', 'mcp-activity'],
    queryFn: async () => {
      const { data } = await apiClient.get('/tools/mcp-activity')
      return data as McpActivity
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
  /** "actual" = Unity Gateway per-user attribution; "estimate" = token-share split */
  cost_by_user_source: 'actual' | 'estimate'
  tokens_by_user: any[]
  /** MODEL_SERVING $ attributed by custom_tag (window aggregate, workspace-agnostic) */
  cost_by_tag: CostByTagRow[]
  /** Actual $ for external LLMs (OpenAI, Foundry, …) routed through the Unity Gateway */
  external_model_spend: ExternalModelSpendRow[]
  /** Gateway-attributed serving $ per Unity Gateway service (billing endpoint_id → service) */
  cost_by_service: ServiceCostRow[]
  /** Gateway-attributed serving $ per user × service (top 100) */
  cost_by_user_service: UserServiceCostRow[]
}

export interface ServiceCostRow {
  service_name: string
  service_type: string | null
  total_dbus: number | string
  total_cost_usd: number | string
  users: number
}

export interface UserServiceCostRow {
  user_identity: string
  service_name: string
  total_cost_usd: number | string
}

/** One row of billing_cost_by_tag: a (tag_key, tag_value) pair and its total cost. */
export interface CostByTagRow {
  tag_key: string
  tag_value: string
  total_cost_usd: number
}

/** One row of billing_external_model_spend: external LLM cost per provider·model·endpoint. */
export interface ExternalModelSpendRow {
  provider: string
  model: string
  endpoint_name: string
  call_count: number
  total_cost_usd: number
  last_seen: string
}

/**
 * Fetch ALL billing data the Governance page needs in a SINGLE request.
 * Runs ~12 small queries on 1 DB connection (~1 s) instead of that many
 * parallel requests each opening a new connection (~14 s from local dev).
 */
export function useBillingPageData(days = 30, workspaceIds?: string[] | null) {
  return useQuery({
    queryKey: ['billing', 'page-data', days, (workspaceIds || []).join(',')],
    queryFn: async () => {
      const params: any = { days }
      if (workspaceIds && workspaceIds.length) params.workspace_ids = workspaceIds.join(',')
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
        cost_by_user_source: 'estimate',
        tokens_by_user: [],
        cost_by_tag: [],
        external_model_spend: [],
        cost_by_service: [],
        cost_by_user_service: [],
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
      // The POST is a no-op since refresh moved to the discovery workflow
      // (workflows/09_discover_billing). The button now means "re-pull from
      // Lakebase" — the workflow may have written new rows since page load.
      const { data } = await apiClient.post('/billing/cache/refresh', null, { params: { days } })
      return data
    },
    onSuccess: () => {
      // Invalidate ALL billing queries so the UI re-pulls Lakebase data.
      // Matches any query whose key starts with ['billing', ...].
      queryClient.invalidateQueries({ queryKey: ['billing'] })
    },
  })
}

// ── Serving inventory ───────────────────────────────────────────

export interface QueryableEndpoint {
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

export function useQueryableEndpoints(options?: { enabled?: boolean }) {
  return useQuery({
    queryKey: ['serving', 'queryable-endpoints'],
    queryFn: async () => {
      const { data } = await apiClient.get('/serving/queryable-endpoints')
      return data as QueryableEndpoint[]
    },
    staleTime: 120_000, // matches backend 2-min cache
    enabled: options?.enabled ?? true,
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

export function useRefreshVectorSearch() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async () => {
      const { data } = await apiClient.post('/vector-search/refresh')
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['vector-search'] })
    },
  })
}

export function useRefreshMlflowCache() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async () => {
      const { data } = await apiClient.post('/mlflow/refresh-cache')
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['mlflow'] })
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

export function useVectorSearchPageData(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'page-data', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/page-data', { params: { days } })
      return {
        cost_summary: { total_dbus: 0, total_cost_usd: 0, endpoint_count: 0, workspace_count: 0, days },
        cost_trend_by_workload: [],
        cost_by_workspace: [],
        cost_by_endpoint: [],
        cost_by_workload: [],
        overview: { total_endpoints: 0, online_endpoints: 0, offline_endpoints: 0, total_indexes: 0, by_status: {}, by_index_type: {} },
        ...data,
      }
    },
  })
}

export function useVectorSearchIndexDetails() {
  return useQuery({
    queryKey: ['vector-search', 'index-details'],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/indexes/details')
      return Array.isArray(data) ? data : []
    },
  })
}

export function useVectorSearchHealthHistory(days = 7) {
  return useQuery({
    queryKey: ['vector-search', 'health-history', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/health/history', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useKnowledgeBasesOverview(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'combined-overview', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/combined/overview', { params: { days } })
      return { vector_search: { total_dbus: 0, total_cost_usd: 0, endpoint_count: 0, workspace_count: 0 },
               lakebase: { total_dbus: 0, total_cost_usd: 0, workspace_count: 0 }, ...data }
    },
  })
}

export function useKnowledgeBasesCostTrend(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'combined-cost-trend', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/combined/cost-trend', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useLakebaseInstances() {
  return useQuery({
    queryKey: ['vector-search', 'lakebase-instances'],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/lakebase/instances')
      return Array.isArray(data) ? data : []
    },
  })
}

export function useLakebaseCostSummary(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'lakebase-cost-summary', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/lakebase/cost/summary', { params: { days } })
      return { total_dbus: 0, total_cost_usd: 0, workspace_count: 0, days, ...data }
    },
  })
}

export function useLakebaseCostTrend(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'lakebase-cost-trend', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/lakebase/cost/trend', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useLakebaseCostByWorkspace(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'lakebase-cost-by-workspace', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/lakebase/cost/by-workspace', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useLakebaseCostByType(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'lakebase-cost-by-type', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/lakebase/cost/by-type', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useKBTopWorkspacesDaily(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'combined-top-ws-daily', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/combined/top-workspaces-daily', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useVSTopWorkspacesDaily(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'vs-top-ws-daily', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/cost/top-workspaces-daily', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}

export function useLBTopWorkspacesDaily(days = 30) {
  return useQuery({
    queryKey: ['vector-search', 'lb-top-ws-daily', days],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/lakebase/cost/top-workspaces-daily', { params: { days } })
      return Array.isArray(data) ? data : []
    },
  })
}

// ── Retrieval & Memory: Vector Search index usage + Lakebase health ──
// Cached in Lakebase by workflows 03/02 (system.access.audit, system.lakebase.*,
// system.billing.usage). Every endpoint fails open to [] when a table isn't synced.

export interface VsUsageTrendRow {
  usage_date: string
  action: string
  request_count: number
  error_count: number
}

export interface VsIndexUsageRow {
  workspace_id: string
  index_name: string
  endpoint_name: string
  managed_by: 'knowledge_assistant' | 'supervisor' | null
  query_count: number
  scan_count: number
  write_count: number
  error_count: number
  peak_daily_callers: number
  last_active_date: string | null
}

export interface VsIndexCallerRow {
  caller: string
  request_count: number
  error_count: number
  last_seen: string | null
}

export interface LakebaseHealthRow {
  usage_date: string
  project_id: string
  project_name: string
  branch_id: string
  branch_name: string
  avg_connections: number
  max_connections: number
  max_db_size_bytes: number
}

export interface LakebaseWaitEventRow {
  wait_class_name: string
  wait_event_name: string
  project_name: string
  branch_name: string
  wait_event_count: number
  wait_event_time: number
}

export interface LakebaseProjectCostRow {
  project_id: string
  branch_id: string
  project_name: string
  branch_name: string
  total_dbus: number
  total_cost_usd: number
}

/** Optional workspace narrowing for the Retrieval & Memory endpoints. The server
 *  intersects it with the caller's access scope, so it can only narrow. */
function wsParam(ids: string[]): { workspace_ids?: string } {
  return ids.length ? { workspace_ids: ids.join(',') } : {}
}

export function useVectorSearchUsageTrend(days = 30, workspaceIds: string[] = []) {
  return useQuery({
    queryKey: ['vector-search', 'usage-trend', days, workspaceIds.join(',')],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/usage/trend', { params: { days, ...wsParam(workspaceIds) } })
      return (Array.isArray(data) ? data : []) as VsUsageTrendRow[]
    },
  })
}

export function useVectorSearchIndexUsage(days = 30, workspaceIds: string[] = []) {
  return useQuery({
    queryKey: ['vector-search', 'usage-indexes', days, workspaceIds.join(',')],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/usage/indexes', { params: { days, ...wsParam(workspaceIds) } })
      return (Array.isArray(data) ? data : []) as VsIndexUsageRow[]
    },
  })
}

/** Top callers for one index. Disabled until an index is selected in the table. */
export function useVectorSearchIndexCallers(indexName: string | null, workspaceId?: string | null) {
  return useQuery({
    queryKey: ['vector-search', 'usage-callers', indexName, workspaceId ?? null],
    enabled: !!indexName,
    queryFn: async () => {
      const params: Record<string, string> = { index_name: indexName as string }
      if (workspaceId) params.workspace_id = workspaceId
      const { data } = await apiClient.get('/vector-search/usage/callers', { params })
      return (Array.isArray(data) ? data : []) as VsIndexCallerRow[]
    },
  })
}

export function useLakebaseHealth(days = 14, workspaceIds: string[] = []) {
  return useQuery({
    queryKey: ['vector-search', 'lakebase-health', days, workspaceIds.join(',')],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/lakebase/health', { params: { days, ...wsParam(workspaceIds) } })
      return (Array.isArray(data) ? data : []) as LakebaseHealthRow[]
    },
  })
}

export function useLakebaseWaitEvents(limit = 25, workspaceIds: string[] = []) {
  return useQuery({
    queryKey: ['vector-search', 'lakebase-wait-events', limit, workspaceIds.join(',')],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/lakebase/wait-events', { params: { limit, ...wsParam(workspaceIds) } })
      return (Array.isArray(data) ? data : []) as LakebaseWaitEventRow[]
    },
  })
}

export function useLakebaseCostByProject(days = 30, workspaceIds: string[] = []) {
  return useQuery({
    queryKey: ['vector-search', 'lakebase-cost-by-project', days, workspaceIds.join(',')],
    queryFn: async () => {
      const { data } = await apiClient.get('/vector-search/lakebase/cost/by-project', { params: { days, ...wsParam(workspaceIds) } })
      return (Array.isArray(data) ? data : []) as LakebaseProjectCostRow[]
    },
  })
}

// ── Genie Ask tab (feature-flagged via FEATURE_GENIE_ENABLED) ──

export interface GenieSpaceInfo {
  available: boolean
  space_id: string | null
  space_url: string | null
}

export function useGenieSpaceInfo(enabled = true) {
  return useQuery({
    queryKey: ['genie', 'space-info'],
    queryFn: async () => {
      const { data } = await apiClient.get('/genie/space-info')
      return data as GenieSpaceInfo
    },
    staleTime: 600_000, // 10 min — the space URL doesn't change between deploys
    enabled,
  })
}

// Conversation API — typed loosely on purpose. Genie's response shape is
// {status, attachments[{query?, text?, suggested_questions?, attachment_id?}]}
// and we want the frontend to render whatever attachments come back.

export interface GenieMessage {
  status: 'SUBMITTED' | 'IN_PROGRESS' | 'COMPLETED' | 'FAILED' | 'CANCELLED' | string
  id?: string
  message_id?: string
  conversation_id?: string
  content?: string
  attachments?: Array<{
    attachment_id?: string
    query?: { query?: string; description?: string }
    text?: { content?: string } | string
    suggested_questions?: { questions?: string[] } | string[]
  }>
}

export interface StartConversationResp {
  conversation: { id: string }
  conversation_id: string
  message: GenieMessage
  message_id: string
}

export async function genieStartConversation(content: string, signal?: AbortSignal): Promise<StartConversationResp> {
  const { data } = await apiClient.post('/genie/conversations', { content }, { signal })
  return data as StartConversationResp
}

export async function genieGetMessage(
  conversationId: string,
  messageId: string,
  signal?: AbortSignal,
): Promise<GenieMessage> {
  const { data } = await apiClient.get(
    `/genie/conversations/${conversationId}/messages/${messageId}`,
    { signal },
  )
  return data as GenieMessage
}

export async function genieFollowUp(
  conversationId: string,
  content: string,
  signal?: AbortSignal,
): Promise<GenieMessage> {
  const { data } = await apiClient.post(
    `/genie/conversations/${conversationId}/messages`,
    { content },
    { signal },
  )
  return data as GenieMessage
}

export interface GenieQueryResult {
  columns: Array<{ name: string; type_text?: string }>
  rows: any[][]
  total_row_count?: number
  truncated: boolean
}

/** Normalise the Databricks statement_response shape into something simple
 *  for the chat bubble to render. Genie returns PROTOBUF_ARRAY format with
 *  `data_typed_array` (rows of `{values: [{str|int|double|bool: ...}]}`), but
 *  some statements come back as JSON_ARRAY with `data_array` (plain arrays). */
function normalizeStatementRows(result: any): any[][] {
  if (Array.isArray(result?.data_array)) return result.data_array
  const typed = result?.data_typed_array
  if (!Array.isArray(typed)) return []
  return typed.map((row: any) =>
    (row?.values ?? []).map((v: any) => {
      if (v == null) return null
      if (typeof v !== 'object') return v
      // PROTOBUF_ARRAY scalar wrapper — first non-undefined value wins.
      if ('str' in v) return v.str
      if ('long' in v) return v.long
      if ('int' in v) return v.int
      if ('double' in v) return v.double
      if ('float' in v) return v.float
      if ('bool' in v) return v.bool
      if ('null' in v) return null
      const k = Object.keys(v)[0]
      return k ? v[k] : null
    })
  )
}

export async function genieGetQueryResult(
  conversationId: string,
  messageId: string,
  signal?: AbortSignal,
): Promise<GenieQueryResult | null> {
  try {
    const { data } = await apiClient.get(
      `/genie/conversations/${conversationId}/messages/${messageId}/query-result`,
      { signal },
    )
    const sr = data?.statement_response
    if (!sr) return null
    const columns = sr.manifest?.schema?.columns ?? []
    const rows = normalizeStatementRows(sr.result)
    const total = sr.manifest?.total_row_count ?? rows.length
    const truncated = sr.result?.next_chunk_index != null || (total != null && rows.length < total)
    return { columns, rows, total_row_count: total, truncated }
  } catch {
    return null
  }
}
