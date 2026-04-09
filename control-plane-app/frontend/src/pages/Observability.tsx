import { useState } from 'react'
import { useQueryClient, useIsFetching } from '@tanstack/react-query'
import {
  useAppConfig,
  useMlflowTraces,
  useMlflowTraceDetail,
  useMlflowExperiments,
  useMlflowRuns,
  useMlflowModels,
  useMlflowModelVersions,
  useMlflowObservabilityWorkspaces,
} from '@/api/hooks'
import { RefreshButton } from '@/components/RefreshButton'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { KpiCard } from '@/components/KpiCard'
import { TablePagination } from '@/components/TablePagination'
import { format } from 'date-fns'
import {
  Search,
  FlaskConical,
  Database,
  Activity,
  ExternalLink,
  ChevronDown,
  ChevronRight,
  CheckCircle2,
  XCircle,
  Clock,
  Loader2,
  ArrowLeft,
  MessageSquare,
  Bot,
  Zap,
  Hash,
  Timer,
  DollarSign,
  Layers,
  User,
  FileText,
  Copy,
  Check,
  Tag,
  BarChart3,
  Package,
  GitBranch,
  Server,
  Calendar,
  Info,
} from 'lucide-react'
import { DB_RED_SHADES } from '@/lib/brand'

/* ── helpers ─────────────────────────────────────────────────── */

/** Try to pretty-print a JSON string; fall back to raw text if parsing fails (e.g. truncated). */
function safePrettyJson(raw: string | undefined | null, maxLen = 1500): string {
  if (!raw) return '—'
  if (typeof raw === 'string' && (raw.startsWith('{') || raw.startsWith('['))) {
    try {
      return JSON.stringify(JSON.parse(raw), null, 2).substring(0, maxLen)
    } catch {
      // JSON was truncated or malformed — show raw
    }
  }
  return String(raw).substring(0, maxLen)
}

function msToReadable(ms: number | undefined | null) {
  if (ms == null) return '—'
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

function tsToDate(ms: number | string | undefined) {
  if (!ms) return '—'
  const n = typeof ms === 'string' ? Number(ms) : ms
  if (!n || isNaN(n)) return '—'
  return format(new Date(n), 'MMM dd, HH:mm:ss')
}

function statusIcon(s: string) {
  const sl = (s || '').toUpperCase()
  if (sl === 'OK' || sl === 'FINISHED')
    return <CheckCircle2 className="w-4 h-4 text-green-500" />
  if (sl === 'ERROR' || sl === 'FAILED')
    return <XCircle className="w-4 h-4 text-red-500" />
  if (sl === 'IN_PROGRESS' || sl === 'RUNNING')
    return <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />
  return <Clock className="w-4 h-4 text-gray-400" />
}

function statusBadge(s: string) {
  const sl = (s || '').toUpperCase()
  const variant =
    sl === 'OK' || sl === 'FINISHED'
      ? 'success'
      : sl === 'ERROR' || sl === 'FAILED'
        ? 'destructive'
        : 'default'
  return <Badge variant={variant as any} className="text-xs">{s}</Badge>
}

function dataSourceBadge(source: string | undefined) {
  if (!source) return null
  const isSystemTable = source === 'system_table'
  return (
    <span
      className={`inline-flex items-center px-1.5 py-0.5 text-[9px] font-medium rounded ${
        isSystemTable
          ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400'
          : 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400'
      }`}
      title={isSystemTable ? 'Discovered via system.mlflow system tables (historical)' : 'Discovered via MLflow REST API (real-time)'}
    >
      {isSystemTable ? 'sys table' : 'REST API'}
    </span>
  )
}

/* ── tab definitions ─────────────────────────────────────────── */
const tabs = [
  { id: 'traces', label: 'Traces', icon: Search },
  { id: 'experiments', label: 'Experiments', icon: FlaskConical },
  { id: 'runs', label: 'Evaluation Runs', icon: Activity },
  { id: 'models', label: 'Model Registry', icon: Database },
] as const

type TabId = (typeof tabs)[number]['id']

/* ── main component ──────────────────────────────────────────── */

export default function ObservabilityPage() {
  const [activeTab, setActiveTab] = useState<TabId>('traces')
  const [selectedWs, setSelectedWs] = useState<string | null>('all') // default to all workspaces
  const { data: config } = useAppConfig()
  const rawHost = (config?.databricks_host || '').replace(/\/$/, '')
  const workspaceUrl = rawHost && !rawHost.startsWith('http') ? `https://${rawHost}` : rawHost
  const queryClient = useQueryClient()
  const isFetchingMlflow = useIsFetching({ queryKey: ['mlflow'] }) > 0
  const mlflowUpdatedAt = queryClient.getQueryState(['mlflow', 'traces'])?.dataUpdatedAt
  const mlflowLastSynced = mlflowUpdatedAt ? new Date(mlflowUpdatedAt).toISOString() : null
  const { data: obsWorkspaces } = useMlflowObservabilityWorkspaces()

  // The workspace_id value to pass to hooks: undefined means current workspace (no param sent)
  const wsParam = selectedWs || undefined

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Observability</h2>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            MLflow traces, experiments, evaluation runs & model registry
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Workspace selector */}
          <select
            value={selectedWs ?? ''}
            onChange={(e) => setSelectedWs(e.target.value || null)}
            className="text-xs border border-gray-300 dark:border-gray-600 rounded-md px-2.5 py-1.5 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 focus:ring-1 focus:ring-red-500 focus:border-red-500"
          >
            <option value="">Current Workspace</option>
            <option value="all">All Workspaces</option>
            {(obsWorkspaces || []).map((ws) => (
              <option key={ws.workspace_id} value={ws.workspace_id}>
                Workspace {ws.workspace_id} ({ws.trace_count} traces)
              </option>
            ))}
          </select>
          <RefreshButton
            onRefresh={() => queryClient.invalidateQueries({ queryKey: ['mlflow'] })}
            isRefreshing={isFetchingMlflow}
            lastSynced={mlflowLastSynced}
            title="Refresh MLflow data"
          />
          {workspaceUrl && !selectedWs && (
            <a
              href={`${workspaceUrl}/ml/experiments`}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400 hover:text-db-red transition-colors"
            >
              Open in Databricks <ExternalLink className="w-3 h-3" />
            </a>
          )}
        </div>
      </div>

      {/* Cross-workspace indicator */}
      {selectedWs && (
        <div className="flex items-center gap-2 px-3 py-2 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg text-xs text-amber-700 dark:text-amber-400">
          <Info className="w-3.5 h-3.5 flex-shrink-0" />
          {selectedWs === 'all'
            ? 'Showing data from all registered workspaces. Cross-workspace queries require OBO authentication.'
            : `Showing data from workspace ${selectedWs}.`}
        </div>
      )}

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

      {/* Tab content */}
      {activeTab === 'traces' && <TracesPanel workspaceUrl={workspaceUrl} workspaceId={wsParam} />}
      {activeTab === 'experiments' && <ExperimentsPanel workspaceUrl={workspaceUrl} workspaceId={wsParam} />}
      {activeTab === 'runs' && <RunsPanel workspaceUrl={workspaceUrl} workspaceId={wsParam} />}
      {activeTab === 'models' && <ModelsPanel workspaceUrl={workspaceUrl} workspaceId={wsParam} />}
    </div>
  )
}

/* ── Traces Panel ────────────────────────────────────────────── */

function TracesPanel({ workspaceUrl, workspaceId }: { workspaceUrl: string; workspaceId?: string }) {
  const { data: traces, isLoading } = useMlflowTraces(workspaceId)
  const [selectedTraceId, setSelectedTraceId] = useState<string | null>(null)
  const [selectedTraceWs, setSelectedTraceWs] = useState<string | undefined>(undefined)
  const [tracePage, setTracePage] = useState(0)
  const [tracePageSize, setTracePageSize] = useState(10)

  const traceList = traces || []
  const okCount = traceList.filter((t: any) => (t.status || t.info?.status) === 'OK').length
  const errCount = traceList.filter((t: any) => {
    const s = t.status || t.info?.status
    return s === 'ERROR'
  }).length
  const avgDur =
    traceList.length > 0
      ? traceList.reduce((sum: number, t: any) => {
          return sum + (t.execution_time_ms || t.info?.execution_time_ms || 0)
        }, 0) / traceList.length
      : 0

  // If a trace is selected, show the detail view
  if (selectedTraceId) {
    return (
      <TraceDetailView
        requestId={selectedTraceId}
        workspaceUrl={workspaceUrl}
        workspaceId={selectedTraceWs}
        onBack={() => { setSelectedTraceId(null); setSelectedTraceWs(undefined) }}
      />
    )
  }

  return (
    <div className="space-y-4">
      {/* KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
        <KpiCard title="Total Traces" value={traceList.length} format="number" />
        <KpiCard title="Successful" value={okCount} format="number" />
        <KpiCard title="Errors" value={errCount} format="number" />
        <KpiCard title="Avg Duration" value={avgDur} format="duration" />
      </div>

      {/* Trace table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Agent Traces</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="flex items-center justify-center py-12 text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading traces…
            </div>
          ) : traceList.length === 0 ? (
            <div className="text-sm text-gray-400 text-center py-12">
              No traces found. Run an agent to generate traces.
            </div>
          ) : (
            <>
            <div className="divide-y divide-gray-100 dark:divide-gray-700">
              {/* Header */}
              <div className={`grid gap-3 py-2 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide ${workspaceId ? 'grid-cols-[2rem_1fr_1fr_5rem_5rem_4rem_4rem_5rem]' : 'grid-cols-12'}`}>
                <div className={workspaceId ? '' : 'col-span-1'} />
                <div className={workspaceId ? '' : 'col-span-3'}>Request ID</div>
                <div className={workspaceId ? '' : 'col-span-2'}>Trace Name</div>
                {workspaceId && <div>Workspace</div>}
                <div className={workspaceId ? '' : 'col-span-2'}>Timestamp</div>
                <div className={workspaceId ? 'text-right' : 'col-span-1 text-right'}>Duration</div>
                <div className={workspaceId ? 'text-center' : 'col-span-1 text-center'}>Status</div>
                <div className={workspaceId ? 'text-right' : 'col-span-2 text-right'}>Actions</div>
              </div>
              {(() => {
                const totalPages = Math.max(1, Math.ceil(traceList.length / tracePageSize))
                const safePage = Math.min(tracePage, totalPages - 1)
                const paged = traceList.slice(safePage * tracePageSize, (safePage + 1) * tracePageSize)
                return paged
              })().map((t: any) => {
                const reqId = t.request_id || t.info?.request_id || '—'
                const status = t.status || t.info?.status || '—'
                const dur = t.execution_time_ms || t.info?.execution_time_ms
                const ts = t.timestamp_ms || t.info?.timestamp_ms
                const traceName = t.tags?.['mlflow.traceName'] || t.info?.tags?.['mlflow.traceName'] || '—'
                const experimentId = t.experiment_id || t.info?.experiment_id
                const traceWsId = t.workspace_id

                return (
                  <div
                    key={`${traceWsId || 'local'}-${reqId}`}
                    className={`grid gap-3 py-2.5 items-center hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer rounded transition-colors ${workspaceId ? 'grid-cols-[2rem_1fr_1fr_5rem_5rem_4rem_4rem_5rem]' : 'grid-cols-12'}`}
                    onClick={() => { setSelectedTraceId(reqId); setSelectedTraceWs(traceWsId) }}
                  >
                    <div className={`${workspaceId ? '' : 'col-span-1'} flex justify-center`}>
                      <ChevronRight className="w-4 h-4 text-gray-400" />
                    </div>
                    <div className={`${workspaceId ? '' : 'col-span-3'} font-mono text-xs text-gray-700 dark:text-gray-300 truncate`}>
                      {reqId}
                    </div>
                    <div className={`${workspaceId ? '' : 'col-span-2'} text-xs text-gray-500 truncate`}>{traceName}</div>
                    {workspaceId && (
                      <div className="text-xs flex items-center gap-1">
                        <Badge variant="default" className="text-[10px] font-mono">
                          {traceWsId ? traceWsId.substring(0, 8) + '…' : 'local'}
                        </Badge>
                        {dataSourceBadge(t.data_source)}
                      </div>
                    )}
                    <div className={`${workspaceId ? '' : 'col-span-2'} text-xs text-gray-500`}>{tsToDate(ts)}</div>
                    <div className={`${workspaceId ? 'text-right' : 'col-span-1 text-right'} text-xs font-medium`}>
                      {msToReadable(dur)}
                    </div>
                    <div className={`${workspaceId ? 'text-center' : 'col-span-1'} flex justify-center`}>{statusIcon(status)}</div>
                    <div className={`${workspaceId ? 'text-right' : 'col-span-2 text-right'}`}>
                      {workspaceUrl && experimentId && !traceWsId && (
                        <a
                          href={`${workspaceUrl}/ml/experiments/${experimentId}?searchFilter=request_id%3D%27${reqId}%27&compareRunsMode=TRACES`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-blue-600 hover:text-blue-800 text-xs inline-flex items-center gap-1"
                          onClick={(e) => e.stopPropagation()}
                        >
                          MLflow <ExternalLink className="w-3 h-3" />
                        </a>
                      )}
                    </div>
                  </div>
                )
              })}
            </div>
            <TablePagination page={tracePage} totalItems={traceList.length} pageSize={tracePageSize} onPageChange={setTracePage} onPageSizeChange={setTracePageSize} />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Trace Detail View ──────────────────────────────────────── */

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text).then(() => {
          setCopied(true)
          setTimeout(() => setCopied(false), 2000)
        })
      }}
      className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
      title="Copy to clipboard"
    >
      {copied ? <Check className="w-3.5 h-3.5 text-green-500" /> : <Copy className="w-3.5 h-3.5" />}
    </button>
  )
}

function MetricTile({ icon: Icon, label, value, sub }: { icon: any; label: string; value: string | number; sub?: string }) {
  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-3 flex items-start gap-3">
      <div className="w-8 h-8 rounded-lg bg-gray-100 dark:bg-gray-700 flex items-center justify-center flex-shrink-0">
        <Icon className="w-4 h-4 text-gray-500 dark:text-gray-400" />
      </div>
      <div className="min-w-0">
        <div className="text-[11px] text-gray-500 dark:text-gray-400 uppercase tracking-wide">{label}</div>
        <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">{value}</div>
        {sub && <div className="text-[11px] text-gray-400 dark:text-gray-500">{sub}</div>}
      </div>
    </div>
  )
}

function TraceDetailView({
  requestId,
  workspaceUrl,
  workspaceId,
  onBack,
}: {
  requestId: string
  workspaceUrl: string
  workspaceId?: string
  onBack: () => void
}) {
  const { data: detail, isLoading, error } = useMlflowTraceDetail(requestId, workspaceId)
  const [showRawRequest, setShowRawRequest] = useState(false)
  const [showFullResponse, setShowFullResponse] = useState(false)

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20 text-gray-400">
        <Loader2 className="w-6 h-6 animate-spin mr-3" /> Loading trace details…
      </div>
    )
  }

  if (error || !detail) {
    return (
      <div className="space-y-4">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 transition-colors">
          <ArrowLeft className="w-4 h-4" /> Back to traces
        </button>
        <Card>
          <CardContent className="py-12 text-center text-gray-400">
            Failed to load trace details. The trace may no longer exist.
          </CardContent>
        </Card>
      </div>
    )
  }

  const tokenUsage = detail.token_usage || {}
  const sizeStats = detail.size_stats || {}
  const durationStr = detail.execution_duration || '—'

  return (
    <div className="space-y-4">
      {/* Navigation */}
      <div className="flex items-center justify-between">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 transition-colors">
          <ArrowLeft className="w-4 h-4" /> Back to traces
        </button>
        {workspaceUrl && detail.experiment_id && (
          <a
            href={`${workspaceUrl}/ml/experiments/${detail.experiment_id}?searchFilter=request_id%3D%27${requestId}%27&compareRunsMode=TRACES`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 text-xs text-blue-600 hover:text-blue-800"
          >
            View in MLflow <ExternalLink className="w-3 h-3" />
          </a>
        )}
      </div>

      {/* Header card */}
      <Card>
        <CardContent className="pt-5 pb-4">
          <div className="flex items-start justify-between">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <h3 className="text-lg font-bold text-gray-900 dark:text-gray-100">{detail.trace_name || 'Trace'}</h3>
                {statusBadge(detail.state)}
              </div>
              <div className="flex items-center gap-3 text-xs text-gray-500">
                <span className="font-mono">{requestId}</span>
                <CopyButton text={requestId} />
              </div>
            </div>
            <div className="text-right text-xs text-gray-500">
              {detail.request_time ? format(new Date(detail.request_time), 'MMM dd, yyyy HH:mm:ss') : '—'}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Metrics grid */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
        <MetricTile
          icon={Timer}
          label="Duration"
          value={durationStr}
        />
        <MetricTile
          icon={Hash}
          label="Input Tokens"
          value={tokenUsage.input_tokens?.toLocaleString() ?? '—'}
          sub={tokenUsage.cache_read_input_tokens ? `${tokenUsage.cache_read_input_tokens} cached` : undefined}
        />
        <MetricTile
          icon={Hash}
          label="Output Tokens"
          value={tokenUsage.output_tokens?.toLocaleString() ?? '—'}
        />
        <MetricTile
          icon={Zap}
          label="Total Tokens"
          value={tokenUsage.total_tokens?.toLocaleString() ?? '—'}
        />
        <MetricTile
          icon={Layers}
          label="Spans"
          value={sizeStats.num_spans ?? '—'}
          sub={sizeStats.total_size_bytes ? `${(sizeStats.total_size_bytes / 1024).toFixed(1)} KB` : undefined}
        />
        <MetricTile
          icon={Bot}
          label="Model"
          value={detail.model_id ? detail.model_id.substring(0, 16) + '…' : '—'}
          sub={detail.model_id ? detail.model_id : undefined}
        />
      </div>

      {/* Conversation view */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base flex items-center gap-2">
            <MessageSquare className="w-4 h-4" /> Conversation
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* User message */}
          {detail.user_message && (
            <div className="flex gap-3">
              <div className="w-7 h-7 rounded-full bg-blue-100 dark:bg-blue-900/40 flex items-center justify-center flex-shrink-0 mt-0.5">
                <User className="w-3.5 h-3.5 text-blue-600" />
              </div>
              <div className="flex-1 min-w-0">
                <div className="text-xs font-semibold text-gray-500 mb-1">User</div>
                <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-100 dark:border-blue-800 rounded-lg p-3 text-sm text-gray-800 dark:text-gray-200">
                  {detail.user_message}
                </div>
              </div>
            </div>
          )}

          {/* Assistant response */}
          {detail.response && (
            <div className="flex gap-3">
              <div className="w-7 h-7 rounded-full bg-green-100 dark:bg-green-900/40 flex items-center justify-center flex-shrink-0 mt-0.5">
                <Bot className="w-3.5 h-3.5 text-green-600" />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-xs font-semibold text-gray-500">Assistant</span>
                  <CopyButton text={detail.response} />
                </div>
                <div className="bg-green-50 dark:bg-green-900/20 border border-green-100 dark:border-green-800 rounded-lg p-3 text-sm text-gray-800 dark:text-gray-200 whitespace-pre-wrap">
                  {showFullResponse
                    ? detail.response
                    : detail.response.length > 500
                      ? detail.response.substring(0, 500) + '…'
                      : detail.response}
                </div>
                {detail.response.length > 500 && (
                  <button
                    onClick={() => setShowFullResponse(!showFullResponse)}
                    className="text-xs text-blue-600 hover:text-blue-800 mt-1"
                  >
                    {showFullResponse ? 'Show less' : 'Show full response'}
                  </button>
                )}
              </div>
            </div>
          )}

          {!detail.user_message && !detail.response && (
            <div className="text-sm text-gray-400 text-center py-6">
              No conversation data available for this trace.
            </div>
          )}
        </CardContent>
      </Card>

      {/* Session & metadata */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Trace metadata */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <FileText className="w-4 h-4" /> Trace Metadata
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 text-sm">
              <MetadataRow label="Trace Name" value={detail.trace_name} />
              <MetadataRow label="Experiment ID" value={detail.experiment_id} />
              <MetadataRow label="Session ID" value={detail.session_id} copyable />
              <MetadataRow label="Model ID" value={detail.model_id} copyable />
              <MetadataRow label="User" value={detail.user} />
              <MetadataRow label="Source" value={detail.source} />
              <MetadataRow label="Schema Version" value={detail.trace_schema_version} />
            </div>
          </CardContent>
        </Card>

        {/* Span size distribution */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <Layers className="w-4 h-4" /> Span Statistics
            </CardTitle>
          </CardHeader>
          <CardContent>
            {sizeStats.num_spans ? (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-gray-50 dark:bg-gray-700 rounded-lg p-3">
                    <div className="text-xs text-gray-500 dark:text-gray-400">Spans</div>
                    <div className="text-xl font-bold text-gray-900 dark:text-gray-100">{sizeStats.num_spans}</div>
                  </div>
                  <div className="bg-gray-50 dark:bg-gray-700 rounded-lg p-3">
                    <div className="text-xs text-gray-500 dark:text-gray-400">Total Size</div>
                    <div className="text-xl font-bold text-gray-900 dark:text-gray-100">
                      {(sizeStats.total_size_bytes / 1024).toFixed(1)} KB
                    </div>
                  </div>
                </div>
                <div>
                  <div className="text-xs font-semibold text-gray-500 mb-2">Span Size Distribution (bytes)</div>
                  <SpanSizeBar stats={sizeStats} />
                  <div className="flex justify-between text-[10px] text-gray-400 mt-1">
                    <span>P25: {sizeStats.p25?.toLocaleString()}</span>
                    <span>P50: {sizeStats.p50?.toLocaleString()}</span>
                    <span>P75: {sizeStats.p75?.toLocaleString()}</span>
                    <span>Max: {sizeStats.max?.toLocaleString()}</span>
                  </div>
                </div>
              </div>
            ) : (
              <div className="text-sm text-gray-400 text-center py-6">No span statistics available.</div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Raw request JSON (collapsible) */}
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">Raw Request Payload</CardTitle>
            <button
              onClick={() => setShowRawRequest(!showRawRequest)}
              className="text-xs text-blue-600 hover:text-blue-800 flex items-center gap-1"
            >
              {showRawRequest ? (
                <><ChevronDown className="w-3.5 h-3.5" /> Hide</>
              ) : (
                <><ChevronRight className="w-3.5 h-3.5" /> Show</>
              )}
            </button>
          </div>
        </CardHeader>
        {showRawRequest && (
          <CardContent>
            <pre className="bg-gray-50 dark:bg-gray-900 border dark:border-gray-700 rounded-lg p-4 text-xs text-gray-700 dark:text-gray-300 overflow-auto max-h-96 whitespace-pre-wrap">
              {safePrettyJson(detail.request_raw, 5000)}
            </pre>
          </CardContent>
        )}
      </Card>
    </div>
  )
}

/* ── Metadata row helper ─────────────────────────────────────── */

function MetadataRow({ label, value, copyable }: { label: string; value?: string | null; copyable?: boolean }) {
  if (!value) return null
  return (
    <div className="flex items-start gap-2 py-1.5 border-b border-gray-100 dark:border-gray-700 last:border-0">
      <span className="text-xs text-gray-500 dark:text-gray-400 w-28 flex-shrink-0">{label}</span>
      <span className="text-xs text-gray-900 dark:text-gray-200 font-mono break-all flex-1">{value}</span>
      {copyable && <CopyButton text={value} />}
    </div>
  )
}

/* ── Span size bar visualization ─────────────────────────────── */

function SpanSizeBar({ stats }: { stats: any }) {
  const max = stats.max || 1
  const p25Pct = ((stats.p25 || 0) / max) * 100
  const p50Pct = ((stats.p50 || 0) / max) * 100
  const p75Pct = ((stats.p75 || 0) / max) * 100

  return (
    <div className="relative h-6 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
      <div
        className="absolute inset-y-0 left-0 rounded-full"
        style={{ width: '100%', backgroundColor: DB_RED_SHADES.max }}
      />
      <div
        className="absolute inset-y-0 left-0 rounded-full"
        style={{ width: `${p75Pct}%`, backgroundColor: DB_RED_SHADES.p75 }}
      />
      <div
        className="absolute inset-y-0 left-0 rounded-full"
        style={{ width: `${p50Pct}%`, backgroundColor: DB_RED_SHADES.p50 }}
      />
      <div
        className="absolute inset-y-0 left-0 rounded-full"
        style={{ width: `${p25Pct}%`, backgroundColor: DB_RED_SHADES.p25 }}
      />
    </div>
  )
}

/* ── Experiments Panel ───────────────────────────────────────── */

function ExperimentsPanel({ workspaceUrl, workspaceId }: { workspaceUrl: string; workspaceId?: string }) {
  const { data: experiments, isLoading } = useMlflowExperiments(workspaceId)
  const [selectedExpId, setSelectedExpId] = useState<string | null>(null)
  const [expPage, setExpPage] = useState(0)
  const [expPageSize, setExpPageSize] = useState(10)
  const expList = experiments || []

  if (selectedExpId) {
    const exp = expList.find((e: any) => e.experiment_id === selectedExpId)
    return (
      <ExperimentDetailView
        experiment={exp}
        experimentId={selectedExpId}
        workspaceUrl={workspaceUrl}
        onBack={() => setSelectedExpId(null)}
      />
    )
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <KpiCard title="Total Experiments" value={expList.length} format="number" />
        <KpiCard
          title="Active"
          value={expList.filter((e: any) => e.lifecycle_stage === 'active').length}
          format="number"
        />
        <KpiCard
          title="Deleted"
          value={expList.filter((e: any) => e.lifecycle_stage === 'deleted').length}
          format="number"
        />
      </div>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Experiments</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="flex items-center justify-center py-12 text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading…
            </div>
          ) : expList.length === 0 ? (
            <div className="text-sm text-gray-400 text-center py-12">No experiments found.</div>
          ) : (
            <>
            <div className="divide-y divide-gray-100 dark:divide-gray-700">
              {/* Header */}
              <div className="grid grid-cols-12 gap-3 py-2 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">
                <div className="col-span-1" />
                <div className="col-span-4">Name</div>
                <div className="col-span-2">ID</div>
                <div className="col-span-1">Status</div>
                <div className="col-span-2">Owner</div>
                <div className="col-span-2 text-right">Created</div>
              </div>
              {(() => {
                const totalPages = Math.max(1, Math.ceil(expList.length / expPageSize))
                const safePage = Math.min(expPage, totalPages - 1)
                return expList.slice(safePage * expPageSize, (safePage + 1) * expPageSize)
              })().map((exp: any) => {
                const name = exp.name || '—'
                const shortName = name.split('/').pop() || name
                // Tags can be [{key, value}] (REST) or {key: value} (cache) or null
                const expTags = exp.tags || {}
                const owner = Array.isArray(expTags)
                  ? expTags.find((t: any) => t.key === 'mlflow.ownerEmail')?.value
                  : expTags['mlflow.ownerEmail']
                  || name.match(/\/Users\/([^/]+)/)?.[1]  // fallback: extract from path
                  || '—'
                return (
                  <div
                    key={exp.experiment_id}
                    className="grid grid-cols-12 gap-3 py-2.5 items-center hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer rounded transition-colors"
                    onClick={() => setSelectedExpId(exp.experiment_id)}
                  >
                    <div className="col-span-1 flex justify-center">
                      <ChevronRight className="w-4 h-4 text-gray-400" />
                    </div>
                    <div className="col-span-4">
                      <div className="font-medium text-gray-900 dark:text-gray-100 text-sm">{shortName}</div>
                      <div className="text-xs text-gray-400 truncate">{name}</div>
                    </div>
                    <div className="col-span-2 font-mono text-xs text-gray-500">
                      {exp.experiment_id}
                    </div>
                    <div className="col-span-1">
                      <Badge
                        variant={exp.lifecycle_stage === 'active' ? 'success' : 'default'}
                        className="text-xs"
                      >
                        {exp.lifecycle_stage}
                      </Badge>
                    </div>
                    <div className="col-span-2 text-xs text-gray-500 truncate flex items-center gap-1">
                      {owner}
                      {exp.data_source && dataSourceBadge(exp.data_source)}
                    </div>
                    <div className="col-span-2 text-xs text-gray-500 text-right">
                      {tsToDate(exp.creation_time || exp.last_update_time)}
                    </div>
                  </div>
                )
              })}
            </div>
            <TablePagination page={expPage} totalItems={expList.length} pageSize={expPageSize} onPageChange={setExpPage} onPageSizeChange={setExpPageSize} />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Experiment Detail View ─────────────────────────────────── */

function ExperimentDetailView({
  experiment,
  experimentId,
  workspaceUrl,
  onBack,
}: {
  experiment: any
  experimentId: string
  workspaceUrl: string
  onBack: () => void
}) {
  const { data: runs, isLoading: runsLoading } = useMlflowRuns(experimentId)
  const runList = runs || []
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const [expRunPage, setExpRunPage] = useState(0)
  const [expRunPageSize, setExpRunPageSize] = useState(10)

  const name = experiment?.name || '—'
  const shortName = name.split('/').pop() || name
  const owner = experiment?.tags?.find((t: any) => t.key === 'mlflow.ownerEmail')?.value || '—'
  const expType = experiment?.tags?.find((t: any) => t.key === 'mlflow.experimentType')?.value || '—'
  const artifactLoc = experiment?.artifact_location || '—'

  const finishedRuns = runList.filter((r: any) => r.info?.status === 'FINISHED').length
  const failedRuns = runList.filter((r: any) => r.info?.status === 'FAILED').length

  // If a run is selected, show run detail
  if (selectedRunId) {
    const run = runList.find((r: any) => r.info?.run_id === selectedRunId)
    return (
      <RunDetailView
        run={run}
        workspaceUrl={workspaceUrl}
        onBack={() => setSelectedRunId(null)}
      />
    )
  }

  return (
    <div className="space-y-4">
      {/* Navigation */}
      <div className="flex items-center justify-between">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 transition-colors">
          <ArrowLeft className="w-4 h-4" /> Back to experiments
        </button>
        {workspaceUrl && (
          <a
            href={`${workspaceUrl}/ml/experiments/${experimentId}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 text-xs text-blue-600 hover:text-blue-800"
          >
            Open in MLflow <ExternalLink className="w-3 h-3" />
          </a>
        )}
      </div>

      {/* Header card */}
      <Card>
        <CardContent className="pt-5 pb-4">
          <div className="flex items-start justify-between">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <FlaskConical className="w-5 h-5 text-gray-400" />
                <h3 className="text-lg font-bold text-gray-900 dark:text-gray-100">{shortName}</h3>
                <Badge variant={experiment?.lifecycle_stage === 'active' ? 'success' : 'default'} className="text-xs">
                  {experiment?.lifecycle_stage}
                </Badge>
              </div>
              <div className="text-xs text-gray-400 font-mono">{name}</div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Metadata grid */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <MetricTile icon={Hash} label="Experiment ID" value={experimentId} />
        <MetricTile icon={User} label="Owner" value={owner} />
        <MetricTile icon={Tag} label="Type" value={expType} />
        <MetricTile icon={Calendar} label="Created" value={tsToDate(experiment?.creation_time)} />
      </div>

      {/* Experiment info */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base flex items-center gap-2">
            <Info className="w-4 h-4" /> Experiment Details
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2 text-sm">
            <MetadataRow label="Full Path" value={name} copyable />
            <MetadataRow label="Artifact Location" value={artifactLoc} copyable />
            <MetadataRow label="Last Updated" value={tsToDate(experiment?.last_update_time)} />
            {experiment?.tags?.filter((t: any) =>
              !['mlflow.ownerEmail', 'mlflow.ownerId', 'mlflow.experimentType', 'mlflow.experiment.sourceName'].includes(t.key)
            ).map((t: any) => (
              <MetadataRow key={t.key} label={t.key} value={t.value} />
            ))}
          </div>
        </CardContent>
      </Card>

      {/* KPIs for runs */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <KpiCard title="Total Runs" value={runList.length} format="number" />
        <KpiCard title="Finished" value={finishedRuns} format="number" />
        <KpiCard title="Failed" value={failedRuns} format="number" />
      </div>

      {/* Runs table */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Runs in this Experiment</CardTitle>
        </CardHeader>
        <CardContent>
          {runsLoading ? (
            <div className="flex items-center justify-center py-12 text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading runs…
            </div>
          ) : runList.length === 0 ? (
            <div className="text-sm text-gray-400 text-center py-12">No runs found for this experiment.</div>
          ) : (
            <>
            <div className="divide-y divide-gray-100 dark:divide-gray-700">
              <div className="grid grid-cols-12 gap-3 py-2 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">
                <div className="col-span-1" />
                <div className="col-span-3">Run Name</div>
                <div className="col-span-2">Status</div>
                <div className="col-span-2">Started</div>
                <div className="col-span-2 text-right">Duration</div>
                <div className="col-span-2">Metrics</div>
              </div>
              {(() => {
                const totalPages = Math.max(1, Math.ceil(runList.length / expRunPageSize))
                const safePage = Math.min(expRunPage, totalPages - 1)
                return runList.slice(safePage * expRunPageSize, (safePage + 1) * expRunPageSize)
              })().map((r: any) => {
                const info = r.info || {}
                const data = r.data || {}
                const metrics = data.metrics || []
                const tags = data.tags || []
                const dur = Number(info.end_time || 0) - Number(info.start_time || 0)
                const runName = tags.find((t: any) => t.key === 'mlflow.runName')?.value || info.run_id?.substring(0, 8)
                return (
                  <div
                    key={info.run_id}
                    className="grid grid-cols-12 gap-3 py-2.5 items-center hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer rounded transition-colors"
                    onClick={() => setSelectedRunId(info.run_id)}
                  >
                    <div className="col-span-1 flex justify-center">
                      <ChevronRight className="w-4 h-4 text-gray-400" />
                    </div>
                    <div className="col-span-3">
                      <div className="font-medium text-gray-900 dark:text-gray-100 text-xs">{runName}</div>
                      <div className="font-mono text-[11px] text-gray-400">{info.run_id?.substring(0, 16)}…</div>
                    </div>
                    <div className="col-span-2 flex items-center gap-1.5">
                      {statusIcon(info.status)} <span className="text-xs">{info.status}</span>
                    </div>
                    <div className="col-span-2 text-xs text-gray-500">{tsToDate(info.start_time)}</div>
                    <div className="col-span-2 text-xs text-right font-medium">{dur > 0 ? msToReadable(dur) : '—'}</div>
                    <div className="col-span-2 flex gap-1 flex-wrap">
                      {metrics.slice(0, 2).map((m: any) => (
                        <Badge key={m.key} variant="default" className="text-[10px]">
                          {m.key}: {Number(m.value).toFixed(3)}
                        </Badge>
                      ))}
                      {metrics.length > 2 && (
                        <Badge variant="default" className="text-[10px]">+{metrics.length - 2}</Badge>
                      )}
                      {metrics.length === 0 && <span className="text-xs text-gray-400">—</span>}
                    </div>
                  </div>
                )
              })}
            </div>
            <TablePagination page={expRunPage} totalItems={runList.length} pageSize={expRunPageSize} onPageChange={setExpRunPage} onPageSizeChange={setExpRunPageSize} />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Runs Panel ──────────────────────────────────────────────── */

/** Normalize a run from either REST API format (nested info/data) or flat format (system table / cache) */
function normalizeRun(r: any) {
  // REST API format has r.info.run_id, system table / cache has r.run_id directly
  const info = r.info || {}
  const data = r.data || {}

  // Tags: REST API returns [{key, value}], system table / cache returns {key: value} map or JSON string
  let tagsMap: Record<string, string> = {}
  const rawTags = r.tags || data.tags || {}
  if (Array.isArray(rawTags)) {
    // REST API format
    rawTags.forEach((t: any) => { if (t.key) tagsMap[t.key] = t.value })
  } else if (typeof rawTags === 'object') {
    tagsMap = rawTags
  }

  // Metrics: REST API returns [{key, value, ...}], system table returns [{metric_name, min_value, max_value, latest_value}]
  let metricsArr: Array<{key: string; value: number}> = []
  const rawMetrics = r.metrics || data.metrics || []
  if (Array.isArray(rawMetrics)) {
    metricsArr = rawMetrics.map((m: any) => ({
      key: m.key || m.metric_name || '',
      value: m.value ?? m.latest_value ?? 0,
    })).filter((m: any) => m.key)
  }

  // Params: REST API returns [{key, value}], system table returns {key: value} map
  let paramsMap: Record<string, string> = {}
  const rawParams = r.params || data.params || {}
  if (Array.isArray(rawParams)) {
    rawParams.forEach((p: any) => { if (p.key) paramsMap[p.key] = p.value })
  } else if (typeof rawParams === 'object') {
    paramsMap = rawParams
  }

  return {
    run_id: info.run_id || r.run_id || '',
    run_name: tagsMap['mlflow.runName'] || r.run_name || info.run_name || '',
    status: info.status || r.status || '—',
    start_time: info.start_time || r.start_time,
    end_time: info.end_time || r.end_time,
    user_id: r.user_id || info.user_id || r.created_by || '',
    experiment_id: info.experiment_id || r.experiment_id || '',
    workspace_id: r.workspace_id || '',
    data_source: r.data_source || '',
    metrics: metricsArr,
    tags: tagsMap,
    params: paramsMap,
  }
}

function RunsPanel({ workspaceUrl, workspaceId }: { workspaceUrl: string; workspaceId?: string }) {
  const { data: runs, isLoading } = useMlflowRuns(undefined, workspaceId)
  const runList = (runs || []).map(normalizeRun)
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const [evalRunPage, setEvalRunPage] = useState(0)
  const [evalRunPageSize, setEvalRunPageSize] = useState(10)

  const finished = runList.filter((r) => r.status === 'FINISHED').length
  const failed = runList.filter((r) => r.status === 'FAILED').length
  const avgDur =
    runList.length > 0
      ? runList.reduce((sum: number, r) => {
          const start = Number(r.start_time || 0)
          const end = Number(r.end_time || 0)
          return sum + (end > start ? end - start : 0)
        }, 0) / runList.length
      : 0

  if (selectedRunId) {
    const run = (runs || []).find((r: any) => (r.info?.run_id || r.run_id) === selectedRunId)
    return (
      <RunDetailView
        run={run}
        workspaceUrl={workspaceUrl}
        onBack={() => setSelectedRunId(null)}
      />
    )
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
        <KpiCard title="Total Runs" value={runList.length} format="number" />
        <KpiCard title="Finished" value={finished} format="number" />
        <KpiCard title="Failed" value={failed} format="number" />
        <KpiCard title="Avg Duration" value={avgDur} format="duration" />
      </div>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Evaluation Runs</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="flex items-center justify-center py-12 text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading…
            </div>
          ) : runList.length === 0 ? (
            <div className="text-sm text-gray-400 text-center py-12">No runs found.</div>
          ) : (
            <>
            <div className="divide-y divide-gray-100 dark:divide-gray-700">
              {/* Header */}
              <div className="grid grid-cols-12 gap-3 py-2 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">
                <div className="col-span-1" />
                <div className="col-span-3">Run Name</div>
                <div className="col-span-2">Status</div>
                <div className="col-span-2">Started</div>
                <div className="col-span-2 text-right">Duration</div>
                <div className="col-span-2">Source</div>
              </div>
              {(() => {
                const totalPages = Math.max(1, Math.ceil(runList.length / evalRunPageSize))
                const safePage = Math.min(evalRunPage, totalPages - 1)
                return runList.slice(safePage * evalRunPageSize, (safePage + 1) * evalRunPageSize)
              })().map((r) => {
                const dur = Number(r.end_time || 0) - Number(r.start_time || 0)
                const displayName = r.run_name || r.run_id?.substring(0, 12) || '—'
                return (
                  <div
                    key={r.run_id}
                    className="grid grid-cols-12 gap-3 py-2.5 items-center hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer rounded transition-colors"
                    onClick={() => setSelectedRunId(r.run_id)}
                  >
                    <div className="col-span-1 flex justify-center">
                      <ChevronRight className="w-4 h-4 text-gray-400" />
                    </div>
                    <div className="col-span-3">
                      <div className="font-medium text-gray-900 dark:text-gray-100 text-xs">{displayName}</div>
                      <div className="font-mono text-[11px] text-gray-400">
                        {r.run_id?.substring(0, 16)}…
                        {r.user_id && <span className="ml-1.5 text-gray-400">by {r.user_id.split('@')[0]}</span>}
                      </div>
                    </div>
                    <div className="col-span-2 flex items-center gap-1.5">
                      {statusIcon(r.status)} <span className="text-xs">{r.status}</span>
                    </div>
                    <div className="col-span-2 text-xs text-gray-500">{tsToDate(r.start_time)}</div>
                    <div className="col-span-2 text-xs text-right font-medium">{dur > 0 ? msToReadable(dur) : '—'}</div>
                    <div className="col-span-2 flex gap-1 flex-wrap items-center">
                      {r.metrics.length > 0 ? (
                        <>
                          {r.metrics.slice(0, 2).map((m: any) => (
                            <Badge key={m.key} variant="default" className="text-[10px]">
                              {m.key.split('.').pop()}: {Number(m.value).toFixed(3)}
                            </Badge>
                          ))}
                          {r.metrics.length > 2 && (
                            <Badge variant="default" className="text-[10px]">+{r.metrics.length - 2}</Badge>
                          )}
                        </>
                      ) : (
                        <span className="text-xs text-gray-400">—</span>
                      )}
                      {dataSourceBadge(r.data_source)}
                    </div>
                  </div>
                )
              })}
            </div>
            <TablePagination page={evalRunPage} totalItems={runList.length} pageSize={evalRunPageSize} onPageChange={setEvalRunPage} onPageSizeChange={setEvalRunPageSize} />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Run Detail View ─────────────────────────────────────────── */

function RunDetailView({
  run,
  workspaceUrl,
  onBack,
}: {
  run: any
  workspaceUrl: string
  onBack: () => void
}) {
  const [showAllTags, setShowAllTags] = useState(false)

  if (!run) {
    return (
      <div className="space-y-4">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 transition-colors">
          <ArrowLeft className="w-4 h-4" /> Back
        </button>
        <Card><CardContent className="py-12 text-center text-gray-400">Run not found.</CardContent></Card>
      </div>
    )
  }

  const info = run.info || {}
  const data = run.data || {}
  const metrics = data.metrics || []
  const params = data.params || []
  const tags = data.tags || []
  const outputs = run.outputs || {}
  const modelOutputs = outputs.model_outputs || []

  const dur = Number(info.end_time || 0) - Number(info.start_time || 0)
  const runName = tags.find((t: any) => t.key === 'mlflow.runName')?.value || info.run_id
  const mlflowUser = tags.find((t: any) => t.key === 'mlflow.user')?.value || '—'
  const sourceType = tags.find((t: any) => t.key === 'mlflow.source.type')?.value || '—'
  const sourceName = tags.find((t: any) => t.key === 'mlflow.source.name')?.value || '—'
  const clusterId = tags.find((t: any) => t.key === 'mlflow.databricks.cluster.id')?.value
  const jobId = tags.find((t: any) => t.key === 'mlflow.databricks.jobID')?.value
  const jobRunId = tags.find((t: any) => t.key === 'mlflow.databricks.jobRunID')?.value

  // Non-standard tags (filter out common mlflow ones for the "all tags" section)
  const commonPrefixes = ['mlflow.runName', 'mlflow.runColor', 'mlflow.user', 'mlflow.source', 'mlflow.databricks']
  const customTags = tags.filter((t: any) => !commonPrefixes.some((p: string) => t.key.startsWith(p)))

  return (
    <div className="space-y-4">
      {/* Navigation */}
      <div className="flex items-center justify-between">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 transition-colors">
          <ArrowLeft className="w-4 h-4" /> Back to runs
        </button>
        {workspaceUrl && info.experiment_id && (
          <a
            href={`${workspaceUrl}/ml/experiments/${info.experiment_id}/runs/${info.run_id}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 text-xs text-blue-600 hover:text-blue-800"
          >
            Open in MLflow <ExternalLink className="w-3 h-3" />
          </a>
        )}
      </div>

      {/* Header card */}
      <Card>
        <CardContent className="pt-5 pb-4">
          <div className="flex items-start justify-between">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <Activity className="w-5 h-5 text-gray-400" />
                <h3 className="text-lg font-bold text-gray-900 dark:text-gray-100">{runName}</h3>
                {statusBadge(info.status)}
              </div>
              <div className="flex items-center gap-3 text-xs text-gray-500">
                <span className="font-mono">{info.run_id}</span>
                <CopyButton text={info.run_id} />
              </div>
            </div>
            <div className="text-right text-xs text-gray-500">
              {tsToDate(info.start_time)}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Metrics grid */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <MetricTile icon={Timer} label="Duration" value={dur > 0 ? msToReadable(dur) : '—'} />
        <MetricTile icon={User} label="User" value={mlflowUser} />
        <MetricTile icon={Server} label="Source" value={sourceType} sub={sourceName.length > 30 ? sourceName.substring(0, 30) + '…' : sourceName} />
        <MetricTile icon={Hash} label="Experiment" value={info.experiment_id || '—'} />
      </div>

      {/* Metrics section */}
      {metrics.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <BarChart3 className="w-4 h-4" /> Metrics ({metrics.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
              {metrics.map((m: any) => (
                <div key={m.key} className="bg-gray-50 dark:bg-gray-700 border border-gray-100 dark:border-gray-600 rounded-lg p-3">
                  <div className="text-[11px] text-gray-500 dark:text-gray-400 uppercase tracking-wide truncate" title={m.key}>
                    {m.key}
                  </div>
                  <div className="text-lg font-bold text-gray-900 dark:text-gray-100 mt-0.5">
                    {typeof m.value === 'number' ? Number(m.value).toFixed(4) : m.value}
                  </div>
                  {m.step !== undefined && m.step !== 0 && (
                    <div className="text-[10px] text-gray-400">Step {m.step}</div>
                  )}
                  {m.timestamp && (
                    <div className="text-[10px] text-gray-400">{tsToDate(m.timestamp)}</div>
                  )}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Parameters section */}
      {params.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <Tag className="w-4 h-4" /> Parameters ({params.length})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-1">
              {params.map((p: any) => (
                <MetadataRow key={p.key} label={p.key} value={p.value} copyable />
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Run info */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <FileText className="w-4 h-4" /> Run Information
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 text-sm">
              <MetadataRow label="Run ID" value={info.run_id} copyable />
              <MetadataRow label="Experiment ID" value={info.experiment_id} copyable />
              <MetadataRow label="Lifecycle" value={info.lifecycle_stage} />
              <MetadataRow label="Start Time" value={tsToDate(info.start_time)} />
              <MetadataRow label="End Time" value={tsToDate(info.end_time)} />
              <MetadataRow label="Artifact URI" value={info.artifact_uri} copyable />
              {clusterId && <MetadataRow label="Cluster ID" value={clusterId} copyable />}
              {jobId && <MetadataRow label="Job ID" value={jobId} />}
              {jobRunId && <MetadataRow label="Job Run ID" value={jobRunId} />}
            </div>
          </CardContent>
        </Card>

        {/* Model outputs */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <Package className="w-4 h-4" /> Model Outputs
            </CardTitle>
          </CardHeader>
          <CardContent>
            {modelOutputs.length > 0 ? (
              <div className="space-y-3">
                {modelOutputs.map((mo: any, i: number) => (
                  <div key={i} className="bg-gray-50 dark:bg-gray-700 border border-gray-100 dark:border-gray-600 rounded-lg p-3">
                    <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">Model {i + 1}</div>
                    <MetadataRow label="Model ID" value={mo.model_id} copyable />
                    {mo.step !== undefined && <MetadataRow label="Step" value={String(mo.step)} />}
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-sm text-gray-400 text-center py-6">No model outputs.</div>
            )}

            {/* Custom tags */}
            {customTags.length > 0 && (
              <div className="mt-4 pt-4 border-t border-gray-100 dark:border-gray-700">
                <div className="text-xs font-semibold text-gray-500 dark:text-gray-400 mb-2">Custom Tags</div>
                <div className="space-y-1">
                  {customTags.map((t: any) => (
                    <MetadataRow key={t.key} label={t.key} value={t.value} />
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* All tags (collapsible) */}
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">All Tags ({tags.length})</CardTitle>
            <button
              onClick={() => setShowAllTags(!showAllTags)}
              className="text-xs text-blue-600 hover:text-blue-800 flex items-center gap-1"
            >
              {showAllTags ? (
                <><ChevronDown className="w-3.5 h-3.5" /> Hide</>
              ) : (
                <><ChevronRight className="w-3.5 h-3.5" /> Show</>
              )}
            </button>
          </div>
        </CardHeader>
        {showAllTags && (
          <CardContent>
            <div className="space-y-1">
              {tags.map((t: any) => (
                <MetadataRow key={t.key} label={t.key} value={t.value} copyable />
              ))}
            </div>
          </CardContent>
        )}
      </Card>
    </div>
  )
}

/* ── Models Panel ────────────────────────────────────────────── */

function ModelsPanel({ workspaceUrl, workspaceId }: { workspaceUrl: string; workspaceId?: string }) {
  const { data: models, isLoading } = useMlflowModels(workspaceId)
  const modelList = models || []
  const [selectedModel, setSelectedModel] = useState<string | null>(null)
  const [modelPage, setModelPage] = useState(0)
  const [modelPageSize, setModelPageSize] = useState(10)

  if (selectedModel) {
    const model = modelList.find((m: any) => m.name === selectedModel)
    return (
      <ModelDetailView
        model={model}
        modelName={selectedModel}
        workspaceUrl={workspaceUrl}
        onBack={() => setSelectedModel(null)}
      />
    )
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <KpiCard title="Registered Models" value={modelList.length} format="number" />
        <KpiCard
          title="System Models"
          value={modelList.filter((m: any) => (m.name || '').startsWith('system.')).length}
          format="number"
        />
        <KpiCard
          title="Custom Models"
          value={modelList.filter((m: any) => !(m.name || '').startsWith('system.')).length}
          format="number"
        />
      </div>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Unity Catalog Model Registry</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="flex items-center justify-center py-12 text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading…
            </div>
          ) : modelList.length === 0 ? (
            <div className="text-sm text-gray-400 text-center py-12">No models found.</div>
          ) : (
            <>
            <div className="divide-y divide-gray-100 dark:divide-gray-700">
              {/* Header */}
              <div className="grid grid-cols-12 gap-3 py-2 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">
                <div className="col-span-1" />
                <div className="col-span-4">Model Name</div>
                <div className="col-span-2">Catalog</div>
                <div className="col-span-2">Aliases</div>
                <div className="col-span-1">Owner</div>
                <div className="col-span-2 text-right">Updated</div>
              </div>
              {(() => {
                const totalPages = Math.max(1, Math.ceil(modelList.length / modelPageSize))
                const safePage = Math.min(modelPage, totalPages - 1)
                return modelList.slice(safePage * modelPageSize, (safePage + 1) * modelPageSize)
              })().map((m: any) => {
                const parts = (m.name || '').split('.')
                const shortName = parts[parts.length - 1] || m.name
                const catalog = parts[0] || '—'
                const aliases = m.aliases || []
                return (
                  <div
                    key={m.name}
                    className="grid grid-cols-12 gap-3 py-2.5 items-center hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer rounded transition-colors"
                    onClick={() => setSelectedModel(m.name)}
                  >
                    <div className="col-span-1 flex justify-center">
                      <ChevronRight className="w-4 h-4 text-gray-400" />
                    </div>
                    <div className="col-span-4">
                      <div className="font-medium text-gray-900 dark:text-gray-100 text-sm">{shortName}</div>
                      <div className="text-xs text-gray-400 truncate">{m.name}</div>
                    </div>
                    <div className="col-span-2">
                      <Badge variant="default" className="text-xs">{catalog}</Badge>
                    </div>
                    <div className="col-span-2 flex gap-1 flex-wrap">
                      {aliases.map((a: any) => (
                        <Badge key={a.alias} variant="success" className="text-[10px]">
                          {a.alias}
                        </Badge>
                      ))}
                      {aliases.length === 0 && <span className="text-xs text-gray-400">—</span>}
                    </div>
                    <div className="col-span-1 text-xs text-gray-500 truncate">
                      {m.user_id || '—'}
                    </div>
                    <div className="col-span-2 text-xs text-gray-500 text-right">
                      {tsToDate(m.last_updated_timestamp)}
                    </div>
                  </div>
                )
              })}
            </div>
            <TablePagination page={modelPage} totalItems={modelList.length} pageSize={modelPageSize} onPageChange={setModelPage} onPageSizeChange={setModelPageSize} />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Model Detail View ──────────────────────────────────────── */

function ModelDetailView({
  model,
  modelName,
  workspaceUrl,
  onBack,
}: {
  model: any
  modelName: string
  workspaceUrl: string
  onBack: () => void
}) {
  const { data: versions, isLoading: versionsLoading } = useMlflowModelVersions(modelName)
  const [showFullDesc, setShowFullDesc] = useState(false)
  const versionList = versions || []

  const parts = modelName.split('.')
  const shortName = parts[parts.length - 1] || modelName
  const catalog = parts[0] || '—'
  const schema = parts[1] || '—'
  const aliases = model?.aliases || []
  const desc = model?.description || ''

  return (
    <div className="space-y-4">
      {/* Navigation */}
      <div className="flex items-center justify-between">
        <button onClick={onBack} className="inline-flex items-center gap-1.5 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 transition-colors">
          <ArrowLeft className="w-4 h-4" /> Back to models
        </button>
        {workspaceUrl && (
          <a
            href={`${workspaceUrl}/explore/data/models/${catalog}/${schema}/${shortName}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 text-xs text-blue-600 hover:text-blue-800"
          >
            Open in Unity Catalog <ExternalLink className="w-3 h-3" />
          </a>
        )}
      </div>

      {/* Header card */}
      <Card>
        <CardContent className="pt-5 pb-4">
          <div className="flex items-start justify-between">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <Database className="w-5 h-5 text-gray-400" />
                <h3 className="text-lg font-bold text-gray-900 dark:text-gray-100">{shortName}</h3>
                <Badge variant="default" className="text-xs">{catalog}</Badge>
              </div>
              <div className="flex items-center gap-3 text-xs text-gray-500">
                <span className="font-mono">{modelName}</span>
                <CopyButton text={modelName} />
              </div>
            </div>
            <div className="flex gap-1.5">
              {aliases.map((a: any) => (
                <Badge key={a.alias} variant="success" className="text-xs">
                  {a.alias} → v{a.version}
                </Badge>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Model info tiles */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <MetricTile icon={Package} label="Catalog" value={catalog} />
        <MetricTile icon={Layers} label="Schema" value={schema} />
        <MetricTile icon={User} label="Owner" value={model?.user_id || '—'} />
        <MetricTile icon={GitBranch} label="Versions" value={versionList.length || '…'} />
      </div>

      {/* Description */}
      {desc && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <FileText className="w-4 h-4" /> Description
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-sm text-gray-700 dark:text-gray-300 whitespace-pre-wrap leading-relaxed">
              {showFullDesc || desc.length <= 300 ? desc : desc.substring(0, 300) + '…'}
            </div>
            {desc.length > 300 && (
              <button
                onClick={() => setShowFullDesc(!showFullDesc)}
                className="text-xs text-blue-600 hover:text-blue-800 mt-2"
              >
                {showFullDesc ? 'Show less' : 'Show full description'}
              </button>
            )}
          </CardContent>
        </Card>
      )}

      {/* Model metadata */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base flex items-center gap-2">
            <Info className="w-4 h-4" /> Model Information
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2 text-sm">
            <MetadataRow label="Full Name" value={modelName} copyable />
            <MetadataRow label="Catalog" value={catalog} />
            <MetadataRow label="Schema" value={schema} />
            <MetadataRow label="Model Name" value={shortName} />
            <MetadataRow label="Created By" value={model?.user_id} />
            <MetadataRow label="Created" value={tsToDate(model?.creation_timestamp)} />
            <MetadataRow label="Last Updated" value={tsToDate(model?.last_updated_timestamp)} />
          </div>
        </CardContent>
      </Card>

      {/* Versions */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <GitBranch className="w-4 h-4" /> Versions ({versionList.length})
          </CardTitle>
        </CardHeader>
        <CardContent>
          {versionsLoading ? (
            <div className="flex items-center justify-center py-8 text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading versions…
            </div>
          ) : versionList.length === 0 ? (
            <div className="text-sm text-gray-400 text-center py-8">No versions found.</div>
          ) : (
            <div className="space-y-3">
              {versionList.map((v: any) => {
                const versionAliases = aliases.filter((a: any) => String(a.version) === String(v.version))
                return (
                  <ModelVersionCard
                    key={v.version}
                    version={v}
                    aliases={versionAliases}
                    workspaceUrl={workspaceUrl}
                    catalog={catalog}
                    schema={schema}
                    shortName={shortName}
                  />
                )
              })}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

/* ── Model Version Card ─────────────────────────────────────── */

function ModelVersionCard({
  version,
  aliases,
  workspaceUrl,
  catalog,
  schema,
  shortName,
}: {
  version: any
  aliases: any[]
  workspaceUrl: string
  catalog: string
  schema: string
  shortName: string
}) {
  const [expanded, setExpanded] = useState(false)
  const desc = version.description || ''

  return (
    <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
      <div
        className="flex items-center gap-3 p-3 hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        {expanded ? (
          <ChevronDown className="w-4 h-4 text-gray-400 flex-shrink-0" />
        ) : (
          <ChevronRight className="w-4 h-4 text-gray-400 flex-shrink-0" />
        )}
        <div className="flex items-center gap-2 flex-1 min-w-0">
          <span className="text-sm font-bold text-gray-900 dark:text-gray-100">v{version.version}</span>
          <Badge
            variant={version.status === 'READY' ? 'success' : 'default'}
            className="text-[10px]"
          >
            {version.status}
          </Badge>
          {aliases.map((a: any) => (
            <Badge key={a.alias} variant="success" className="text-[10px]">
              @{a.alias}
            </Badge>
          ))}
        </div>
        <div className="flex items-center gap-3 flex-shrink-0 text-xs text-gray-500">
          <span>{version.user_id}</span>
          <span>{tsToDate(version.creation_timestamp)}</span>
        </div>
      </div>

      {expanded && (
        <div className="px-4 pb-4 pt-1 border-t border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 space-y-3">
          {desc && (
            <div>
              <div className="text-xs font-semibold text-gray-500 mb-1">Description</div>
                <div className="text-xs text-gray-700 dark:text-gray-300 whitespace-pre-wrap leading-relaxed max-h-40 overflow-auto">
                {desc}
              </div>
            </div>
          )}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-1">
            <MetadataRow label="Version" value={String(version.version)} />
            <MetadataRow label="Status" value={version.status} />
            <MetadataRow label="Created By" value={version.user_id} />
            <MetadataRow label="Created" value={tsToDate(version.creation_timestamp)} />
            <MetadataRow label="Updated" value={tsToDate(version.last_updated_timestamp)} />
            {version.source && <MetadataRow label="Source" value={version.source} copyable />}
            {version.run_id && <MetadataRow label="Run ID" value={version.run_id} copyable />}
          </div>
          {workspaceUrl && (
            <a
              href={`${workspaceUrl}/explore/data/models/${catalog}/${schema}/${shortName}/version/${version.version}`}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1.5 text-xs text-blue-600 hover:text-blue-800"
            >
              Open version in Databricks <ExternalLink className="w-3 h-3" />
            </a>
          )}
        </div>
      )}
    </div>
  )
}
