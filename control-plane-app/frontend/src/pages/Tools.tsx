import { useState, useMemo } from 'react'
import {
  useToolsOverview,
  useMcpServers,
  useUcFunctions,
  useToolUsage,
  useSyncTools,
} from '@/api/hooks'
import { RefreshButton } from '@/components/RefreshButton'
import { SortableHeader, useSort, sortRows } from '@/components/SortableTable'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { KpiCard } from '@/components/KpiCard'
import { TablePagination } from '@/components/TablePagination'
import {
  Wrench,
  Server,
  Code2,
  Zap,
  Search,
  LayoutDashboard,
} from 'lucide-react'

type TabKey = 'overview' | 'mcp' | 'functions' | 'usage'

export default function ToolsPage() {
  const [tab, setTab] = useState<TabKey>('overview')
  const [searchQuery, setSearchQuery] = useState('')
  const [overviewPage, setOverviewPage] = useState(0)
  const [overviewPageSize, setOverviewPageSize] = useState(10)
  const [mcpPage, setMcpPage] = useState(0)
  const [mcpPageSize, setMcpPageSize] = useState(10)
  const [fnPage, setFnPage] = useState(0)
  const [fnPageSize, setFnPageSize] = useState(10)
  const [usagePage, setUsagePage] = useState(0)
  const [usagePageSize, setUsagePageSize] = useState(10)

  const { data: overview, isLoading: overviewLoading } = useToolsOverview()
  const { data: mcpServers, isLoading: mcpLoading } = useMcpServers()
  const { data: ucFunctions, isLoading: fnLoading } = useUcFunctions()
  const { data: toolUsage, isLoading: usageLoading } = useToolUsage(7)
  const syncTools = useSyncTools()
  const overviewSort = useSort<string>('name', 'asc')
  const mcpSort = useSort<string>('name', 'asc')
  const fnSort = useSort<string>('name', 'asc')
  const usageSort = useSort<string>('calls', 'desc')

  const q = searchQuery.toLowerCase().trim()

  // Unified tool list for Overview tab
  const allTools = useMemo(() => {
    const mcp = (mcpServers || []).map((s: any) => ({
      id: s.tool_id,
      name: s.name,
      kind: 'MCP Server',
      source: s.config?.url || s.endpoint_name || '',
      status: s.status || 'ACTIVE',
      description: s.description || '',
      sub_type: s.sub_type,
    }))
    const fns = (ucFunctions || []).map((f: any) => ({
      id: f.tool_id,
      name: f.name,
      kind: 'UC Function',
      source: [f.catalog_name, f.schema_name].filter(Boolean).join('.'),
      status: f.status || 'ACTIVE',
      description: f.description || '',
      sub_type: f.sub_type,
    }))
    return [...mcp, ...fns]
  }, [mcpServers, ucFunctions])

  const filteredAll = useMemo(() => {
    if (!q) return allTools
    return allTools.filter((t) =>
      t.name.toLowerCase().includes(q) ||
      t.kind.toLowerCase().includes(q) ||
      t.source.toLowerCase().includes(q) ||
      t.description.toLowerCase().includes(q),
    )
  }, [allTools, q])

  const filteredMcp = (mcpServers || []).filter((s: any) =>
    !q || s.name?.toLowerCase().includes(q) || s.config?.url?.toLowerCase().includes(q),
  )
  const filteredFn = (ucFunctions || []).filter((f: any) =>
    !q || f.name?.toLowerCase().includes(q) || f.catalog_name?.toLowerCase().includes(q) || f.schema_name?.toLowerCase().includes(q),
  )
  const filteredUsage = (toolUsage || []).filter((t: any) =>
    !q || t.tool_name?.toLowerCase().includes(q),
  )

  const tabs: { key: TabKey; label: string; icon: any }[] = [
    { key: 'overview',   label: 'Overview',     icon: LayoutDashboard },
    { key: 'mcp',        label: 'MCP Servers',   icon: Server },
    { key: 'functions',  label: 'UC Functions',  icon: Code2 },
    { key: 'usage',      label: 'Usage',         icon: Zap },
  ]

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Tools</h2>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            MCP servers, UC functions, and tool call analytics from agent traces
          </p>
        </div>
        <div className="flex items-center gap-3">
          <RefreshButton
            onRefresh={() => syncTools.mutate()}
            isPending={syncTools.isPending}
            isRefreshing={overview?.is_refreshing}
            lastSynced={overview?.last_refreshed ?? null}
            title="Refresh tools from UC connections and catalogs"
          />
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none" />
            <input
              type="text"
              placeholder="Search tools…"
              value={searchQuery}
              onChange={(e) => {
                setSearchQuery(e.target.value)
                setOverviewPage(0)
                setMcpPage(0)
                setFnPage(0)
                setUsagePage(0)
              }}
              className="pl-8 pr-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-800 dark:text-gray-200 focus:ring-2 focus:ring-db-red/30 focus:border-db-red w-56"
            />
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-gray-200 dark:border-gray-700">
        {tabs.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={`flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
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

      {/* TAB: Overview */}
      {tab === 'overview' && (
        <div className="space-y-6">
          {overviewLoading || mcpLoading || fnLoading ? (
            <div className="flex items-center justify-center h-32 text-gray-400 dark:text-gray-500">Loading…</div>
          ) : (
            <>
              {/* KPI row */}
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <KpiCard title="Total Tools" value={overview?.total_tools || 0} format="number" />
                <KpiCard title="MCP Servers" value={overview?.mcp_servers || 0} format="number" />
                <KpiCard title="UC Functions" value={overview?.uc_functions || 0} format="number" />
                <KpiCard title="Tool Calls (traces)" value={toolUsage?.length || 0} format="number" />
              </div>

              {/* All-tools table */}
              {(() => {
                const sorted = sortRows(filteredAll, overviewSort.sort, (r: any, k) => {
                  if (k === 'name') return r.name.toLowerCase()
                  if (k === 'kind') return r.kind.toLowerCase()
                  if (k === 'source') return r.source.toLowerCase()
                  if (k === 'status') return r.status.toLowerCase()
                  if (k === 'description') return r.description.toLowerCase()
                  return ''
                })
                const totalPages = Math.max(1, Math.ceil(sorted.length / overviewPageSize))
                const safePage = Math.min(overviewPage, totalPages - 1)
                const paged = sorted.slice(safePage * overviewPageSize, (safePage + 1) * overviewPageSize)

                return (
                  <Card>
                    <CardHeader className="pb-2">
                      <CardTitle className="text-base dark:text-gray-100">
                        All Tools
                        <span className="ml-2 text-xs font-normal text-gray-400">({filteredAll.length})</span>
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="pt-0">
                      {filteredAll.length === 0 ? (
                        <div className="py-12 text-center text-gray-400">
                          {q ? 'No tools match your search.' : 'No tools discovered. Click refresh to scan UC connections and catalogs.'}
                        </div>
                      ) : (
                        <>
                          <div className="overflow-x-auto">
                            <table className="w-full text-sm">
                              <thead>
                                <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                                  <SortableHeader label="Name" sortKey="name" current={overviewSort.sort} onToggle={overviewSort.toggle} />
                                  <SortableHeader label="Type" sortKey="kind" current={overviewSort.sort} onToggle={overviewSort.toggle} />
                                  <SortableHeader label="Source / Catalog" sortKey="source" current={overviewSort.sort} onToggle={overviewSort.toggle} />
                                  <SortableHeader label="Status" sortKey="status" current={overviewSort.sort} onToggle={overviewSort.toggle} />
                                  <SortableHeader label="Description" sortKey="description" current={overviewSort.sort} onToggle={overviewSort.toggle} />
                                </tr>
                              </thead>
                              <tbody>
                                {paged.map((t: any) => (
                                  <tr key={t.id} className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800/50">
                                    <td className="py-2.5">
                                      <div className="flex items-center gap-2">
                                        {t.kind === 'MCP Server'
                                          ? <Server className="w-4 h-4 text-gray-400 flex-shrink-0" />
                                          : <Code2 className="w-4 h-4 text-purple-500 flex-shrink-0" />}
                                        <span className="font-medium dark:text-gray-200">{t.name}</span>
                                      </div>
                                    </td>
                                    <td className="py-2.5">
                                      <Badge
                                        variant={t.kind === 'MCP Server' ? 'default' : 'info'}
                                        className="text-xs"
                                      >
                                        {t.kind}
                                      </Badge>
                                    </td>
                                    <td className="py-2.5 max-w-xs">
                                      {t.source ? (
                                        <span className="text-xs font-mono text-gray-500 dark:text-gray-400 truncate block" title={t.source}>
                                          {t.source.length > 50 ? t.source.slice(0, 50) + '…' : t.source}
                                        </span>
                                      ) : (
                                        <span className="text-gray-400 text-xs">—</span>
                                      )}
                                    </td>
                                    <td className="py-2.5">
                                      <Badge
                                        variant={t.status === 'ACTIVE' ? 'default' : 'info'}
                                        className="text-xs"
                                      >
                                        {t.status}
                                      </Badge>
                                    </td>
                                    <td className="py-2.5 text-xs text-gray-500 dark:text-gray-400 max-w-xs truncate">
                                      {t.description || '—'}
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                          <TablePagination
                            page={safePage}
                            totalItems={filteredAll.length}
                            pageSize={overviewPageSize}
                            onPageChange={setOverviewPage}
                            onPageSizeChange={setOverviewPageSize}
                          />
                        </>
                      )}
                    </CardContent>
                  </Card>
                )
              })()}
            </>
          )}
        </div>
      )}

      {/* TAB: MCP Servers */}
      {tab === 'mcp' && (() => {
        const sorted = sortRows(filteredMcp, mcpSort.sort, (r: any, k) => {
          if (k === 'name') return (r.name || '').toLowerCase()
          if (k === 'type') return (r.sub_type || '').toLowerCase()
          if (k === 'url') return (r.config?.url || r.endpoint_name || '').toLowerCase()
          if (k === 'owner') return (r.config?.owner || r.config?.creator || '').toLowerCase()
          if (k === 'status') return (r.status || '').toLowerCase()
          return ''
        })
        const mcpTotalPages = Math.max(1, Math.ceil(sorted.length / mcpPageSize))
        const safeMcpPage = Math.min(mcpPage, mcpTotalPages - 1)
        const pagedMcp = sorted.slice(safeMcpPage * mcpPageSize, (safeMcpPage + 1) * mcpPageSize)

        return (
          <div className="space-y-4">
            {mcpLoading ? (
              <div className="flex items-center justify-center h-32 text-gray-400 dark:text-gray-500">Loading…</div>
            ) : filteredMcp.length === 0 ? (
              <Card>
                <CardContent className="py-12 text-center text-gray-400">
                  {q ? 'No MCP servers match your search.' : 'No MCP servers discovered. Click refresh to scan UC connections and Apps.'}
                </CardContent>
              </Card>
            ) : (
              <Card>
                <CardContent className="pt-4">
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                          <SortableHeader label="Name" sortKey="name" current={mcpSort.sort} onToggle={mcpSort.toggle} />
                          <SortableHeader label="Type" sortKey="type" current={mcpSort.sort} onToggle={mcpSort.toggle} />
                          <SortableHeader label="URL / Host" sortKey="url" current={mcpSort.sort} onToggle={mcpSort.toggle} />
                          <SortableHeader label="Owner" sortKey="owner" current={mcpSort.sort} onToggle={mcpSort.toggle} />
                          <SortableHeader label="Status" sortKey="status" current={mcpSort.sort} onToggle={mcpSort.toggle} />
                        </tr>
                      </thead>
                      <tbody>
                        {pagedMcp.map((srv: any) => {
                          const url = srv.config?.url || srv.endpoint_name || ''
                          const owner = srv.config?.owner || srv.config?.creator || ''
                          const isApp = srv.sub_type === 'custom_app'
                          return (
                            <tr key={srv.tool_id} className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800/50">
                              <td className="py-2.5">
                                <div className="flex items-center gap-2">
                                  <Server className="w-4 h-4 text-gray-400" />
                                  <span className="font-medium">{srv.name}</span>
                                </div>
                              </td>
                              <td className="py-2.5">
                                <Badge variant={isApp ? 'info' : 'default'} className="text-xs">
                                  {isApp ? 'Custom (App)' : 'Managed'}
                                </Badge>
                              </td>
                              <td className="py-2.5 max-w-xs">
                                {url ? (
                                  <span className="text-xs font-mono text-gray-500 dark:text-gray-400 truncate block" title={url}>
                                    {url.length > 50 ? url.slice(0, 50) + '…' : url}
                                  </span>
                                ) : (
                                  <span className="text-gray-400 text-xs">—</span>
                                )}
                              </td>
                              <td className="py-2.5 text-xs text-gray-600 dark:text-gray-400">{owner || '—'}</td>
                              <td className="py-2.5">
                                <Badge variant={srv.status === 'ACTIVE' ? 'default' : 'info'} className="text-xs">
                                  {srv.status || '—'}
                                </Badge>
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                  <TablePagination page={safeMcpPage} totalItems={filteredMcp.length} pageSize={mcpPageSize} onPageChange={setMcpPage} onPageSizeChange={setMcpPageSize} />
                </CardContent>
              </Card>
            )}
          </div>
        )
      })()}

      {/* TAB: UC Functions */}
      {tab === 'functions' && (() => {
        const sorted = sortRows(filteredFn, fnSort.sort, (r: any, k) => {
          if (k === 'name') return (r.name || '').toLowerCase()
          if (k === 'catalog') return (r.catalog_name || '').toLowerCase()
          if (k === 'schema') return (r.schema_name || '').toLowerCase()
          if (k === 'type') return (r.sub_type || '').toLowerCase()
          if (k === 'description') return (r.description || '').toLowerCase()
          return ''
        })
        const fnTotalPages = Math.max(1, Math.ceil(sorted.length / fnPageSize))
        const safeFnPage = Math.min(fnPage, fnTotalPages - 1)
        const pagedFn = sorted.slice(safeFnPage * fnPageSize, (safeFnPage + 1) * fnPageSize)

        return (
          <div className="space-y-4">
            {fnLoading ? (
              <div className="flex items-center justify-center h-32 text-gray-400 dark:text-gray-500">Loading…</div>
            ) : filteredFn.length === 0 ? (
              <Card>
                <CardContent className="py-12 text-center text-gray-400">
                  {q ? 'No UC functions match your search.' : 'No UC functions discovered. Click refresh to scan catalogs.'}
                </CardContent>
              </Card>
            ) : (
              <Card>
                <CardContent className="pt-4">
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                          <SortableHeader label="Function Name" sortKey="name" current={fnSort.sort} onToggle={fnSort.toggle} />
                          <SortableHeader label="Catalog" sortKey="catalog" current={fnSort.sort} onToggle={fnSort.toggle} />
                          <SortableHeader label="Schema" sortKey="schema" current={fnSort.sort} onToggle={fnSort.toggle} />
                          <SortableHeader label="Type" sortKey="type" current={fnSort.sort} onToggle={fnSort.toggle} />
                          <SortableHeader label="Description" sortKey="description" current={fnSort.sort} onToggle={fnSort.toggle} />
                        </tr>
                      </thead>
                      <tbody>
                        {pagedFn.map((fn: any) => (
                          <tr key={fn.tool_id} className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800/50">
                            <td className="py-2.5">
                              <div className="flex items-center gap-2">
                                <Code2 className="w-4 h-4 text-purple-500" />
                                <span className="font-medium">{fn.name}</span>
                              </div>
                            </td>
                            <td className="py-2.5 font-mono text-xs">{fn.catalog_name}</td>
                            <td className="py-2.5 font-mono text-xs">{fn.schema_name}</td>
                            <td className="py-2.5">
                              <Badge variant="default" className="text-xs">{fn.sub_type || 'FUNCTION'}</Badge>
                            </td>
                            <td className="py-2.5 text-xs text-gray-500 dark:text-gray-400 max-w-xs truncate">{fn.description || '—'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <TablePagination page={safeFnPage} totalItems={filteredFn.length} pageSize={fnPageSize} onPageChange={setFnPage} onPageSizeChange={setFnPageSize} />
                </CardContent>
              </Card>
            )}
          </div>
        )
      })()}

      {/* TAB: Usage */}
      {tab === 'usage' && (() => {
        const sorted = sortRows(filteredUsage, usageSort.sort, (r: any, k) => {
          if (k === 'tool_name') return (r.tool_name || '').toLowerCase()
          if (k === 'span_type') return (r.span_type || '').toLowerCase()
          if (k === 'calls') return Number(r.call_count || 0)
          if (k === 'errors') return Number(r.error_count || 0)
          if (k === 'error_rate') return Number(r.error_rate || 0)
          if (k === 'avg_latency') return Number(r.avg_latency_ms || 0)
          return ''
        })
        const usageTotalPages = Math.max(1, Math.ceil(sorted.length / usagePageSize))
        const safeUsagePage = Math.min(usagePage, usageTotalPages - 1)
        const pagedUsage = sorted.slice(safeUsagePage * usagePageSize, (safeUsagePage + 1) * usagePageSize)

        return (
          <div className="space-y-4">
            {usageLoading ? (
              <div className="flex items-center justify-center h-32 text-gray-400 dark:text-gray-500">Loading…</div>
            ) : filteredUsage.length === 0 ? (
              <Card>
                <CardContent className="py-12 text-center text-gray-400">
                  {q ? 'No tools match your search.' : 'No tool call data from MLflow traces yet.'}
                </CardContent>
              </Card>
            ) : (
              <Card>
                <CardHeader className="pb-2"><CardTitle className="text-base dark:text-gray-100">Tool Call Frequency &amp; Latency</CardTitle></CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                          <SortableHeader label="Tool Name" sortKey="tool_name" current={usageSort.sort} onToggle={usageSort.toggle} />
                          <SortableHeader label="Span Type" sortKey="span_type" current={usageSort.sort} onToggle={usageSort.toggle} />
                          <SortableHeader label="Calls" sortKey="calls" current={usageSort.sort} onToggle={usageSort.toggle} align="right" />
                          <SortableHeader label="Errors" sortKey="errors" current={usageSort.sort} onToggle={usageSort.toggle} align="right" />
                          <SortableHeader label="Error Rate" sortKey="error_rate" current={usageSort.sort} onToggle={usageSort.toggle} align="right" />
                          <SortableHeader label="Avg Latency" sortKey="avg_latency" current={usageSort.sort} onToggle={usageSort.toggle} align="right" />
                        </tr>
                      </thead>
                      <tbody>
                        {pagedUsage.map((t: any) => (
                          <tr key={t.tool_name} className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800/50">
                            <td className="py-2.5">
                              <div className="flex items-center gap-2">
                                <Wrench className="w-4 h-4 text-gray-400" />
                                <span className="font-medium">{t.tool_name}</span>
                              </div>
                            </td>
                            <td className="py-2.5">
                              <Badge variant="default" className="text-xs">{t.span_type}</Badge>
                            </td>
                            <td className="py-2.5 text-right font-semibold">{t.call_count}</td>
                            <td className="py-2.5 text-right">{t.error_count}</td>
                            <td className="py-2.5 text-right">
                              <span className={t.error_rate > 5 ? 'text-red-600 font-medium' : ''}>
                                {t.error_rate}%
                              </span>
                            </td>
                            <td className="py-2.5 text-right">{t.avg_latency_ms}ms</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <TablePagination page={safeUsagePage} totalItems={filteredUsage.length} pageSize={usagePageSize} onPageChange={setUsagePage} onPageSizeChange={setUsagePageSize} />
                </CardContent>
              </Card>
            )}
          </div>
        )
      })()}
    </div>
  )
}
