import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useAgents, usePerformanceMetrics, useUsageMetrics, useHealthMetrics } from '@/api/hooks'
import { SortableHeader, useSort, sortRows } from '@/components/SortableTable'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StatusBadge } from '@/components/StatusBadge'
import { KpiCard } from '@/components/KpiCard'
import { TablePagination } from '@/components/TablePagination'
import { LineChart } from '@/components/charts/LineChart'
import { BarChart } from '@/components/charts/BarChart'
import { DB_CHART } from '@/lib/brand'

export default function OperationsPage() {
  const [days, setDays] = useState(30)
  const [agentPage, setAgentPage] = useState(0)
  const [agentPageSize, setAgentPageSize] = useState(10)
  const agentSort = useSort<string>('name', 'asc')
  const { data: agents } = useAgents()
  const { data: perf } = usePerformanceMetrics(days)
  const { data: usage } = useUsageMetrics(days)
  const { data: health } = useHealthMetrics()

  const getHealthColor = (agent: any) => {
    const er = Number(agent.error_rate || 0)
    const lat = Number(agent.avg_latency || 0)
    const st = agent.endpoint_status
    if (st !== 'ONLINE') return 'border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-700'
    if (er > 5 || lat > 5000) return 'border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-700'
    if (er > 1 || lat > 2000) return 'border-yellow-300 bg-yellow-50 dark:bg-yellow-900/20 dark:border-yellow-700'
    return 'border-green-300 bg-green-50 dark:bg-green-900/20 dark:border-green-700'
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Operations</h2>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">Performance metrics, uptime, health & status of all agents</p>
        </div>
        <select value={days} onChange={(e) => setDays(Number(e.target.value))} className="border dark:border-gray-600 rounded-lg px-3 py-2 text-sm bg-white dark:bg-gray-800 dark:text-gray-200">
          <option value={7}>Last 7 days</option>
          <option value={30}>Last 30 days</option>
          <option value={90}>Last 90 days</option>
        </select>
      </div>

      {/* Health Cards */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Agent Health Status</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {health?.agent_health?.map((a: any) => (
              <div key={a.agent_id} className={`p-3 border rounded-lg ${getHealthColor(a)}`}>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm font-semibold dark:text-gray-100">{a.name}</span>
                  <StatusBadge status={a.endpoint_status} />
                </div>
                <div className="grid grid-cols-3 gap-2 text-xs text-gray-600 dark:text-gray-300 mt-2">
                  <div>
                    <div className="text-gray-400 dark:text-gray-500">Requests</div>
                    <div className="font-medium">{a.request_count || 0}</div>
                  </div>
                  <div>
                    <div className="text-gray-400 dark:text-gray-500">Error Rate</div>
                    <div className="font-medium">{Number(a.error_rate || 0).toFixed(1)}%</div>
                  </div>
                  <div>
                    <div className="text-gray-400 dark:text-gray-500">Avg Latency</div>
                    <div className="font-medium">{a.avg_latency ? `${Number(a.avg_latency).toFixed(0)}ms` : '—'}</div>
                  </div>
                </div>
              </div>
            ))}
            {(!health?.agent_health || health.agent_health.length === 0) && (
              <div className="col-span-3 text-sm text-gray-400 text-center py-6">No health data available</div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Performance Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader><CardTitle className="text-base">Response Time Trend</CardTitle></CardHeader>
          <CardContent>
            {perf?.response_time_series?.length ? (
              <LineChart data={perf.response_time_series} name="Avg Latency (ms)" color={DB_CHART.info} />
            ) : <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>}
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">Request Volume</CardTitle></CardHeader>
          <CardContent>
            {usage?.usage_over_time?.length ? (
              <LineChart data={usage.usage_over_time} name="Requests" color={DB_CHART.primary} />
            ) : <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>}
          </CardContent>
        </Card>
      </div>

      {/* Latency by Agent Type + Error Rate */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader><CardTitle className="text-base">Latency by Agent Type</CardTitle></CardHeader>
          <CardContent>
            {perf?.response_time_by_agent_type && Object.keys(perf.response_time_by_agent_type).length ? (
              <BarChart
                data={Object.entries(perf.response_time_by_agent_type).map(([name, value]) => ({ name, value: Number(value) }))}
                dataKey="value" nameKey="name" multiColor
              />
            ) : <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>}
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-base">Error Rate Trend</CardTitle></CardHeader>
          <CardContent>
            {health?.error_rate_trend?.length ? (
              <LineChart data={health.error_rate_trend} name="Error Rate (%)" color={DB_CHART.error} />
            ) : <div className="text-gray-400 dark:text-gray-500 text-center py-12">No data</div>}
          </CardContent>
        </Card>
      </div>

      {/* Agent Inventory */}
      <Card>
        <CardHeader><CardTitle className="text-base">Agent Inventory</CardTitle></CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
                  <SortableHeader label="Name" sortKey="name" current={agentSort.sort} onToggle={agentSort.toggle} />
                  <SortableHeader label="Type" sortKey="type" current={agentSort.sort} onToggle={agentSort.toggle} />
                  <SortableHeader label="Endpoint" sortKey="endpoint" current={agentSort.sort} onToggle={agentSort.toggle} />
                  <SortableHeader label="Status" sortKey="status" current={agentSort.sort} onToggle={agentSort.toggle} />
                  <SortableHeader label="Version" sortKey="version" current={agentSort.sort} onToggle={agentSort.toggle} />
                  <th className="pb-2 font-medium">Details</th>
                </tr>
              </thead>
              <tbody>
                {(() => {
                  const sorted = sortRows(agents || [], agentSort.sort, (r: any, k) => {
                    if (k === 'name') return (r.name || '').toLowerCase()
                    if (k === 'type') return (r.type || '').toLowerCase()
                    if (k === 'endpoint') return (r.endpoint_name || r.app_url || '').toLowerCase()
                    if (k === 'status') return (r.endpoint_status || '').toLowerCase()
                    if (k === 'version') return (r.version || '').toLowerCase()
                    return ''
                  })
                  const totalPages = Math.max(1, Math.ceil(sorted.length / agentPageSize))
                  const safePage = Math.min(agentPage, totalPages - 1)
                  return sorted.slice(safePage * agentPageSize, (safePage + 1) * agentPageSize)
                })().map((a: any) => (
                  <tr key={a.agent_id} className="border-b border-gray-100 dark:border-gray-700">
                    <td className="py-2.5 font-medium dark:text-gray-200">{a.name}</td>
                    <td className="py-2.5 text-gray-500 dark:text-gray-400">{a.type}</td>
                    <td className="py-2.5 text-gray-500 dark:text-gray-400 text-xs font-mono">{a.endpoint_name || a.app_url || '—'}</td>
                    <td className="py-2.5"><StatusBadge status={a.endpoint_status || 'UNKNOWN'} /></td>
                    <td className="py-2.5 text-gray-500 dark:text-gray-400">{a.version || '—'}</td>
                    <td className="py-2.5">
                      <Link to={`/operations/agents/${a.agent_id}`} className="text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 text-xs">
                        View
                      </Link>
                    </td>
                  </tr>
                ))}
                {(!agents || agents.length === 0) && (
                  <tr><td colSpan={6} className="py-8 text-center text-gray-400">No agents registered</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <TablePagination page={agentPage} totalItems={(agents || []).length} pageSize={agentPageSize} onPageChange={setAgentPage} onPageSizeChange={setAgentPageSize} />
        </CardContent>
      </Card>
    </div>
  )
}
