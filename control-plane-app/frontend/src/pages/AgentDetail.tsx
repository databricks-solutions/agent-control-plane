import { useParams } from 'react-router-dom'
import { useAgent, useAgentMetrics } from '@/api/hooks'
import { StatusBadge } from '@/components/StatusBadge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

// A metric with no source is null → "—", never a misleading 0.
function fmt(v: number | null | undefined, f: (v: number) => string): string {
  return v === null || v === undefined ? '—' : f(Number(v))
}

export default function AgentDetailPage() {
  const { agentId } = useParams<{ agentId: string }>()
  const { data: agent, isLoading } = useAgent(agentId || '')
  const { data: metrics } = useAgentMetrics(agentId || '', 168)

  if (isLoading) return <div>Loading agent details...</div>
  if (!agent) return <div>Agent not found</div>

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-3xl font-bold text-gray-900">{agent.name}</h2>
        <p className="mt-1 text-sm text-gray-500">{agent.description || "No description"}</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader><CardTitle>Basic Information</CardTitle></CardHeader>
          <CardContent className="space-y-2">
            <div><strong>Agent ID:</strong> {agent.agent_id}</div>
            <div><strong>Type:</strong> {agent.type}</div>
            <div><strong>Status:</strong> <StatusBadge status={agent.endpoint_status || "UNKNOWN"} /></div>
            <div><strong>Endpoint Type:</strong> {agent.endpoint_type || "N/A"}</div>
            <div><strong>Version:</strong> {agent.version || "N/A"}</div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Performance Metrics (last {metrics?.data?.window_days ?? 7} days)</CardTitle></CardHeader>
          <CardContent>
            {metrics?.data?.endpoint_name ? (
              <div className="space-y-2">
                <div><strong>Requests:</strong> {fmt(metrics.data.request_count, v => v.toLocaleString())}</div>
                <div><strong>Error Rate:</strong> {fmt(metrics.data.error_rate, v => `${v.toFixed(2)}%`)}</div>
                <div><strong>P50 Latency:</strong> {fmt(metrics.data.p50_latency, v => `${Math.round(v)} ms`)}</div>
                <div><strong>P95 Latency:</strong> {fmt(metrics.data.p95_latency, v => `${Math.round(v)} ms`)}</div>
                <div><strong>Serving Cost:</strong> {fmt(metrics.data.total_cost, v => `$${v.toFixed(2)}`)}</div>
                <p className="text-xs text-gray-500 pt-1">
                  Latency is only available for endpoints routed through Unity Gateway (30-day window).
                </p>
              </div>
            ) : (
              <div>No serving endpoint linked to this agent, so there are no usage metrics.</div>
            )}
          </CardContent>
        </Card>
      </div>

      {agent.config && (
        <Card>
          <CardHeader><CardTitle>Configuration</CardTitle></CardHeader>
          <CardContent>
            <pre className="bg-gray-100 p-4 rounded text-sm overflow-auto">
              {JSON.stringify(agent.config, null, 2)}
            </pre>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
