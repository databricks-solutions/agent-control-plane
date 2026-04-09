import { useParams } from 'react-router-dom'
import { useAgent, useAgentMetrics } from '@/api/hooks'
import { StatusBadge } from '@/components/StatusBadge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

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
          <CardHeader><CardTitle>Performance Metrics (24h)</CardTitle></CardHeader>
          <CardContent>
            {metrics?.data ? (
              <div className="space-y-2">
                <div><strong>Requests:</strong> {metrics.data.request_count || 0}</div>
                <div><strong>Avg Latency:</strong> {metrics.data.avg_latency?.toFixed(2) || "N/A"}ms</div>
                <div><strong>P95 Latency:</strong> {metrics.data.p95_latency?.toFixed(2) || "N/A"}ms</div>
                <div><strong>Error Rate:</strong> {metrics.data.error_rate?.toFixed(2) || "0"}%</div>
                <div><strong>Total Cost:</strong> ${metrics.data.total_cost?.toFixed(4) || "0.0000"}</div>
              </div>
            ) : (
              <div>No metrics available</div>
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
