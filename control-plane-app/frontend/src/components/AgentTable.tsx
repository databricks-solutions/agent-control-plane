import { useAgents } from "@/api/hooks"
import { StatusBadge } from "./StatusBadge"
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card"

export function AgentTable() {
  const { data: agents, isLoading } = useAgents()

  if (isLoading) {
    return <div>Loading agents...</div>
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Agents</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full border-collapse">
            <thead>
              <tr className="border-b">
                <th className="text-left p-2">Name</th>
                <th className="text-left p-2">Type</th>
                <th className="text-left p-2">Status</th>
                <th className="text-left p-2">Endpoint Type</th>
              </tr>
            </thead>
            <tbody>
              {agents?.map((agent: any) => (
                <tr key={agent.agent_id} className="border-b hover:bg-gray-50">
                  <td className="p-2">{agent.name}</td>
                  <td className="p-2">{agent.type}</td>
                  <td className="p-2">
                    <StatusBadge status={agent.endpoint_status || "UNKNOWN"} />
                  </td>
                  <td className="p-2">{agent.endpoint_type || "N/A"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  )
}
