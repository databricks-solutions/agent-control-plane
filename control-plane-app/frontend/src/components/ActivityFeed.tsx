import { useRecentRequests } from "@/api/hooks"
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card"
import { format } from "date-fns"
import { formatDuration } from "@/lib/utils"

export function ActivityFeed() {
  const { data, isLoading } = useRecentRequests(20)

  if (isLoading) {
    return <div>Loading activity...</div>
  }

  const requests = data?.data || []

  return (
    <Card>
      <CardHeader>
        <CardTitle>Recent Activity</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-2 max-h-96 overflow-y-auto">
          {requests.map((req: any) => (
            <div key={req.request_id} className="flex items-center justify-between p-2 border-b">
              <div className="flex-1">
                <div className="text-sm font-medium">{req.agent_id || "Unknown Agent"}</div>
                <div className="text-xs text-gray-500">
                  {format(new Date(req.timestamp), "MMM dd, HH:mm:ss")}
                </div>
              </div>
              <div className="text-right">
                {req.latency_ms && (
                  <div className="text-sm">{formatDuration(req.latency_ms)}</div>
                )}
                <div className={`text-xs ${req.status_code >= 400 ? "text-red-600" : "text-green-600"}`}>
                  {req.status_code || "N/A"}
                </div>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}
