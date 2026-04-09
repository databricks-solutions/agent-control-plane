import { Card, CardContent, CardHeader, CardTitle } from "./ui/card"
import { formatNumber, formatCurrency, formatDuration } from "@/lib/utils"

interface KpiCardProps {
  title: string
  value: number | string
  unit?: string
  trend?: {
    value: number
    direction: "up" | "down" | "stable"
  }
  format?: "number" | "currency" | "duration" | "percentage"
}

export function KpiCard({ title, value, unit, trend, format = "number" }: KpiCardProps) {
  const formatValue = (val: number | string) => {
    if (typeof val === "string") return val
    switch (format) {
      case "currency":
        return formatCurrency(val)
      case "duration":
        return formatDuration(val)
      case "percentage":
        return `${val.toFixed(2)}%`
      default:
        return formatNumber(val)
    }
  }

  const trendColor = trend?.direction === "up" ? "text-green-600 dark:text-green-400" : trend?.direction === "down" ? "text-red-600 dark:text-red-400" : "text-gray-600 dark:text-gray-400"
  const trendIcon = trend?.direction === "up" ? "↑" : trend?.direction === "down" ? "↓" : "→"

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium dark:text-gray-300">{title}</CardTitle>
        {trend && (
          <span className={`text-xs ${trendColor}`}>
            {trendIcon} {Math.abs(trend.value).toFixed(1)}%
          </span>
        )}
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold dark:text-gray-100">
          {formatValue(value)}
          {unit && <span className="text-sm font-normal text-gray-500 dark:text-gray-400 ml-1">{unit}</span>}
        </div>
      </CardContent>
    </Card>
  )
}
