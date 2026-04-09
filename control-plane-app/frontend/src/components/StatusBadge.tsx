import { Badge } from "./ui/badge"
import { STATUS_COLORS } from "@/lib/constants"

interface StatusBadgeProps {
  status: string
  className?: string
}

export function StatusBadge({ status, className }: StatusBadgeProps) {
  const statusUpper = status.toUpperCase()
  
  return (
    <Badge
      variant={
        statusUpper === "ONLINE" ? "success" :
        statusUpper === "OFFLINE" ? "error" :
        statusUpper === "PROVISIONING" ? "warning" :
        "default"
      }
      className={className}
    >
      {status}
    </Badge>
  )
}
