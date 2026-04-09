interface DatabricksLogoProps {
  className?: string
  size?: number
}

/**
 * Official Databricks logomark loaded from /databricks-logo.svg (in public/).
 */
export default function DatabricksLogo({ className = '', size = 28 }: DatabricksLogoProps) {
  return (
    <img
      src="/databricks-logo.svg"
      alt="Databricks"
      width={size}
      height={size}
      className={className}
      style={{ objectFit: 'contain' }}
    />
  )
}
