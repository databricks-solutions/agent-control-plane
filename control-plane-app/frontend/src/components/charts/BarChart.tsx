import {
  BarChart as RechartsBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { LazyChart } from './LazyChart'
import { DB_RED, DB_GRID, DB_AXIS_TEXT, DB_COLORS } from '@/lib/brand'

interface BarChartProps {
  data: Array<Record<string, any>>
  dataKey: string
  nameKey: string
  color?: string
  /** If true, each bar gets a unique color from the Databricks palette */
  multiColor?: boolean
  height?: number
}

export function BarChart({
  data,
  dataKey,
  nameKey,
  color,
  multiColor = false,
  height = 300,
}: BarChartProps) {
  const mainColor = color ?? DB_RED

  return (
    <LazyChart height={height}>
      <ResponsiveContainer width="100%" height={height}>
        <RechartsBarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke={DB_GRID} />
          <XAxis dataKey={nameKey} tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} />
          <YAxis tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} />
          <Tooltip
            contentStyle={{
              borderRadius: 8,
              border: `1px solid ${DB_GRID}`,
              fontSize: 13,
              backgroundColor: 'var(--tooltip-bg, #fff)',
              color: 'var(--tooltip-text, #1f2937)',
            }}
          />
          <Legend wrapperStyle={{ fontSize: 13 }} />
          <Bar dataKey={dataKey} fill={mainColor} radius={[4, 4, 0, 0]}>
            {multiColor &&
              data.map((_, index) => (
                <Cell key={`cell-${index}`} fill={DB_COLORS[index % DB_COLORS.length]} />
              ))}
          </Bar>
        </RechartsBarChart>
      </ResponsiveContainer>
    </LazyChart>
  )
}
