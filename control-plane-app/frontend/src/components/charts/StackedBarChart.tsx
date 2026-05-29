import {
  BarChart as RechartsBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { LazyChart } from './LazyChart'
import { DB_GRID, DB_AXIS_TEXT, DB_COLORS } from '@/lib/brand'

interface StackedBarChartProps {
  /** Pre-pivoted data: one row per X category, with one numeric field per series. */
  data: Array<Record<string, any>>
  /** Field name for the X-axis category. */
  nameKey: string
  /** Series field names — each becomes a stacked segment. */
  series: string[]
  height?: number
  valueFormatter?: (v: any) => string
  tooltipFormatter?: (v: any) => string
}

export function StackedBarChart({
  data,
  nameKey,
  series,
  height = 300,
  valueFormatter,
  tooltipFormatter,
}: StackedBarChartProps) {
  const ttFmt = tooltipFormatter ?? valueFormatter
  return (
    <LazyChart height={height}>
      <ResponsiveContainer width="100%" height={height}>
        <RechartsBarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke={DB_GRID} />
          <XAxis dataKey={nameKey} tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} />
          <YAxis
            tick={{ fontSize: 12, fill: DB_AXIS_TEXT }}
            tickFormatter={valueFormatter}
            width={valueFormatter ? 60 : undefined}
          />
          <Tooltip
            cursor={{ fill: '#000', fillOpacity: 0.04 }}
            formatter={ttFmt ? (v: any) => ttFmt(v) : undefined}
            contentStyle={{
              borderRadius: 8,
              border: `1px solid ${DB_GRID}`,
              fontSize: 13,
              backgroundColor: 'var(--tooltip-bg, #fff)',
              color: 'var(--tooltip-text, #1f2937)',
            }}
          />
          <Legend wrapperStyle={{ fontSize: 13 }} />
          {series.map((s, i) => (
            <Bar
              key={s}
              dataKey={s}
              stackId="x"
              fill={DB_COLORS[i % DB_COLORS.length]}
              radius={i === series.length - 1 ? [4, 4, 0, 0] : 0}
            />
          ))}
        </RechartsBarChart>
      </ResponsiveContainer>
    </LazyChart>
  )
}
