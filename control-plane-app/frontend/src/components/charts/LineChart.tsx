import {
  LineChart as RechartsLineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { format } from 'date-fns'
import { LazyChart } from './LazyChart'
import { DB_RED, DB_GRID, DB_AXIS_TEXT, DB_COLORS } from '@/lib/brand'

interface LineChartProps {
  data: Array<{ timestamp: string; value: number; label?: string }>
  /** Extra series keyed by field name → display label */
  series?: Record<string, string>
  dataKey?: string
  name?: string
  color?: string
  height?: number
  /** Formatter for Y-axis ticks (compact, e.g. 1.38B). */
  valueFormatter?: (v: any) => string
  /** Formatter for tooltip values (full precision, defaults to valueFormatter). */
  tooltipFormatter?: (v: any) => string
}

export function LineChart({
  data,
  series,
  dataKey = 'value',
  name = 'Value',
  color,
  height = 300,
  valueFormatter,
  tooltipFormatter,
}: LineChartProps) {
  const chartData = data.map((item) => ({
    ...item,
    time: format(new Date(item.timestamp), 'MMM dd'),
  }))

  const mainColor = color ?? DB_RED
  const ttFmt = tooltipFormatter ?? valueFormatter

  return (
    <LazyChart height={height}>
      <ResponsiveContainer width="100%" height={height}>
        <RechartsLineChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke={DB_GRID} />
          <XAxis dataKey="time" tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} />
          <YAxis tick={{ fontSize: 12, fill: DB_AXIS_TEXT }} tickFormatter={valueFormatter} width={valueFormatter ? 60 : undefined} />
          <Tooltip
            cursor={{ stroke: DB_GRID, strokeWidth: 1 }}
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
          <Line
            type="monotone"
            dataKey={dataKey}
            stroke={mainColor}
            strokeWidth={2}
            dot={{ r: 3, fill: mainColor }}
            activeDot={{ r: 5, stroke: mainColor, strokeWidth: 2, fill: '#fff' }}
            name={name}
          />
          {series &&
            Object.entries(series).map(([key, label], idx) => (
              <Line
                key={key}
                type="monotone"
                dataKey={key}
                stroke={DB_COLORS[(idx + 1) % DB_COLORS.length]}
                strokeWidth={2}
                dot={{ r: 3, fill: DB_COLORS[(idx + 1) % DB_COLORS.length] }}
                name={label}
              />
            ))}
        </RechartsLineChart>
      </ResponsiveContainer>
    </LazyChart>
  )
}
