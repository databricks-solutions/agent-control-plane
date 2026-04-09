import {
  PieChart as RechartsPieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { LazyChart } from './LazyChart'
import { DB_COLORS, DB_GRID } from '@/lib/brand'

interface PieChartProps {
  data: Array<{ name: string; value: number }>
  height?: number
}

export function PieChart({ data, height = 300 }: PieChartProps) {
  return (
    <LazyChart height={height}>
      <ResponsiveContainer width="100%" height={height}>
        <RechartsPieChart>
        <Pie
          data={data}
          cx="50%"
          cy="50%"
          labelLine={false}
          label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
          outerRadius={80}
          fill={DB_COLORS[0]}
          dataKey="value"
        >
          {data.map((_, index) => (
            <Cell key={`cell-${index}`} fill={DB_COLORS[index % DB_COLORS.length]} />
          ))}
        </Pie>
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
      </RechartsPieChart>
    </ResponsiveContainer>
    </LazyChart>
  )
}
