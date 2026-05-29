/** Compact + currency-aware number formatters used by Ask Genie and the
 *  shared chart components. Pure functions, no React. */

const COMPACT_BREAKPOINTS: Array<{ threshold: number; divisor: number; suffix: string }> = [
  { threshold: 1e12, divisor: 1e12, suffix: 'T' },
  { threshold: 1e9, divisor: 1e9, suffix: 'B' },
  { threshold: 1e6, divisor: 1e6, suffix: 'M' },
  { threshold: 1e3, divisor: 1e3, suffix: 'K' },
]

/** "1,376,017,533" → "1.38B" (or "$1.38B"). Below 1,000 falls back to a
 *  locale-formatted integer/decimal — no scientific notation. */
export function formatCompact(value: number, opts: { currency?: boolean; digits?: number } = {}): string {
  if (!Number.isFinite(value)) return String(value)
  const { currency = false, digits = 2 } = opts
  const prefix = currency ? '$' : ''
  const sign = value < 0 ? '-' : ''
  const abs = Math.abs(value)
  if (abs < 1000) {
    const s = Number.isInteger(abs)
      ? abs.toLocaleString()
      : abs.toLocaleString(undefined, { maximumFractionDigits: digits })
    return `${sign}${prefix}${s}`
  }
  for (const b of COMPACT_BREAKPOINTS) {
    if (abs >= b.threshold) {
      const scaled = abs / b.divisor
      // Drop trailing zeros: 1.40 -> 1.4
      const s = scaled.toLocaleString(undefined, {
        maximumFractionDigits: scaled >= 100 ? 0 : scaled >= 10 ? 1 : digits,
      })
      return `${sign}${prefix}${s}${b.suffix}`
    }
  }
  return `${sign}${prefix}${abs.toLocaleString()}`
}

/** Hint whether a column name implies currency. Cheap heuristic — matches
 *  cost/spend/usd/$/price/revenue/amount. */
export function isCurrencyColumn(name: string): boolean {
  return /\b(cost|spend|usd|dollar|price|revenue|amount|charge|bill)\b|\$/i.test(name)
}

/** Hint whether a column already holds a percentage value (0–100, not 0–1). */
export function isPercentColumn(name: string): boolean {
  return /\b(pct|percent|percentage|ratio)\b|_pct$|_percent$/i.test(name)
}

/** Full-precision tooltip value. Compact for the axis, full for hover. */
export function formatFull(value: number, opts: { currency?: boolean } = {}): string {
  if (!Number.isFinite(value)) return String(value)
  const prefix = opts.currency ? '$' : ''
  const s = Number.isInteger(value)
    ? value.toLocaleString()
    : value.toLocaleString(undefined, { maximumFractionDigits: 4 })
  return `${prefix}${s}`
}

/** Parse a Genie-returned date/timestamp value into a Date, or null if not
 *  parseable. Handles ISO ("2026-05-16T00:00:00.000Z"), date-only
 *  ("2026-05-16"), and millisecond epochs. Date-only strings are anchored at
 *  UTC midnight to avoid off-by-one in negative-UTC timezones. */
export function parseDateCell(v: any): Date | null {
  if (v == null) return null
  if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v
  if (typeof v === 'number') {
    const d = new Date(v)
    return Number.isNaN(d.getTime()) ? null : d
  }
  if (typeof v !== 'string') return null
  const s = v.trim()
  if (!s) return null
  // Date-only "YYYY-MM-DD" → anchor at UTC noon so all TZs see the same day
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const d = new Date(s + 'T12:00:00Z')
    return Number.isNaN(d.getTime()) ? null : d
  }
  const d = new Date(s)
  return Number.isNaN(d.getTime()) ? null : d
}

const MONTHS_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

/** "2026-05-16T00:00:00.000Z" → "May 16" (or "May 16, 2025" cross-year).
 *  Includes time only if the value has a non-midnight time component. */
export function formatDateShort(v: any): string {
  const d = parseDateCell(v)
  if (!d) return v == null ? '' : String(v)
  const now = new Date()
  const sameYear = d.getUTCFullYear() === now.getUTCFullYear()
  const month = MONTHS_SHORT[d.getUTCMonth()]
  const day = d.getUTCDate()
  const hasTime = d.getUTCHours() !== 0 || d.getUTCMinutes() !== 0 || d.getUTCSeconds() !== 0
  const base = sameYear ? `${month} ${day}` : `${month} ${day}, ${d.getUTCFullYear()}`
  if (!hasTime) return base
  const hh = String(d.getUTCHours()).padStart(2, '0')
  const mm = String(d.getUTCMinutes()).padStart(2, '0')
  return `${base} ${hh}:${mm}`
}

/** Full ISO string for tooltip / title attribute. */
export function formatDateFull(v: any): string {
  const d = parseDateCell(v)
  if (!d) return v == null ? '' : String(v)
  return d.toISOString().replace('T', ' ').replace(/\.\d+Z$/, ' UTC')
}

/** True for column type_text values that hold a date/timestamp. */
export function isDateColumn(typeText: string | undefined): boolean {
  if (!typeText) return false
  return /DATE|TIMESTAMP/i.test(typeText)
}

/** Build a value formatter from a column name + optional override. Useful
 *  when callers want a single function to pass to chart tickFormatter. */
export function makeValueFormatter(columnName: string | undefined, opts: { compact?: boolean; currency?: boolean } = {}) {
  const currency = opts.currency ?? (columnName ? isCurrencyColumn(columnName) : false)
  const percent = columnName ? isPercentColumn(columnName) : false
  const compact = opts.compact ?? true
  return (raw: any): string => {
    if (raw == null) return ''
    const n = typeof raw === 'number' ? raw : Number(raw)
    if (!Number.isFinite(n)) return String(raw)
    const formatted = compact ? formatCompact(n, { currency }) : formatFull(n, { currency })
    return percent ? `${formatted}%` : formatted
  }
}
