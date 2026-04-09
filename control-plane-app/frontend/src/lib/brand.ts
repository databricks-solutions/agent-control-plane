/**
 * Databricks brand color palette for data visualizations.
 * Source: https://brand.databricks.com/
 */

// Primary brand color
export const DB_RED = '#FF3621'

// Full data-viz palette (ordered for categorical series)
export const DB_COLORS = [
  '#FF3621',  // Databricks Red (primary)
  '#1B3139',  // Navy
  '#00A972',  // Green / Teal
  '#FF5F46',  // Orange / Coral
  '#F2C94C',  // Gold / Yellow
  '#00B0D8',  // Teal / Cyan
  '#9B51E0',  // Purple
  '#E02E1B',  // Dark Red
] as const

// Semantic mapping for charts
export const DB_CHART = {
  primary: '#FF3621',
  secondary: '#1B3139',
  success: '#00A972',
  warning: '#F2C94C',
  error: '#E02E1B',
  info: '#00B0D8',
  accent: '#FF5F46',
  muted: '#9B51E0',
} as const

// Grid / axis
export const DB_GRID = '#E5E7EB'
export const DB_AXIS_TEXT = '#6B7280'

// Gradient helpers (for area/bar fills)
export const DB_RED_LIGHT = '#FF6B5A'
export const DB_RED_BG = 'rgba(255, 54, 33, 0.08)'

// Red shades for heatmap / bar visualisations (p25→max)
export const DB_RED_SHADES = {
  p25: '#FF3621',   // Databricks Red (darkest)
  p50: '#FF6B5A',   // Red-light
  p75: '#FF9E8F',   // Mid-light
  max: '#FFD6D0',   // Lightest
} as const
