/** @type {import('tailwindcss').Config} */
export default {
  darkMode: 'class',
  content: [
    "./frontend/index.html",
    "./frontend/src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Databricks brand palette
        db: {
          red: '#FF3621',        // Primary Databricks red
          'red-dark': '#E02E1B',
          'red-light': '#FF6B5A',
          orange: '#FF5F46',
          navy: '#1B3139',       // Dark sidebar / headers
          'navy-light': '#243B44',
          'navy-900': '#0B2026',  // Navy 900 — sidebar text
          'oat-medium': '#EEEDE9', // Oat Medium
          'oat-light': '#F9F7F4', // Oat Light — sidebar bg
          green: '#00A972',      // Databricks green/teal
          yellow: '#F2C94C',     // Databricks gold
          teal: '#00B0D8',       // Databricks teal/cyan
          purple: '#9B51E0',     // Databricks purple
          charcoal: '#2D2D2D',
          slate: '#3C4043',
          'gray-900': '#1B1B1B',
          'gray-800': '#2D3748',
          'gray-600': '#6B7280',
          'gray-100': '#F7F8FA',
          'gray-50': '#FAFBFC',
          white: '#FFFFFF',
        },
      },
      fontFamily: {
        sans: ['"DM Sans"', 'Inter', 'system-ui', '-apple-system', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
