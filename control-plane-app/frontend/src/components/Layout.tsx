import { useState, Suspense } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import {
  Bot,
  Wrench,
  Waypoints,
  Shield,
  Eye,
  Database,
  UserCog,
  PanelLeftClose,
  PanelLeftOpen,
  Sun,
  Moon,
  Globe,
  ShieldAlert,
} from 'lucide-react'
import DatabricksLogo from './DatabricksLogo'
import { useTheme } from '@/context/ThemeContext'
import { useCurrentUser, useHealthStatus } from '@/api/hooks'
import AskGenieOverlay from './AskGenieOverlay'

const navItems = [
  { to: '/', label: 'Governance', icon: Shield, exact: true },
  { to: '/agents', label: 'Agents', icon: Bot },
  { to: '/ai-gateway', label: 'AI Gateway', icon: Waypoints },
  { to: '/vector-search', label: 'Knowledge Bases', icon: Database },
  { to: '/tools', label: 'Tools', icon: Wrench },
  { to: '/observability', label: 'Observability', icon: Eye },
  { to: '/workspaces', label: 'Workspaces', icon: Globe },
  { to: '/admin', label: 'Admin', icon: UserCog },
]

export default function Layout() {
  const [collapsed, setCollapsed] = useState(false)
  const { theme, toggleTheme } = useTheme()
  const isDark = theme === 'dark'
  const { data: user } = useCurrentUser()

  // Real connection health — /health/status checks DB connectivity every 30s.
  const { data: health, isError: healthError, isLoading: healthLoading } = useHealthStatus()
  const conn = healthError
    ? { label: 'Disconnected', dot: 'bg-red-500', wrap: 'bg-red-50 dark:bg-red-900/30 text-red-700 dark:text-red-400', pulse: false }
    : healthLoading && !health
    ? { label: 'Connecting…', dot: 'bg-gray-400', wrap: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300', pulse: true }
    : health?.status === 'healthy'
    ? { label: 'Connected', dot: 'bg-green-500', wrap: 'bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-400', pulse: true }
    : { label: 'Degraded', dot: 'bg-amber-500', wrap: 'bg-amber-50 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400', pulse: false }

  // Per-tab alert badges (none currently).
  const badges: Record<string, number> = {}

  return (
    <div className="flex h-screen bg-db-gray-50 dark:bg-gray-900">
      {/* Sidebar */}
      <aside
        className={`sidebar-transition bg-gray-100 dark:bg-gray-800 text-db-navy-900 dark:text-gray-100 flex flex-col flex-shrink-0 ${
          collapsed ? 'w-[68px]' : 'w-60'
        }`}
      >
        {/* Logo / Brand */}
        <div className="h-14 flex items-center px-2 border-b border-gray-200/60 dark:border-gray-700/60">
          <div className={`flex items-center gap-2.5 overflow-hidden ${collapsed ? 'justify-center w-full' : 'px-3'}`}>
            <DatabricksLogo size={18} />
            {!collapsed && (
              <span className="text-[15px] font-semibold tracking-tight whitespace-nowrap text-db-navy-900 dark:text-gray-100">
                Agent Control Plane
              </span>
            )}
          </div>
        </div>

        {/* Navigation */}
        <nav className="flex-1 px-2 py-3 space-y-0.5 overflow-y-auto">
          {navItems.map(({ to, label, icon: Icon, exact }) => {
            const badge = badges[to] ?? 0
            return (
              <NavLink
                key={to}
                to={to}
                end={exact}
                className={({ isActive }) =>
                  `group relative flex items-center gap-3 rounded-lg text-sm font-medium transition-colors
                  ${collapsed ? 'justify-center px-0 py-2.5' : 'px-3 py-2'}
                  ${
                    isActive
                      ? 'bg-db-red text-white shadow-sm shadow-db-red/30'
                      : 'text-db-navy-900/70 dark:text-gray-400 hover:bg-db-navy-900/8 dark:hover:bg-gray-700 hover:text-db-navy-900 dark:hover:text-gray-100'
                  }`
                }
              >
                <Icon className="w-[18px] h-[18px] flex-shrink-0" />
                {!collapsed && <span>{label}</span>}
                {/* Alert badge — pill when expanded, dot when collapsed.
                    Ring matches sidebar bg so it stays visible on the active red bg. */}
                {badge > 0 && (
                  collapsed ? (
                    <span
                      aria-label={`${badge} alerts`}
                      className="absolute top-1 right-1 w-2.5 h-2.5 bg-db-red rounded-full ring-2 ring-gray-100 dark:ring-gray-800"
                    />
                  ) : (
                    <span
                      aria-label={`${badge} alerts`}
                      className="ml-auto inline-flex items-center justify-center min-w-[20px] h-5 px-1.5 text-[10px] font-bold rounded-full bg-db-red text-white ring-2 ring-gray-100 dark:ring-gray-800"
                    >
                      {badge > 99 ? '99+' : badge}
                    </span>
                  )
                )}
                {/* Tooltip when collapsed */}
                {collapsed && <span className="sidebar-tooltip">{label}{badge > 0 ? ` (${badge})` : ''}</span>}
              </NavLink>
            )
          })}
        </nav>

        {/* Collapse toggle */}
        <div className="border-t border-db-navy-900/10 dark:border-gray-700">
          <button
            onClick={() => setCollapsed(!collapsed)}
            className="w-full flex items-center gap-3 px-3 py-3 text-db-navy-900/50 dark:text-gray-500 hover:text-db-navy-900 dark:hover:text-gray-200 hover:bg-db-navy-900/5 dark:hover:bg-gray-700 transition-colors"
          >
            {collapsed ? (
              <PanelLeftOpen className="w-[18px] h-[18px] mx-auto" />
            ) : (
              <>
                <PanelLeftClose className="w-[18px] h-[18px]" />
                <span className="text-xs">Collapse</span>
              </>
            )}
          </button>
        </div>

        {/* Footer */}
        {!collapsed && (
          <div className="px-3 py-2.5 border-t border-db-navy-900/10 dark:border-gray-700">
            <div className="text-[10px] text-db-navy-900/40 dark:text-gray-600 leading-tight">
              Powered by Databricks
            </div>
          </div>
        )}
      </aside>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top Bar */}
        <header className="h-14 bg-white dark:bg-gray-800 border-b border-gray-200/60 dark:border-gray-700/60 flex items-center justify-end px-6 flex-shrink-0 gap-3">
          {/* User identity */}
          {user && user.username !== 'anonymous' && (
            <div className="flex items-center gap-2 mr-auto">
              <div className="w-7 h-7 rounded-full bg-db-red/10 dark:bg-db-red/20 flex items-center justify-center text-db-red text-xs font-bold">
                {user.display_name?.charAt(0)?.toUpperCase() || '?'}
              </div>
              <div className="text-sm">
                <span className="font-medium text-db-navy-900 dark:text-gray-100">{user.display_name}</span>
                {user.is_account_admin ? (
                  <span
                    className="ml-1.5 px-1.5 py-0.5 text-[10px] font-semibold rounded bg-purple-100 dark:bg-purple-900/40 text-purple-700 dark:text-purple-400"
                    title="Account admin — unrestricted access across all workspaces"
                  >
                    Account Admin
                  </span>
                ) : user.is_admin ? (
                  <span
                    className="ml-1.5 px-1.5 py-0.5 text-[10px] font-semibold rounded bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-400"
                    title="Workspace admin — scoped to the workspaces you administer"
                  >
                    Workspace Admin
                  </span>
                ) : null}
              </div>
            </div>
          )}

          {/* Theme Toggle */}
          <button
            onClick={toggleTheme}
            aria-label={`Switch to ${isDark ? 'light' : 'dark'} mode`}
            className="inline-flex items-center gap-1.5 px-2 py-1 rounded-full text-xs font-medium
              bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300
              hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
          >
            {isDark ? (
              <>
                <Sun className="w-3.5 h-3.5" />
                <span>Light</span>
              </>
            ) : (
              <>
                <Moon className="w-3.5 h-3.5" />
                <span>Dark</span>
              </>
            )}
          </button>

          {/* Connection health badge — reflects the live /health/status check */}
          <span
            className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${conn.wrap}`}
            title={health?.timestamp ? `Last checked ${new Date(health.timestamp).toLocaleTimeString()}` : undefined}
          >
            <span className={`w-1.5 h-1.5 rounded-full ${conn.dot} ${conn.pulse ? 'animate-pulse' : ''}`} />
            {conn.label}
          </span>
        </header>

        {/* Page Content */}
        <main className="flex-1 overflow-y-auto p-6 bg-db-gray-50 dark:bg-gray-900">
          {/* Every data endpoint already enforces this server-side (see
              backend/utils/access_scope.py) — this is just the matching UI
              state so a caller with no workspace access sees an explicit
              message instead of a dashboard full of empty charts/tables. */}
          {user && user.username !== 'anonymous' && user.has_workspace_access === false ? (
            <NoWorkspaceAccess displayName={user.display_name} />
          ) : (
            <Suspense fallback={<PageLoading />}>
              <Outlet />
            </Suspense>
          )}
        </main>
      </div>

      {/* Floating chatbot — Ask Genie. Gated on FEATURE_GENIE_ENABLED;
          renders nothing when the flag is off so the bundle stays clean. */}
      <AskGenieOverlay />
    </div>
  )
}

function PageLoading() {
  return (
    <div className="h-full flex items-center justify-center py-20" role="status" aria-label="Loading">
      <span className="w-6 h-6 rounded-full border-2 border-gray-300 dark:border-gray-600 border-t-db-red animate-spin" />
    </div>
  )
}

function NoWorkspaceAccess({ displayName }: { displayName: string }) {
  return (
    <div className="h-full flex items-center justify-center">
      <div className="max-w-md text-center">
        <div className="mx-auto mb-4 w-12 h-12 rounded-full bg-amber-100 dark:bg-amber-900/40 flex items-center justify-center">
          <ShieldAlert className="w-6 h-6 text-amber-600 dark:text-amber-400" />
        </div>
        <h2 className="text-lg font-semibold text-db-navy-900 dark:text-gray-100 mb-1.5">
          No workspace access
        </h2>
        <p className="text-sm text-db-navy-900/60 dark:text-gray-400">
          Hi {displayName}, this dashboard only shows data for workspaces you
          administer. You aren't currently an admin of any workspace and
          aren't an account admin, so there's nothing to show. Ask an account
          admin to grant you workspace-admin access if you need to view this
          data.
        </p>
      </div>
    </div>
  )
}
