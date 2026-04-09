import { useState } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import {
  Bot,
  Wrench,
  Waypoints,
  Shield,
  Eye,
  UserCog,
  PanelLeftClose,
  PanelLeftOpen,
  Sun,
  Moon,
  Globe,
} from 'lucide-react'
import DatabricksLogo from './DatabricksLogo'
import { useTheme } from '@/context/ThemeContext'
import { useCurrentUser } from '@/api/hooks'

const navItems = [
  { to: '/', label: 'Governance', icon: Shield, exact: true },
  { to: '/agents', label: 'Agents', icon: Bot },
  { to: '/tools', label: 'Tools', icon: Wrench },
  { to: '/ai-gateway', label: 'AI Gateway', icon: Waypoints },
  { to: '/observability', label: 'Observability', icon: Eye },
  { to: '/workspaces', label: 'Workspaces', icon: Globe },
  { to: '/admin', label: 'Admin', icon: UserCog },
]

export default function Layout() {
  const [collapsed, setCollapsed] = useState(false)
  const { theme, toggleTheme } = useTheme()
  const isDark = theme === 'dark'
  const { data: user } = useCurrentUser()

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
          {navItems.map(({ to, label, icon: Icon, exact }) => (
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
              {/* Tooltip when collapsed */}
              {collapsed && <span className="sidebar-tooltip">{label}</span>}
            </NavLink>
          ))}
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
                {user.is_admin && (
                  <span className="ml-1.5 px-1.5 py-0.5 text-[10px] font-semibold rounded bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-400">
                    Admin
                  </span>
                )}
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

          {/* Connected badge */}
          <span className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-400 rounded-full text-xs font-medium">
            <span className="w-1.5 h-1.5 bg-green-500 rounded-full animate-pulse" />
            Connected
          </span>
        </header>

        {/* Page Content */}
        <main className="flex-1 overflow-y-auto p-6 bg-db-gray-50 dark:bg-gray-900">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
