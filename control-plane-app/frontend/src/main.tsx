import React, { lazy } from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import './index.css'

import { ThemeProvider } from './context/ThemeContext'
import ErrorBoundary from './components/ErrorBoundary'
import Layout from './components/Layout'

// Route-level code splitting: each page ships as its own chunk, loaded on
// demand, so the initial bundle stays small. Layout (the app shell) is eager
// and provides the Suspense fallback around <Outlet /> while a chunk loads.
const AgentsPage = lazy(() => import('./pages/Agents'))
const AIGatewayPage = lazy(() => import('./pages/AIGateway'))
const GovernancePage = lazy(() => import('./pages/Governance'))
const ObservabilityPage = lazy(() => import('./pages/Observability'))
const AgentDetailPage = lazy(() => import('./pages/AgentDetail'))
const AdminPage = lazy(() => import('./pages/Admin'))
const ToolsPage = lazy(() => import('./pages/Tools'))
const WorkspacesPage = lazy(() => import('./pages/Workspaces'))
const VectorSearchPage = lazy(() => import('./pages/VectorSearch'))

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      refetchOnMount: 'always',   // refetch when a component mounts (tab change)
      staleTime: 120_000,         // 2 min — matches workflow refresh cadence (15 min)
      retry: 1,
    },
  },
})

const rootElement = document.getElementById('root')
if (!rootElement) {
  throw new Error('Root element not found')
}

ReactDOM.createRoot(rootElement).render(
  <React.StrictMode>
    <ErrorBoundary>
    <ThemeProvider>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<GovernancePage />} />
            <Route path="/agents" element={<AgentsPage />} />
            <Route path="/agents/detail/:agentId" element={<AgentDetailPage />} />
            <Route path="/tools" element={<ToolsPage />} />
            <Route path="/vector-search" element={<VectorSearchPage />} />
            <Route path="/ai-gateway" element={<AIGatewayPage />} />
            <Route path="/observability" element={<ObservabilityPage />} />
            <Route path="/workspaces" element={<WorkspacesPage />} />
            <Route path="/admin" element={<AdminPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
    </ThemeProvider>
    </ErrorBoundary>
  </React.StrictMode>,
)
