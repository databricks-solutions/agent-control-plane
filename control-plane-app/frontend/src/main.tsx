import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import './index.css'

import { ThemeProvider } from './context/ThemeContext'
import Layout from './components/Layout'
import AgentsPage from './pages/Agents'
import AIGatewayPage from './pages/AIGateway'
import GovernancePage from './pages/Governance'
import ObservabilityPage from './pages/Observability'
import AgentDetailPage from './pages/AgentDetail'
import AdminPage from './pages/Admin'
import ToolsPage from './pages/Tools'
import PlaygroundPage from './pages/Playground'
import WorkspacesPage from './pages/Workspaces'
import TopologyView from './pages/TopologyView'
import VectorSearchPage from './pages/VectorSearch'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      refetchOnMount: 'always',   // refetch when a component mounts (tab change)
      staleTime: 0,               // treat data as stale immediately
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
            <Route path="/playground" element={<PlaygroundPage />} />
            <Route path="/workspaces" element={<WorkspacesPage />} />
            <Route path="/topology" element={<TopologyView />} />
            <Route path="/admin" element={<AdminPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
    </ThemeProvider>
  </React.StrictMode>,
)
