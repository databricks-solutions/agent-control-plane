import { useCallback, useEffect, useState } from 'react'
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  Handle,
  Position,
  MarkerType,
} from '@xyflow/react'
import type { Node, Edge, NodeProps } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import * as Dagre from '@dagrejs/dagre'
import { Bot, Server, Code, GitMerge, Zap, Brain, X, ExternalLink, RefreshCw } from 'lucide-react'
import { useTopology } from '@/api/hooks'

// ── Types ───────────────────────────────────────────────────────────────────

interface TopoNode {
  id: string
  node_type: string
  label: string
  status: string
  meta: Record<string, any>
  onSelect: (node: TopoNode) => void
}

// ── DAG layout via dagre ─────────────────────────────────────────────────────

const NODE_W = 210
const NODE_H = 68

function applyDagreLayout(nodes: Node[], edges: Edge[]): Node[] {
  const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: 'TB', nodesep: 70, ranksep: 110, marginx: 40, marginy: 40 })
  nodes.forEach((n) => g.setNode(n.id, { width: NODE_W, height: NODE_H }))
  edges.forEach((e) => g.setEdge(e.source, e.target))
  Dagre.layout(g)
  return nodes.map((n) => {
    const pos = g.node(n.id)
    return { ...n, position: { x: pos.x - NODE_W / 2, y: pos.y - NODE_H / 2 } }
  })
}

// ── Node visual config ───────────────────────────────────────────────────────

const NODE_CONFIG: Record<string, { color: string; bg: string; Icon: any; badge: string }> = {
  multi_agent_supervisor: { color: '#FF3621', bg: '#FFF5F3', Icon: GitMerge, badge: 'MAS' },
  knowledge_assistant:    { color: '#1B3139', bg: '#F0F4F5', Icon: Brain,    badge: 'KA' },
  app:                    { color: '#1B3139', bg: '#F0F4F5', Icon: Bot,       badge: 'App' },
  custom_model:           { color: '#1B3139', bg: '#F0F4F5', Icon: Bot,       badge: 'Model' },
  external_model:         { color: '#475569', bg: '#F8FAFC', Icon: Bot,       badge: 'External' },
  genie_space:            { color: '#00B0D8', bg: '#F0FBFF', Icon: Zap,       badge: 'Genie' },
  mcp_server:             { color: '#00A972', bg: '#F0FBF7', Icon: Server,    badge: 'MCP' },
  uc_function:            { color: '#9B51E0', bg: '#F8F3FF', Icon: Code,      badge: 'UC Fn' },
  span_tool:              { color: '#F2C94C', bg: '#FFFCF0', Icon: Zap,       badge: 'Tool' },
}
const DEFAULT_CFG = { color: '#6B7280', bg: '#F9FAFB', Icon: Bot, badge: 'Node' }

// ── Custom React Flow node ───────────────────────────────────────────────────

function TopoNodeComponent({ data }: NodeProps) {
  const topo = (data as any).topo as TopoNode
  const cfg = NODE_CONFIG[topo.node_type] ?? DEFAULT_CFG
  const { Icon } = cfg
  const online = ['ONLINE', 'ACTIVE', 'READY'].includes((topo.status || '').toUpperCase())

  return (
    <div
      onClick={() => topo.onSelect(topo)}
      style={{ borderLeft: `4px solid ${cfg.color}`, background: cfg.bg }}
      className="w-[210px] h-[68px] rounded-lg shadow-sm border border-gray-200 px-3 py-2 flex items-center gap-2.5 cursor-pointer hover:shadow-md transition-shadow select-none"
    >
      <Handle
        type="target"
        position={Position.Top}
        style={{ background: cfg.color, width: 8, height: 8, border: 'none' }}
      />
      <div style={{ color: cfg.color }} className="flex-shrink-0">
        <Icon className="w-5 h-5" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="text-xs font-semibold text-gray-800 truncate leading-tight">{topo.label}</div>
        <div className="flex items-center gap-1.5 mt-1">
          <span
            className="text-[10px] font-semibold px-1.5 py-0.5 rounded leading-none"
            style={{ background: cfg.color + '22', color: cfg.color }}
          >
            {cfg.badge}
          </span>
          <span
            className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${online ? 'bg-green-500' : 'bg-gray-300'}`}
            title={topo.status}
          />
          <span className="text-[10px] text-gray-400 truncate">{topo.status || '—'}</span>
        </div>
      </div>
      <Handle
        type="source"
        position={Position.Bottom}
        style={{ background: cfg.color, width: 8, height: 8, border: 'none' }}
      />
    </div>
  )
}

const nodeTypes = { topo: TopoNodeComponent }

// ── Detail panel ─────────────────────────────────────────────────────────────

function DetailPanel({ node, onClose }: { node: TopoNode; onClose: () => void }) {
  const cfg = NODE_CONFIG[node.node_type] ?? DEFAULT_CFG
  const { Icon } = cfg
  const meta = node.meta || {}

  return (
    <div className="absolute right-4 top-4 w-72 bg-white dark:bg-gray-800 rounded-xl shadow-xl border border-gray-200 dark:border-gray-700 z-10 overflow-hidden">
      <div
        className="flex items-center gap-2.5 px-4 py-3 border-b border-gray-100 dark:border-gray-700"
        style={{ borderTop: `3px solid ${cfg.color}` }}
      >
        <Icon className="w-4 h-4 flex-shrink-0" style={{ color: cfg.color }} />
        <span className="font-semibold text-sm text-gray-900 dark:text-gray-100 flex-1 truncate">
          {node.label}
        </span>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 transition-colors"
        >
          <X className="w-4 h-4" />
        </button>
      </div>

      <div className="px-4 py-3 space-y-3 text-sm">
        <div className="flex items-center gap-2">
          <span
            className="text-xs font-semibold px-2 py-0.5 rounded"
            style={{ background: cfg.color + '22', color: cfg.color }}
          >
            {cfg.badge}
          </span>
          <span
            className={`text-xs font-medium ${
              ['ONLINE', 'ACTIVE', 'READY'].includes((node.status || '').toUpperCase())
                ? 'text-green-600 dark:text-green-400'
                : 'text-gray-400'
            }`}
          >
            {node.status || '—'}
          </span>
        </div>

        {meta.description && (
          <p className="text-xs text-gray-500 dark:text-gray-400 leading-relaxed">{meta.description}</p>
        )}

        <div className="space-y-2 border-t border-gray-100 dark:border-gray-700 pt-2">
          {meta.endpoint_name && <MetaRow label="Endpoint" value={meta.endpoint_name} />}
          {meta.endpoint_type && <MetaRow label="Type" value={meta.endpoint_type} />}
          {meta.sub_type && <MetaRow label="Sub-type" value={meta.sub_type} />}
          {meta.catalog_name && (
            <MetaRow label="Catalog" value={`${meta.catalog_name}.${meta.schema_name || ''}`} />
          )}
          {meta.agent_id && <MetaRow label="ID" value={meta.agent_id} mono />}
          {meta.tool_id && <MetaRow label="Tool ID" value={meta.tool_id} mono />}
          {meta.app_url && (
            <div className="flex items-start gap-1.5">
              <span className="text-xs text-gray-400 w-16 flex-shrink-0 pt-0.5">App URL</span>
              <a
                href={meta.app_url}
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-blue-500 hover:underline flex items-center gap-1 truncate"
              >
                Open <ExternalLink className="w-3 h-3 flex-shrink-0" />
              </a>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function MetaRow({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex items-start gap-1.5">
      <span className="text-xs text-gray-400 w-16 flex-shrink-0 pt-0.5">{label}</span>
      <span
        className={`text-xs text-gray-700 dark:text-gray-300 break-all leading-relaxed ${mono ? 'font-mono' : ''}`}
      >
        {value}
      </span>
    </div>
  )
}

// ── Legend ───────────────────────────────────────────────────────────────────

const LEGEND_ITEMS = [
  'multi_agent_supervisor',
  'knowledge_assistant',
  'app',
  'mcp_server',
  'uc_function',
  'span_tool',
] as const

function Legend() {
  return (
    <div className="absolute bottom-16 left-4 bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700 px-3 py-2 z-10">
      <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1.5">
        Legend
      </div>
      <div className="flex flex-col gap-1">
        {LEGEND_ITEMS.map((type) => {
          const cfg = NODE_CONFIG[type] ?? DEFAULT_CFG
          return (
            <div key={type} className="flex items-center gap-2">
              <div className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ background: cfg.color }} />
              <span className="text-xs text-gray-600 dark:text-gray-400">{cfg.badge}</span>
              <span className="text-xs text-gray-400 dark:text-gray-500">
                {type === 'multi_agent_supervisor' && '— Multi-Agent Supervisor'}
                {type === 'knowledge_assistant' && '— Knowledge Assistant'}
                {type === 'app' && '— App / Custom / External'}
                {type === 'mcp_server' && '— MCP Server'}
                {type === 'uc_function' && '— UC Function'}
                {type === 'span_tool' && '— Tool (from traces)'}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Empty state ───────────────────────────────────────────────────────────────

function EmptyState() {
  return (
    <div className="flex-1 flex flex-col items-center justify-center gap-3 text-center p-8">
      <GitMerge className="w-12 h-12 text-gray-300" />
      <p className="text-sm font-medium text-gray-500">No topology data available</p>
      <p className="text-xs text-gray-400 max-w-sm">
        Register agents and tools to see how they connect. Agent→tool edges are discovered
        automatically from MLflow trace spans.
      </p>
    </div>
  )
}

// ── React Flow canvas ────────────────────────────────────────────────────────

function TopologyCanvas({ data }: { data: any }) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [selected, setSelected] = useState<TopoNode | null>(null)

  const handleSelect = useCallback((topo: TopoNode) => {
    setSelected(topo)
  }, [])

  useEffect(() => {
    if (!data?.nodes?.length) return

    const rfNodes: Node[] = data.nodes.map((n: any) => ({
      id: n.id,
      type: 'topo',
      position: { x: 0, y: 0 },
      data: { topo: { ...n, onSelect: handleSelect } },
    }))

    const rfEdges: Edge[] = data.edges.map((e: any) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      label: e.call_count ? `${e.call_count}×` : (e.label || ''),
      animated: e.animated ?? false,
      markerEnd: { type: MarkerType.ArrowClosed, color: '#9CA3AF', width: 16, height: 16 },
      style: { stroke: '#9CA3AF', strokeWidth: 1.5 },
      labelStyle: { fontSize: 10, fill: '#6B7280' },
      labelBgStyle: { fill: 'white', fillOpacity: 0.85 },
    }))

    const layouted = applyDagreLayout(rfNodes, rfEdges)
    setNodes(layouted)
    setEdges(rfEdges)
  }, [data, handleSelect])

  if (!data?.nodes?.length) return <EmptyState />

  return (
    <div className="flex-1 relative">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.15}
        maxZoom={2}
        onPaneClick={() => setSelected(null)}
      >
        <Background color="#E5E7EB" gap={20} />
        <Controls />
        <MiniMap
          nodeColor={(n) => {
            const t = (n.data as any)?.topo?.node_type
            return NODE_CONFIG[t]?.color ?? '#6B7280'
          }}
          maskColor="rgba(255,255,255,0.75)"
          style={{ borderRadius: 8 }}
        />
      </ReactFlow>

      <Legend />

      {selected && (
        <DetailPanel node={selected} onClose={() => setSelected(null)} />
      )}
    </div>
  )
}

// ── Page ─────────────────────────────────────────────────────────────────────

export default function TopologyView() {
  const [refreshKey, setRefreshKey] = useState(0)
  const { data, isLoading, error } = useTopology(refreshKey)

  const handleRefresh = () => setRefreshKey((k) => k + 1)

  const stats = data?.stats

  return (
    <ReactFlowProvider>
      <div className="flex flex-col -m-6" style={{ height: 'calc(100vh - 3.5rem)' }}>
        {/* Header bar */}
        <div className="flex items-center gap-4 px-5 py-2.5 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 flex-shrink-0">
          <h1 className="text-base font-semibold text-gray-900 dark:text-gray-100">Topology View</h1>

          {stats && (
            <div className="flex items-center gap-3">
              {[
                { label: 'Nodes', value: stats.total_nodes },
                { label: 'Agents', value: stats.agent_nodes },
                { label: 'Tools', value: stats.tool_nodes },
                { label: 'Edges', value: stats.total_edges },
              ].map(({ label, value }) => (
                <div key={label} className="flex items-center gap-1">
                  <span className="text-xs text-gray-400">{label}</span>
                  <span className="text-xs font-semibold text-gray-700 dark:text-gray-200 bg-gray-100 dark:bg-gray-700 px-1.5 py-0.5 rounded">
                    {value}
                  </span>
                </div>
              ))}
            </div>
          )}

          <div className="ml-auto">
            <button
              onClick={handleRefresh}
              disabled={isLoading}
              className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors disabled:opacity-50"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${isLoading ? 'animate-spin' : ''}`} />
              {isLoading ? 'Loading…' : 'Refresh'}
            </button>
          </div>
        </div>

        {/* Canvas area */}
        {isLoading && (
          <div className="flex-1 flex flex-col items-center justify-center gap-3">
            <div className="w-8 h-8 border-2 border-db-red border-t-transparent rounded-full animate-spin" />
            <p className="text-sm text-gray-500">Building topology graph…</p>
          </div>
        )}

        {error && !isLoading && (
          <div className="flex-1 flex items-center justify-center">
            <p className="text-sm text-red-500">Failed to load topology data. Check backend logs.</p>
          </div>
        )}

        {data && !isLoading && <TopologyCanvas data={data} />}
      </div>
    </ReactFlowProvider>
  )
}
