/**
 * Agent-centric topology tab — embedded in the Agents page.
 *
 * The canvas stays empty until the user selects one or more agents.
 * Selecting agents builds a subgraph of those agents + all their
 * directly connected nodes (tools, MCP servers, UC functions, child agents).
 */
import { useCallback, useEffect, useRef, useMemo, useState } from 'react'
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
import { Bot, Server, Code, GitMerge, Zap, Brain, X, Search, RefreshCw, GitFork, RotateCcw, Pin } from 'lucide-react'
import { useTopology, useSyncAgents } from '@/api/hooks'
import { usePinnedAgents } from '@/lib/usePinnedAgents'

// ── Visual config ─────────────────────────────────────────────────────────────

const NODE_CFG: Record<string, { color: string; bg: string; Icon: any; badge: string }> = {
  multi_agent_supervisor: { color: '#FF3621', bg: '#FFF5F3', Icon: GitMerge, badge: 'MAS' },
  knowledge_assistant:    { color: '#1B3139', bg: '#F0F4F5', Icon: Brain,    badge: 'KA' },
  information_extraction: { color: '#E67E22', bg: '#FFF8F0', Icon: Bot,      badge: 'IE' },
  app:                    { color: '#1B3139', bg: '#F0F4F5', Icon: Bot,       badge: 'App' },
  custom_app:             { color: '#1B3139', bg: '#F0F4F5', Icon: Bot,       badge: 'App' },
  custom_agent:           { color: '#1B3139', bg: '#F0F4F5', Icon: Bot,       badge: 'Agent' },
  custom_model:           { color: '#1B3139', bg: '#F0F4F5', Icon: Bot,       badge: 'Model' },
  custom_llm:             { color: '#1B3139', bg: '#F0F4F5', Icon: Bot,       badge: 'LLM' },
  external_model:         { color: '#475569', bg: '#F8FAFC', Icon: Bot,       badge: 'External' },
  genie_space:            { color: '#00B0D8', bg: '#F0FBFF', Icon: Zap,       badge: 'Genie' },
  mcp_server:             { color: '#00A972', bg: '#F0FBF7', Icon: Server,    badge: 'MCP' },
  uc_function:            { color: '#9B51E0', bg: '#F8F3FF', Icon: Code,      badge: 'UC Fn' },
  span_tool:              { color: '#F2C94C', bg: '#FFFCF0', Icon: Zap,       badge: 'Tool' },
}
const DEFAULT_CFG = { color: '#6B7280', bg: '#F9FAFB', Icon: Bot, badge: 'Node' }

const AGENT_TYPES = new Set([
  'multi_agent_supervisor', 'knowledge_assistant', 'information_extraction',
  'app', 'custom_app', 'custom_agent', 'custom_model', 'custom_llm',
  'external_model', 'genie_space',
])

// ── DAG layout ────────────────────────────────────────────────────────────────

const NODE_W = 210
const NODE_H = 68

function applyLayout(nodes: Node[], edges: Edge[]): Node[] {
  if (!nodes.length) return nodes
  const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: 'TB', nodesep: 80, ranksep: 120, marginx: 40, marginy: 40 })
  nodes.forEach((n) => g.setNode(n.id, { width: NODE_W, height: NODE_H }))
  edges.forEach((e) => { try { g.setEdge(e.source, e.target) } catch {} })
  Dagre.layout(g)
  return nodes.map((n) => {
    const pos = g.node(n.id)
    return { ...n, position: { x: pos.x - NODE_W / 2, y: pos.y - NODE_H / 2 } }
  })
}

// ── Subgraph builder ──────────────────────────────────────────────────────────

function buildSubgraph(
  allNodes: any[],
  allEdges: any[],
  selectedAgentIds: Set<string>,
): { nodes: any[]; edges: any[] } {
  if (!selectedAgentIds.size) return { nodes: [], edges: [] }

  // BFS flood-fill: start from selected agents, follow all edges in both
  // directions until no new nodes are reachable. This surfaces the full
  // connected subgraph regardless of chain depth (n hops).
  const visited = new Set([...selectedAgentIds].map((id) => `agent:${id}`))
  let frontier = new Set(visited)

  while (frontier.size > 0) {
    const next = new Set<string>()
    for (const e of allEdges) {
      if (frontier.has(e.source) && !visited.has(e.target)) {
        next.add(e.target)
      }
      if (frontier.has(e.target) && !visited.has(e.source)) {
        next.add(e.source)
      }
    }
    next.forEach((id) => visited.add(id))
    frontier = next
  }

  return {
    nodes: allNodes.filter((n) => visited.has(n.id)),
    edges: allEdges.filter((e) => visited.has(e.source) && visited.has(e.target)),
  }
}

// ── Custom React Flow node ────────────────────────────────────────────────────

function TopoNodeComponent({ data }: NodeProps) {
  const topo = (data as any).topo
  const cfg = NODE_CFG[topo.node_type] ?? DEFAULT_CFG
  const { Icon } = cfg
  const online = ['ONLINE', 'ACTIVE', 'READY'].includes((topo.status || '').toUpperCase())
  const isRoot = topo.isRoot as boolean

  return (
    <div
      onClick={() => topo.onSelect(topo)}
      style={{
        borderLeft: `4px solid ${cfg.color}`,
        background: cfg.bg,
        boxShadow: isRoot ? `0 0 0 2px ${cfg.color}` : undefined,
      }}
      className="w-[210px] h-[68px] rounded-lg shadow-sm border border-gray-200 px-3 py-2 flex items-center gap-2.5 cursor-pointer hover:shadow-md transition-shadow select-none"
    >
      <Handle type="target" position={Position.Top} style={{ background: cfg.color, width: 8, height: 8, border: 'none' }} />
      <div style={{ color: cfg.color }} className="flex-shrink-0">
        <Icon className="w-5 h-5" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="text-xs font-semibold text-gray-800 truncate leading-tight">{topo.label}</div>
        <div className="flex items-center gap-1.5 mt-1">
          <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded leading-none" style={{ background: cfg.color + '22', color: cfg.color }}>{cfg.badge}</span>
          <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${online ? 'bg-green-500' : 'bg-gray-300'}`} />
          <span className="text-[10px] text-gray-400 truncate">{topo.status || '—'}</span>
        </div>
      </div>
      <Handle type="source" position={Position.Bottom} style={{ background: cfg.color, width: 8, height: 8, border: 'none' }} />
    </div>
  )
}

const nodeTypes = { topo: TopoNodeComponent }

// ── Detail panel ──────────────────────────────────────────────────────────────

function DetailPanel({ node, onClose }: { node: any; onClose: () => void }) {
  const cfg = NODE_CFG[node.node_type] ?? DEFAULT_CFG
  const { Icon } = cfg
  const meta = node.meta || {}
  return (
    <div className="absolute right-4 top-4 w-64 bg-white dark:bg-gray-800 rounded-xl shadow-xl border border-gray-200 dark:border-gray-700 z-10 overflow-hidden">
      <div className="flex items-center gap-2.5 px-3 py-2.5 border-b border-gray-100 dark:border-gray-700" style={{ borderTop: `3px solid ${cfg.color}` }}>
        <Icon className="w-4 h-4 flex-shrink-0" style={{ color: cfg.color }} />
        <span className="font-semibold text-sm text-gray-900 dark:text-gray-100 flex-1 truncate">{node.label}</span>
        <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors"><X className="w-3.5 h-3.5" /></button>
      </div>
      <div className="px-3 py-2.5 space-y-2 text-sm">
        <div className="flex items-center gap-2">
          <span className="text-xs font-semibold px-1.5 py-0.5 rounded" style={{ background: cfg.color + '22', color: cfg.color }}>{cfg.badge}</span>
          <span className={`text-xs ${['ONLINE','ACTIVE','READY'].includes((node.status||'').toUpperCase()) ? 'text-green-600' : 'text-gray-400'}`}>{node.status || '—'}</span>
        </div>
        {meta.description && <p className="text-xs text-gray-500 leading-relaxed">{meta.description}</p>}
        <div className="space-y-1.5 border-t border-gray-100 pt-2">
          {meta.endpoint_name && <MetaRow label="Endpoint" value={meta.endpoint_name} />}
          {meta.creator && <MetaRow label="Creator" value={meta.creator} />}
          {meta.catalog_name && <MetaRow label="Catalog" value={`${meta.catalog_name}.${meta.schema_name || ''}`} />}
          {meta.sub_type && <MetaRow label="Sub-type" value={meta.sub_type} />}
          {(meta.agent_id || meta.tool_id) && <MetaRow label="ID" value={meta.agent_id || meta.tool_id} mono />}
        </div>
      </div>
    </div>
  )
}

function MetaRow({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex items-start gap-1.5">
      <span className="text-xs text-gray-400 w-14 flex-shrink-0 pt-0.5">{label}</span>
      <span className={`text-xs text-gray-700 dark:text-gray-300 break-all ${mono ? 'font-mono' : ''}`}>{value}</span>
    </div>
  )
}

// ── Canvas ────────────────────────────────────────────────────────────────────

function Canvas({
  data,
  selectedAgentIds,
}: {
  data: any
  selectedAgentIds: Set<string>
}) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [detail, setDetail] = useState<any>(null)

  const handleSelect = useCallback((topo: any) => setDetail(topo), [])

  const { nodes: subNodes, edges: subEdges } = useMemo(
    () => buildSubgraph(data?.nodes ?? [], data?.edges ?? [], selectedAgentIds),
    [data, selectedAgentIds],
  )

  useEffect(() => {
    if (!subNodes.length) { setNodes([]); setEdges([]); return }

    const rfNodes: Node[] = subNodes.map((n: any) => ({
      id: n.id,
      type: 'topo',
      position: { x: 0, y: 0 },
      data: {
        topo: {
          ...n,
          onSelect: handleSelect,
          isRoot: AGENT_TYPES.has(n.node_type) && selectedAgentIds.has(n.meta?.agent_id ?? ''),
        },
      },
    }))

    const rfEdges: Edge[] = subEdges.map((e: any) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      label: e.call_count ? `${e.call_count}×` : (e.label || ''),
      animated: e.animated ?? false,
      markerEnd: { type: MarkerType.ArrowClosed, color: '#9CA3AF', width: 14, height: 14 },
      style: { stroke: '#9CA3AF', strokeWidth: 1.5 },
      labelStyle: { fontSize: 10, fill: '#6B7280' },
      labelBgStyle: { fill: 'white', fillOpacity: 0.85 },
    }))

    setNodes(applyLayout(rfNodes, rfEdges))
    setEdges(rfEdges)
    setDetail(null)
  }, [subNodes, subEdges, handleSelect, selectedAgentIds])

  if (!selectedAgentIds.size) {
    return (
      <div className="absolute inset-0 flex flex-col items-center justify-center gap-3 text-center p-8">
        <GitFork className="w-12 h-12 text-gray-200" />
        <p className="text-sm font-medium text-gray-400">Select one or more agents</p>
        <p className="text-xs text-gray-300 max-w-xs">
          Choose agents from the left panel to visualize their connections to tools, MCP servers, and other agents.
        </p>
      </div>
    )
  }

  if (!subNodes.length) {
    return (
      <div className="absolute inset-0 flex flex-col items-center justify-center gap-3 text-center p-8">
        <GitFork className="w-10 h-10 text-gray-200" />
        <p className="text-sm text-gray-400">No connections found for selected agents</p>
        <p className="text-xs text-gray-300 max-w-xs leading-relaxed">
          Connections are built from:<br />
          • App <span className="font-mono">config.resources</span> (serving endpoint deps)<br />
          • MAS config child-agent routing<br />
          • MLflow trace spans (TOOL/FUNCTION/RETRIEVER)<br /><br />
          If agents have no declared resources and no MLflow traces, no edges can be inferred.
          Use the Refresh button to re-analyze recent traces.
        </p>
      </div>
    )
  }

  return (
    <div className="absolute inset-0">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.18 }}
        minZoom={0.15}
        maxZoom={2}
        onPaneClick={() => setDetail(null)}
      >
        <Background color="#E5E7EB" gap={20} />
        <Controls />
        <MiniMap
          nodeColor={(n) => NODE_CFG[(n.data as any)?.topo?.node_type]?.color ?? '#6B7280'}
          maskColor="rgba(255,255,255,0.75)"
          style={{ borderRadius: 8 }}
        />
      </ReactFlow>
      {detail && <DetailPanel node={detail} onClose={() => setDetail(null)} />}
    </div>
  )
}

// ── Agent selector panel ──────────────────────────────────────────────────────

function AgentSelector({
  agents,
  selected,
  onToggle,
  onSelectAll,
  onClear,
}: {
  agents: any[]
  selected: Set<string>
  onToggle: (id: string) => void
  onSelectAll: () => void
  onClear: () => void
}) {
  const [search, setSearch] = useState('')
  const { pinned, togglePin } = usePinnedAgents()

  const filtered = useMemo(
    () => agents.filter((a) => !search || a.label.toLowerCase().includes(search.toLowerCase())),
    [agents, search],
  )

  // Pinned agents shown first as a separate group, then grouped by type
  const pinnedAgents = useMemo(
    () => filtered.filter((a) => pinned.has(a.meta?.agent_id ?? '')),
    [filtered, pinned],
  )

  const grouped = useMemo(() => {
    const groups: Record<string, any[]> = {}
    filtered.forEach((a) => {
      if (pinned.has(a.meta?.agent_id ?? '')) return // shown in pinned section
      const t = a.node_type || 'other'
      if (!groups[t]) groups[t] = []
      groups[t].push(a)
    })
    return groups
  }, [filtered, pinned])

  const typeOrder = [
    'multi_agent_supervisor', 'knowledge_assistant', 'information_extraction',
    'app', 'custom_app', 'custom_agent', 'custom_model', 'custom_llm',
    'external_model', 'genie_space',
  ]

  const AgentRow = ({ a }: { a: any }) => {
    const id = a.meta?.agent_id ?? ''
    const isChecked = selected.has(id)
    const isPinned = pinned.has(id)
    return (
      <div
        className={`flex items-center gap-1 px-2 py-1.5 text-xs group ${
          isChecked
            ? 'bg-db-red/8 text-gray-800 dark:text-gray-100'
            : 'text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700/50'
        }`}
      >
        <input
          type="checkbox"
          checked={isChecked}
          onChange={() => onToggle(id)}
          className="w-3.5 h-3.5 rounded accent-db-red flex-shrink-0 cursor-pointer"
        />
        <span className="truncate flex-1 cursor-pointer" onClick={() => onToggle(id)}>{a.label}</span>
        <button
          onClick={(e) => { e.stopPropagation(); togglePin(id) }}
          className={`flex-shrink-0 p-0.5 rounded transition-colors ${
            isPinned
              ? 'text-db-red opacity-100'
              : 'text-gray-300 dark:text-gray-600 opacity-0 group-hover:opacity-100 hover:text-gray-500'
          }`}
          title={isPinned ? 'Unpin' : 'Pin to top'}
        >
          <Pin className="w-2.5 h-2.5" />
        </button>
      </div>
    )
  }

  return (
    <div className="w-56 flex-shrink-0 flex flex-col border-r border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800">
      {/* Header */}
      <div className="px-3 pt-3 pb-2 border-b border-gray-100 dark:border-gray-700 space-y-2">
        <div className="flex items-center justify-between">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
            Agents
            {selected.size > 0 && (
              <span className="ml-1.5 text-[10px] bg-db-red text-white px-1.5 py-0.5 rounded-full font-bold">
                {selected.size}
              </span>
            )}
          </p>
          <div className="flex gap-1.5">
            <button
              onClick={onSelectAll}
              className="text-[10px] text-gray-400 hover:text-db-red transition-colors font-medium"
            >
              All
            </button>
            {selected.size > 0 && (
              <>
                <span className="text-gray-200">·</span>
                <button
                  onClick={onClear}
                  className="text-[10px] text-gray-400 hover:text-gray-600 transition-colors font-medium"
                >
                  Clear
                </button>
              </>
            )}
          </div>
        </div>
        <div className="relative">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400 pointer-events-none" />
          <input
            type="text"
            placeholder="Filter agents…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full pl-6 pr-2 py-1 text-xs border border-gray-200 dark:border-gray-600 rounded-md bg-gray-50 dark:bg-gray-700 dark:text-gray-200 focus:outline-none focus:ring-1 focus:ring-db-red/40"
          />
        </div>
      </div>

      {/* Agent list */}
      <div className="flex-1 overflow-y-auto py-1">
        {/* Pinned section */}
        {pinnedAgents.length > 0 && (
          <div>
            <div className="px-3 py-1 text-[10px] font-semibold uppercase tracking-wider text-db-red sticky top-0 bg-white dark:bg-gray-800 flex items-center gap-1">
              <Pin className="w-2.5 h-2.5" /> Pinned
            </div>
            {pinnedAgents.map((a) => <AgentRow key={a.id} a={a} />)}
          </div>
        )}

        {/* Grouped by type */}
        {typeOrder.map((type) => {
          const group = grouped[type]
          if (!group?.length) return null
          const cfg = NODE_CFG[type] ?? DEFAULT_CFG
          return (
            <div key={type}>
              <div className="px-3 py-1 text-[10px] font-semibold uppercase tracking-wider text-gray-400 sticky top-0 bg-white dark:bg-gray-800">
                {cfg.badge}
              </div>
              {group.map((a) => <AgentRow key={a.id} a={a} />)}
            </div>
          )
        })}

        {filtered.length === 0 && (
          <p className="px-3 py-4 text-xs text-gray-400 text-center">No agents found</p>
        )}
      </div>
    </div>
  )
}

// ── Legend ────────────────────────────────────────────────────────────────────

const LEGEND = [
  { type: 'multi_agent_supervisor', label: 'MAS — supervisor' },
  { type: 'knowledge_assistant',    label: 'KA — knowledge assistant' },
  { type: 'app',                    label: 'App / Model / External' },
  { type: 'mcp_server',             label: 'MCP server' },
  { type: 'uc_function',            label: 'UC function' },
  { type: 'span_tool',              label: 'Tool (from traces)' },
] as const

function Legend() {
  return (
    <div className="absolute bottom-14 left-2 bg-white/90 dark:bg-gray-800/90 backdrop-blur-sm rounded-lg shadow border border-gray-200 dark:border-gray-700 px-2.5 py-2 z-10">
      <p className="text-[9px] font-semibold text-gray-400 uppercase tracking-wider mb-1.5">Legend</p>
      {LEGEND.map(({ type, label }) => {
        const cfg = NODE_CFG[type] ?? DEFAULT_CFG
        return (
          <div key={type} className="flex items-center gap-1.5 mb-0.5">
            <div className="w-2 h-2 rounded-sm flex-shrink-0" style={{ background: cfg.color }} />
            <span className="text-[10px] text-gray-500 dark:text-gray-400">{label}</span>
          </div>
        )
      })}
    </div>
  )
}

// ── Main export ───────────────────────────────────────────────────────────────

export default function TopologyTab() {
  // refreshKey increments on every manual refresh — guarantees a new fetch each time
  const [refreshKey, setRefreshKey] = useState(0)
  const { data, isLoading, error } = useTopology(refreshKey)
  const syncAgents = useSyncAgents()
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())

  const refresh = useCallback(() => setRefreshKey((k) => k + 1), [])

  // After a sync completes, also force-refresh topology to pick up new connections
  const handleSync = useCallback(async () => {
    await syncAgents.mutateAsync()
    refresh()
  }, [syncAgents, refresh])

  const agentNodes = useMemo(
    () => (data?.nodes ?? []).filter((n: any) => AGENT_TYPES.has(n.node_type)),
    [data],
  )

  const toggle = useCallback((id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }, [])

  const selectAll = useCallback(() => {
    setSelectedIds(new Set(agentNodes.map((a: any) => a.meta?.agent_id ?? '')))
  }, [agentNodes])

  const clear = useCallback(() => setSelectedIds(new Set()), [])

  // Summary line for selected agents
  const selectedNames = useMemo(() => {
    if (!selectedIds.size) return null
    const names = agentNodes
      .filter((a: any) => selectedIds.has(a.meta?.agent_id ?? ''))
      .map((a: any) => a.label)
    if (names.length <= 2) return names.join(', ')
    return `${names.slice(0, 2).join(', ')} +${names.length - 2} more`
  }, [selectedIds, agentNodes])

  return (
    <ReactFlowProvider>
      <div className="flex flex-col" style={{ height: 'calc(100vh - 260px)', minHeight: 500 }}>

        {/* Toolbar */}
        <div className="flex items-center gap-3 pb-3 flex-shrink-0">
          {selectedNames ? (
            <p className="text-xs text-gray-500">
              Showing connections for: <span className="font-medium text-gray-700 dark:text-gray-200">{selectedNames}</span>
            </p>
          ) : (
            <p className="text-xs text-gray-400">Select agents on the left to build their topology</p>
          )}
          <div className="ml-auto flex items-center gap-2">
            {data?.stats && (
              <span className="text-xs text-gray-400">
                {data.stats.agent_nodes} agents · {data.stats.tool_nodes} tools · {data.stats.total_edges} edges
              </span>
            )}
            <button
              onClick={handleSync}
              disabled={syncAgents.isPending || isLoading}
              title="Re-discover all agents then rebuild topology"
              className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 transition-colors disabled:opacity-50"
            >
              <RotateCcw className={`w-3.5 h-3.5 ${syncAgents.isPending ? 'animate-spin' : ''}`} />
              {syncAgents.isPending ? 'Syncing…' : 'Re-discover'}
            </button>
            <button
              onClick={refresh}
              disabled={isLoading}
              title="Rebuild topology graph from current DB data"
              className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 transition-colors disabled:opacity-50"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${isLoading ? 'animate-spin' : ''}`} />
              {isLoading ? 'Loading…' : 'Refresh'}
            </button>
          </div>
        </div>

        {/* Body */}
        <div className="flex flex-1 min-h-0 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
          {/* Left: agent multi-selector */}
          <AgentSelector
            agents={agentNodes}
            selected={selectedIds}
            onToggle={toggle}
            onSelectAll={selectAll}
            onClear={clear}
          />

          {/* Right: canvas */}
          <div className="flex-1 relative bg-gray-50 dark:bg-gray-900">
            {isLoading && (
              <div className="absolute inset-0 flex flex-col items-center justify-center gap-3">
                <div className="w-7 h-7 border-2 border-db-red border-t-transparent rounded-full animate-spin" />
                <p className="text-sm text-gray-400">Building topology…</p>
              </div>
            )}
            {error && !isLoading && (
              <div className="absolute inset-0 flex items-center justify-center">
                <p className="text-sm text-red-500">Failed to load topology data.</p>
              </div>
            )}
            {!isLoading && !error && (
              <>
                <Canvas data={data} selectedAgentIds={selectedIds} />
                {selectedIds.size > 0 && <Legend />}
              </>
            )}
          </div>
        </div>
      </div>
    </ReactFlowProvider>
  )
}
