import { useState } from 'react'
import { X } from 'lucide-react'
import { useGrants, useUpdateGrants, type GrantKind } from '@/api/hooks'
import { Badge } from '@/components/ui/badge'
import { PrincipalAutocomplete } from '@/components/PrincipalAutocomplete'

const UC_KINDS = new Set<GrantKind>([
  'model_service', 'mcp_service', 'model_provider_service', 'agent_service', 'skill',
  'catalog', 'schema', 'table', 'volume', 'function', 'connection',
])

/** Turn a platform refusal into something a person can act on; keep the raw text for the tooltip. */
function explain(error: string): string {
  if (/warehouse/i.test(error) && /CAN_USE|permission|does not have/i.test(error)) {
    return "You need permission to use the app's SQL warehouse to manage Unity Catalog access."
  }
  if (/PERMISSION_DENIED|does not have|not authorized|403/i.test(error)) {
    return "You don't have permission to view or change access on this object. You need MANAGE or ownership."
  }
  if (/not found|does not exist|404/i.test(error)) return 'Object not found, or not visible to you.'
  return error
}

/**
 * Who can use an object, and changes to that — made as the signed-in user, so the
 * platform checks their own privileges. Direct grants can be revoked here;
 * inherited ones are shown for context and changed on the parent.
 */
export function AccessPanel({ kind, name }: { kind: GrantKind; name: string }) {
  const { data, isLoading } = useGrants(kind, name)
  const update = useUpdateGrants()
  const [principal, setPrincipal] = useState('')
  const [privilege, setPrivilege] = useState('')
  const [actionError, setActionError] = useState<string | null>(null)

  const options = data?.privileges ?? []
  const chosen = privilege || options[0] || ''
  const writable = !!data?.writable && !data?.error
  const platform = UC_KINDS.has(kind) ? 'Unity Catalog' : 'Databricks'

  const change = (body: { principal: string; add?: string[]; remove?: string[] }, onDone?: () => void) => {
    setActionError(null)
    update.mutate({ kind, name, ...body }, {
      onSuccess: (r) => (r.ok ? onDone?.() : setActionError(r.error || 'Change failed')),
      onError: (e: any) => setActionError(e?.response?.data?.detail || e?.message || 'Change failed'),
    })
  }

  const revoke = (who: string, priv: string) => {
    if (!window.confirm(`Revoke ${priv} from ${who}?`)) return
    change({ principal: who, remove: [priv] })
  }

  if (isLoading) return <div className="text-xs text-gray-400 dark:text-gray-500">Loading access…</div>

  return (
    <div className="space-y-2 text-xs">
      {data?.error && (
        <div className="text-amber-600 dark:text-amber-400" title={data.error}>{explain(data.error)}</div>
      )}
      {!data?.error && (data?.grants ?? []).length === 0 && (
        <div className="text-gray-400 dark:text-gray-500">No grants</div>
      )}
      {(data?.grants ?? []).map((g) => (
        <div key={g.principal} className="flex flex-wrap items-center gap-1.5">
          <span className="font-mono truncate max-w-[260px] dark:text-gray-200" title={g.principal}>{g.principal}</span>
          {g.privileges.map((p) => (
            <Badge key={p} variant="info" className="text-[10px] gap-1">
              {p}
              {writable && (
                <button
                  type="button"
                  aria-label={`Revoke ${p} from ${g.principal}`}
                  title="Revoke"
                  disabled={update.isPending}
                  onClick={() => revoke(g.principal, p)}
                  className="rounded-full hover:text-db-red focus-visible:outline focus-visible:outline-1 disabled:opacity-50"
                >
                  <X className="w-3 h-3" />
                </button>
              )}
            </Badge>
          ))}
          {g.inherited.map((p) => (
            <Badge key={`${p.privilege}:${p.from}`} className="text-[10px] opacity-70" title={`Inherited from ${p.from}`}>
              {p.privilege} · inherited
            </Badge>
          ))}
        </div>
      ))}

      {writable && (
        <form
          className="flex flex-wrap items-center gap-2 pt-1"
          onSubmit={(e) => {
            e.preventDefault()
            const who = principal.trim()
            if (who && chosen) change({ principal: who, add: [chosen] }, () => setPrincipal(''))
          }}
        >
          <PrincipalAutocomplete value={principal} onChange={setPrincipal} placeholder="User, group or service principal" className="min-w-[220px] flex-1" />
          <select
            aria-label="Privilege"
            value={chosen}
            onChange={(e) => setPrivilege(e.target.value)}
            className="px-2 py-1 border border-gray-200 dark:border-gray-600 rounded bg-white dark:bg-gray-800 dark:text-gray-200"
          >
            {options.map((o) => <option key={o} value={o}>{o}</option>)}
          </select>
          <button
            type="submit"
            disabled={!principal.trim() || !chosen || update.isPending}
            className="px-3 py-1 font-medium bg-db-red text-white rounded hover:bg-db-red/90 disabled:opacity-50"
          >
            {update.isPending ? 'Saving…' : 'Grant'}
          </button>
        </form>
      )}
      {actionError && <div className="text-red-600 dark:text-red-400" title={actionError}>{explain(actionError)}</div>}
      {data && !data.error && (
        <div className="text-[10px] text-gray-400 dark:text-gray-500">
          {data.notice
            ? data.notice
            : data.writable === false
            ? `Read-only: ${kind === 'skill' ? 'skill' : 'agent service'} access can't be changed from the app yet.`
            : data.via === 'app'
              ? "Changes are made by the app's service principal."
              : `Changes are made as you; ${platform} checks your privileges.`}
        </div>
      )}
    </div>
  )
}
