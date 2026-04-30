# ADR: Cross-workspace observability via Unity Catalog, not MLflow trace fan-out

**Status:** Accepted (2026-04-29)
**Defers (does not abandon):** the cross-workspace MLflow REST API fan-out approach. The prototype is parked; whether to revive it as a future direction will be re-evaluated as Databricks platform constraints evolve and as we see how Tier 2 adoption plays out (see *Trigger conditions* below).
**Related:** [RCA: Cross-workspace trace discovery silently returning empty](../rca/2026-04-28-cross-workspace-trace-discovery.md)

## Context

The Observability page needs to surface agent activity across all workspaces in a Databricks account. The original design fanned out MLflow REST API calls (`/api/2.0/mlflow/traces`) per workspace from the discovery workflow.

Investigation (full detail in the RCA) showed this is structurally blocked by Databricks platform constraints:

1. **Cross-workspace REST API calls from Databricks Serverless and Databricks Apps Compute are rejected** at the destination workspace's gateway with `403 Cert validation failed. Both workspace comparison and snp system trusted checks did not pass.` This applies whenever the destination workspace has any Network Policy attached, even an allow-all one. Confirmed via direct probes from both runtimes.
2. **Classic compute is a viable alternative** in principle but requires the destination region/account to provision classic worker environments — not available in our test workspaces, and provisioning was further constrained by an industry-wide supply-chain security incident affecting Python dependency provisioning during the investigation window.
3. **An external runner works** (proven via direct HTTPS calls from outside Databricks compute, returning trace data using the same service-principal credentials), but is operationally heavyweight: requires external scheduling, cross-system secret management, and per-deployment service-principal provisioning across all target workspaces.
4. **A native self-serve platform control for cross-workspace API access is on the Databricks roadmap.** No public GA confirmation as of this ADR's date.

## Decision

**For the near term, source cross-workspace observability data from Unity Catalog (AI Gateway inference tables and UC-stored MLflow traces), not from per-workspace MLflow REST API fan-out.**

The architectural reframe: anything in Unity Catalog is queryable cross-workspace via a SQL warehouse — no per-workspace fan-out, no service principal on each target workspace, no platform-level isolation to fight at the destination. Both AI Gateway inference logs and UC-bound MLflow trace tables live in UC. The MLflow REST API fan-out path is deferred to a future iteration as platform constraints lift; this is a pragmatic choice to ship now, not a long-term architectural commitment against MLflow REST traces.

The product's surface area becomes:

A note on terminology used below: MLflow traces have **two storage backends** — the original control-plane backend (referred to as the *default backend* below; this is what `@mlflow.trace`-decorated code writes to unless an experiment is explicitly bound to a UC trace location) and the newer **Unity Catalog OTel backend** (`<prefix>_otel_spans` Delta tables, MLflow 3.11+). These are *storage* concepts, separate from the compute that does the discovery (Serverless, Apps, classic clusters, or external runners).

- **Tier 1 (today):** Local-workspace MLflow traces stored in the default backend. Fetched via `/api/2.0/mlflow/traces` REST against the local workspace — the existing Serverless workflow path. Captures `@mlflow.trace`-decorated agent code that uses the default backend. Spans, tool calls, payloads.
- **Tier 2 (today):** Any agent observability data that lives in Unity Catalog. Because UC governance is account-level, an account admin (or any principal with the right UC grants) can discover and read these tables via a SQL warehouse from any workspace — there's no per-workspace boundary at the data layer, no per-workspace API to fan out across, no platform isolation to fight. Three sub-sources, one SQL-warehouse implementation:
  - **2a. AI Gateway inference tables** — `<catalog>.<schema>.<endpoint_name>_payload`. One row per gateway-served request: input, output, status, latency, model, tokens. Request-level. **The default first-touch source** for cross-workspace observability today.
  - **2b. UC-stored MLflow OTel traces, account-wide** — discovered via `SELECT FROM system.information_schema.tables WHERE table_name LIKE '%_otel_spans'` across the metastore. Each matching table is queried directly. Verified empirically: a single SQL warehouse query lists every OTel spans table in the account regardless of which workspace's experiment created it; SELECT against the discovered tables succeeds whenever UC grants permit. Span-level, full agent execution detail. Covers both local-workspace UC-bound experiments *and* UC-bound experiments from every other workspace in the account, in a single uniform query path.
  - The legacy MLflow REST API does *not* return UC-stored traces (verified empirically — `/api/2.0/mlflow/traces` responds with `experiment does not exist` for UC-bound experiments), which is why even the local-workspace UC slice is served by SQL rather than by the existing Tier 1 REST path.
- **Tier 3 (deferred):** Cross-workspace coverage for MLflow traces in the **default backend** — the residual gap that Tier 2 doesn't reach (because UC SQL only sees UC-stored data; default-backend traces in other workspaces aren't in UC at all). Reaching them would require per-workspace MLflow REST calls, which the destination workspace's gateway blocks today. A working prototype of that fan-out exists internally and may be useful if Tier 2 adoption signals a meaningful gap *and* the platform changes in *Trigger conditions* below make this path operationally tractable. Whether to promote it from "parked" to a shipped feature is a future decision, not a current commitment.

Framing for the README and operator-facing docs: *"MLflow traces from the local workspace work today via the existing workflow when those traces use MLflow's default storage backend. Anything in Unity Catalog — AI Gateway request logs and UC-stored MLflow traces — works account-wide via SQL warehouse, with no per-workspace setup beyond the UC grants. The remaining gap is default-backend MLflow traces from other workspaces; how we close it will depend on adoption signals and how the Databricks platform evolves."*

## Alternatives considered (and current status)

| Option | Status today | What would change it |
|---|---|---|
| Cross-workspace MLflow REST fan-out from Serverless workflow | Deferred — blocked at destination workspace (HTTP 403 cert validation) | Native self-serve cross-workspace API control ships in the Databricks platform |
| Cross-workspace MLflow REST fan-out from Databricks Apps Compute | Deferred — same 403, Apps Compute is also subject to the same destination filter | Same as above |
| Cross-workspace MLflow REST fan-out from classic compute job | Deferred — classic compute was not available in our test workspaces; provisioning issues during investigation prevented validation | Reliable classic-compute provisioning across customer workspaces, AND classic egress confirmed not subject to the same filter at destination |
| External runner (cron / scheduler outside Databricks) | Proven working but deferred — operationally heavy: external scheduler, cross-system secret management, operator-side setup burden too high for a deployable app | Demand pulls us into shipping it as an opt-in Tier-2.5 anyway |
| One-off platform-level allowlist exceptions | Not viable as a product path — bespoke per-customer engagement, not a feature customers can self-enable | Won't change. Useful only for specific internal scenarios. |
| Wait for the platform-level self-serve control | Tracking | When it ships, opens a path to revisit Tier 3 (native cross-workspace MLflow REST) |

**These alternatives are deferred, not abandoned.** MLflow REST cross-workspace fan-out may eventually unlock trace coverage that the Unity Catalog approach doesn't reach — agents that don't route through AI Gateway and don't use UC-bound experiments. But it carries its own complexity: operational overhead, per-deployment service-principal provisioning across all target workspaces, and dependence on Databricks platform changes that aren't yet generally available. Whether it becomes a long-term direction will depend on how Tier 2 adoption plays out and how the platform evolves; for now it's not prioritized at this stage.

## Consequences

**Positive:**
- Ships today without an external scheduler.
- Customer setup is a documented Databricks-native step (enable AI Gateway request logging on relevant endpoints; optionally bind MLflow experiments to a UC trace location for richer span-level data) — not a multi-workspace service-principal provisioning project.
- No reliance on platform-level exceptions or out-of-band approvals.
- Aligns the app with Unity Catalog as the canonical cross-workspace observability surface — both AI Gateway data and UC MLflow traces flow through the same architectural pattern (UC SQL warehouse query, no fan-out).
- The MLflow REST fan-out prototype is parked, not deleted — if it becomes the right direction in the future, we resume that work without starting over.

**Negative:**
- Span-level coverage of *other* workspaces requires those workspaces' MLflow experiments to be UC-bound. MLflow 3.11+ with explicit UC trace binding is still Beta and region-limited. Default-backend traces in other workspaces fall into the deferred Tier 3 gap until either an operator opts into an external runner or the platform evolves.
- AI Gateway coverage gap: direct foundation-model calls that bypass the gateway aren't captured by Tier 2a. Tier 2b's UC OTel traces cover those calls if the deployment has bound its experiments to UC.
- The OTel spans table doesn't carry a `workspace_id` column; trace data is conceptually account-level once it lands in UC. If the UI needs to attribute traces to originating workspaces, that mapping has to come from the experiment-to-workspace association (e.g., `system.mlflow.experiments_latest`) rather than from the OTel rows themselves.
- Documentation must clearly explain: (i) local-workspace default-backend traces work via the existing workflow, (ii) anything in UC (gateway logs, UC-bound traces) is covered account-wide via SQL with the right UC grants, (iii) default-backend traces from other workspaces are a known coverage gap with no committed roadmap timing.
- Two ingestion paths into the shared cache schema: (a) MLflow REST for the local workspace's default-backend experiments, (b) UC SQL for everything in UC (gateway logs and UC-bound traces). The UC SQL path is one warehouse-query implementation with two normalizers — gateway-payload and OTel-spans.

**Code-level impact:**

Keep (still useful, no rework):
- `observability_trace_details` Lakebase cache table + DDL — works for both Tier 1 local default-backend traces *and* Tier 2b UC OTel traces (same shape).
- `_parse_trace_info_to_detail` shared helper in `mlflow_service.py`.
- Cache-first read in `get_trace_detail_for_workspace`.
- Frontend 7/14/30/90-day window filter.
- Per-trace detail caching in `04_discover_observability.py` for the local workspace.

Park behind a feature flag (Tier 3 prototype, deferred not deleted):
- The service-principal token-exchange path in `04_discover_observability.py`.
- `discovery_sp_secret_scope` and `narrow_test_workspace_id` bundle variables (default empty/off).
- Accounts API and `system.access.workspaces_latest` fallback for cross-workspace host resolution.
- The fan-out parallelism that targets remote workspaces.

Rationale for "park, don't delete": if we revisit this direction later, this is the closest starting point we have. Re-enabling behind a flag is cheaper than rebuilding from scratch.

Add:
- **UC MLflow OTel trace discovery** — pure SQL implementation, account-wide. Step 1: discover OTel tables via `SELECT FROM system.information_schema.tables WHERE table_name LIKE '%_otel_spans'`. Step 2: for each discovered table, query it over the retention window and normalize into the existing `observability_trace_details` schema. No per-workspace MLflow API calls, no experiment enumeration, no SP-on-each-workspace setup — UC governance is the only auth boundary. The same query path covers local and remote UC-bound traces uniformly. Optionally join with `system.mlflow.experiments_latest` if experiment-name context is needed in the UI.
- **`gateway_inference_logs` Lakebase cache table** — for AI Gateway request logs (Tier 2a).
- **AI Gateway inference table discovery** in `06_discover_gateway_usage.py` (or split into a new `07_discover_gateway_inference_logs.py`) — enumerates endpoints with logging enabled, queries `<catalog>.<schema>.<endpoint_name>_payload` over the retention window, upserts to Lakebase.
- **New "Gateway requests" panel** in the Observability page, alongside the existing "Traces" panel. Gateway-request rows and trace rows can be visually distinguished but share the same cross-workspace querying primitives.
- **README section**: "Enabling cross-workspace observability" — covers AI Gateway request-logging setup and UC MLflow trace binding (with link to the public docs page on storing traces in Unity Catalog), and the UC grants the operator's principal needs (USE_CATALOG, USE_SCHEMA, SELECT on the OTel tables and gateway payload tables).

## Trigger conditions for revisiting this decision

Re-open this ADR if any of the following change. The first three would make MLflow REST cross-workspace fan-out more operationally tractable than it is today; the fourth would put pressure on us to close coverage gaps regardless of the operational cost.

1. **A native Databricks self-serve control for cross-workspace API access ships.** At that point, the parked Tier 3 code could be re-evaluated as a viable path directly from the Databricks workflow, without external infrastructure.
2. **Classic compute workspace provisioning becomes reliable** *and* classic egress is confirmed not subject to the same destination filter as Serverless. Could open a Databricks-native path for the deferred prototype.
3. **A `system.mlflow.traces` (or equivalent) Unity Catalog system table ships**, making default-backend MLflow traces queryable cross-workspace via SQL the same way AI Gateway logs and UC OTel traces already are. This would close the Tier 3 gap entirely without needing fan-out at all — everything would route through UC SQL.
4. **AI Gateway + UC MLflow trace adoption is too narrow** in operator feedback. If most deployments use direct model calls outside the gateway *and* haven't adopted UC-bound experiments, Tier 2 misses too much, and Tier 3's gap (default-backend traces in other workspaces) is the dominant unmet need. At that point, we'd reconsider the trade-off and may decide that the operational cost of an external runner (or a then-newly-tractable native option) is worth shipping despite the complexity.

## References

- RCA: [`docs/rca/2026-04-28-cross-workspace-trace-discovery.md`](../rca/2026-04-28-cross-workspace-trace-discovery.md) (full investigation and diagnostic matrix).
- Storing MLflow traces in Unity Catalog (Databricks public documentation): https://learn.microsoft.com/en-us/azure/databricks/mlflow3/genai/tracing/trace-unity-catalog
- AI Gateway inference table / request logging — see Databricks Mosaic AI Gateway documentation.
