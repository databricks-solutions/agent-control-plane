# RCA: Cross-workspace trace discovery silently returning empty

**Date:** 2026-04-28 (updated 2026-04-29)
**Status:** Diagnosed; root cause is a Databricks platform-level limitation. Cross-workspace data is sourced via Unity Catalog instead. See [related ADR](../decisions/2026-04-29-cross-workspace-observability-pivot.md).
**Severity:** Feature failure, no data loss

## Summary

The discovery workflow's cross-workspace MLflow trace fan-out has been silently returning zero traces from every workspace except the runner's local one. The Observability page's cross-workspace traces never actually surfaced data from any other workspace. Local workspace caching succeeded, masking the cross-workspace failure from casual inspection.

## Symptom

The cached `observability_traces` table contained rows from a single workspace only — the workspace where the discovery workflow ran. System tables showed many more workspaces with MLflow experiments (~80) but none of those translated into cached traces.

## Investigation

The discovery workflow logs showed:
- The Lakebase `workspace_registry` host-mapping table was empty (it had only ever been populated by an admin endpoint that was never invoked).
- Adding a fallback that resolves hosts via `system.access.workspaces_latest` recovered ~13 reachable workspaces.
- Even after host resolution worked, fan-out returned `error_breakdown: {}` (no errors) and `total_traces: 0` from every remote workspace.
- Auditing the few cached traces that did exist confirmed every row traced back to an experiment owned by the workflow runner in the runner's local workspace.

Initial hypothesis: an MLflow permission-filtering issue (other users' experiments not visible to the runner principal). Provisioning a service principal with workspace admin on each target workspace and re-running the fan-out tested this hypothesis directly.

## Root cause

The MLflow API call from one workspace's compute to another workspace's API is rejected at the destination workspace with:

```
HTTP 403  {"error_code":403,"message":"Cert validation failed.
Both workspace comparison and snp system trusted checks did not pass."}
```

This is **not** an authentication or authorization issue. The OIDC token exchange succeeds. The service principal grant is valid. The destination workspace's API gateway rejects the request based on its origin — Databricks' platform isolation between workspaces blocks cross-workspace REST API calls from inside another workspace's compute.

The block applies whenever the destination workspace has any Network Policy attached — even one that allows everything. This is documented Databricks platform behavior.

## Diagnostic matrix

We tested every viable runtime to confirm the constraint. Same caller identity, same target, same endpoint — only the runtime differs:

| Caller runtime | Token exchange | Cross-workspace MLflow API call |
|---|---|---|
| Databricks Serverless workflow | ✅ | ❌ 403 cert validation |
| Databricks Apps Compute | ✅ | ❌ 403 cert validation |
| Databricks Classic compute | not available in our test environment | not testable |
| External runner (HTTP from outside Databricks) | ✅ | ✅ 200 + trace data |

The destination filter is enforced at the receiving workspace's gateway, not by egress controls at the source. External callers — those not originating from a Databricks workspace's compute layer — pass through.

## Why this wasn't caught earlier

- The local workspace path always worked (the workflow runner could read their own experiments), so the cache always had data. The page rendered, masking the cross-workspace failure.
- Fan-out errors were caught and logged as "0 traces" — indistinguishable from "succeeded but no data".
- No alerting compared `workspaces_queried` against `workspaces_with_traces`. A 14:1 ratio in production would have been an obvious signal.
- Account admin permissions in Databricks ≠ implicit MLflow read access to every workspace's experiments — a subtle distinction.

## Workarounds we evaluated

| Approach | Status |
|---|---|
| Use Classic compute instead of Serverless (different egress path) | Could not test in our environment — workspaces were provisioned serverless-only and didn't have classic worker environments associated, partly due to a recent industry-wide supply-chain security incident affecting Python dependency provisioning |
| Run the discovery from outside any Databricks workspace (laptop / external scheduler) | **Proven working** — the same service principal credentials, hitting the same MLflow API, return real trace data when the request originates outside Databricks compute. Operationally heavier (external scheduler, secret management). |
| Engineering-only platform exception | Per-workspace-pair allowlist managed by the Databricks platform team. Bespoke per-customer engagement, not a feature customers can self-enable. Useful for one-off internal scenarios only. |
| Wait for native platform-level customer control | A user-facing control for cross-workspace API access is on the Databricks platform roadmap. No public GA confirmation as of this RCA's update date. |

## Decision

Documented separately in the [companion ADR](../decisions/2026-04-29-cross-workspace-observability-pivot.md). Briefly: source cross-workspace observability data from Unity Catalog (AI Gateway request-logging tables and UC-stored MLflow OTel traces) instead of fanning out per-workspace REST API calls. UC is account-level and naturally cross-workspace queryable via SQL warehouse — no platform isolation to fight. The MLflow REST fan-out path is preserved on a feature branch to be re-enabled when the platform-level control ships.

## Action items

- [x] Confirm the root cause via direct probes from each runtime.
- [x] Document the diagnostic matrix and the decision in the companion ADR.
- [ ] Implement Tier 2 (Unity Catalog–sourced cross-workspace observability) — see ADR for scope.
- [ ] Add an observability metric: alert when `workspaces_queried > workspaces_with_traces × N` to catch silent regressions of the same shape (cross-workspace fan-out reporting success but returning empty).
- [ ] Document the deployment requirements for cross-workspace observability in the install guide.
- [ ] Track the platform-level self-serve control on the Databricks roadmap; revisit the deferred MLflow REST fan-out path when it ships.
