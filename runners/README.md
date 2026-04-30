# External runners

Scripts that run **outside** Databricks compute (laptop, GitHub Actions,
self-hosted runner) to fill discovery gaps that the in-cloud workflow can't
cover from Serverless / Apps Compute.

## tier3_local.py — cross-workspace MLflow trace fan-out

The default-backend MLflow trace path (Tier 3 in the architecture doc) calls
each workspace's MLflow REST API directly. From Databricks Serverless it's
blocked by the destination workspace's SNP filter; an external runner is not.

### Prerequisites

1. **Account membership/access on the workspaces you want to cover.** The
   runner authenticates as the user; the user needs at least workspace USER
   (and resource ACLs to read experiments) on each target workspace.
2. **Account-level OAuth profile.** The script uses the user's account
   profile. Set up once:
   ```
   databricks auth login --host https://accounts.cloud.databricks.com
   ```
   This creates a CLI profile (often named for your email) — pass it via
   `--account-profile` or set `DATABRICKS_CONFIG_PROFILE`.
3. **Lakebase config in env.** The runner writes to the same Lakebase
   instance the in-cloud workflow uses. Source the app's `.env` first:
   ```
   source control-plane-app/.env
   ```
4. **Python deps.**
   ```
   pip install psycopg2-binary databricks-sdk requests
   ```

### Usage

```bash
# Smoke test: dry-run against 5 workspaces, no DB writes
python runners/tier3_local.py --dry-run --max-workspaces 5

# Single-workspace targeted run
python runners/tier3_local.py --workspace-id 7474649325766269

# Real run, all workspaces in the account, last 90 days
python runners/tier3_local.py --retention-days 90

# Larger workspace cap, lower concurrency for shaky networks
python runners/tier3_local.py --concurrency 3 --max-traces-per-exp 500
```

Flags:

| Flag | Default | Meaning |
|------|---------|---------|
| `--dry-run` | off | Don't write to Lakebase; print counts only. |
| `--max-workspaces N` | 0 (no cap) | Limit fan-out to first N workspaces (testing). |
| `--workspace-id ID` | — | Run against a single workspace_id only. |
| `--retention-days N` | 90 | Trace recency window. |
| `--max-experiments-per-ws N` | 50 | Cap per workspace. |
| `--max-traces-per-exp N` | 200 | Cap per experiment. |
| `--account-profile P` | env or `kaan...` | CLI profile to authenticate with. |
| `--concurrency N` | 6 | Parallel workers. Lower for 429s. |

### What lands in Lakebase

Rows are upserted into `observability_traces` and
`observability_trace_details` with `data_source = 'rest_fanout'` (so they're
distinguishable from Tier 1's `rest_api`, Tier 2a's `gateway`, and Tier 2b's
`uc_otel` / `uc_trace_logs`). The app's existing endpoints surface them
without any code change.

### Scheduling

For a real Tier-3 setup you'd want this to run periodically. Two clean paths:

- **GitHub Actions** — cron schedule, account-OAuth client credentials of
  a service principal stored as repo secrets, runs on Actions runners which
  are not Databricks Serverless and so aren't subject to the SNP filter.
- **A self-hosted runner / launchd / systemd timer** on a host that has a
  routable path to `*.cloud.databricks.com`.

Both keep the data flow uniform (Lakebase upsert) so the rest of the app
doesn't need to know whether traces came from in-cloud or external.

### Caveats

- **429s.** The MLflow REST API rate-limits aggressively at the account
  level; default concurrency of 6 with backoff is conservative but you
  may still see retries on big runs.
- **Permissions.** A workspace where the user has no membership returns 403
  silently; the runner counts it as an error and continues.
- **Default-backend only.** This path doesn't add anything for UC-stored
  traces — those are already covered by Tier 2b (UC SQL).
