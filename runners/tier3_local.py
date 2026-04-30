#!/usr/bin/env python3
"""Tier-3 cross-workspace MLflow trace runner — runs locally to bypass the
serverless cross-workspace SNP block.

Authenticates with the user's account-OAuth credentials, fans out to each
workspace's MLflow REST API to search experiments + traces, and writes the
result into the same Lakebase observability tables that the in-cloud
discovery workflow uses (with data_source = 'rest_fanout' to distinguish
them from the Tier 1 / 2a / 2b paths).

Why local: the SNP filter at each destination workspace rejects calls
originating from Databricks Serverless / Apps Compute. An external runner
(laptop, GH Actions) doesn't trigger the filter — auth still flows through
account OAuth, so coverage matches the operator's permissions.

Quick start:
    pip install psycopg2-binary databricks-sdk requests
    cd /path/to/agent-control-plane
    source control-plane-app/.env                # for Lakebase config
    export DATABRICKS_CONFIG_PROFILE=kaan.kuguoglu@databricks.com
    python runners/tier3_local.py --dry-run --max-workspaces 5

Real run:
    python runners/tier3_local.py --retention-days 90

Flags:
    --dry-run               Don't write to Lakebase. Print counts only.
    --max-workspaces N      Limit fan-out to first N workspaces (for testing).
    --retention-days N      Trace recency window (default: 90).
    --max-traces-per-exp N  Cap traces fetched per experiment (default: 200).
    --account-profile P     Account-OAuth profile (default: env-or-DEFAULT).
    --workspace-id WS_ID    Run against a single workspace_id only.
    --concurrency N         Parallel workers (default: 6 — keep low for 429).
"""
import argparse
import json
import os
import subprocess
import sys
import time
import threading
import urllib.error
import urllib.request as ur
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────

DEFAULT_PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE") or "kaan.kuguoglu@databricks.com"
ACCOUNT_HOST = "https://accounts.cloud.databricks.com"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--max-workspaces", type=int, default=0, help="0 = no cap")
    p.add_argument("--retention-days", type=int, default=90)
    p.add_argument("--max-traces-per-exp", type=int, default=200)
    p.add_argument("--max-experiments-per-ws", type=int, default=50)
    p.add_argument("--account-profile", default=DEFAULT_PROFILE)
    p.add_argument("--workspace-id", default=None)
    p.add_argument("--concurrency", type=int, default=6)
    # SP M2M auth (used for cross-workspace REST). Either pass via flags, env
    # vars (ACP_SP_CLIENT_ID, ACP_SP_CLIENT_SECRET), or a Databricks secret
    # scope where the runner reads them.
    p.add_argument("--sp-client-id", default=os.environ.get("ACP_SP_CLIENT_ID"))
    p.add_argument("--sp-client-secret", default=os.environ.get("ACP_SP_CLIENT_SECRET"))
    p.add_argument("--sp-secret-profile", default="acp-sandbox",
                   help="Databricks profile to read the SP creds from a secret scope.")
    p.add_argument("--sp-secret-scope", default="acp-discovery")
    return p.parse_args()


def load_sp_creds(args):
    """Resolve client_id + client_secret from flags / env / secret scope."""
    cid = args.sp_client_id
    csec = args.sp_client_secret
    if cid and csec:
        return cid, csec
    # Fall back to a Databricks secret scope (default: 'acp-discovery' on sandbox).
    # Secret values come back base64-encoded — decode before use.
    import base64 as _b64
    try:
        for key, target in (("client_id", "cid"), ("client_secret", "csec")):
            rc, out, err = cli(args.sp_secret_profile, ["secrets", "get-secret", args.sp_secret_scope, key, "--output", "json"])
            if rc != 0:
                continue
            v = json.loads(out).get("value", "")
            if not v:
                continue
            try:
                decoded = _b64.b64decode(v).decode().strip()
            except Exception:
                decoded = v.strip()
            if target == "cid": cid = decoded
            else: csec = decoded
    except Exception as e:
        print(f"  (couldn't read secret scope: {e})")
    if not cid or not csec:
        sys.exit(
            "Missing SP credentials. Either:\n"
            "  - export ACP_SP_CLIENT_ID and ACP_SP_CLIENT_SECRET, or\n"
            "  - keep them in a Databricks secret scope (default scope 'acp-discovery'\n"
            "    on the 'acp-sandbox' profile, with keys client_id and client_secret).\n"
        )
    return cid, csec


# ── Auth helpers ──────────────────────────────────────────────────

def cli(profile, args):
    p = subprocess.run(["databricks", "--profile", profile] + args,
                       capture_output=True, text=True, timeout=30)
    return p.returncode, p.stdout.strip(), p.stderr.strip()


def get_account_token(profile):
    """Returns the account-OAuth access token. Used for the account API
    (workspace listing). NOT valid for workspace REST APIs — those need
    token-exchange via the SDK's per-workspace client (see ws_token)."""
    rc, out, err = cli(profile, ["auth", "token", "--output", "json"])
    if rc != 0:
        sys.exit(f"auth token fetch failed: {err}")
    return json.loads(out)["access_token"]


# Per-workspace token cache — minting a workspace-scoped token is non-trivial
# (the SDK does it via OIDC token-exchange against the account host). Cache
# tokens for ~30 min so we don't pay the cost on every API call.
_ws_token_cache: dict = {}
_ws_token_lock = threading.Lock()


def ws_token(ws_host, sp_client_id, sp_client_secret):
    """Mint a workspace-scoped token via SP M2M client_credentials flow.

    POST {ws_host}/oidc/v1/token with HTTP Basic auth (client_id:client_secret)
    yields an access_token accepted by that workspace's REST API. Tokens are
    short-lived (~1h); cache for 30 min.
    """
    with _ws_token_lock:
        cached = _ws_token_cache.get(ws_host)
        if cached and cached[1] > time.time():
            return cached[0], None
    import base64
    cred = base64.b64encode(f"{sp_client_id}:{sp_client_secret}".encode()).decode()
    body = "grant_type=client_credentials&scope=all-apis"
    req = ur.Request(
        f"{ws_host}/oidc/v1/token",
        data=body.encode(),
        method="POST",
        headers={
            "Authorization": f"Basic {cred}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with ur.urlopen(req, timeout=15) as r:
            d = json.loads(r.read().decode())
            token = d.get("access_token")
    except urllib.error.HTTPError as e:
        try: msg = e.read().decode()[:200]
        except Exception: msg = str(e.reason)
        return None, f"{e.code} {msg}"
    except Exception as e:
        return None, str(e)[:120]
    if not token:
        return None, "empty token"
    with _ws_token_lock:
        _ws_token_cache[ws_host] = (token, time.time() + 1800)
    return token, None


def list_account_workspaces(profile):
    rc, out, err = cli(profile, ["account", "workspaces", "list", "--output", "json"])
    if rc != 0:
        sys.exit(f"workspaces list failed: {err}")
    return json.loads(out)


# ── HTTP with backoff ─────────────────────────────────────────────

def _do_request(method, url, token, body=None, max_retries=5, timeout=30):
    delay = 1.0
    for _ in range(max_retries):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Authorization": f"Bearer {token}"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        req = ur.Request(url, data=data, method=method, headers=headers)
        try:
            with ur.urlopen(req, timeout=timeout) as r:
                txt = r.read().decode()
                return r.status, (json.loads(txt) if txt else {})
        except urllib.error.HTTPError as e:
            if e.code in (429,) or e.code >= 500:
                time.sleep(delay); delay = min(delay * 2, 30); continue
            try: msg = e.read().decode()[:300]
            except Exception: msg = str(e.reason)
            return e.code, msg
        except Exception:
            time.sleep(delay); delay = min(delay * 2, 30); continue
    return -1, "retries_exhausted"


# ── MLflow API per workspace ──────────────────────────────────────

def search_experiments(ws_host, token, max_results=50):
    code, body = _do_request(
        "POST", f"{ws_host}/api/2.0/mlflow/experiments/search", token,
        body={"max_results": max_results, "order_by": ["last_update_time DESC"]},
    )
    if code == 200 and isinstance(body, dict):
        return body.get("experiments", []), None
    return [], (code, str(body)[:200])


def search_traces(ws_host, token, experiment_ids, retention_days, max_results=200):
    """`GET /api/2.0/mlflow/traces?experiment_ids=...&max_results=...` —
    matches the endpoint the in-cloud workflow (04_discover_observability.py)
    uses. The MLflow REST trace API takes one experiment_id at a time, so we
    iterate and apply the retention filter client-side."""
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=retention_days)).timestamp() * 1000)
    import urllib.parse as up
    out = []
    for eid in experiment_ids:
        qs = up.urlencode({"experiment_ids": eid, "max_results": max_results})
        code, body = _do_request("GET", f"{ws_host}/api/2.0/mlflow/traces?{qs}", token)
        if code != 200 or not isinstance(body, dict):
            return out, (code, str(body)[:200])
        for t in body.get("traces", []):
            info = t.get("info") or t
            ts = info.get("timestamp_ms") or t.get("timestamp_ms") or 0
            try: ts = int(ts)
            except Exception: ts = 0
            if ts and ts < cutoff_ms:
                continue
            out.append(t)
    return out, None


def get_trace_detail(ws_host, token, request_id):
    """MLflow 3.x trace fetch by request_id."""
    # URL-encode the request_id since it may contain ':' / '/' (UC traces).
    import urllib.parse as up
    rid = up.quote(request_id, safe="")
    code, body = _do_request(
        "GET", f"{ws_host}/api/2.0/mlflow/traces/{rid}", token,
    )
    if code == 200 and isinstance(body, dict):
        return body, None
    return None, (code, str(body)[:200])


# Span fetching uses the MLflow Python client because the basic REST trace
# endpoint only returns trace_info; spans live in artifact storage and only
# the client knows how to assemble them. The client reads auth from env
# vars (DATABRICKS_HOST / DATABRICKS_TOKEN), which is global state — guard
# with a lock so concurrent workspace workers don't clobber each other.
_mlflow_lock = threading.Lock()


def fetch_span_data(ws_host: str, token: str, request_id: str):
    """Return (spans_list, request_str, response_str, error_str) for a trace.

    Uses mlflow.MlflowClient.get_trace which transparently handles MLflow's
    artifact-store fetch. Returns ([], "", "", err) on missing data — that
    case is normal (failed agents may not persist spans)."""
    try:
        from mlflow.tracking import MlflowClient
    except ImportError:
        return [], "", "", "mlflow not installed"
    with _mlflow_lock:
        prev_host = os.environ.get("DATABRICKS_HOST")
        prev_tok = os.environ.get("DATABRICKS_TOKEN")
        prev_uri = os.environ.get("MLFLOW_TRACKING_URI")
        os.environ["DATABRICKS_HOST"] = ws_host
        os.environ["DATABRICKS_TOKEN"] = token
        os.environ["MLFLOW_TRACKING_URI"] = "databricks"
        try:
            client = MlflowClient(tracking_uri="databricks")
            try:
                trace = client.get_trace(request_id)
            except Exception as e:
                return [], "", "", str(e)[:120]
            if not trace or not getattr(trace, "data", None):
                return [], "", "", "empty"
            data = trace.data
            spans = []
            for s in (getattr(data, "spans", None) or []):
                # Span objects expose to_dict() in MLflow 3.x; fall back to
                # a hand-rolled projection of the public attributes.
                if hasattr(s, "to_dict"):
                    try:
                        spans.append(s.to_dict()); continue
                    except Exception: pass
                spans.append({
                    "name": getattr(s, "name", None),
                    "span_id": getattr(s, "span_id", None),
                    "parent_id": getattr(s, "parent_id", None),
                    "start_time_ns": getattr(s, "start_time_ns", None),
                    "end_time_ns": getattr(s, "end_time_ns", None),
                    "span_type": getattr(s, "span_type", None),
                    "status": str(getattr(s, "status", None)),
                    "inputs": getattr(s, "inputs", None),
                    "outputs": getattr(s, "outputs", None),
                    "attributes": getattr(s, "attributes", None),
                })
            req = getattr(data, "request", "") or ""
            resp = getattr(data, "response", "") or ""
            return spans, req, resp, None
        finally:
            # Restore env vars so other (non-MLflow) code paths aren't poisoned.
            for k, v in (("DATABRICKS_HOST", prev_host), ("DATABRICKS_TOKEN", prev_tok), ("MLFLOW_TRACKING_URI", prev_uri)):
                if v is None: os.environ.pop(k, None)
                else: os.environ[k] = v


# ── Lakebase write ────────────────────────────────────────────────

def get_lakebase_creds(profile=None):
    """Mint a Lakebase Postgres password using the same flow as
    setup_lakebase_tables.py — works for both Autoscaling and Provisioned.

    `profile` selects which Databricks workspace's API to call for
    credential minting (must match the workspace that hosts the Lakebase
    instance). Defaults to LAKEBASE_PROFILE env or 'acp-sandbox'."""
    import requests
    from databricks.sdk import WorkspaceClient
    profile = profile or os.environ.get("LAKEBASE_PROFILE") or "acp-sandbox"
    w = WorkspaceClient(profile=profile)
    me = w.current_user.me()
    pg_user = me.user_name
    headers = w.config.authenticate()
    token = headers["Authorization"].replace("Bearer ", "")
    host = w.config.host.rstrip("/")
    endpoint = os.environ.get("LAKEBASE_ENDPOINT_PATH", "")
    instance = os.environ.get("LAKEBASE_INSTANCE", "")
    if endpoint:
        r = requests.post(f"{host}/api/2.0/postgres/credentials",
                          headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                          json={"endpoint": endpoint})
        r.raise_for_status()
        return pg_user, r.json()["token"]
    if instance:
        r = requests.post(f"{host}/api/2.0/database/credentials",
                          headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                          json={"instance_names": [instance], "request_id": str(uuid.uuid4())})
        r.raise_for_status()
        return pg_user, r.json()["token"]
    sys.exit("Set LAKEBASE_ENDPOINT_PATH (Autoscaling) or LAKEBASE_INSTANCE (Provisioned).")


def lakebase_connection():
    import psycopg2
    user, password = get_lakebase_creds()
    conn = psycopg2.connect(
        host=os.environ["LAKEBASE_DNS"],
        port=5432,
        database=os.environ.get("LAKEBASE_DATABASE", "control_plane"),
        user=user,
        password=password,
        sslmode="require",
    )
    return conn


# ── Per-trace projection into Lakebase rows ───────────────────────

def trace_to_rows(workspace_id, ws_name, exp_id, trace_obj, detail_obj):
    """Project an MLflow trace (search hit + detail) into the
    (observability_traces, observability_trace_details) row shapes used by
    02_sync_to_lakebase.py — but with data_source='rest_fanout'.

    The MLflow REST `/api/2.0/mlflow/traces` response uses flat top-level
    keys (no `info` wrapper); the detail call's response uses
    `{trace: {trace_info: {...}, trace_data: {...}}}`. We accept both.
    """
    # Flat fallbacks: a search hit IS the info dict.
    info = trace_obj.get("info") or trace_obj or {}
    if detail_obj:
        # Prefer detail's trace_info when richer (it carries trace_metadata),
        # but the search hit's primitive numeric fields are more reliable —
        # detail sometimes returns execution_time as "0s"-style duration
        # strings from MLflow 3.x. Merge but let the search hit win for keys
        # that already had a value.
        di = (detail_obj.get("trace") or {}).get("trace_info") or detail_obj.get("trace_info") or {}
        if di:
            merged = dict(di)
            merged.update({k: v for k, v in info.items() if v not in (None, "", 0)})
            info = merged
    request_id = info.get("request_id") or ""

    def _to_int(v):
        if v is None or v == "": return None
        try: return int(v)
        except (TypeError, ValueError):
            # MLflow 3.x sometimes returns durations like "0s" or "5.7s"
            if isinstance(v, str) and v.endswith("s"):
                try: return int(float(v[:-1]) * 1000)
                except Exception: return None
            return None

    ts = _to_int(info.get("timestamp_ms")) or _to_int(info.get("request_time"))
    duration = _to_int(info.get("execution_time_ms")) or _to_int(info.get("execution_duration"))
    state = info.get("status") or info.get("state") or ""
    # tags / request_metadata can be list of {key,value} (REST) or dict (cache)
    tags = info.get("tags") or {}
    if isinstance(tags, list):
        tags = {t.get("key", ""): t.get("value", "") for t in tags if isinstance(t, dict)}
    trace_name = (tags.get("mlflow.traceName") if isinstance(tags, dict) else "") or ""
    # Token usage / model id from request_metadata (MLflow 3.x list-of-pairs)
    meta = info.get("request_metadata") or info.get("trace_metadata") or info.get("metadata") or {}
    if isinstance(meta, list):
        meta = {m.get("key", ""): m.get("value", "") for m in meta if isinstance(m, dict)}
    tu_str = meta.get("mlflow.trace.tokenUsage") if isinstance(meta, dict) else None
    try:
        tu_obj = json.loads(tu_str) if isinstance(tu_str, str) else (tu_str or {})
    except Exception:
        tu_obj = {}
    model_id = meta.get("mlflow.modelId") if isinstance(meta, dict) else None
    user = meta.get("mlflow.user") if isinstance(meta, dict) else None
    source = meta.get("mlflow.source.name") if isinstance(meta, dict) else None

    # Best-effort user_message/response_preview from the detail object.
    request_raw = ""
    response_raw = ""
    response_preview = ""
    user_message = ""
    # MLflow detail response is one of two shapes — `{trace_info, trace_data}`
    # at top level (3.x) or `{trace: {trace_info, trace_data}}` (older). Try
    # both so we don't drop spans for no good reason.
    td_outer = {}
    if detail_obj:
        td_outer = detail_obj.get("trace_data") or (detail_obj.get("trace") or {}).get("trace_data") or {}
        request_raw = info.get("request") or td_outer.get("request") or ""
        response_raw = info.get("response") or td_outer.get("response") or ""
        response_preview = info.get("response_preview") or ""

    # MLflow REST exposes request/response as values inside request_metadata
    # rather than as top-level fields. Pull them out so the dedicated columns
    # (`user_message`, `response_preview`, `request_raw`, `response_raw`) get
    # populated for Tier-3 rows the same way Tier-1/2 rows do.
    if isinstance(meta, dict):
        meta_req = meta.get("mlflow.trace.request") or ""
        meta_resp = meta.get("mlflow.trace.response") or ""
        if not user_message and meta_req:
            user_message = meta_req
        if not request_raw and meta_req:
            request_raw = meta_req
        if not response_raw and meta_resp:
            response_raw = meta_resp
        # Build a readable response preview if we don't have one yet —
        # capped so the slim list row stays small.
        if not response_preview and meta_resp:
            response_preview = meta_resp[:1000]

    trace_row = {
        "request_id": request_id,
        "workspace_id": str(workspace_id),
        "experiment_id": str(exp_id),
        "trace_name": trace_name,
        "state": state,
        "request_time": str(ts) if ts is not None else "",
        "execution_duration": duration,
        "user_message": user_message,
        "response_preview": response_preview,
        "token_usage": json.dumps(tu_obj),
        "model_id": model_id or "",
        "session_id": (meta.get("mlflow.trace.session") if isinstance(meta, dict) else "") or "",
        "trace_user": user or "",
        "source": source or "",
        "tags": json.dumps(tags) if isinstance(tags, dict) else "{}",
        "data_source": "rest_fanout",
    }

    detail_row = None
    if detail_obj:
        ti_json = json.dumps(info)
        td_json = json.dumps(td_outer or {})
        size_bytes = len(ti_json) + len(td_json)
        detail_row = {
            "workspace_id": str(workspace_id),
            "request_id": request_id,
            "experiment_id": str(exp_id),
            "trace_info": ti_json,
            "trace_data": td_json,
            "request_raw": request_raw,
            "response_raw": response_raw,
            "size_bytes": size_bytes,
            "source_type": "rest_fanout",
        }

    return trace_row, detail_row


def upsert_lakebase(trace_rows, detail_rows):
    if not trace_rows and not detail_rows:
        return 0, 0
    import psycopg2.extras
    conn = lakebase_connection()
    now = datetime.now(timezone.utc)
    n_traces = 0
    n_details = 0
    try:
        with conn.cursor() as cur:
            if trace_rows:
                vals = [
                    (
                        r["request_id"], r["workspace_id"], r["experiment_id"],
                        r["trace_name"], r["state"], r["request_time"],
                        r["execution_duration"], r["user_message"], r["response_preview"],
                        r["token_usage"], r["model_id"], r["session_id"],
                        r["trace_user"], r["source"], r["tags"], r["data_source"], now,
                    )
                    for r in trace_rows if r["request_id"]
                ]
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO observability_traces
                       (request_id, workspace_id, experiment_id, trace_name, state,
                        request_time, execution_duration, user_message, response_preview,
                        token_usage, model_id, session_id, trace_user, source, tags,
                        data_source, last_synced)
                    VALUES %s
                    ON CONFLICT (workspace_id, request_id) DO UPDATE SET
                        trace_name = EXCLUDED.trace_name, state = EXCLUDED.state,
                        request_time = EXCLUDED.request_time,
                        execution_duration = EXCLUDED.execution_duration,
                        user_message = EXCLUDED.user_message,
                        response_preview = EXCLUDED.response_preview,
                        token_usage = EXCLUDED.token_usage,
                        model_id = EXCLUDED.model_id, session_id = EXCLUDED.session_id,
                        trace_user = EXCLUDED.trace_user, source = EXCLUDED.source,
                        tags = EXCLUDED.tags, data_source = EXCLUDED.data_source,
                        last_synced = EXCLUDED.last_synced
                """, vals, page_size=100)
                n_traces = len(vals)
            if detail_rows:
                vals = [
                    (
                        r["workspace_id"], r["request_id"], r["experiment_id"],
                        r["trace_info"], r["trace_data"], r["request_raw"],
                        r["response_raw"], r["size_bytes"], r["source_type"], now,
                    )
                    for r in detail_rows if r["request_id"]
                ]
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO observability_trace_details
                       (workspace_id, request_id, experiment_id, trace_info, trace_data,
                        request_raw, response_raw, size_bytes, source_type, cached_at)
                    VALUES %s
                    ON CONFLICT (workspace_id, request_id) DO UPDATE SET
                        experiment_id = EXCLUDED.experiment_id,
                        trace_info = EXCLUDED.trace_info,
                        trace_data = EXCLUDED.trace_data,
                        request_raw = EXCLUDED.request_raw,
                        response_raw = EXCLUDED.response_raw,
                        size_bytes = EXCLUDED.size_bytes,
                        source_type = EXCLUDED.source_type,
                        cached_at = EXCLUDED.cached_at
                """, vals, page_size=50)
                n_details = len(vals)
            conn.commit()
    finally:
        conn.close()
    return n_traces, n_details


# ── Per-workspace processing ──────────────────────────────────────

def process_workspace(w, args, progress, sp_client_id, sp_client_secret):
    """Returns (workspace_id, n_experiments, n_traces, errors)."""
    ws_id = str(w.get("workspace_id") or "")
    deployment = w.get("deployment_name") or ""
    ws_name = w.get("workspace_name") or ws_id
    if not deployment:
        return ws_id, 0, 0, [(ws_name, "no deployment_name")]
    ws_host = f"https://{deployment}.cloud.databricks.com"
    errors = []

    # Mint a workspace-scoped token via the SP's M2M client_credentials flow.
    token, err = ws_token(ws_host, sp_client_id, sp_client_secret)
    if not token:
        return ws_id, 0, 0, [(ws_name, f"token-exchange: {err}")]

    exps, err = search_experiments(ws_host, token, max_results=args.max_experiments_per_ws)
    if err:
        return ws_id, 0, 0, [(ws_name, f"search_experiments {err[0]}: {err[1]}")]
    if not exps:
        return ws_id, 0, 0, []

    # Search traces across all this workspace's experiments in one call
    exp_ids = [str(e.get("experiment_id")) for e in exps if e.get("experiment_id")]
    traces, err = search_traces(ws_host, token, exp_ids,
                                args.retention_days, max_results=args.max_traces_per_exp)
    if err:
        return ws_id, len(exps), 0, [(ws_name, f"search_traces {err[0]}: {err[1]}")]

    trace_rows, detail_rows = [], []
    for t in traces:
        info = t.get("info") or t
        rid = info.get("request_id") or t.get("request_id")
        exp_id = info.get("experiment_id") or t.get("experiment_id") or ""
        if not rid:
            continue
        # Fetch detail (best-effort; on error we still include the slim row)
        detail, derr = get_trace_detail(ws_host, token, rid)
        # Fetch full span data via the MLflow Python client. Failures here
        # are expected for traces whose agent crashed before persisting
        # spans — emit a slim row anyway.
        spans, req_full, resp_full, span_err = fetch_span_data(ws_host, token, rid)
        # Splice the spans into the detail's trace_data so trace_to_rows /
        # the existing detail-row builder picks them up uniformly.
        if detail is None:
            detail = {}
        # Both shapes — top-level trace_data and nested under 'trace':
        td_top = detail.get("trace_data") or {}
        if not isinstance(td_top, dict): td_top = {}
        if spans:
            td_top["spans"] = spans
        if req_full and not td_top.get("request"): td_top["request"] = req_full
        if resp_full and not td_top.get("response"): td_top["response"] = resp_full
        detail["trace_data"] = td_top
        tr, dr = trace_to_rows(ws_id, ws_name, exp_id, t, detail)
        trace_rows.append(tr)
        if dr:
            detail_rows.append(dr)
        if derr:
            errors.append((ws_name, f"detail {derr[0]} for {rid[:24]}"))
        if span_err and span_err not in ("empty",):
            # Don't flood logs with the routine "missing span data" message.
            if "missing span data" not in span_err:
                errors.append((ws_name, f"spans: {span_err[:80]}"))

    written_t, written_d = (0, 0)
    if not args.dry_run and trace_rows:
        written_t, written_d = upsert_lakebase(trace_rows, detail_rows)

    progress.tick(ws_name, len(exps), len(trace_rows), written_t, written_d)
    return ws_id, len(exps), len(trace_rows), errors


class Progress:
    def __init__(self, total):
        self.total = total
        self.done = 0
        self.lock = threading.Lock()
    def tick(self, ws_name, n_exps, n_traces, w_t, w_d):
        with self.lock:
            self.done += 1
            tag = f"[{self.done:>3d}/{self.total}]"
            mark = "·" if n_traces == 0 else "✓"
            print(f"  {tag} {mark} {ws_name[:48]:<48s}  exps={n_exps:>3d}  traces={n_traces:>4d}  wrote={w_t}/{w_d}")


def main():
    args = parse_args()
    # Account API (workspaces list) uses the user's account-OAuth profile.
    # Workspace REST APIs use SP M2M tokens minted per-workspace.
    sp_cid, sp_csec = load_sp_creds(args)
    print(f"sp      : {sp_cid[:8]}…  (M2M client_credentials)")

    if args.workspace_id:
        # Targeted run — just one workspace
        all_ws = list_account_workspaces(args.account_profile)
        workspaces = [w for w in all_ws if str(w.get("workspace_id")) == str(args.workspace_id)]
        if not workspaces:
            sys.exit(f"workspace_id {args.workspace_id} not found in account")
    else:
        workspaces = list_account_workspaces(args.account_profile)
        if args.max_workspaces:
            workspaces = workspaces[:args.max_workspaces]

    print(f"profile : {args.account_profile}")
    print(f"target  : {len(workspaces)} workspaces")
    print(f"window  : {args.retention_days}d  cap/exp: {args.max_traces_per_exp}  exps/ws: {args.max_experiments_per_ws}")
    print(f"dry-run : {args.dry_run}\n")

    progress = Progress(len(workspaces))
    total_exps = 0; total_traces = 0; all_errors = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(process_workspace, w, args, progress, sp_cid, sp_csec) for w in workspaces]
        for fut in as_completed(futures):
            try:
                ws_id, n_exps, n_traces, errors = fut.result()
                total_exps += n_exps
                total_traces += n_traces
                all_errors.extend(errors)
            except Exception as e:
                import traceback as _tb
                all_errors.append(("?", f"unexpected: {e}\n{_tb.format_exc()[:1500]}"))
    print(f"\ndone in {time.time()-t0:.1f}s")
    print(f"experiments scanned : {total_exps}")
    print(f"traces collected    : {total_traces}")
    print(f"errors              : {len(all_errors)}")
    if all_errors[:8]:
        from collections import Counter
        c = Counter(msg.split(" ")[0] for _, msg in all_errors)
        print(f"  by status: {dict(c.most_common(8))}")
        for ws, msg in all_errors[:5]:
            print(f"  - {ws[:40]}  {msg}")


if __name__ == "__main__":
    main()
