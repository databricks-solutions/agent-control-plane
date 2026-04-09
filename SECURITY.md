# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, please email security@databricks.com with:
- A description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

We will acknowledge receipt within 48 hours and provide a timeline for a fix.

## Security Considerations

### Authentication
- The app uses Databricks Apps OBO (On-Behalf-Of) authentication
- All API endpoints require authentication (except `/api/health` and `/api/config`)
- Without OBO enabled, the app falls back to a read-only service principal with no admin privileges

### Data Access
- The app accesses data through the authenticated user's permissions
- System table queries (`system.mlflow.*`, `system.billing.*`, `system.serving.*`) respect Unity Catalog ACLs
- Cross-workspace data requires account-level access

### Secrets Management
- Never commit secrets (`.env`, tokens, keys) to the repository
- Use Databricks Secrets for service principal credentials
- The `deploy.sh` script generates `app.yaml` from `.env` at deploy time — `app.yaml` is not committed with real values

### CORS
- CORS is disabled by default in production (same-origin)
- Only enable `CORS_ORIGINS` for local development
