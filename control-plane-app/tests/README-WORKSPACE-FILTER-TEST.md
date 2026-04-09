# Governance Workspace Filter Test

## Quick Start (after authentication)

1. **One-time: Save your Databricks session**
   ```bash
   npm run test:auth
   ```
   A browser opens. Complete the Databricks SSO login. After reaching the Governance page, your session is saved to `.auth/databricks-auth.json`.

2. **Run the workspace filter test**
   ```bash
   npm run test:governance
   ```

## What the test does

1. Navigates to the Governance page (default page)
2. Takes a screenshot with "All Workspaces" selected
3. Selects a specific workspace from the dropdown
4. Takes a screenshot after selection
5. Verifies:
   - Dropdown stays on the selected workspace (doesn't reset)
   - Data (KPIs) changes after selection
   - API call to `/api/billing/page-data` is made with `workspace_id` parameter
   - Response status is 200

## Manual testing checklist

If you prefer to test manually:

1. Go to your deployed app URL (set APP_URL env var)
2. Log in via Databricks SSO if prompted
3. On the Governance page, find the workspace dropdown (top-right, next to the days selector)
4. Screenshot with "All Workspaces" selected
5. Select a specific workspace
6. Observe:
   - [ ] Did the data (KPIs, charts, tables) change?
   - [ ] Did the dropdown stay on the selected workspace?
   - [ ] Were there loading indicators?
7. Open DevTools (F12) → Network tab
8. Change workspace again
9. Check:
   - [ ] Was `/api/billing/page-data` called?
   - [ ] Did the request include `workspace_id` query param?
   - [ ] Response status 200?

## Code flow (for reference)

- **Frontend**: `Governance.tsx` → `useBillingPageData(days, workspaceId)` → `apiClient.get('/billing/page-data', { params: { days, workspace_id } })`
- **Backend**: `billing.py` → `page_data(days, workspace_id)` → `get_all_page_data(days, workspace_id)` → SQL filtered by `workspace_id`
