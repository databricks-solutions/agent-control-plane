/**
 * One-time auth setup: Log in to Databricks and save session.
 *
 * Run: npx playwright test --project=setup
 *
 * A browser will open. Complete the Databricks SSO login flow.
 * After successful login, the session is saved to .auth/databricks-auth.json
 *
 * Then run the workspace filter test:
 *   AUTH_STATE_PATH=.auth/databricks-auth.json npx playwright test tests/governance-workspace-filter.spec.ts
 */
import { test as setup } from '@playwright/test'

const APP_URL = process.env.APP_URL || 'http://localhost:8000'
const authFile = '.auth/databricks-auth.json'

setup('authenticate', async ({ page }) => {
  const { mkdirSync, existsSync } = await import('fs')
  if (!existsSync('.auth')) mkdirSync('.auth', { recursive: true })

  await page.goto(APP_URL, { waitUntil: 'networkidle', timeout: 60000 })

  // If we see "Log in", we need to authenticate
  const loginVisible = await page.locator('text=Log in').isVisible().catch(() => false)
  if (loginVisible) {
    await page.click('text=Continue with SSO')
    // User must complete SSO in the opened browser (you have ~2 min)
    await page.waitForURL(/databricksapps\.com/, { timeout: 120000 })
  }

  // Wait for Governance page (confirms we're logged in)
  await page.waitForSelector('text=Governance', { timeout: 30000 })
  await page.context().storageState({ path: authFile })
})
