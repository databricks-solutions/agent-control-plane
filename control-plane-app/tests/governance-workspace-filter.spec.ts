/**
 * Test: Governance page workspace filter
 *
 * Run: npx playwright test tests/governance-workspace-filter.spec.ts --headed
 *
 * Prerequisites:
 * - You must be logged into Databricks (or the app will redirect to login)
 * - Run with --headed to complete OAuth if needed on first run
 */
import { test, expect } from '@playwright/test'

const APP_URL = process.env.APP_URL || 'http://localhost:8000'

test.describe('Governance page workspace filter', () => {
  test.beforeEach(async ({ page }) => {
    // Listen for page-data API calls
    await page.goto(APP_URL, { waitUntil: 'networkidle', timeout: 60000 })
  })

  test('workspace filter: All Workspaces -> specific workspace', async ({ page }) => {
    // Wait for Governance page to load (it's the default)
    await page.waitForSelector('text=Governance', { timeout: 15000 })
    await page.waitForSelector('text=Cost attribution', { timeout: 15000 })

    // Find the workspace dropdown (should show "All Workspaces" initially)
    const workspaceSelect = page.locator('select').filter({ has: page.locator('option[value="__all__"]') }).first()
    await expect(workspaceSelect).toBeVisible({ timeout: 10000 })

    // 1. Screenshot with "All Workspaces" selected
    await page.screenshot({ path: 'test-results/01-all-workspaces.png', fullPage: true })

    // Capture initial KPI values for comparison
    const initialKpis = await page.locator('[class*="KpiCard"]').textContent().catch(() => '')
    const initialCost = await page.locator('text=Total Serving Cost').locator('..').textContent().catch(() => '')

    // Find workspace options (excluding "All Workspaces")
    const options = await workspaceSelect.locator('option').all()
    const specificWorkspaceOptions: typeof options = []
    for (const o of options) {
      const v = await o.getAttribute('value')
      if (v && v !== '__all__') specificWorkspaceOptions.push(o)
    }

    if (specificWorkspaceOptions.length === 0) {
      test.skip(true, 'No specific workspaces available in dropdown')
    }

    // Get the first specific workspace value
    const firstSpecificValue = await specificWorkspaceOptions[0].getAttribute('value')
    const firstSpecificLabel = await specificWorkspaceOptions[0].textContent()

    // 2. Select the specific workspace
    const pageDataRequests: Array<{ url: string; params: Record<string, string>; status: number }> = []
    page.on('request', (req) => {
      const url = req.url()
      if (url.includes('/api/billing/page-data') || url.includes('page-data')) {
        const params = Object.fromEntries(new URL(req.url()).searchParams)
        pageDataRequests.push({ url, params, status: 0 })
      }
    })
    page.on('response', async (res) => {
      const url = res.url()
      if (url.includes('/api/billing/page-data') || url.includes('page-data')) {
        const match = pageDataRequests.find((r) => r.url === url)
        if (match) match.status = res.status()
      }
    })

    await workspaceSelect.selectOption(firstSpecificValue!)

    // Wait for potential loading/refetch
    await page.waitForTimeout(2000)

    // 3. Screenshot after selecting workspace
    await page.screenshot({ path: 'test-results/02-specific-workspace.png', fullPage: true })

    // 4. Verify dropdown stayed on selected workspace
    const selectedValue = await workspaceSelect.inputValue()
    const dropdownStayed = selectedValue === firstSpecificValue

    // 5. Check if data changed (compare KPIs)
    const afterKpis = await page.locator('[class*="KpiCard"]').textContent().catch(() => '')
    const afterCost = await page.locator('text=Total Serving Cost').locator('..').textContent().catch(() => '')
    const dataChanged = initialCost !== afterCost

    // 6. Check for API call with workspace_id
    await page.waitForTimeout(1000)
    const pageDataCalls = await page.evaluate(() => {
      const entries = (performance as any).getEntriesByType?.('resource') || []
      return entries.filter((e: any) => e.name?.includes('page-data')).map((e: any) => e.name)
    })

    // Log observations
    console.log('\n=== WORKSPACE FILTER TEST OBSERVATIONS ===')
    console.log('1. Dropdown stayed on selected workspace:', dropdownStayed ? 'YES' : 'NO')
    console.log('2. Selected workspace:', firstSpecificLabel, '(value:', firstSpecificValue, ')')
    console.log('3. Data (KPIs) changed after selection:', dataChanged ? 'YES' : 'NO')
    console.log('4. Page-data API calls detected:', pageDataCalls.length)
    console.log('5. Page data requests captured:', JSON.stringify(pageDataRequests, null, 2))
  })

  test('Network: page-data includes workspace_id when filter applied', async ({ page }) => {
    await page.waitForSelector('text=Governance', { timeout: 15000 })
    const workspaceSelect = page.locator('select').filter({ has: page.locator('option[value="__all__"]') }).first()
    await expect(workspaceSelect).toBeVisible({ timeout: 10000 })

    const options = await workspaceSelect.locator('option').all()
    let wsValue: string | null = null
    for (const o of options) {
      const v = await o.getAttribute('value')
      if (v && v !== '__all__') {
        wsValue = v
        break
      }
    }
    if (!wsValue) test.skip(true, 'No workspaces')

    // Capture requests
    const requests: Array<{ url: string; params: Record<string, string>; status?: number }> = []
    page.on('request', (req) => {
      const url = req.url()
      if (url.includes('page-data')) {
        const u = new URL(url)
        const params: Record<string, string> = {}
        u.searchParams.forEach((v, k) => (params[k] = v))
        requests.push({ url, params })
      }
    })
    page.on('response', async (res) => {
      const url = res.url()
      if (url.includes('page-data')) {
        const r = requests.find((x) => x.url === url)
        if (r) (r as any).status = res.status()
      }
    })

    await workspaceSelect.selectOption(wsValue)
    await page.waitForTimeout(3000)

    const pageDataCalls = requests.filter((r) => r.url.includes('page-data'))
    const withWorkspaceId = pageDataCalls.filter((r) => r.params?.workspace_id === wsValue)
    const status = pageDataCalls[pageDataCalls.length - 1]?.status

    console.log('\n=== NETWORK: /api/billing/page-data ===')
    console.log('Calls after workspace change:', pageDataCalls.length)
    console.log('Calls with workspace_id param:', withWorkspaceId.length)
    console.log('workspace_id value in request:', withWorkspaceId[0]?.params?.workspace_id)
    console.log('Response status:', status)

    expect(pageDataCalls.length).toBeGreaterThanOrEqual(1)
    expect(withWorkspaceId.length).toBeGreaterThanOrEqual(1)
    expect(status).toBe(200)
  })
})
