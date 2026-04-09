import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './tests',
  outputDir: './test-results',
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: 0,
  workers: 1,
  reporter: 'list',
  use: {
    baseURL: process.env.APP_URL || 'http://localhost:8000',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    storageState: '.auth/databricks-auth.json',
  },
  projects: [
    {
      name: 'setup',
      testMatch: /.*auth\.setup\.ts/,
      use: { ...devices['Desktop Chrome'], headless: false, storageState: undefined },
    },
    { name: 'chromium', use: { ...devices['Desktop Chrome'] }, dependencies: ['setup'] },
  ],
  timeout: 60000,
})
