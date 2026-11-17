import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/browser',
  testMatch: 'search.spec.js',
  fullyParallel: false,
  workers: 1,
  timeout: 90000,
  expect: { timeout: 15000 },
  outputDir: 'artifacts/m2-02/browser-results',
  reporter: [['list'], ['json', { outputFile: 'artifacts/m2-02/browser-test-result.json' }]],
  use: {
    baseURL: 'http://127.0.0.1:18082',
    channel: 'chrome',
    headless: true,
    viewport: { width: 1440, height: 1000 },
    screenshot: 'only-on-failure',
    trace: 'off'
  }
});
