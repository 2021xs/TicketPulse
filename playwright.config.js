import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/browser',
  testMatch: 'mvp.spec.js',
  fullyParallel: false,
  workers: 1,
  timeout: 90000,
  expect: { timeout: 10000 },
  outputDir: 'artifacts/m1-04/browser-results',
  reporter: [['list'], ['json', { outputFile: 'artifacts/m1-04/browser-test-result.json' }]],
  use: {
    baseURL: 'http://127.0.0.1:18081',
    channel: 'chrome',
    headless: true,
    viewport: { width: 1440, height: 1000 },
    screenshot: 'only-on-failure',
    trace: 'off'
  }
});
