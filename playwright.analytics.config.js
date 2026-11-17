import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/browser', testMatch: 'analytics.spec.js', fullyParallel: false,
  workers: 1, timeout: 90000, expect: { timeout: 15000 },
  outputDir: 'artifacts/m3-01/browser-results',
  reporter: [['list'], ['json', { outputFile: 'artifacts/m3-01/browser-test-result.json' }]],
  use: { baseURL: 'http://127.0.0.1:18083', channel: 'chrome', headless: true,
    viewport: { width: 1440, height: 1000 }, screenshot: 'only-on-failure', trace: 'off' }
});
