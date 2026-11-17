import { defineConfig } from '@playwright/test';
import { readFileSync, mkdirSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const marker = JSON.parse(readFileSync(resolve(root, process.env.DEMO02_MARKER || '.local/demo-02/current.json')));
const env = JSON.parse(readFileSync(resolve(root, marker.runtime, 'environment.json')));
if (env.DEMO02_API !== 'http://127.0.0.1:18102' || !/^ticketpulse_test_demo02_[a-f0-9]{16}$/.test(env.DEMO02_SCHEMA)) throw Error('Private DEMO-02 resource identity required');
const attempt = new Date().toISOString().replace(/[:.]/g, '-');
const output = resolve(root, marker.evidence, 'browser', attempt);
mkdirSync(output, { recursive: true });

export default defineConfig({
  testDir: dirname(fileURLToPath(import.meta.url)), testMatch: 'demo02.spec.js',
  fullyParallel: false, workers: 1, retries: 0, timeout: 60000,
  expect: { timeout: 10000 }, outputDir: resolve(output, 'results'),
  reporter: [['list'], ['json', { outputFile: resolve(output, 'report.json') }]],
  use: { baseURL: env.DEMO02_API, channel: 'chrome', headless: false,
    viewport: { width: 1440, height: 1000 }, trace: 'off', video: 'off', screenshot: 'off' }
});
