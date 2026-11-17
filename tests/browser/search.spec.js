import { test, expect } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { writeFileSync, mkdirSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

const output = resolve('artifacts/m2-02');
mkdirSync(output, { recursive: true });
function fixture(action, value) {
  const args = ['scripts/search_fixture.py', action];
  if (value !== undefined) args.push(String(value));
  return execFileSync('python3', args, { encoding: 'utf8' });
}
async function login(page, username) {
  await page.goto('/login');
  await page.getByLabel('账号', { exact: true }).fill(username);
  await page.getByLabel('密码', { exact: true }).fill('TicketPulse!2026');
  await page.getByRole('button', { name: '登录', exact: true }).click();
  await expect(page).toHaveURL(/\/workspace/);
  await expect(page.locator('#workspace-data table, #workspace-data .empty').first()).toBeVisible();
}
async function search(page, q) {
  await page.goto('/search?q=' + encodeURIComponent(q));
  await expect(page.getByRole('heading', { name: '搜索工单', exact: true })).toBeVisible();
  await expect(page.locator('#search-data')).not.toContainText('正在搜索');
}
async function create(page, title) {
  await page.goto('/tickets/new');
  await page.getByLabel('标题', { exact: false }).fill(title);
  await page.getByLabel('问题描述', { exact: false }).fill('Actual browser ticket for Binlog → Kafka → ES search');
  await page.getByLabel('优先级', { exact: false }).selectOption('high');
  await page.getByRole('button', { name: '提交工单', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v1');
  return Number(new URL(page.url()).pathname.split('/').at(-1));
}
async function eventuallyFound(page, title) {
  await expect(async () => {
    await search(page, title);
    await expect(page.locator('#search-data tbody')).toContainText(title, { timeout: 1500 });
  }).toPass({ timeout: 20000 });
}
async function screenshot(page, label) { await page.screenshot({ path: resolve(output, label + '.png'), fullPage: true }); }

test('Search: login, real CDC create, filters, stable pages, detail and current permissions', async ({ page }) => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await login(page, 'alice');
  const suffix = randomUUID().slice(0, 8);
  const title = 'Browsersearch ' + suffix;
  const id = await create(page, title);
  await page.getByRole('link', { name: '搜索工单', exact: true }).click();
  await expect(page).toHaveURL(/\/search/);
  await eventuallyFound(page, title);
  await expect(page.locator('#search-freshness')).toContainText('页面刷新');
  await expect(page.locator('#search-data')).toContainText('总数暂不可用');
  await page.locator('#search-data .ticket-link').click();
  await expect(page.locator('#ticket-version')).toHaveText('v1');
  await expect(page.getByRole('heading', { name: title, exact: true })).toBeVisible();

  // More than one page of actual Cookie-authenticated creates; independent expected order.
  const ids = await page.evaluate(async ({ suffix }) => {
    const me = await (await fetch('/api/v1/me')).json();
    const result = [];
    for (let i = 0; i < 12; i++) {
      const response = await fetch('/api/v1/tickets', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': me.csrf_token }, body: JSON.stringify({ request_id: crypto.randomUUID(), title: 'Searchpaging ' + suffix + ' ' + i, description: 'Browser paging sample', category: 'network', priority: i % 2 ? 'high' : 'low' }) });
      if (response.status !== 201) throw new Error('Create paging sample failed');
      result.push((await response.json()).ticket.id);
    }
    return result;
  }, { suffix });
  await expect(async () => { await page.goto('/search?q=Searchpaging%20' + suffix + '&sort=id&order=asc&page_size=50'); await expect(page.locator('#search-data tbody tr')).toHaveCount(12, { timeout: 2000 }); }).toPass({ timeout: 20000 });
  await page.goto('/search?q=Searchpaging%20' + suffix + '&sort=id&order=asc&page_size=10');
  await expect(page.locator('#search-data tbody tr')).toHaveCount(10);
  const first = await page.locator('#search-data .ticket-link').evaluateAll(links => links.map(link => Number(link.getAttribute('href').split('/').at(-1))));
  expect(first).toEqual(ids.slice(0, 10));
  await page.getByRole('button', { name: '下一页', exact: true }).click();
  await expect(page.locator('#search-data tbody tr')).toHaveCount(2);
  const second = await page.locator('#search-data .ticket-link').evaluateAll(links => links.map(link => Number(link.getAttribute('href').split('/').at(-1))));
  expect(second).toEqual(ids.slice(10));
  await page.getByRole('button', { name: '上一页', exact: true }).click();
  await expect(page.locator('#search-data tbody tr')).toHaveCount(10);
  if (!(await page.locator('#search-filters').evaluate(el => el.open))) {
    await page.locator('#search-filters summary').click();
  }
  await page.getByLabel('优先级（可多选）', { exact: true }).selectOption(['high']);
  await page.getByLabel('排序字段', { exact: true }).selectOption('priority');
  await page.getByLabel('排序方向', { exact: true }).selectOption('desc');
  await page.getByRole('button', { name: '搜索', exact: true }).click();
  await expect(page.locator('#search-data tbody tr')).toHaveCount(6);
  await expect(page.locator('#search-data .badge.low')).toHaveCount(0);
  await screenshot(page, 'search-filters-and-results');

  await page.getByRole('button', { name: '退出登录' }).click();
  await login(page, 'it-agent');
  await page.goto('/tickets/' + id);
  await page.getByRole('button', { name: '认领工单', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v2');
  await page.getByRole('button', { name: '开始处理', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v3');
  await eventuallyFound(page, title);
  await page.getByRole('button', { name: '退出登录' }).click();
  await login(page, 'supervisor');
  await page.goto('/tickets/' + id);
  fixture('stop-worker');
  try {
    await page.getByRole('button', { name: '跨团队转派', exact: true }).click();
    await page.getByLabel('目标团队', { exact: true }).selectOption({ label: '设备服务台' });
    await page.getByLabel('操作原因', { exact: true }).fill('浏览器跨团队搜索授权验收');
    await page.getByRole('button', { name: '确认跨团队转派', exact: true }).click();
    await expect(page.locator('#ticket-version')).toHaveText('v4');
    await page.getByRole('button', { name: '退出登录' }).click();
    await login(page, 'it-agent');
    await search(page, title);
    await expect(page.locator('#search-data .empty')).toContainText('没有符合条件');
    await expect(page.locator('#search-data tbody')).toHaveCount(0);
    await screenshot(page, 'search-stale-permission');
    await page.goto('/tickets/' + id);
    await expect(page.locator('.empty')).toContainText('没有查看');
  } finally { fixture('start-worker'); }
  await page.getByRole('button', { name: '退出登录' }).click();
  await login(page, 'device-agent');
  await eventuallyFound(page, title);
  await expect(page.locator('#search-data tbody')).toContainText('设备服务台');
  await page.locator('#search-data .ticket-link').click();
  await expect(page.locator('#ticket-version')).toHaveText('v4');
  await expect(page.getByRole('button', { name: '认领工单', exact: true })).toBeVisible();
  expect(errors).toEqual([]);
  writeFileSync(resolve(output, 'browser-search-success.json'), JSON.stringify({ browser: 'Chrome', passed: true, actual_created_ticket: id, current_version: 4, paging_expected: ids, first, second, stale_index_denied: true, cookie_login: true, page_errors: errors }, null, 2) + '\n');
});

test('Search: loading, parameter errors, real ES outage and escaped text', async ({ page }) => {
  await login(page, 'alice');
  await page.route('**/api/v1/search/tickets?*', async route => { await new Promise(resolve => setTimeout(resolve, 700)); await route.continue(); });
  await page.goto('/search?q=doesnotexist');
  await expect(page.locator('#search-data')).toContainText('正在搜索');
  await expect(page.locator('#search-data .empty')).toContainText('没有符合条件');
  await page.unroute('**/api/v1/search/tickets?*');
  await page.goto('/search?sort=wrong');
  await expect(page.locator('#search-data')).toContainText('sort 为');
  const title = 'Escapesearch ' + randomUUID().slice(0, 8) + ' <svg onload=alert(1)>';
  await create(page, title);
  await eventuallyFound(page, title);
  await expect(page.locator('#search-data svg')).toHaveCount(0);
  await expect(page.locator('#search-data .ticket-link')).toContainText('<svg');
  execFileSync('python3', ['scripts/local_search.py', 'stop', '--component=es'], { encoding: 'utf8' });
  try {
    await page.goto('/search?q=Escapesearch');
    await expect(page.locator('#search-data')).toContainText('搜索服务暂不可用');
    await expect(page.locator('#search-data tbody')).toHaveCount(0);
    await screenshot(page, 'search-service-unavailable');
  } finally { execFileSync('python3', ['scripts/local_search.py', 'start'], { encoding: 'utf8' }); }
  await page.getByRole('button', { name: '重试搜索', exact: true }).click();
  await expect(page.locator('#search-data tbody')).toContainText(title);
  await page.setViewportSize({ width: 760, height: 1000 });
  await expect(page.locator('#search-form')).toBeVisible();
  await screenshot(page, 'search-responsive');
  writeFileSync(resolve(output, 'browser-search-errors.json'), JSON.stringify({ browser: 'Chrome', passed: true, loading: true, invalid_parameters: true, native_elasticsearch_stopped: true, escaped_title: title, resumed_search: true }, null, 2) + '\n');
});
