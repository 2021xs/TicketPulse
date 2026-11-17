// Real Chrome user flow for DEMO-01. Keep contexts across controlled API
// restarts; control steps are run separately through the official Go CLI.
import { chromium, expect } from '@playwright/test';
import { readFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createInterface } from 'node:readline';
import { randomUUID } from 'node:crypto';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const marker = JSON.parse(readFileSync(resolve(root, '.local/demo-01/current.json')));
const runtime = resolve(root, marker.runtime), output = resolve(root, marker.evidence, 'browser');
const env = JSON.parse(readFileSync(resolve(runtime, 'environment.json')));
const login = JSON.parse(readFileSync(resolve(runtime, 'login.json')));
if (env.DEMO_API !== '127.0.0.1:18101' || !/^ticketpulse_test_demo_[a-f0-9]{16}$/.test(env.DEMO_SCHEMA)) throw Error('Private DEMO identity required');
mkdirSync(output, { recursive: true });
const base = 'http://' + env.DEMO_API;
const browser = await chromium.launch({ channel: 'chrome', headless: false });
const pages = {}, contexts = {}, requests = [], events = [];
let ticket, extra, aCursor, bCursor;

function save(name, value) {
  const file = resolve(output, name + '-' + Date.now() + '.json');
  writeFileSync(file, JSON.stringify(value, null, 2) + '\n', { flag: 'wx' });
  return file;
}
async function shot(page, label) {
  await page.screenshot({ path: resolve(output, label + '-' + Date.now() + '.png'), fullPage: true,
    mask: [page.locator('.login-help'), page.locator('#password')] });
}
for (const role of ['alice', 'it-agent', 'supervisor', 'device-agent']) {
  contexts[role] = await browser.newContext({ baseURL: base, viewport: { width: 1440, height: 1000 } });
  pages[role] = await contexts[role].newPage();
  pages[role].on('response', async response => {
    const url = new URL(response.url());
    if (url.origin !== base || !/^\/api\/v1\/(tickets|search|analytics|read-target)/.test(url.pathname)) return;
    const item = { at: new Date().toISOString(), role, method: response.request().method(), path: url.pathname + url.search,
      status: response.status(), request: response.request().postData(), target: {} };
    const headers = response.headers();
    for (const [key, value] of Object.entries(headers)) if (key.startsWith('x-ticketpulse-')) item.target[key] = value;
    try { item.body = await response.json(); } catch { item.body_unavailable = true; }
    requests.push(item);
  });
}

async function signIn(role) {
  const page = pages[role];
  await page.goto('/login');
  await page.getByLabel('账号', { exact: true }).fill(role);
  await page.getByLabel('密码', { exact: true }).fill(login.password);
  await page.getByRole('button', { name: '登录', exact: true }).click();
  await expect(page).toHaveURL(/\/workspace/);
  await expect(page.locator('#workspace-data table, #workspace-data .empty').first()).toBeVisible();
}
async function create(role, title) {
  const page = pages[role];
  await page.goto('/tickets/new');
  await page.getByLabel('标题', { exact: false }).fill(title);
  await page.getByLabel('问题描述', { exact: false }).fill('独立产品预演：网络异常影响支持协作，请检查设备与业务连通性。');
  await page.getByLabel('类别', { exact: false }).selectOption('network');
  await page.getByLabel('优先级', { exact: false }).selectOption('high');
  await page.getByRole('button', { name: '提交工单', exact: true }).click();
  await expect(page).toHaveURL(/\/tickets\/\d+$/);
  await expect(page.locator('#ticket-version')).toHaveText('v1');
  return Number(new URL(page.url()).pathname.split('/').at(-1));
}
async function open(role, id) {
  await pages[role].goto('/tickets/' + id);
  await expect(pages[role].locator('#ticket-version')).toBeVisible();
}
async function action(role, name, version) {
  await pages[role].getByRole('button', { name, exact: true }).click();
  await expect(pages[role].locator('#ticket-version')).toHaveText('v' + version);
}
async function comment(role, content, version) {
  await pages[role].getByLabel('补充评论', { exact: true }).fill(content);
  await pages[role].getByRole('button', { name: '发送评论', exact: true }).click();
  await expect(pages[role].locator('#ticket-version')).toHaveText('v' + version);
  await expect(pages[role].locator('#comment-list')).toContainText(content);
}
async function result(page, path) {
  return page.evaluate(async path => {
    const r = await fetch('/api/v1' + path);
    return { status: r.status, target: Object.fromEntries([...r.headers].filter(([key]) => key.startsWith('x-ticketpulse-'))), body: await r.json() };
  }, path);
}
function target(value, version, revision, scope) {
  expect(value.status).toBe(200);
  expect(value.target['x-ticketpulse-read-version']).toBe(version);
  expect(value.target['x-ticketpulse-read-revision']).toBe(String(revision));
  if (scope) expect(value.target['x-ticketpulse-status-scope']).toBe(scope);
}
async function visibleQueries(label, version, revision) {
  const page = pages.supervisor;
  await page.goto('/search?sort=id&order=asc&page_size=10');
  await expect(page.locator('#search-data tbody tr')).toHaveCount(10);
  await expect(page.locator('#search-next')).toBeEnabled();
  const search = await result(page, '/search/tickets?sort=id&order=asc&page_size=10');
  target(search, version, revision);
  expect(search.body.freshness.version_id).toBe(version);
  expect(search.body.freshness.read_revision).toBe(revision);
  await shot(page, label + '-search');
  await page.goto('/analytics?start=2026-11-01&end=2026-11-04');
  await expect(page.locator('#metric-created')).toHaveText('10');
  await expect(page.locator('#metric-resolved')).toHaveText('6');
  const analysis = await result(page, '/analytics?start=2026-11-01&end=2026-11-04');
  target(analysis, version, revision);
  expect(analysis.body.overview.average_first_resolution_seconds).toBe(60600);
  expect(analysis.body.sync.version_id).toBe(version);
  expect(analysis.body.sync.read_revision).toBe(revision);
  await shot(page, label + '-analytics');
  save(label + '-queries', { search, analytics: analysis, session_still_active: !new URL(page.url()).pathname.includes('login') });
  return search.body.next_cursor;
}

async function stage(name) {
  if (name === 'login-only') {
    for (const role of Object.keys(pages)) await signIn(role);
    return { users_logged_in_for_read_only_observations: true };
  }
  if (name === 'product') {
    for (const role of Object.keys(pages)) await signIn(role);
    ticket = await create('alice', 'DEMO 浏览器跨团队网络工单');
    await comment('alice', '提单人补充：会议室网络反复断开。', 2);
    await shot(pages.alice, 'created-comment');
    await open('it-agent', ticket);
    await action('it-agent', '认领工单', 3);
    await action('it-agent', '开始处理', 4);
    await open('supervisor', ticket);
    await pages.supervisor.getByRole('button', { name: '跨团队转派', exact: true }).click();
    await pages.supervisor.getByLabel('目标团队', { exact: true }).selectOption({ label: '设备服务台' });
    await pages.supervisor.getByLabel('操作原因', { exact: true }).fill('交换机异常，交由设备团队处理。');
    await pages.supervisor.getByRole('button', { name: '确认跨团队转派', exact: true }).click();
    await expect(pages.supervisor.locator('#ticket-version')).toHaveText('v5');
    await shot(pages.supervisor, 'transferred');
    await pages['it-agent'].goto('/tickets/' + ticket);
    await expect(pages['it-agent'].locator('.empty')).toContainText('没有查看');
    await shot(pages['it-agent'], 'old-team-denied');
    await open('device-agent', ticket);
    await action('device-agent', '认领工单', 6);
    await action('device-agent', '开始处理', 7);
    await comment('device-agent', '设备团队已完成交换机替换。', 8);
    await pages['device-agent'].getByRole('button', { name: '提交解决', exact: true }).click();
    await pages['device-agent'].getByLabel('解决说明', { exact: true }).fill('连接测试通过，请提单人确认。');
    await pages['device-agent'].getByRole('button', { name: '确认提交解决', exact: true }).click();
    await expect(pages['device-agent'].locator('#ticket-version')).toHaveText('v9');
    await open('alice', ticket);
    await action('alice', '确认关闭', 10);
    await shot(pages.alice, 'closed-history');
    extra = await create('alice', 'DEMO 浏览器分页与维护工单');
    save('product', { state: 'PASS', ticket, extra, lifecycle_versions: 10, old_team_detail_denied: true });
    return { ticket, extra };
  }
  if (name === 'a1') {
    aCursor = await visibleQueries('a1', 'active_original', 1);
    return { read_revision: 1, pagination_cursor_saved_in_memory: Boolean(aCursor) };
  }
  if (name === 'during-rebuild') {
    const page = pages.supervisor;
    await page.goto('/search?q=DEMO');
    await expect(page.locator('#search-data tbody tr')).toHaveCount(2);
    const data = await result(page, '/search/tickets?q=DEMO');
    target(data, 'active_original', 1);
    await shot(page, 'a-serving-during-rebuild');
    return { A_still_serves_search: true, returned: data.body.items.length };
  }
  if (name === 'freeze') {
    await open('alice', extra);
    await pages.alice.getByLabel('补充评论', { exact: true }).fill('冻结期间这条评论应被拒绝。');
    const response = pages.alice.waitForResponse(r => r.url().endsWith('/api/v1/tickets/comments'));
    await pages.alice.getByRole('button', { name: '发送评论', exact: true }).click();
    const r = await response, body = await r.json();
    expect(r.status()).toBe(503);
    expect(body.error.code).toBe('BUSINESS_WRITES_FROZEN');
    await expect(pages.alice.locator('#detail-error')).toBeVisible();
    const text = await pages.alice.locator('#detail-error').innerText();
    await shot(pages.alice, 'frozen-write-error');
    const data = await result(pages.alice, '/tickets/' + extra);
    expect(data.status).toBe(200);
    expect(data.body.ticket.version).toBe(1);
    return { status: r.status(), code: body.error.code, visible_message: text, read_available: true, version_unchanged: 1 };
  }
  if (name === 'planned') {
    const page = pages.supervisor;
    await page.goto('/search');
    await expect(page.locator('#search-retry')).toBeVisible();
    const s = await result(page, '/search/tickets');
    expect(s.status).toBe(503);
    await shot(page, 'planned-search');
    await page.goto('/analytics');
    await expect(page.locator('#analytics-retry')).toBeVisible();
    expect(await page.locator('#analytics-data .loading').count()).toBe(0);
    const visible = await page.locator('#analytics-data').innerText();
    await shot(page, 'planned-analytics');
    return { search_status: s.status, search_code: s.body.error.code, analytics_visible: visible,
      analytics_failure_notice: await page.locator('#analytics-freshness').innerText() };
  }
  if (name === 'offline') {
    const page = pages.supervisor;
    // Existing document remains open while the private API process is stopped.
    await page.getByRole('button', { name: '重新加载统计', exact: true }).click();
    await expect(page.locator('#analytics-retry')).toBeVisible();
    await expect(page.locator('#analytics-data')).toContainText('无法连接服务');
    await shot(page, 'api-offline');
    return { visible: await page.locator('#analytics-data').innerText(), loading_ended: true };
  }
  if (name === 'b2') {
    bCursor = await visibleQueries('b2', 'candidate_v1', 2);
    const id = await create('alice', 'DEMO B 服务期间真实新工单');
    await comment('alice', 'B 已对外服务，A 应继续同步这次业务写入。', 2);
    await shot(pages.alice, 'b-live-new-ticket');
    save('b-business', { state: 'PASS', ticket: id, version: 2, used_browser_cookie_after_restart: true });
    return { read_revision: 2, new_ticket: id };
  }
  if (name === 'a3') {
    await visibleQueries('a3', 'active_original', 3);
    const id = await create('alice', 'DEMO A 回滚后真实新工单');
    await comment('alice', 'A3 已恢复业务，两套投影继续保留。', 2);
    await shot(pages.alice, 'a3-live-new-ticket');
    return { read_revision: 3, new_ticket: id, login_not_repeated: true };
  }
  if (name === 'cursor') {
    const page = pages.supervisor;
    const old = [];
    for (const [label, cursor] of [['a1', aCursor], ['b2', bCursor]]) {
      const value = await result(page, '/search/tickets?sort=id&order=asc&page_size=10&cursor=' + encodeURIComponent(cursor));
      expect(value.status).toBe(409);expect(value.body.error.code).toBe('SEARCH_CURSOR_TARGET_CHANGED');
      old.push({ label, ...value });
    }
    await page.goto('/search?sort=id&order=asc&page_size=10&cursor=' + encodeURIComponent(aCursor) + '&page=2');
    await expect(page.locator('#search-retry')).toBeVisible();
    const before = await page.locator('#search-data').innerText();
    const retry = page.waitForResponse(r => r.url().includes('/api/v1/search/tickets'));
    await page.locator('#search-retry').click();
    const response = await retry;
    await expect(page.locator('#search-retry')).toBeVisible();
    await shot(page, 'old-cursor-retry');
    const unchangedURL = new URL(page.url()).searchParams.has('cursor');
    // The existing search form is the verified way to start a new first page.
    const fresh = page.waitForResponse(r => r.url().includes('/api/v1/search/tickets') && !r.url().includes('cursor='));
    await page.getByRole('button', { name: '搜索', exact: true }).click();
    await fresh;
    await expect(page.locator('#search-data tbody tr')).toHaveCount(10);
    await page.locator('#search-next').click();
    await expect(page.locator('.page-index')).toHaveText('2');
    await expect(page.locator('#search-data tbody tr')).toHaveCount(4);
    await shot(page, 'a3-fresh-page2');
    const data = { old, retry_status: response.status(), old_cursor_retained_by_retry: unchangedURL,
      visible_error: before, new_search_form_recovers: true, fresh_page2_items: 4 };
    save('cursor-roundtrip', data);return data;
  }
  if (name === 'final') {
    await visibleQueries('final-a3', 'active_original', 3);
    await pages['it-agent'].goto('/tickets/' + ticket);
    await expect(pages['it-agent'].locator('.empty')).toContainText('没有查看');
    await open('device-agent', ticket);
    const data = await result(pages['device-agent'], '/tickets/' + ticket);
    expect(data.body.ticket.version).toBe(10);
    expect(data.body.history).toHaveLength(10);
    save('final-detail', data);
    save('requests', requests);save('events', events);
    return { pages_verified: true, permission_still_current: true };
  }
  if (name === 'current-metrics') {
    const page = pages.supervisor;
    const day = offset => { const d = new Date(Date.now() + 8 * 3600000); d.setUTCDate(d.getUTCDate() + offset); return d.toISOString().slice(0, 10); };
    await page.goto('/analytics?start=' + day(0) + '&end=' + day(1));
    await expect(page.locator('#metric-created')).toHaveText('4');
    await expect(page.locator('#metric-resolved')).toHaveText('1');
    await expect(page.locator('#metric-backlog')).toHaveText('8');
    const analytics = await result(page, '/analytics?start=' + day(0) + '&end=' + day(1));
    target(analytics, 'active_original', 3);
    const search = await result(page, '/search/tickets?q=DEMO&sort=id&order=asc&page_size=100');
    expect(search.body.items).toHaveLength(4);
    expect(search.body.items.find(t => t.title === 'DEMO 浏览器跨团队网络工单').version).toBe(10);
    await shot(page, 'a3-today-real-business');
    save('current-business-metrics', { analytics, search });
    return { created_today: 4, first_resolved_today: 1, current_backlog: 8, new_business_visible: 4 };
  }
  if (name === 'permissions') {
    const title = 'DEMO 浏览器跨团队网络工单';
    const evidence = {};
    for (const role of ['it-agent', 'device-agent']) {
      const page = pages[role];
      await page.goto('/search?q=' + encodeURIComponent(title));
      if (role === 'it-agent') await expect(page.locator('#search-data')).toContainText('没有符合条件的工单');
      else await expect(page.locator('#search-data tbody tr')).toHaveCount(1);
      const data = await result(page, '/search/tickets?q=' + encodeURIComponent(title));
      target(data, 'active_original', 3);
      expect(data.body.items).toHaveLength(role === 'it-agent' ? 0 : 1);
      if (role === 'device-agent') expect(data.body.items[0].version).toBe(10);
      evidence[role] = data;
      await shot(page, 'a3-search-permission-' + role);
    }
    await pages.alice.goto('/analytics');
    await expect(pages.alice.locator('.empty')).toContainText('仅对当前有效主管或管理员开放');
    const refused = await result(pages.alice, '/analytics?start=2026-11-01&end=2026-11-04');
    expect(refused.status).toBe(403);
    evidence.requester_analytics = refused;
    save('a3-current-permissions', evidence);
    return { old_team_search_items: 0, current_team_search_items: 1, version: 10, requester_analytics_status: 403 };
  }
  if (name === 'overlap') {
    const page = pages.supervisor;
    await page.goto('/search?q=DEMO');
    await expect(page.locator('#search-data tbody tr')).toHaveCount(4);
    const samples = [];
    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
      const value = await result(page, '/search/tickets?q=DEMO');
      target(value, 'active_original', 3);
      samples.push({ at: new Date().toISOString(), status: value.status, target: value.target, items: value.body.items.length });
      await new Promise(resolve => setTimeout(resolve, 250));
    }
    await shot(page, 'a3-search-during-actual-rebuild');
    save('overlap-samples', samples);
    return { concurrent_search_samples: samples.length, first: samples[0].at, last: samples.at(-1).at };
  }
  throw Error('Unknown browser stage');
}
process.stdout.write(JSON.stringify({ state: 'BROWSER_READY', browser: 'Chrome', headed: true, api: env.DEMO_API }) + '\n');
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
  const name = line.trim();
  if (!name) continue;
  if (name === 'close') { save('requests', requests);save('events', events);await browser.close();break; }
  const at = new Date().toISOString();
  try {
    const value = await stage(name);
    const event = { at, completed_at: new Date().toISOString(), stage: name, state: 'PASS', value };
    events.push(event);save(name + '-result', event);save('requests', requests);
    process.stdout.write(JSON.stringify({ state: 'PASS', stage: name, value }) + '\n');
  } catch (error) {
    const event = { at, stage: name, state: 'FAIL', error: String(error), stack: error.stack };
    events.push(event);save(name + '-failure', event);save('requests', requests);
    for (const [role, page] of Object.entries(pages)) await shot(page, name + '-failure-' + role).catch(() => {});
    process.stdout.write(JSON.stringify({ state: 'FAIL', stage: name, error: String(error) }) + '\n');
  }
}
