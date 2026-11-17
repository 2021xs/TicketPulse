import { test, expect } from '@playwright/test';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createCipheriv, createDecipheriv, randomBytes, randomUUID, createHash } from 'node:crypto';
import { createServer } from 'node:http';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const marker = JSON.parse(readFileSync(resolve(root, process.env.DEMO02_MARKER || '.local/demo-02/current.json')));
const env = JSON.parse(readFileSync(resolve(root, marker.runtime, 'environment.json')));
if (env.DEMO02_API !== 'http://127.0.0.1:18102' || !/^ticketpulse_test_demo02_[a-f0-9]{16}$/.test(env.DEMO02_SCHEMA)) throw Error('Private fixture required');
const gold = JSON.parse(readFileSync(resolve(root, 'tests/analytics/prd_gold.json')));
const ids = [];
const query = new URLSearchParams([
  ['q', 'Demo02'], ['status', 'pending'], ['status', 'in_progress'], ['priority', 'high'],
  ['category', 'network'], ['current_team_id', '10'], ['assignee_id', '0'],
  ['sort', 'id'], ['order', 'asc'], ['page_size', '10'], ['created_from', '1970-01-01T00:00:00Z']
]);
const analyticsURL = '/analytics?start=2026-11-01&end=2026-11-04';
const hash = value => createHash('sha256').update(value).digest('hex');

function save(info, name, value) {
  writeFileSync(info.outputPath(name + '.json'), JSON.stringify(value, null, 2) + '\n', { flag: 'wx' });
}
async function screenshot(page, info, name) {
  await page.screenshot({ path: info.outputPath(name + '.png'), fullPage: true, mask: [page.locator('#password')] });
}
async function signIn(page, user = 'supervisor') {
  await page.goto('/login');
  await page.getByLabel('账号', { exact: true }).fill(user);
  await page.getByLabel('密码', { exact: true }).fill(env.DEMO02_PASSWORD);
  await page.getByRole('button', { name: '登录', exact: true }).click();
  await expect(page).toHaveURL(/\/workspace/);
  await expect(page.locator('#workspace-data table, #workspace-data .empty').first()).toBeVisible();
}
async function get(page, path) {
  return page.evaluate(async path => {
    const r = await fetch('/api/v1' + path);
    return { status: r.status, body: await r.json() };
  }, path);
}
async function pageIDs(page) {
  return page.locator('#search-data .ticket-link').evaluateAll(links => links.map(link => Number(link.getAttribute('href').split('/').at(-1))));
}
async function dashboard(page) {
  await page.goto(analyticsURL);
  await expect(page.locator('#metric-created')).toHaveText('10');
  await expect(page.locator('#metric-resolved')).toHaveText('6');
}
async function noWritePromise(page) {
  await expect(page.locator('#analytics-data')).not.toContainText('提交结果待确认');
  await expect(page.locator('#analytics-data')).not.toContainText('工单操作不受影响');
  await expect(page.locator('#analytics-data')).not.toContainText('工单创建和处理可以继续');
  await expect(page.locator('#analytics-freshness')).not.toContainText('已追平');
  await expect(page.locator('#analytics-data')).not.toHaveClass('loading');
}
async function simulatedError(page, code, message, status = 503) {
  await page.route('**/api/v1/analytics?*', route => route.fulfill({ status, contentType: 'application/json',
    headers: { 'x-demo02-evidence': 'simulated' }, body: JSON.stringify({ error: { code, message } }) }));
}
function cursorWith(cursor, fields) {
  // Test input only: authenticate a deliberately old/expired cursor with the
  // fixture's private key. The real unchanged Go API decides 409 versus 400.
  const bytes = Buffer.from(cursor, 'base64url'), nonce = bytes.subarray(0, 12);
  const decoder = createDecipheriv('aes-256-gcm', Buffer.from(env.TICKETPULSE_CURSOR_KEY, 'hex'), nonce);
  decoder.setAAD(Buffer.from('ticketpulse-search-v1'));decoder.setAuthTag(bytes.subarray(-16));
  const data = JSON.parse(Buffer.concat([decoder.update(bytes.subarray(12, -16)), decoder.final()]));
  const payload = { ...data, ...fields(data) }, fresh = randomBytes(12);
  const encoder = createCipheriv('aes-256-gcm', Buffer.from(env.TICKETPULSE_CURSOR_KEY, 'hex'), fresh);
  encoder.setAAD(Buffer.from('ticketpulse-search-v1'));
  const ciphertext = Buffer.concat([encoder.update(JSON.stringify(payload)), encoder.final()]);
  return Buffer.concat([fresh, ciphertext, encoder.getAuthTag()]).toString('base64url');
}

test.describe.configure({ mode: 'serial' });
let requests, pending, pageErrors;
test.beforeAll(async ({ browser }) => {
  const sampleFile = resolve(root, marker.runtime, 'browser-samples.json');
  if (existsSync(sampleFile)) {
    const samples = JSON.parse(readFileSync(sampleFile));
    if (samples.schema !== env.DEMO02_SCHEMA || samples.ids.length !== 12) throw Error('Private sample identity differs');
    ids.push(...samples.ids);
    for (const [key, values] of Object.entries(samples.query)) { query.delete(key);for (const value of values) query.append(key, value); }
    return;
  }
  const batch = 'Demo02 ' + randomUUID().slice(0, 8);
  query.set('q', batch);
  const context = await browser.newContext({ baseURL: env.DEMO02_API });
  const page = await context.newPage();
  await signIn(page, 'alice');
  for (let i = 0; i < 12; i++) {
    await page.goto('/tickets/new');
    await page.getByLabel('标题', { exact: false }).fill(batch + ' search retry ' + String(i).padStart(2, '0'));
    await page.getByLabel('问题描述', { exact: false }).fill('独立浏览器定向回归，保留关键词、团队筛选和排序。');
    await page.getByLabel('优先级', { exact: false }).selectOption('high');
    await page.getByRole('button', { name: '提交工单', exact: true }).click();
    await expect(page.locator('#ticket-version')).toHaveText('v1');
    ids.push(Number(new URL(page.url()).pathname.split('/').at(-1)));
  }
  await expect(async () => {
    const data = await get(page, '/search/tickets?q=' + encodeURIComponent(batch) + '&page_size=100');
    expect(data.status).toBe(200);expect(data.body.items).toHaveLength(12);
  }).toPass({ timeout: 25000 });
  writeFileSync(sampleFile, JSON.stringify({ schema: env.DEMO02_SCHEMA, ids,
    query: Object.fromEntries([...new Set(query.keys())].map(key => [key, query.getAll(key)])) }), { flag: 'wx', mode: 0o600 });
  writeFileSync(resolve(root, marker.evidence, 'browser-samples-' + Date.now() + '.json'), JSON.stringify({
    evidence: 'RUNTIME_REAL', schema: env.DEMO02_SCHEMA, source: '12 actual Chrome form creates', ids,
    es_visible: true, no_credentials_recorded: true
  }, null, 2) + '\n', { flag: 'wx' });
  await context.close();
});
test.beforeEach(async ({ page }, info) => {
  requests = [];pending = [];pageErrors = [];
  page.on('pageerror', e => pageErrors.push(e.message));
  page.on('response', response => {
    const url = new URL(response.url());
    if (!url.pathname.startsWith('/api/v1/')) return;
    const params = Object.fromEntries([...new Set(url.searchParams.keys())].map(key => [key, key === 'cursor'
      ? { present: true, sha256: hash(url.searchParams.get(key)) } : url.searchParams.getAll(key)]));
    const item = { at: new Date().toISOString(), method: response.request().method(), path: url.pathname, params,
      status: response.status(), evidence: response.headers()['x-demo02-evidence'] === 'simulated' ? 'SIMULATED_RESPONSE' : 'RUNTIME_REAL' };
    requests.push(item);
    if (['/api/v1/auth/login', '/api/v1/auth/logout', '/api/v1/me'].includes(url.pathname)) return;
    pending.push(response.json().then(body => {
      if (body.error) item.error = body.error;
      if (body.items) { item.ids = body.items.map(t => t.id);item.versions = body.items.map(t => t.version);item.has_more = body.has_more; }
      if (body.overview) item.overview = body.overview;
      if (body.ticket) item.ticket = { id: body.ticket.id, version: body.ticket.version, status: body.ticket.status };
    }).catch(() => { item.non_json = true; }));
  });
});
test.afterEach(async ({ page }, info) => {
  await Promise.allSettled(pending);
  save(info, 'requests', { api: env.DEMO02_API, schema: env.DEMO02_SCHEMA, requests, pageErrors });
  if (info.status !== info.expectedStatus) await screenshot(page, info, 'failure').catch(() => {});
  expect(pageErrors).toEqual([]);
});

test('Login: no static password; real credentials, cookie and fixed session remain correct', async ({ page, context }, info) => {
  await page.goto('/login');
  await expect(page.locator('.login-help')).toContainText('演示负责人提供');
  await expect(page.locator('.login-help')).not.toContainText('演示密码');
  await expect(page.locator('.login-help code')).toHaveCount(0);
  expect(await page.locator('body').innerText()).not.toContain(env.DEMO02_PASSWORD);
  await screenshot(page, info, 'login-no-password');
  await page.getByLabel('账号', { exact: true }).fill('alice');
  await page.getByLabel('密码', { exact: true }).fill(randomUUID());
  const wrong = page.waitForResponse(r => r.url().endsWith('/api/v1/auth/login'));
  await page.getByRole('button', { name: '登录', exact: true }).click();
  const rejected = await wrong;
  expect(rejected.status()).toBe(401);expect((await rejected.json()).error.code).toBe('INVALID_CREDENTIALS');
  await expect(page.locator('#login-error')).toBeVisible();
  await page.getByLabel('密码', { exact: true }).fill(env.DEMO02_PASSWORD);
  await page.getByRole('button', { name: '登录', exact: true }).click();
  await expect(page).toHaveURL(/\/workspace/);
  const cookie = (await context.cookies()).find(c => c.name === 'ticketpulse_session');
  expect(cookie.httpOnly).toBe(true);expect(cookie.sameSite).toBe('Strict');expect(cookie.path).toBe('/');
  expect(await page.evaluate(() => document.cookie)).not.toContain('ticketpulse_session');
  const before = (await get(page, '/me')).body.expires_at;
  await page.reload();await expect(page.getByRole('heading', { name: '个人工作台' })).toBeVisible();
  const after = (await get(page, '/me')).body.expires_at;
  expect(after).toBe(before);expect((await context.cookies()).find(c => c.name === cookie.name).value === cookie.value).toBe(true);
  await page.getByRole('button', { name: '退出登录', exact: true }).click();
  await expect(page.getByRole('heading', { name: '登录工作台' })).toBeVisible();
  expect((await get(page, '/me')).status).toBe(401);
  save(info, 'session', { evidence: 'RUNTIME_REAL', wrong_login: 401, correct_login: 200, httponly: true,
    same_site: 'Strict', password_hint_absent: true, reload_preserves_cookie: true, expires_at_fixed: true, logout_revokes: true });
});

test('Search: normal pages and real 409 restart retain every filter and sorting', async ({ page }, info) => {
  await signIn(page, 'alice');
  await page.goto('/search?' + query);
  await expect(page.locator('#search-data tbody tr')).toHaveCount(10);
  expect(await pageIDs(page)).toEqual(ids.slice(0, 10));
  await page.getByRole('button', { name: '下一页', exact: true }).click();
  await expect(page.locator('#search-data tbody tr')).toHaveCount(2);
  expect(await pageIDs(page)).toEqual(ids.slice(10));
  await expect(page.locator('.page-index')).toHaveText('2');
  const original = new URL(page.url()).searchParams.get('cursor');
  const stale = cursorWith(original, c => ({ t: c.t + ':previous-test-target' }));
  const rejected = page.waitForResponse(r => r.url().includes('/api/v1/search/tickets') && r.status() === 409);
  await page.goto('/search?' + query + '&cursor=' + encodeURIComponent(stale) + '&page=2');
  const response = await rejected;
  expect((await response.json()).error.code).toBe('SEARCH_CURSOR_TARGET_CHANGED');
  await expect(page.getByRole('button', { name: '重新搜索', exact: true })).toBeVisible();
  const prior = requests.filter(r => r.path === '/api/v1/search/tickets').length;
  await page.waitForTimeout(300);
  expect(requests.filter(r => r.path === '/api/v1/search/tickets')).toHaveLength(prior);
  await screenshot(page, info, 'real-409');
  const first = page.waitForResponse(r => r.url().includes('/api/v1/search/tickets') && !new URL(r.url()).searchParams.has('cursor'));
  await page.getByRole('button', { name: '重新搜索', exact: true }).click();
  expect((await first).status()).toBe(200);
  await expect(page.locator('.page-index')).toHaveText('1');
  await expect(page.locator('#search-data tbody tr')).toHaveCount(10);
  const now = new URL(page.url()).searchParams;
  expect(now.has('cursor')).toBe(false);expect(now.has('page')).toBe(false);expect(now.has('search_after')).toBe(false);
  for (const key of new Set(query.keys())) expect(now.getAll(key)).toEqual(query.getAll(key));
  expect(await pageIDs(page)).toEqual(ids.slice(0, 10));
  await expect(page.locator('#search-prev')).toBeDisabled();
  await screenshot(page, info, 'fresh-first-page');
  await page.locator('#search-next').click();await expect(page.locator('#search-data tbody tr')).toHaveCount(2);
  expect(await pageIDs(page)).toEqual(ids.slice(10));
  save(info, 'restart', { evidence: 'RUNTIME_REAL', injection: 'Authenticated test cursor with a different target; no actual M4 switch',
    backend_error: 409, cursor_removed: true, page_reset: 1, query: Object.fromEntries([...new Set(query.keys())].map(k => [k, query.getAll(k)])),
    automatic_retry_count: 0, new_pages: [10, 2] });
});

test('Search: simulated target error also removes search_after without losing the query', async ({ page }, info) => {
  await signIn(page, 'alice');
  let simulated = 0;
  await page.route('**/api/v1/search/tickets?*', async route => {
    const params = new URL(route.request().url()).searchParams;
    if (!params.has('search_after')) return route.continue();
    simulated++;
    return route.fulfill({ status: 409, contentType: 'application/json', headers: { 'x-demo02-evidence': 'simulated' },
      body: JSON.stringify({ error: { code: 'SEARCH_CURSOR_TARGET_CHANGED', message: '搜索目标版本已变化，请重新发起搜索' } }) });
  });
  await page.goto('/search?' + query + '&page=4&cursor=simulated-old&search_after=simulated-position');
  await page.getByRole('button', { name: '重新搜索', exact: true }).click();
  await expect(page.locator('#search-data tbody tr')).toHaveCount(10);
  const now = new URL(page.url()).searchParams;
  for (const key of ['cursor', 'search_after', 'page']) expect(now.has(key)).toBe(false);
  for (const key of new Set(query.keys())) expect(now.getAll(key)).toEqual(query.getAll(key));
  expect(simulated).toBe(1);
  save(info, 'simulation', { evidence: 'SIMULATED_RESPONSE', error: 409, simulated_requests: 1,
    note: 'search_after is internal ES state, not a public Go API argument; stale URL case is a defensive simulated test', real_first_page: 200 });
});

test('Search: an authenticated truly expired cursor still gets real 400 and ordinary retry', async ({ page }, info) => {
  await signIn(page, 'alice');
  const current = await get(page, '/search/tickets?' + query);
  expect(current.status).toBe(200);
  const expired = cursorWith(current.body.next_cursor, () => ({ e: Math.floor(Date.now() / 1000) - 60 }));
  const request = page.waitForResponse(r => r.url().includes('/api/v1/search/tickets') && r.status() === 400);
  await page.goto('/search?' + query + '&cursor=' + encodeURIComponent(expired) + '&page=2');
  const response = await request;
  expect((await response.json()).error.code).toBe('INVALID_ARGUMENT');
  await expect(page.locator('#search-data')).toContainText('过期');
  await expect(page.getByRole('button', { name: '重试搜索', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: '重新搜索', exact: true })).toHaveCount(0);
  const again = page.waitForResponse(r => r.url().includes('/api/v1/search/tickets') && r.status() === 400);
  await page.locator('#search-retry').click();expect((await again).status()).toBe(400);
  await screenshot(page, info, 'real-expired-400');
  save(info, 'expiry', { evidence: 'RUNTIME_REAL', injection: 'Authenticated test cursor with past expiration; no backend change', status: 400,
    code: 'INVALID_ARGUMENT', not_misclassified_as_target_change: true, ordinary_retry_unchanged: true });
});

test('Analytics: real Gold, filters and ordinary user authorization remain correct', async ({ page, browser }, info) => {
  await signIn(page);await dashboard(page);
  const data = await get(page, '/analytics?start=2026-11-01&end=2026-11-04');
  expect(data.status).toBe(200);expect(data.body.daily).toEqual(gold.daily);
  expect(data.body.overview.average_first_resolution_seconds).toBe(60600);
  await expect(page.locator('#metric-duration')).toHaveText('16 小时 50 分');
  await page.getByLabel('团队（可多选）', { exact: true }).selectOption(['10']);
  await page.getByRole('button', { name: '应用筛选', exact: true }).click();
  await expect(page.locator('#metric-created')).toHaveText('7');await expect(page.locator('#metric-resolved')).toHaveText('4');
  await screenshot(page, info, 'normal-statistics');
  const context = await browser.newContext({ baseURL: env.DEMO02_API }), requester = await context.newPage();
  await signIn(requester, 'alice');await requester.goto(analyticsURL);
  await expect(requester.locator('.empty')).toContainText('仅对当前有效主管或管理员开放');
  expect((await get(requester, '/analytics?start=2026-11-01&end=2026-11-04')).status).toBe(403);
  await context.close();save(info, 'gold', { evidence: 'RUNTIME_REAL', created: 10, first_resolved: 6, seconds: 60600, team10: [7, 4], requester_status: 403 });
});

test('Analytics: explicit maintenance code gives accurate copy (simulated response)', async ({ page }, info) => {
  await signIn(page);
  await simulatedError(page, 'BUSINESS_WRITES_FROZEN', '业务写入已暂停维护');
  await page.goto(analyticsURL);
  await expect(page.locator('#analytics-data')).toContainText('系统正在维护，统计数据暂时不可用，请稍后重试。');
  await noWritePromise(page);await screenshot(page, info, 'simulated-maintenance');
  save(info, 'maintenance', { evidence: 'SIMULATED_RESPONSE', status: 503, code: 'BUSINESS_WRITES_FROZEN',
    note: 'No actual source frozen; real managed Analytics maps maintenance to opaque ANALYTICS_UNAVAILABLE' });
});

test('Analytics: opaque service unavailable never promises business writes (simulated response)', async ({ page }, info) => {
  await signIn(page);
  await simulatedError(page, 'ANALYTICS_UNAVAILABLE', '统计服务暂不可用，请稍后重试；工单操作不受影响');
  await page.goto(analyticsURL);
  await expect(page.locator('#analytics-data')).toContainText('统计数据暂时不可用，请稍后重试。');
  await noWritePromise(page);await screenshot(page, info, 'simulated-unavailable');
  await page.unroute('**/api/v1/analytics?*');
  await page.locator('#analytics-retry').click();await expect(page.locator('#metric-created')).toHaveText('10');
  save(info, 'opaque-error', { evidence: 'SIMULATED_RESPONSE', code: 'ANALYTICS_UNAVAILABLE', false_write_promise_removed: true, retry_to_real_service: 200 });
});

test('Analytics: real invalid date HTTP 400 preserves useful error and query', async ({ page }, info) => {
  await signIn(page);
  const response = page.waitForResponse(r => r.url().includes('/api/v1/analytics') && r.status() === 400);
  await page.goto('/analytics?start=2026-11-04&end=2026-11-04');
  expect((await response).status()).toBe(400);
  await expect(page.locator('#analytics-data')).toContainText('日期窗口');
  await noWritePromise(page);
  await page.getByLabel('开始日期（北京时间）', { exact: true }).fill('2026-11-01');
  await page.getByRole('button', { name: '应用筛选', exact: true }).click();
  await expect(page.locator('#metric-created')).toHaveText('10');
  save(info, 'real-query-failure', { evidence: 'RUNTIME_REAL', invalid_window_status: 400, corrected_query_status: 200 });
});

test('Analytics: HTTP 500 and REQUEST_TIMEOUT GET copy is neutral (simulated responses)', async ({ page }, info) => {
  await signIn(page);
  for (const [code, status] of [['INTERNAL_ERROR', 500], ['REQUEST_TIMEOUT', 503]]) {
    await simulatedError(page, code, '创建结果待确认，提交结果待确认。', status);
    await page.goto(analyticsURL);
    await expect(page.locator('#analytics-data')).toContainText(code === 'INTERNAL_ERROR' ? '服务暂时不可用' : '查询超时');
    await noWritePromise(page);await page.unroute('**/api/v1/analytics?*');
  }
  save(info, 'http-faults', { evidence: 'SIMULATED_RESPONSE', statuses: [500, 503], codes: ['INTERNAL_ERROR', 'REQUEST_TIMEOUT'], GET_copy_only: true });
});

test('Analytics: non-JSON HTTP failure stays a read failure (simulated response)', async ({ page }, info) => {
  await signIn(page);
  await page.route('**/api/v1/analytics?*', route => route.fulfill({ status: 502, contentType: 'text/html',
    headers: { 'x-demo02-evidence': 'simulated' }, body: '<html>Gateway unavailable</html>' }));
  await page.goto(analyticsURL);
  await expect(page.locator('#analytics-data')).toContainText('服务返回异常，请重新加载。');
  await noWritePromise(page);save(info, 'gateway', { evidence: 'SIMULATED_RESPONSE', status: 502, invalid_response: true });
});

test('Analytics: browser network offline uses neutral copy and real recovery', async ({ page, context }, info) => {
  await signIn(page);await dashboard(page);
  await context.setOffline(true);
  await page.locator('#analytics-refresh').click();
  await expect(page.locator('#analytics-data')).toContainText('暂时无法连接服务，请稍后重试。');
  await noWritePromise(page);await screenshot(page, info, 'browser-offline');
  await context.setOffline(false);
  await page.locator('#analytics-retry').click();await expect(page.locator('#metric-created')).toHaveText('10');
  save(info, 'network', { evidence: 'SIMULATED_BROWSER_NETWORK', fault: 'Playwright context.setOffline; services never stopped',
    neutral_copy: true, real_service_recovery: 200 });
});

test('Analytics: browser request timeout stays a GET query timeout (simulated latency)', async ({ page }, info) => {
  await signIn(page);
  // Let native fetch establish a real HTTP connection before withholding the
  // reply. A paused Playwright route would turn its later abort into TypeError.
  const delay = createServer((request, response) => {
    const timer = setTimeout(() => { response.writeHead(503);response.end(); }, 20000);
    response.on('close', () => clearTimeout(timer));
  });
  await new Promise(resolve => delay.listen(0, '127.0.0.1', resolve));
  const port = delay.address().port;
  try {
    await page.route('**/api/v1/analytics?*', route => {
      const path = new URL(route.request().url());
      return route.continue({ url: 'http://127.0.0.1:' + port + path.pathname + path.search });
    });
    await page.goto(analyticsURL);
    await expect(page.locator('#analytics-data')).toContainText('查询超时，请稍后重试。', { timeout: 15000 });
    await noWritePromise(page);
    save(info, 'timeout', { evidence: 'SIMULATED_HTTP_LATENCY', private_delay_port: port, held_response_ms: 20000,
      app_timeout_ms: 12000, GET_copy_only: true, original_services_unchanged: true });
  } finally {
    await page.unroute('**/api/v1/analytics?*');
    delay.closeAllConnections();
    await new Promise(resolve => delay.close(resolve));
  }
});

test('Business: POST unknown result, real comment and lifecycle still work', async ({ page, browser }, info) => {
  await signIn(page, 'alice');
  await page.goto('/tickets/new');
  await page.getByLabel('标题', { exact: false }).fill('Demo02 lifecycle ' + randomUUID().slice(0, 8));
  await page.getByLabel('问题描述', { exact: false }).fill('独立生命周期回归，不修改搜索分页样本。');
  await page.getByRole('button', { name: '提交工单', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v1');
  const id = Number(new URL(page.url()).pathname.split('/').at(-1));
  await page.route('**/api/v1/tickets/comments', route => route.abort('failed'));
  await page.getByLabel('补充评论', { exact: true }).fill('网络恢复后确认这条评论。');
  await page.getByRole('button', { name: '发送评论', exact: true }).click();
  await expect(page.locator('#detail-error')).toContainText('提交结果待确认');
  expect((await get(page, '/tickets/' + id)).body.ticket.version).toBe(1);
  await page.unroute('**/api/v1/tickets/comments');
  await page.getByRole('button', { name: '发送评论', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v2');
  const context = await browser.newContext({ baseURL: env.DEMO02_API }), agent = await context.newPage();
  await signIn(agent, 'it-agent');await agent.goto('/tickets/' + id);await expect(agent.locator('#ticket-version')).toHaveText('v2');
  for (const [name, version] of [['认领工单', 'v3'], ['开始处理', 'v4']]) {
    await agent.getByRole('button', { name, exact: true }).click();await expect(agent.locator('#ticket-version')).toHaveText(version);
  }
  await agent.getByRole('button', { name: '提交解决', exact: true }).click();
  await agent.getByLabel('解决说明', { exact: true }).fill('私有浏览器回归：连接和处理已完成。');
  await agent.getByRole('button', { name: '确认提交解决', exact: true }).click();await expect(agent.locator('#ticket-version')).toHaveText('v5');
  await page.reload();await expect(page.locator('#ticket-version')).toHaveText('v5');
  await page.getByRole('button', { name: '确认关闭', exact: true }).click();await expect(page.locator('#ticket-version')).toHaveText('v6');
  const detail = (await get(page, '/tickets/' + id)).body;
  expect(detail.history.map(h => h.version)).toEqual([1, 2, 3, 4, 5, 6]);expect(detail.comments).toHaveLength(1);expect(detail.ticket.status).toBe('closed');
  await screenshot(page, info, 'real-lifecycle');await context.close();
  save(info, 'lifecycle', { evidence: 'RUNTIME_REAL', ticket: id, version: 6, histories: 6, comments: 1,
    status: 'closed', POST_network_fault: 'SIMULATED_BROWSER_NETWORK', unknown_write_copy_retained: true });
});
