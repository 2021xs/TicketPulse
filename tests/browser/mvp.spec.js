import { test, expect } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { writeFileSync, mkdirSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

const output = resolve('artifacts/m1-04');
mkdirSync(output, { recursive: true });
function fixture(action, value) {
  const args = ['scripts/browser_fixture.py', action];
  if (value !== undefined) args.push(String(value));
  const text = execFileSync('python3', args, { encoding: 'utf8' });
  return action === 'inspect' ? JSON.parse(text) : text;
}
function save(label, data) { writeFileSync(resolve(output, label + '.json'), JSON.stringify(data, null, 2) + '\n'); }
async function screenshot(page, label) { await page.evaluate(() => window.scrollTo(0, 0)); await page.screenshot({ path: resolve(output, label + '.png'), fullPage: true }); }
async function login(page, username) {
  await page.goto('/login');
  await page.getByLabel('账号', { exact: true }).fill(username);
  await page.getByLabel('密码', { exact: true }).fill('TicketPulse!2026');
  await page.getByRole('button', { name: '登录', exact: true }).click();
  await expect(page).toHaveURL(/\/workspace/);
  await expect(page.locator('#workspace-data table, #workspace-data .empty').first()).toBeVisible();
}
async function logout(page) {
  await page.getByRole('button', { name: '退出登录' }).click();
  await expect(page.getByRole('heading', { name: '登录工作台' })).toBeVisible();
}
async function create(page, title, description = '网络故障影响业务，请支持团队协助排查。') {
  await page.getByRole('button', { name: '＋ 创建工单', exact: true }).click();
  await page.getByLabel('标题', { exact: false }).fill(title);
  await page.getByLabel('问题描述', { exact: false }).fill(description);
  await page.getByLabel('类别', { exact: false }).selectOption('network');
  await page.getByLabel('优先级', { exact: false }).selectOption('high');
  await page.getByRole('button', { name: '提交工单', exact: true }).click();
  await expect(page).toHaveURL(/\/tickets\/\d+$/);
  await expect(page.locator('#ticket-version')).toHaveText('v1');
  await expect(page.getByRole('heading', { name: title, exact: true })).toBeVisible();
  return Number(new URL(page.url()).pathname.split('/').at(-1));
}
async function open(page, id) {
  await page.goto('/tickets/' + id);
  await expect(page.locator('#ticket-version')).toBeVisible();
}
async function action(page, name, version) {
  await page.getByRole('button', { name, exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v' + version);
}
async function resolveTicket(page, note, version) {
  await page.getByRole('button', { name: '提交解决', exact: true }).click();
  await page.getByLabel('解决说明', { exact: true }).fill(note);
  await page.getByRole('button', { name: '确认提交解决', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v' + version);
}
async function comment(page, content, version) {
  await page.getByLabel('补充评论', { exact: true }).fill(content);
  await page.getByRole('button', { name: '发送评论', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v' + version);
  await expect(page.locator('#comment-list')).toContainText(content);
}
function verify(data, version) {
  expect(data.ticket.version).toBe(version);
  expect(data.history).toHaveLength(version);
  expect(data.history.map(h => h.version)).toEqual(Array.from({ length: version }, (_, i) => i + 1));
  for (let i = 1; i < version; i++) expect(data.history[i].before).toEqual(data.history[i - 1].after);
  expect(data.history.at(-1).after.status).toBe(data.ticket.status);
  for (const c of data.comments) {
    expect(data.history[c.version - 1].action).toBe('commented');
    expect(data.history[c.version - 1].comment_id).toBe(c.id);
  }
}

test('A: browser login, create, claim, process, comment, resolve and requester close', async ({ page, context }) => {
  await page.goto('/login');
  await screenshot(page, 'login');
  await login(page, 'alice');
  const cookies = await context.cookies();
  const cookie = cookies.find(c => c.name === 'ticketpulse_session');
  expect(cookie.httpOnly).toBe(true);
  expect(cookie.sameSite).toBe('Strict');
  expect(await page.evaluate(() => document.cookie)).not.toContain('ticketpulse_session');
  await page.reload();
  await expect(page.getByRole('heading', { name: '个人工作台' })).toBeVisible();
  const title = '浏览器普通工单 ' + randomUUID().slice(0, 8);
  const id = await create(page, title);
  await expect(page.locator('.detail-right')).toContainText('IT 服务台');
  await screenshot(page, 'created-ticket');
  await logout(page);
  await login(page, 'it-agent');
  await page.getByRole('button', { name: '本团队待处理', exact: true }).click();
  await page.getByRole('link', { name: title, exact: true }).click();
  await action(page, '认领工单', 2);
  await action(page, '开始处理', 3);
  await comment(page, '工程师已检查主线路，正在恢复连接。', 4);
  await resolveTicket(page, '主线路修复完成，连通性检查全部通过。', 5);
  await logout(page);
  await login(page, 'alice');
  await open(page, id);
  await action(page, '确认关闭', 6);
  await expect(page.locator('.detail-meta .badge.closed')).toHaveText('已关闭');
  await screenshot(page, 'ordinary-closed');
  const data = fixture('inspect', id);
  verify(data, 6);
  expect(data.ticket.status).toBe('closed');
  expect(data.comments).toHaveLength(1);
  expect(data.ticket.first_resolved_at).not.toBeNull();
  expect(data.ticket.closed_at).not.toBeNull();
  expect(data.history.map(h => h.action)).toEqual(['created', 'claimed', 'processing_started', 'commented', 'resolved', 'closed']);
  save('browser-scenario-a', { browser: 'Chrome', passed: true, ...data });
});

test('B: browser cross-team handoff, old team loses access, management exceptions', async ({ page }) => {
  await login(page, 'alice');
  const title = '浏览器跨团队工单 ' + randomUUID().slice(0, 8);
  const id = await create(page, title);
  await logout(page);
  await login(page, 'it-agent');
  await open(page, id);
  await action(page, '认领工单', 2);
  await action(page, '开始处理', 3);
  await logout(page);
  await login(page, 'supervisor');
  await open(page, id);
  await expect(page.getByRole('button', { name: '提交解决', exact: true })).toHaveCount(0);
  await page.getByRole('button', { name: '跨团队转派', exact: true }).click();
  await page.getByLabel('目标团队', { exact: true }).selectOption({ label: '设备服务台' });
  await page.getByLabel('操作原因', { exact: true }).fill('网络设备故障，交由设备团队更换交换机。');
  await page.getByRole('button', { name: '确认跨团队转派', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v4');
  await expect(page.locator('.detail-right')).toContainText('设备服务台');
  await logout(page);
  await login(page, 'it-agent');
  await page.goto('/tickets/' + id);
  await expect(page.locator('.empty')).toContainText('没有查看');
  await expect(page.getByRole('heading', { name: title })).toHaveCount(0);
  const denied = await page.evaluate(async id => {
    const me = await (await fetch('/api/v1/me')).json();
    const r = await fetch('/api/v1/tickets/comments', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': me.csrf_token }, body: JSON.stringify({ ticket_id: id, expected_version: 4, content: '旧团队不能评论' }) });
    return { status: r.status, code: (await r.json()).error.code };
  }, id);
  expect(denied).toEqual({ status: 403, code: 'ACTION_FORBIDDEN' });
  await logout(page);
  await login(page, 'device-agent');
  await page.getByRole('button', { name: '本团队待处理', exact: true }).click();
  await page.getByRole('link', { name: title, exact: true }).click();
  await action(page, '认领工单', 5);
  await action(page, '开始处理', 6);
  await comment(page, '设备团队已更换交换机，并检查固件。', 7);
  await resolveTicket(page, '设备更换完成，等待验证。', 8);
  const first = fixture('inspect', id).ticket.first_resolved_at;
  await logout(page);
  await login(page, 'supervisor');
  await open(page, id);
  await page.getByRole('button', { name: '管理重开', exact: true }).click();
  await page.getByLabel('恢复团队', { exact: true }).selectOption('0');
  await page.getByLabel('操作原因', { exact: true }).fill('员工反馈仍有异常，保留设备团队继续处理。');
  await page.getByRole('button', { name: '确认管理重开', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v9');
  await logout(page);
  await login(page, 'device-agent');
  await open(page, id);
  await resolveTicket(page, '第二次验证完成，网络设备工作正常。', 10);
  await logout(page);
  await login(page, 'supervisor');
  await open(page, id);
  await page.getByRole('button', { name: '人工关闭', exact: true }).click();
  await page.getByLabel('操作原因', { exact: true }).fill('已与员工核验修复结果，主管人工确认关闭。');
  await page.getByRole('button', { name: '确认人工关闭', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v11');
  await screenshot(page, 'cross-team-closed');
  const data = fixture('inspect', id);
  verify(data, 11);
  expect(data.ticket.initial_team_id).toBe(10);
  expect(data.ticket.current_team_id).toBe(20);
  expect(data.ticket.assignee_id).toBe(4);
  expect(data.ticket.first_resolved_at).toBe(first);
  expect(data.ticket.reopen_count).toBe(1);
  expect(data.history[3].before.status).toBe('in_progress');
  expect(data.history[3].after.status).toBe('pending');
  expect(data.history.at(-1).reason).toContain('主管人工确认关闭');
  save('browser-scenario-b', { browser: 'Chrome', old_team_denied: denied, passed: true, ...data });
  await page.goto('/workspace?view=created_by_me');
  await expect(page.locator('#workspace-data .empty')).toBeVisible();
  await page.getByRole('button', { name: '本团队待处理', exact: true }).click();
  await expect(page.locator('#workspace-data')).toBeVisible();
  await logout(page);
  await login(page, 'alice');
  await expect(page.locator('#workspace-data table')).toBeVisible();
  await screenshot(page, 'workspace');
});

test('C: browser authentication, retries, input retention, conflicts and recovery', async ({ page, context, browser }) => {
  await page.goto('/login');
  await page.getByLabel('账号', { exact: true }).fill('alice');
  await page.getByLabel('密码', { exact: true }).fill('incorrect');
  await page.getByRole('button', { name: '登录', exact: true }).click();
  await expect(page.locator('#login-error')).toContainText('账号或密码错误');
  await page.getByLabel('账号', { exact: true }).fill('disabled');
  await page.getByLabel('密码', { exact: true }).fill('TicketPulse!2026');
  await page.getByRole('button', { name: '登录', exact: true }).click();
  await expect(page.locator('#login-error')).toContainText('账号或密码错误');
  await login(page, 'alice');
  const savedCookie = (await context.cookies()).find(c => c.name === 'ticketpulse_session');
  await logout(page);
  await page.goto('/workspace');
  await expect(page.getByRole('heading', { name: '登录工作台' })).toBeVisible();
  await context.addCookies([savedCookie]);
  const revoked = await page.evaluate(async () => (await fetch('/api/v1/me')).status);
  expect(revoked).toBe(401);
  await context.clearCookies();
  await login(page, 'alice');
  fixture('expire', 1);
  await page.reload();
  await expect(page.getByRole('heading', { name: '登录工作台' })).toBeVisible();
  await expect(page.locator('.login-card .notice')).toContainText('过期');
  await login(page, 'alice');
  const csrf = await page.evaluate(async () => (await (await fetch('/api/v1/me')).json()).csrf_token);
  const missingCSRF = await page.evaluate(async () => {
    const r = await fetch('/api/v1/tickets', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ request_id: 'CSRF-reject', title: '不应创建的工单', description: '没有 CSRF', category: 'network', priority: 'medium' }) });
    return r.status;
  });
  expect(missingCSRF).toBe(403);
  const requestIDs = [];
  let loseFirst = true;
  await page.route('**/api/v1/tickets', async route => {
    if (route.request().method() !== 'POST') return route.continue();
    requestIDs.push(route.request().postDataJSON().request_id);
    if (loseFirst) { loseFirst = false; await route.fetch(); await route.abort('failed'); }
    else await route.continue();
  });
  const retryTitle = '安全重试工单 ' + randomUUID().slice(0, 8);
  await page.getByRole('button', { name: '＋ 创建工单', exact: true }).click();
  await page.getByLabel('标题', { exact: false }).fill(retryTitle);
  await page.getByLabel('问题描述', { exact: false }).fill('<img src=x onerror="window.__unsafe=1"> 网络问题需要排查。');
  await page.getByRole('button', { name: '提交工单', exact: true }).click();
  await expect(page.locator('#create-error')).toContainText('结果待确认');
  await expect(page.getByLabel('标题', { exact: false })).toHaveAttribute('readonly');
  await page.getByRole('button', { name: '安全重试本次创建', exact: true }).click();
  await expect(page).toHaveURL(/\/tickets\/\d+$/);
  const id = Number(new URL(page.url()).pathname.split('/').at(-1));
  await expect(page.locator('#ticket-version')).toHaveText('v1');
  expect(requestIDs).toHaveLength(2);
  expect(requestIDs[0]).toBe(requestIDs[1]);
  await page.unroute('**/api/v1/tickets');
  await expect(page.locator('#ticket-description img')).toHaveCount(0);
  expect(await page.evaluate(() => window.__unsafe)).toBeUndefined();
  const forged = await page.evaluate(async ({ id, csrf }) => {
    const r = await fetch('/api/v1/tickets/transfer-team', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf, 'X-Role': 'admin', 'X-User-ID': '6' }, body: JSON.stringify({ ticket_id: id, expected_version: 1, team_id: 20, reason: '伪造管理身份' }) });
    return { status: r.status, code: (await r.json()).error.code };
  }, { id, csrf });
  expect(forged).toEqual({ status: 403, code: 'ACTION_FORBIDDEN' });
  const content = '<svg onload="window.__unsafe=2"></svg> 评论失败应保留全部输入。';
  fixture('fail-comments');
  try {
    await page.getByLabel('补充评论', { exact: true }).fill(content);
    await page.getByRole('button', { name: '发送评论', exact: true }).click();
    await expect(page.locator('#detail-error')).toContainText('输入已保留');
    await expect(page.getByLabel('补充评论', { exact: true })).toHaveValue(content);
    expect(fixture('inspect', id).ticket.version).toBe(1);
  } finally { fixture('restore-comments'); }
  await page.getByRole('button', { name: '发送评论', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v2');
  await expect(page.locator('#comment-list svg')).toHaveCount(0);
  expect(await page.evaluate(() => window.__unsafe)).toBeUndefined();
  const otherContext = await browser.newContext({ baseURL: 'http://127.0.0.1:18081' });
  const other = await otherContext.newPage();
  await login(other, 'supervisor');
  await open(other, id);
  await comment(other, '主管并发补充了进展。', 3);
  await page.getByLabel('补充评论', { exact: true }).fill('旧页面的输入不会因版本冲突丢失。');
  await page.getByRole('button', { name: '发送评论', exact: true }).click();
  await expect(page.locator('#detail-error')).toContainText('其他人更新');
  await expect(page.getByLabel('补充评论', { exact: true })).toHaveValue('旧页面的输入不会因版本冲突丢失。');
  await screenshot(page, 'version-conflict');
  await page.getByRole('button', { name: '↻ 刷新最新状态', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v3');
  await expect(page.getByLabel('补充评论', { exact: true })).toHaveValue('旧页面的输入不会因版本冲突丢失。');
  await page.getByRole('button', { name: '发送评论', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v4');
  await otherContext.close();
  const retryData = fixture('inspect', id);
  verify(retryData, 4);
  await logout(page);
  await login(page, 'alice');
  const recoveryID = await create(page, '浏览器异常恢复 ' + randomUUID().slice(0, 8));
  await logout(page);
  await login(page, 'it-agent');
  await open(page, recoveryID);
  await action(page, '认领工单', 2);
  await action(page, '开始处理', 3);
  await resolveTicket(page, '初次处理完成，等待用户验证。', 4);
  const firstResolved = fixture('inspect', recoveryID).ticket.first_resolved_at;
  await logout(page);
  fixture('disable-user', 3);
  try {
    await login(page, 'alice');
    await open(page, recoveryID);
    await action(page, '重新打开', 5);
    await expect(page.locator('.detail-meta .badge.pending')).toBeVisible();
    expect(fixture('inspect', recoveryID).ticket.assignee_id).toBeNull();
  } finally { fixture('enable-user', 3); }
  await logout(page);
  await login(page, 'it-agent-2');
  await open(page, recoveryID);
  await action(page, '认领工单', 6);
  await action(page, '开始处理', 7);
  await resolveTicket(page, '重新入队后继续处理。', 8);
  await logout(page);
  fixture('disable-team', 10);
  try {
    await login(page, 'alice');
    await open(page, recoveryID);
    await page.getByRole('button', { name: '重新打开', exact: true }).click();
    await expect(page.locator('#detail-error')).toContainText('主管');
    expect(fixture('inspect', recoveryID).ticket.version).toBe(8);
    await logout(page);
    await login(page, 'supervisor');
    await open(page, recoveryID);
    await page.getByRole('button', { name: '管理重开', exact: true }).click();
    await page.getByLabel('恢复团队', { exact: true }).selectOption({ label: '设备服务台' });
    await page.getByLabel('操作原因', { exact: true }).fill('原团队停用，重开并交由设备团队恢复。');
    await page.getByRole('button', { name: '确认管理重开', exact: true }).click();
    await expect(page.locator('#ticket-version')).toHaveText('v9');
  } finally { fixture('enable-team', 10); }
  await logout(page);
  await login(page, 'device-agent');
  await open(page, recoveryID);
  await action(page, '认领工单', 10);
  await action(page, '开始处理', 11);
  await resolveTicket(page, '有效团队完成恢复和验证。', 12);
  await logout(page);
  await login(page, 'supervisor');
  await open(page, recoveryID);
  await page.getByRole('button', { name: '人工关闭', exact: true }).click();
  await page.getByLabel('操作原因', { exact: true }).fill('主管核验异常恢复结果，人工关闭。');
  await page.getByRole('button', { name: '确认人工关闭', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v13');
  const recovery = fixture('inspect', recoveryID);
  verify(recovery, 13);
  expect(recovery.ticket.first_resolved_at).toBe(firstResolved);
  expect(recovery.ticket.reopen_count).toBe(2);
  expect(recovery.history[4].action).toBe('reopened_requeued');
  expect(recovery.history[8].action).toBe('management_reopened');
  await screenshot(page, 'recovered-closed');
  await page.setViewportSize({ width: 760, height: 1000 });
  await expect(page.locator('.detail-layout')).toBeVisible();
  const width = await page.evaluate(() => document.documentElement.scrollWidth);
  expect(width).toBeLessThanOrEqual(760);
  await screenshot(page, 'responsive-detail');
  save('browser-scenario-c', { browser: 'Chrome', passed: true, checks: ['invalid login', 'disabled login', 'logout revoked', 'session expired', 'CSRF', 'same request retry', 'forged identity denied', 'XSS escaped', 'comment failure retained', 'version conflict retained', 'owner requeue', 'inactive team recovery', 'responsive layout'], retry: retryData, recovery });
});

test('D: browser same-team transfer, click protection, list paging and loading/error states', async ({ page }) => {
  const pageErrors = [];
  page.on('pageerror', error => pageErrors.push(error.message));
  await login(page, 'alice');
  const title = '浏览器同团队交接 ' + randomUUID().slice(0, 8);
  let posts = 0;
  page.on('request', request => { if (request.method() === 'POST' && new URL(request.url()).pathname === '/api/v1/tickets') posts++; });
  await page.getByRole('button', { name: '＋ 创建工单', exact: true }).click();
  await page.getByLabel('标题', { exact: false }).fill(title);
  await page.getByLabel('问题描述', { exact: false }).fill('同团队交接和双击提交保护测试。');
  await page.getByRole('button', { name: '提交工单', exact: true }).evaluate(button => { button.click(); button.click(); });
  await expect(page.locator('#ticket-version')).toHaveText('v1');
  expect(posts).toBe(1);
  const id = Number(new URL(page.url()).pathname.split('/').at(-1));
  await logout(page);
  await login(page, 'it-agent');
  await open(page, id);
  await action(page, '认领工单', 2);
  await action(page, '开始处理', 3);
  await page.getByRole('button', { name: '同团队转派', exact: true }).click();
  await page.getByLabel('新负责人', { exact: true }).selectOption({ label: 'IT 第二工程师' });
  await page.getByLabel('操作原因', { exact: false }).fill('交接给第二位工程师继续处理。');
  await page.getByRole('button', { name: '确认同团队转派', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v4');
  await expect(page.getByRole('button', { name: '提交解决', exact: true })).toHaveCount(0);
  await expect(page.locator('.detail-meta .badge.in_progress')).toBeVisible();
  await logout(page);
  await login(page, 'it-agent-2');
  await open(page, id);
  await page.getByRole('button', { name: '提交解决', exact: true }).click();
  const note = '解决说明遇到服务失败时保留，检查无误后重新提交。';
  await page.getByLabel('解决说明', { exact: true }).fill(note);
  await page.route('**/api/v1/tickets/resolve', route => route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: { code: 'INTERNAL_ERROR', message: 'test response failure' } }) }));
  await page.getByRole('button', { name: '确认提交解决', exact: true }).click();
  await expect(page.locator('#detail-error')).toContainText('输入已保留');
  await expect(page.getByLabel('解决说明', { exact: true })).toHaveValue(note);
  expect(fixture('inspect', id).ticket.version).toBe(4);
  await page.unroute('**/api/v1/tickets/resolve');
  await page.getByRole('button', { name: '确认提交解决', exact: true }).click();
  await expect(page.locator('#ticket-version')).toHaveText('v5');
  await logout(page);
  await login(page, 'alice');
  await open(page, id);
  await action(page, '确认关闭', 6);
  const data = fixture('inspect', id);verify(data, 6);
  expect(data.history[3].action).toBe('assignee_transferred');
  expect(data.ticket.assignee_id).toBe(11);
  // Populate the real list through the existing authenticated API, without changing ordering rules.
  const created = await page.evaluate(async () => {
    const me = await (await fetch('/api/v1/me')).json();
    const ids = [];
    for (let i = 0; i < 12; i++) {
      const r = await fetch('/api/v1/tickets', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': me.csrf_token }, body: JSON.stringify({ request_id: crypto.randomUUID(), title: '列表分页样本 ' + i, description: '可控合成工单，用于真实列表分页。', category: 'network', priority: 'low' }) });
      if (r.status !== 201) throw new Error('Cannot create list sample');
      ids.push((await r.json()).ticket.id);
    }
    return ids;
  });
  expect(created).toHaveLength(12);
  await page.goto('/workspace?view=created_by_me');
  await expect(page.locator('tbody tr')).toHaveCount(10);
  await page.getByRole('button', { name: '下一页', exact: true }).click();
  await expect(page).toHaveURL(/page=2/);
  await expect(page.locator('tbody tr').first()).toBeVisible();
  await screenshot(page, 'paginated-workspace');
  await page.getByRole('button', { name: '上一页', exact: true }).click();
  await expect(page).toHaveURL(/page=1/);
  await page.route('**/api/v1/tickets?*', async route => { await new Promise(resolve => setTimeout(resolve, 500)); await route.continue(); });
  await page.getByRole('button', { name: '↻ 刷新', exact: true }).click();
  await expect(page.locator('#workspace-data')).toContainText('正在加载');
  await expect(page.locator('tbody tr')).toHaveCount(10);
  await page.unroute('**/api/v1/tickets?*');
  await page.route('**/api/v1/tickets?*', route => route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: { code: 'INTERNAL_ERROR', message: 'test list failure' } }) }));
  await page.getByRole('button', { name: '↻ 刷新', exact: true }).click();
  await expect(page.locator('.empty')).toContainText('服务暂时');
  await page.unroute('**/api/v1/tickets?*');
  await page.getByRole('button', { name: '重新加载', exact: true }).click();
  await expect(page.locator('tbody tr')).toHaveCount(10);
  expect(pageErrors).toEqual([]);
  save('browser-scenario-d', { browser: 'Chrome', passed: true, submitted_once: posts >= 1, double_click_create_requests: 1, list_page_size: 10, ...data });
});
