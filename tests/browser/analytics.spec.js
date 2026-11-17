import { test, expect } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const output=resolve('artifacts/m3-01');mkdirSync(output,{recursive:true});
const gold=JSON.parse(readFileSync(resolve('tests/analytics/prd_gold.json'),'utf8'));
const fixture=(action)=>execFileSync('python3',['scripts/analytics_fixture.py',action],{encoding:'utf8'});
async function login(page,username) {
  await page.goto('/login');await page.getByLabel('账号',{exact:true}).fill(username);
  await page.getByLabel('密码',{exact:true}).fill('TicketPulse!2026');
  await page.getByRole('button',{name:'登录',exact:true}).click();
  await expect(page).toHaveURL(/\/workspace/);
  await expect(page.locator('#workspace-data')).not.toContainText('正在加载');
}
async function dashboard(page,query='start=2026-11-01&end=2026-11-04') {
  await page.goto('/analytics?'+query);await expect(page.locator('#analytics-data')).not.toContainText('正在加载运营指标');
}
async function caughtUp(page) {
  await expect(async()=>{await dashboard(page);await expect(page.locator('#analytics-sync-state')).toHaveText('分析数据已追平',{timeout:1500});}).toPass({timeout:25000});
}
async function capture(page,name){await page.evaluate(()=>window.scrollTo(0,0));await page.screenshot({path:resolve(output,name+'.png'),fullPage:true});}

test('Analytics: real login, independent PRD metrics, team/date filters and precise current backlog',async({page})=>{
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await login(page,'supervisor');
  await page.getByRole('link',{name:'运营分析',exact:true}).click();
  await expect(page).toHaveURL(/\/analytics/);await caughtUp(page);
  await expect(page.locator('#metric-created')).toHaveText('10');
  await expect(page.locator('#metric-resolved')).toHaveText('6');
  await expect(page.locator('#metric-duration')).toHaveText('16 小时 50 分');
  await expect(page.locator('#metric-backlog')).toHaveText('5');
  const api=await page.evaluate(async()=>{const response=await fetch('/api/v1/analytics?start=2026-11-01&end=2026-11-04');return response.json();});
  expect(api.daily).toEqual(gold.daily);expect(api.overview).toEqual(gold.overview);
  expect(api.sync.caught_up).toBe(true);
  expect(new Date(api.refreshed_at).getTime()).toBeGreaterThanOrEqual(new Date(api.sync.confirmed_through).getTime());
  await expect(page.locator('#analytics-refreshed')).not.toHaveText('尚未确认');
  await expect(page.locator('#analytics-watermark')).not.toHaveText('尚未确认');
  await expect(page.locator('#analytics-trend')).toBeVisible();
  await page.getByText('查看每日数值',{exact:true}).click();
  await expect(page.locator('#analytics-daily-table tbody tr')).toHaveCount(3);
  const daily=await page.locator('#analytics-daily-table tbody tr').allTextContents();
  expect(daily[0]).toContain('2 小时');expect(daily[1]).toContain('12 小时 30 分');expect(daily[2]).toContain('24 小时 40 分');
  await expect(page.locator('.analytics-cards a')).toHaveCount(0);
  await expect(page.locator('#analytics-daily-table a')).toHaveCount(0);
  await capture(page,'analytics-desktop-prd');
  await page.getByLabel('开始日期（北京时间）',{exact:true}).fill('2026-11-02');
  await page.getByLabel('结束日期（不含）',{exact:true}).fill('2026-11-03');
  await page.getByLabel('团队（可多选）',{exact:true}).selectOption(['20']);
  await page.getByRole('button',{name:'应用筛选',exact:true}).click();
  await expect(page.locator('#metric-created')).toHaveText('1');
  await expect(page.locator('#metric-resolved')).toHaveText('1');
  await expect(page.locator('#metric-duration')).toHaveText('1 小时');
  await expect(page.locator('#metric-backlog')).toHaveText('4');
  await page.locator('.backlog-link[data-team="20"]').click();
  await expect(page).toHaveURL(/\/search\?/);
  const query=new URL(page.url()).searchParams;
  expect(query.getAll('status').sort()).toEqual(['in_progress','pending']);expect(query.get('current_team_id')).toBe('20');
  expect(query.has('created_from')).toBe(false);
  await expect(page.locator('#search-data tbody tr')).toHaveCount(4);
  const ids=await page.locator('#search-data .ticket-link').evaluateAll(links=>links.map(a=>Number(a.getAttribute('href').split('/').at(-1))).sort((a,b)=>a-b));
  expect(ids).toEqual([3,4,6,8]);
  await page.locator('#search-data .ticket-link').filter({hasText:'T04'}).click();
  await expect(page.locator('#resolution-note')).toContainText('第 6 版解决说明');
  await expect(page.locator('#history-list')).toContainText('IT 服务台');
  await expect(page.locator('#history-list')).toContainText('设备服务台');
  await page.getByRole('button',{name:'退出登录',exact:true}).click();
  await login(page,'it-agent');await expect(page.getByRole('link',{name:'运营分析',exact:true})).toHaveCount(0);
  await page.goto('/analytics');await expect(page.locator('.empty')).toContainText('仅对当前有效主管或管理员');
  const unauthorized=await page.evaluate(async()=>{const response=await fetch('/api/v1/analytics?role=admin');return {status:response.status,data:await response.json()};});
  expect(unauthorized.status).toBe(403);expect(unauthorized.data.error.code).toBe('ANALYTICS_FORBIDDEN');
  expect(errors).toEqual([]);
  writeFileSync(resolve(output,'browser-analytics-correctness.json'),JSON.stringify({passed:true,browser:'Chrome',cookie_login:true,prd_daily:api.daily,overview:api.overview,B_backlog_ids:ids,current_role_enforced:true,history_not_drilled_down:true,page_errors:errors},null,2)+'\n');
});

test('Analytics: loading, empty, zero, delayed sync, actual CK outage, permission change and escaped labels',async({page})=>{
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await login(page,'supervisor');
  await page.route('**/api/v1/analytics?*',async route=>{await new Promise(r=>setTimeout(r,700));await route.continue();});
  await page.goto('/analytics?start=2026-11-01&end=2026-11-04');
  await expect(page.locator('#analytics-data')).toContainText('正在加载运营指标');
  await expect(page.locator('#metric-created')).toHaveText('10');await page.unroute('**/api/v1/analytics?*');
  await dashboard(page,'start=2026-11-04&end=2026-11-05');
  await expect(page.locator('#metric-created')).toHaveText('0');
  await expect(page.locator('#metric-duration')).toHaveText('—');
  await expect(page.locator('#metric-backlog')).toHaveText('5');
  await expect(page.locator('#analytics-no-history')).toContainText('没有新增或首次解决事件');
  await dashboard(page,'start=2026-11-04&end=2026-11-01');
  await expect(page.locator('#analytics-data')).toContainText('日期窗口须为');
  await expect(page.locator('.analytics-cards')).toHaveCount(0);
  fixture('stop-ck');
  try {await dashboard(page);await expect(page.locator('#analytics-sync-state')).toHaveText('同步已停止');await expect(page.locator('#analytics-freshness')).toContainText('页面刷新不代表分析数据已追平');await capture(page,'analytics-delayed-sync');}
  finally{fixture('start-ck');}
  await caughtUp(page);
  execFileSync('python3',['scripts/local_analytics.py','stop'],{encoding:'utf8'});
  try {await dashboard(page);await expect(page.locator('#analytics-data')).toContainText('统计服务暂不可用');await expect(page.locator('.analytics-cards')).toHaveCount(0);await capture(page,'analytics-ck-unavailable');}
  finally{execFileSync('python3',['scripts/local_analytics.py','start'],{encoding:'utf8'});}
  await page.getByRole('button',{name:'重新加载统计',exact:true}).click();await expect(page.locator('#metric-created')).toHaveText('10');
  fixture('downgrade-supervisor');
  try {await page.goto('/analytics');await expect(page.locator('.empty')).toContainText('仅对当前有效主管或管理员');await expect(page.locator('.analytics-cards')).toHaveCount(0);const denied=await page.evaluate(async()=> (await fetch('/api/v1/analytics')).status);expect(denied).toBe(403);}
  finally{fixture('restore-supervisor');}
  fixture('escape-team');
  try {await dashboard(page);await expect(page.locator('#analytics-teams-table')).toContainText('IT <svg onload=alert(1)>');await expect(page.locator('#analytics-data svg')).toHaveCount(1);await expect(page.locator('#analytics-teams-table svg')).toHaveCount(0);}
  finally{fixture('restore-team');}
  await dashboard(page);await page.setViewportSize({width:760,height:1000});
  await expect(page.locator('#analytics-form')).toBeVisible();await expect(page.locator('#analytics-trend')).toBeVisible();await capture(page,'analytics-responsive');
  await page.getByRole('button',{name:'退出登录',exact:true}).click();await login(page,'empty-admin');
  await dashboard(page);await expect(page.locator('#analytics-empty')).toContainText('当前组织还没有工单');await expect(page.locator('#metric-created')).toHaveText('0');await expect(page.locator('#metric-backlog')).toHaveText('0');
  expect(errors).toEqual([]);
  writeFileSync(resolve(output,'browser-analytics-errors.json'),JSON.stringify({passed:true,browser:'Chrome',loading:true,zero_with_current_backlog:true,empty_organization:true,invalid_window:true,independent_consumer_pause:true,native_ck_stopped:true,current_role_change:true,escaped_team_label:true,responsive:true,page_errors:errors},null,2)+'\n');
});
