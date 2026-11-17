const app = document.querySelector('#app');
const views = {created_by_me:'我提交的',assigned_to_me:'分配给我的',team_pending:'本团队待处理'};
const statusNames = {pending:'待处理',in_progress:'处理中',awaiting_confirmation:'待确认',closed:'已关闭'};
const priorityNames = {urgent:'紧急',high:'高',medium:'中',low:'低'};
const categoryNames = {network:'网络',account:'账号',device:'设备',software:'软件',other:'其他'};
const roleNames = {requester:'提单人',agent:'处理人员',supervisor:'团队主管',admin:'管理员'};
const actionNames = {claim:'认领工单',start:'开始处理',resolve:'提交解决',reopen:'重新打开',close:'确认关闭',transfer:'同团队转派','transfer-team':'跨团队转派','management-reopen':'管理重开','manual-close':'人工关闭'};
const historyNames = {created:'创建了工单',claimed:'认领了工单',processing_started:'开始处理',resolved:'提交了解决结果',reopened:'重新打开了工单',closed:'确认关闭了工单',assignee_transferred:'转派了负责人',team_transferred:'跨团队转派',reopened_requeued:'重开并重新入队',management_reopened:'执行了管理重开',manually_closed:'人工关闭了工单',commented:'添加了评论'};
let profile = null, routeRun = 0, detailState = null, operationBusy = false;
let flash = '', expiresTimer = null;
const memoryDrafts = new Map();
const e = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const badge = (value,names) => '<span class="badge '+e(value)+'">'+e(names[value] || value)+'</span>';
const date = value => value ? new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}).format(new Date(value)) : '—';
const avatar = (name,green=false) => '<span class="avatar '+(green?'green':'')+'">'+e(Array.from(name || '?')[0])+'</span>';
function readDraft(key) { try {return JSON.parse(sessionStorage.getItem(key) || 'null');} catch {return memoryDrafts.get(key) || null;} }
function saveDraft(key,value) { memoryDrafts.set(key,value); try {sessionStorage.setItem(key,JSON.stringify(value));} catch {} }
function removeDraft(key) { memoryDrafts.delete(key); try {sessionStorage.removeItem(key);} catch {} }
function workspacePath() { const value=readDraft('ticketpulse:workspace:'+profile.user.id);return typeof value==='string'&&/^\/workspace(?:\?|$)/.test(value)?value:'/workspace'; }
function toast(message) { const el=document.querySelector('#toast'); el.textContent=message; el.hidden=false; clearTimeout(toast.timer); toast.timer=setTimeout(()=>el.hidden=true,4000); }
function navigate(path,replace=false) { history[replace?'replaceState':'pushState']({},'',path); renderRoute(); }
function expire(code='SESSION_EXPIRED') {
  if (!profile) return;
  saveCurrentInputs();
  profile=null;detailState=null;operationBusy=false;clearTimeout(expiresTimer);
  navigate('/login?reason='+encodeURIComponent(code==='SESSION_EXPIRED'?'expired':'unauthenticated'),true);
}
async function api(path,{method='GET',body,auth=true}={}) {
  const requestUser=profile?.user.id;
  const requestSession=profile?.csrf_token;
  const headers={'Content-Type':'application/json'};
  if (method!=='GET' && profile?.csrf_token) headers['X-CSRF-Token']=profile.csrf_token;
  const controller=new AbortController();const timeout=setTimeout(()=>controller.abort(),12000);
  try {
    const response=await fetch('/api/v1'+path,{method,headers,credentials:'same-origin',body:body===undefined?undefined:JSON.stringify(body),signal:controller.signal});
    let value;try {value=await response.json();} catch {
      const invalid=new Error(method==='GET'?'服务返回异常，请重新加载。':'提交结果待确认，请保留原输入并读取最新状态。');
      invalid.code=method==='GET'?'INVALID_RESPONSE':'NETWORK_UNKNOWN';throw invalid;
    }
    if (!response.ok) {
      const error=new Error(value.error?.message || '请求失败');error.code=value.error?.code;error.status=response.status;error.method=method;
      if (auth && response.status===401 && profile?.user.id===requestUser && profile?.csrf_token===requestSession) expire(error.code);
      throw error;
    }
    return value;
  } catch(error) {
    if (error.name==='AbortError') {const timed=new Error(method==='GET'?'查询超时，请稍后重试。':'请求超时，提交结果待确认。');timed.code=method==='GET'?'NETWORK_TIMEOUT':'NETWORK_UNKNOWN';throw timed;}
    if (error instanceof TypeError) {const offline=new Error(method==='GET'?'暂时无法连接服务，请稍后重试。':'无法连接服务，提交结果待确认。');offline.code=method==='GET'?'NETWORK_UNAVAILABLE':'NETWORK_UNKNOWN';throw offline;}
    throw error;
  } finally {clearTimeout(timeout);}
}
function errorText(error) {
  if(error.method==='GET'&&error.code==='REQUEST_TIMEOUT')return '查询超时，请稍后重试。';
  if(error.method==='GET'&&error.code==='INTERNAL_ERROR')return '服务暂时不可用，请稍后重试。';
  const messages={VERSION_CONFLICT:'工单已被其他人更新。请刷新最新状态后重新操作，输入内容已保留。',ACTION_FORBIDDEN:'你已没有执行此操作的权限，请刷新最新工单。',FORBIDDEN:'你已没有查看这张工单的权限，工单可能已转派到其他团队。',INVALID_STATE:'当前状态不允许此操作，请刷新工单。',TEAM_UNAVAILABLE:'当前负责团队已停用。请联系主管选择有效团队，执行带原因的管理重开。',INVALID_ASSIGNEE:'目标人员已失效，请刷新候选人员。',INVALID_TEAM:'目标团队已失效，请刷新候选团队。',CSRF_REJECTED:'请求校验已失效，请刷新页面后重试。',INTERNAL_ERROR:'服务暂时无法完成操作，输入已保留。请刷新工单确认结果。',ACTION_RESULT_UNKNOWN:'提交结果待确认，请刷新工单和历史后确认。',SESSION_EXPIRED:'登录已过期，请重新登录。'};
  return messages[error.code] || error.message || '请求失败，请稍后重试。';
}
function setSession(value) {
  profile=value;clearTimeout(expiresTimer);
  if (value.expires_at) saveDraft('ticketpulse:last-expiry',value.expires_at);
  if (value.expires_at) expiresTimer=setTimeout(()=>expire(),Math.max(0,new Date(value.expires_at)-Date.now()));
}
function brand() {return '<a class="brand" href="/workspace" data-nav><span class="brandmark">tp</span>TicketPulse</a>';}
const isManager = () => ['supervisor','admin'].includes(profile?.user.role);
function shell(content,crumb='工作台') {
  const current=new URL(location.href).searchParams.get('view') || 'created_by_me';
  app.innerHTML='<div class="layout"><aside class="sidebar">'+brand()+'<div class="brand-caption">SUPPORT, IN SYNC</div><div class="nav-label">工作空间</div><nav class="nav">'+Object.entries(views).map(([key,label],i)=>'<a href="/workspace?view='+key+'" data-nav class="'+(location.pathname==='/workspace'&&current===key?'active':'')+'"><span class="nav-icon">'+['▤','▣','◷'][i]+'</span>'+label+'</a>').join('')+'<a href="/search" data-nav class="'+(location.pathname==='/search'?'active':'')+'"><span class="nav-icon" aria-hidden="true">⌕</span>搜索工单</a>'+(isManager()?'<a href="/analytics" data-nav class="'+(location.pathname==='/analytics'?'active':'')+'"><span class="nav-icon" aria-hidden="true">▥</span>运营分析</a>':'')+'<a href="/tickets/new" data-nav class="'+(location.pathname==='/tickets/new'?'active':'')+'"><span class="nav-icon">＋</span>创建工单</a></nav><div class="sidebar-foot"><span class="live-dot"></span>服务工作空间<br><br>让每一次支持，都有迹可循。</div></aside><main class="main"><header class="topbar"><span class="crumb">工作空间 <span> / </span> <strong>'+e(crumb)+'</strong></span><div class="user-menu">'+avatar(profile.user.name)+'<div><div class="user-name">'+e(profile.user.name)+'</div><div class="user-role">'+e(roleNames[profile.user.role])+'</div></div><button class="plain small" id="logout">退出登录</button></div></header><div class="content">'+(flash?'<div class="notice success" role="status">'+e(flash)+'</div>':'')+content+'</div></main></div>';
  flash='';
  document.querySelector('#logout').onclick=async()=>{saveCurrentInputs();try{await api('/auth/logout',{method:'POST',body:{}});profile=null;detailState=null;removeDraft('ticketpulse:last-expiry');clearTimeout(expiresTimer);navigate('/login',true);}catch(error){if(profile)toast(errorText(error));}};
}
function loginPage(message='') {
  const reason=new URL(location.href).searchParams.get('reason');
  const notice=message || (reason==='expired'?'登录已过期，请重新登录。':reason?'请重新登录后继续操作。':'');
  app.innerHTML='<div class="login-layout"><section class="login-story">'+brand()+'<div class="story-main"><div class="story-kicker">ONE WORKSPACE. EVERY REQUEST.</div><h1>让问题有回应，<br>让协作有进展。</h1><p>从一张工单开始，连接提单人和支持团队。清晰的负责人、可追溯的进展，让每一次服务都有着落。</p><div class="story-line"></div><div class="story-flow"><span>提出问题</span><span>团队协作</span><span>解决与确认</span></div></div><div class="login-foot">TicketPulse · 企业支持工作空间</div></section><section class="login-form-area"><div class="login-card"><div class="eyebrow">WELCOME BACK</div><h1>登录工作台</h1><p class="subtitle">使用你的本地测试账号，继续团队协作。</p>'+(notice?'<div class="notice login-expiry" role="status">'+e(notice)+'</div>':'')+'<form id="login-form"><div class="field"><label for="username">账号</label><input id="username" name="username" autocomplete="username" required maxlength="64" placeholder="例如 alice"></div><div class="field"><label for="password">密码</label><input id="password" name="password" type="password" autocomplete="current-password" required maxlength="72" placeholder="输入账号密码"></div><div id="login-error" class="error-line" role="alert" hidden></div><button class="primary" type="submit">登录</button></form><div class="login-help"><strong>本地体验账号</strong><br>提单人 alice · 工程师 it-agent / device-agent<br>主管 supervisor · 管理员 admin<br>请使用演示负责人提供的账号和密码。</div></div></section></div>';
  document.querySelector('#login-form').onsubmit=async event=>{
    event.preventDefault();const form=event.currentTarget,button=form.querySelector('button');if(button.disabled)return;button.disabled=true;button.textContent='正在登录…';
    try {const login=await api('/auth/login',{method:'POST',body:{username:form.username.value,password:form.password.value},auth:false});setSession(login);const me=await api('/me');setSession(me);navigate('/workspace',true);}catch(error){const el=document.querySelector('#login-error');if(el){el.hidden=false;el.textContent=errorText(error);button.disabled=false;button.textContent='登录';}}
  };
}
function pageError(error,retry) {
  shell('<div class="panel"><div class="empty"><div class="empty-icon">!</div><strong>'+e(errorText(error))+'</strong><span>你可以返回工作台，或重新获取最新数据。</span><p><button id="page-retry">重新加载</button> <a href="/workspace" data-nav>返回工作台</a></p></div></div>','工单');
  document.querySelector('#page-retry').onclick=retry;
}
async function workspace(run) {
  const url=new URL(location.href),view=views[url.searchParams.get('view')]?url.searchParams.get('view'):'created_by_me';
  const page=Math.max(1,Number(url.searchParams.get('page'))||1),pageSize=10;
  saveDraft('ticketpulse:workspace:'+profile.user.id,'/workspace?view='+view+'&page='+page);
  shell('<div class="page-head"><div><div class="eyebrow">YOUR SERVICE DESK</div><h1>个人工作台</h1><p class="subtitle">'+e(profile.teams?.length?profile.teams.map(t=>t.name).join(' · '):'跟进你的请求，查看团队最新进展')+'</p></div><button class="primary" data-go="/tickets/new">＋ 创建工单</button></div><div id="workspace-data" class="loading">正在加载工单…</div>');
  try {
    const values=await Promise.all(Object.keys(views).map(key=>api('/tickets?view='+key+'&page='+(key===view?page:1)+'&page_size='+(key===view?pageSize:1))));
    if(run!==routeRun)return;
    const result=values[Object.keys(views).indexOf(view)];
    const stats='<div class="cards">'+Object.entries(views).map(([key,label],i)=>'<div class="stat"><div><div class="stat-label">'+label+'</div><div class="stat-value">'+values[i].total+'</div><div class="stat-caption">'+['本人提交的全部工单','当前分配给我的工单','当前有效团队的待处理工单'][i]+'</div></div><span class="stat-icon">'+['▤','▣','◷'][i]+'</span></div>').join('')+'</div>';
    const rows=result.items.map(t=>'<tr><td><a class="ticket-link" href="/tickets/'+t.id+'" data-nav>'+e(t.title)+'</a><span class="ticket-no">'+e(t.ticket_no)+'</span></td><td>'+badge(t.priority,priorityNames)+'</td><td>'+badge(t.status,statusNames)+'</td><td>'+e(t.current_team_name)+'</td><td><span class="owner-cell">'+(t.assignee_name?avatar(t.assignee_name,true)+e(t.assignee_name):'<span class="section-note">尚未分配</span>')+'</span></td><td class="cell-date">'+e(date(t.updated_at))+'</td></tr>').join('');
    document.querySelector('#workspace-data').className='';
    document.querySelector('#workspace-data').innerHTML=stats+'<section class="panel"><div class="panel-head"><div class="tabs">'+Object.entries(views).map(([key,label])=>'<button class="'+(view===key?'active':'')+'" data-view="'+key+'">'+label+'</button>').join('')+'</div><button id="list-refresh" class="plain small">↻ 刷新</button></div>'+(rows?'<div class="table-wrap"><table><thead><tr><th>工单</th><th>优先级</th><th>状态</th><th>负责团队</th><th>负责人</th><th>更新时间</th></tr></thead><tbody>'+rows+'</tbody></table></div>':'<div class="empty"><div class="empty-icon">▤</div><strong>这里还没有工单</strong><span>新的请求和团队任务会显示在这里。</span></div>')+'<div class="pagination"><span>共 '+result.total+' 张工单 · 每页 '+pageSize+' 条</span><div class="page-controls"><button class="small" id="prev-page" '+(page===1?'disabled':'')+'>上一页</button><span class="page-index">'+page+'</span><button class="small" id="next-page" '+(!result.has_more?'disabled':'')+'>下一页</button></div></div></section>';
    document.querySelector('#list-refresh').onclick=()=>renderRoute();
    document.querySelector('#prev-page').onclick=()=>navigate('/workspace?view='+view+'&page='+(page-1));
    document.querySelector('#next-page').onclick=()=>navigate('/workspace?view='+view+'&page='+(page+1));
  }catch(error){if(run===routeRun&&profile)pageError(error,()=>renderRoute());}
}
async function searchPage(run) {
  const params=new URL(location.href).searchParams;
  const page=Math.max(1,Number(params.get('page'))||1);
  const pagingQuery=new URLSearchParams(params);pagingQuery.delete('cursor');pagingQuery.delete('page');pagingQuery.sort();
  const pagingKey='ticketpulse:search-page:'+profile.user.id+':'+pagingQuery.toString()+':';
  const field=(name,label,type='text')=>'<div class="field"><label for="search-'+name+'">'+label+'</label><input id="search-'+name+'" name="'+name+'" type="'+type+'" value="'+e(params.get(name)||'')+'" '+(name==='q'?'maxlength="200" placeholder="在标题与描述中搜索"':'')+'></div>';
  const select=(name,label,values,multiple=false)=>'<div class="field"><label for="search-'+name+'">'+label+'</label><select id="search-'+name+'" name="'+name+'" '+(multiple?'multiple size="4"':'')+'>'+(!multiple?'<option value="">全部</option>':'')+values.map(([value,text])=>'<option value="'+e(value)+'" '+(params.getAll(name).includes(String(value))?'selected':'')+'>'+e(text)+'</option>').join('')+'</select></div>';
  shell('<div class="page-head"><div><div class="eyebrow">FIND THE RIGHT REQUEST</div><h1>搜索工单</h1><p class="subtitle">按关键词与业务条件，找到当前有权查看的工单。</p></div><a href="/workspace" data-nav>返回工作台</a></div><section class="panel search-panel"><div class="panel-body"><form id="search-form"><div class="search-main">'+field('q','关键词')+field('ticket_no','工单号（精确匹配）')+'<button class="primary" type="submit">搜索</button></div><details id="search-filters"><summary>筛选与排序</summary><div class="search-filter-grid">'+select('status','状态（可多选）',Object.entries(statusNames),true)+select('priority','优先级（可多选）',Object.entries(priorityNames),true)+select('category','类别（可多选）',Object.entries(categoryNames),true)+select('current_team_id','当前团队',[])+select('assignee_id','当前负责人',[['0','未分配']])+select('sort','排序字段',[['updated_at','更新时间'],['created_at','创建时间'],['priority','优先级'],['id','工单序号']])+select('order','排序方向',[['desc','降序'],['asc','升序']])+select('page_size','每页条数',[['10','10 条'],['20','20 条'],['50','50 条']])+field('created_from','创建时间起点（北京时间）','datetime-local')+field('created_to','创建时间终点（不含）','datetime-local')+field('updated_from','更新时间起点（北京时间）','datetime-local')+field('updated_to','更新时间终点（不含）','datetime-local')+'</div><p class="section-note">同字段多选为“或”，不同条件为“且”；时间窗口左闭右开。</p></details><div class="form-actions"><button type="button" id="search-reset" class="plain small">清除条件</button></div></form></div></section><div id="search-freshness" class="notice">搜索异步更新，可能有短暂延迟；详情与基础列表以当前业务数据为准。</div><div id="search-data" class="loading" role="status">正在搜索…</div>','搜索工单');
  const form=document.querySelector('#search-form');
  form.elements.sort.value=params.get('sort')||'updated_at';form.elements.order.value=params.get('order')||'desc';form.elements.page_size.value=params.get('page_size')||'20';
  for(const name of ['created_from','created_to','updated_from','updated_to']){const value=params.get(name);if(value&&!Number.isNaN(Date.parse(value))){const shifted=new Date(Date.parse(value)+8*3600000);form.elements[name].value=shifted.toISOString().slice(0,16);}}
  if([...params.keys()].some(name=>!['q','page','cursor'].includes(name)))document.querySelector('#search-filters').open=true;
  form.onsubmit=event=>{event.preventDefault();const next=new URLSearchParams();for(const [name,value] of new FormData(form)){if(!value)continue;if(name.endsWith('_from')||name.endsWith('_to')){next.set(name,new Date(value+':00+08:00').toISOString());}else next.append(name,value);}navigate('/search?'+next);};
  document.querySelector('#search-reset').onclick=()=>navigate('/search');
  api('/search/options').then(options=>{
    if(run!==routeRun)return;
    for(const [name,items] of [['current_team_id',options.teams],['assignee_id',options.assignees]]){const input=form.elements[name];input.innerHTML='<option value="">全部</option>'+(name==='assignee_id'?'<option value="0">未分配</option>':'')+items.map(item=>'<option value="'+item.id+'">'+e(item.name)+'</option>').join('');const value=params.get(name);if(value){if(![...input.options].some(o=>o.value===value)){const option=document.createElement('option');option.value=value;option.textContent='筛选 ID '+value;input.add(option);}input.value=value;}}
  }).catch(error=>{if(run===routeRun&&profile)toast(errorText(error));});
  const query=new URLSearchParams(params);query.delete('page');
  try {
    const result=await api('/search/tickets?'+query);
    if(run!==routeRun)return;
    const f=result.freshness;
    document.querySelector('#search-freshness').textContent='搜索异步更新，可能有短暂延迟。页面刷新：'+date(result.refreshed_at)+'；最近投影成功：'+date(f.last_success_at)+'；同步状态：'+({running:'消费运行中',retrying:'正在重试',stalled:'消费未运行',stopped:'消费已停止',degraded:'存在隔离记录',unknown:'水位未确认'}[f.state]||f.state)+'。尚未确认全链路追平。';
    const rows=result.items.map(t=>'<tr><td><a class="ticket-link" href="/tickets/'+t.id+'" data-nav>'+e(t.title)+'</a><span class="ticket-no">'+e(t.ticket_no)+'</span></td><td>'+badge(t.priority,priorityNames)+'</td><td>'+badge(t.status,statusNames)+'</td><td>'+e(t.current_team_name)+'</td><td>'+e(t.assignee_name||'尚未分配')+'</td><td class="cell-date">'+e(date(t.updated_at))+'</td></tr>').join('');
    const data=document.querySelector('#search-data');data.className='panel';
    data.innerHTML='<div class="panel-head"><h2>搜索结果</h2><button id="search-refresh" class="plain small">↻ 刷新</button></div>'+(rows?'<div class="table-wrap"><table><thead><tr><th>工单</th><th>优先级</th><th>状态</th><th>当前团队</th><th>负责人</th><th>更新时间</th></tr></thead><tbody>'+rows+'</tbody></table></div>':'<div class="empty"><div class="empty-icon">⌕</div><strong>没有符合条件的工单</strong><span>可以调整筛选；新工单或更新可能尚在同步中。</span></div>')+'<div class="pagination"><span>本页 '+result.items.length+' 张 · 总数暂不可用</span><div class="page-controls"><button id="search-prev" class="small" '+(page===1?'disabled':'')+'>上一页</button><span class="page-index">'+page+'</span><button id="search-next" class="small" '+(!result.has_more?'disabled':'')+'>下一页</button></div></div>';
    document.querySelector('#search-refresh').onclick=()=>renderRoute();
    document.querySelector('#search-prev').onclick=()=>{const previous=readDraft(pagingKey+(page-1));if(previous)navigate(previous);else{const first=new URLSearchParams(params);first.delete('cursor');first.delete('page');navigate('/search?'+first);}};
    document.querySelector('#search-next').onclick=()=>{saveDraft(pagingKey+page,location.pathname+location.search);const next=new URLSearchParams(params);next.set('cursor',result.next_cursor);next.set('page',String(page+1));navigate('/search?'+next);};
  }catch(error){if(run===routeRun&&profile){
    const targetChanged=error.code==='SEARCH_CURSOR_TARGET_CHANGED';
    const data=document.querySelector('#search-data');data.className='panel';
    data.innerHTML='<div class="empty"><strong>'+e(errorText(error))+'</strong><span>可修改查询条件，或使用工作台和工单详情。</span><p><button id="search-retry">'+(targetChanged?'重新搜索':'重试搜索')+'</button></p></div>';
    document.querySelector('#search-retry').onclick=()=>{
      if(!targetChanged){renderRoute();return;}
      const first=new URLSearchParams(params);first.delete('cursor');first.delete('search_after');first.delete('page');
      navigate('/search?'+first,true);
    };
  }}
}
function resolutionDuration(seconds) {
  if(seconds===null||seconds===undefined)return '—';
  if(seconds<60)return Math.round(seconds)+' 秒';
  const minutes=Math.round(seconds/60),hours=Math.floor(minutes/60);
  return (hours?hours+' 小时 ':'')+(minutes%60?minutes%60+' 分':hours?'':'0 分');
}
const preciseTime = value => value ? new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'}).format(new Date(value)) : '尚未确认';
function analyticsTrend(daily) {
  const width=760,height=250,left=40,right=24,top=22,bottom=38;
  const maximum=Math.max(1,...daily.flatMap(d=>[d.created,d.first_resolved]));
  const x=i=>left+(width-left-right)*i/Math.max(1,daily.length-1);
  const y=value=>height-bottom-value/maximum*(height-top-bottom);
  const grid=Array.from({length:5},(_,i)=>{const v=maximum*i/4;return '<line x1="'+left+'" x2="'+(width-right)+'" y1="'+y(v)+'" y2="'+y(v)+'" class="trend-grid"/><text x="'+(left-10)+'" y="'+(y(v)+4)+'" text-anchor="end" class="trend-label">'+Number(v.toFixed(1))+'</text>';}).join('');
  const lines=['created','first_resolved'].map((key,k)=>'<polyline class="trend-line '+(k?'resolved':'created')+'" points="'+daily.map((d,i)=>x(i)+','+y(d[key])).join(' ')+'"/>'+daily.map((d,i)=>'<circle class="trend-point '+(k?'resolved':'created')+'" cx="'+x(i)+'" cy="'+y(d[key])+'" r="3"><title>'+e(d.date+' '+(k?'首次解决':'新增')+' '+d[key]+' 张')+'</title></circle>').join('')).join('');
  const step=Math.max(1,Math.ceil(daily.length/7));
  const labels=daily.map((d,i)=>i%step===0||i===daily.length-1?'<text class="trend-label" x="'+x(i)+'" y="'+(height-12)+'" text-anchor="middle">'+e(d.date.slice(5))+'</text>':'').join('');
  return '<svg id="analytics-trend" viewBox="0 0 '+width+' '+height+'" role="img" aria-label="每日新增与首次解决趋势，详细数值见下方日期表格">'+grid+lines+labels+'</svg>';
}
async function analyticsPage(run) {
  const params=new URL(location.href).searchParams;
  const day=offset=>{const value=new Date(Date.now()+8*3600000);value.setUTCDate(value.getUTCDate()+offset);return value.toISOString().slice(0,10);};
  const start=params.get('start')||day(-6),end=params.get('end')||day(1);
  if(!isManager()){shell('<section class="panel"><div class="empty"><strong>运营分析仅对当前有效主管或管理员开放</strong><span>你可以在工作台跟进工单。</span><p><a href="/workspace" data-nav>返回工作台</a></p></div></section>','运营分析');return;}
  const selected=params.getAll('team_id');
  shell('<div class="page-head"><div><div class="eyebrow">SUPPORT OPERATIONS</div><h1>运营分析</h1><p class="subtitle">了解团队工作量、首次解决效率和当前待处理积压。</p></div><button id="analytics-refresh" class="small">↻ 刷新数据</button></div><section class="panel analytics-filters"><div class="panel-body"><form id="analytics-form"><div class="analytics-filter-grid"><div class="field"><label for="analytics-start">开始日期（北京时间）</label><input id="analytics-start" name="start" type="date" required value="'+e(start)+'"></div><div class="field"><label for="analytics-end">结束日期（不含）</label><input id="analytics-end" name="end" type="date" required value="'+e(end)+'"></div><div class="field"><label for="analytics-teams">团队（可多选）</label><select id="analytics-teams" name="team_id" multiple size="3">'+selected.map(id=>'<option selected value="'+e(id)+'">团队 '+e(id)+'</option>').join('')+'</select><small>不选择表示全部团队</small></div><button class="primary" type="submit">应用筛选</button></div><p class="section-note">日期按左闭右开窗口统计历史事件；当前积压始终按最新状态计算，不受日期筛选影响。</p></form></div></section><div id="analytics-freshness" class="notice" role="status">正在读取统计同步状态…</div><div id="analytics-data" class="loading" role="status">正在加载运营指标…</div>','运营分析');
  document.querySelector('#analytics-refresh').onclick=()=>renderRoute();
  document.querySelector('#analytics-form').onsubmit=event=>{event.preventDefault();const next=new URLSearchParams(new FormData(event.currentTarget));navigate('/analytics?'+next);};
  try {
    const query=new URLSearchParams(params);if(!query.has('start'))query.set('start',start);if(!query.has('end'))query.set('end',end);
    const result=await api('/analytics?'+query);
    if(run!==routeRun)return;
    document.querySelector('#analytics-teams').innerHTML=result.available_teams.map(t=>'<option value="'+t.id+'" '+(selected.includes(String(t.id))?'selected':'')+'>'+e(t.name)+(t.active?'':'（已停用，保留历史）')+'</option>').join('');
    const sync=result.sync,stateNames={running:'已追平',catching_up:'同步追赶中',retrying:'正在重试',degraded:'存在隔离记录',stalled:'同步未运行',not_started:'尚未初始化',broker_unavailable:'消息服务暂不可用',unknown:'水位未确认',stopped:'同步已停止'};
    const freshness=document.querySelector('#analytics-freshness');
    freshness.className='notice analytics-sync '+(sync.caught_up?'success':'');
    freshness.innerHTML='<div class="sync-heading"><strong id="analytics-sync-state">'+e(sync.caught_up?'分析数据已追平':stateNames[sync.state]||'数据尚未追平')+'</strong><span>积压 '+sync.pending+' · 待确认 '+sync.ack_pending+' · 重试 '+sync.retrying+' · 隔离 '+sync.isolated+'</span></div><div class="sync-times"><span>页面刷新：<time id="analytics-refreshed">'+e(preciseTime(result.refreshed_at))+'</time></span><span>分析确认水位：<time id="analytics-watermark">'+e(preciseTime(sync.confirmed_through))+'</time></span><span>源端水位：'+e(preciseTime(sync.source_watermark_at))+'</span></div>'+(!sync.caught_up?'<p>下方是当前可观测数据，仍可能缺少尚未同步的事件；页面刷新不代表分析数据已追平。</p>':'');
    const data=document.querySelector('#analytics-data');data.className='';
    const overview=result.overview;
    const cards=[['created','新增工单',overview.created,'所选日期 · 创建时初始团队'],['resolved','首次解决',overview.first_resolved,'所选日期 · 首次解决时团队'],['duration','平均首次解决时长',resolutionDuration(overview.average_first_resolution_seconds),'从创建到第一次待确认'],['backlog','当前待处理积压',overview.current_backlog,'待处理 + 处理中 · 当前团队']];
    const teamRows=result.teams.map(t=>'<tr><td>'+e(t.team_name)+(t.active?'':'<span class="badge">已停用</span>')+'</td><td>'+t.created+'</td><td>'+t.first_resolved+'</td><td>'+e(resolutionDuration(t.average_first_resolution_seconds))+'</td><td><strong>'+t.current_backlog+'</strong></td><td>'+(t.current_backlog?'<a data-nav class="backlog-link" data-team="'+t.team_id+'" href="'+e(t.backlog_drilldown_url)+'">查看当前明细 →</a>':'<span class="section-note">无待处理积压</span>')+'</td></tr>').join('');
    const dayRows=result.daily.map(d=>'<tr><td>'+e(d.date)+'</td><td>'+d.created+'</td><td>'+d.first_resolved+'</td><td>'+e(resolutionDuration(d.average_first_resolution_seconds))+'</td></tr>').join('');
    const noHistory=overview.created===0&&overview.first_resolved===0;
    data.innerHTML=(result.data_state==='empty'?'<div class="notice" id="analytics-empty">当前组织还没有工单；指标为零，首次解决平均时长暂不可计算。</div>':'')+'<div class="cards analytics-cards">'+cards.map(([key,label,value,note])=>'<section class="stat"><div><div class="stat-label">'+label+'</div><div class="stat-value" id="metric-'+key+'">'+e(value)+'</div><div class="stat-caption">'+note+'</div></div></section>').join('')+'</div><section class="panel analytics-chart"><div class="panel-head"><h2>每日工作量趋势</h2><div class="chart-legend"><span class="created">新增工单</span><span class="resolved">首次解决</span></div></div><div class="panel-body">'+(noHistory?'<p id="analytics-no-history" class="subtitle">所选日期和团队没有新增或首次解决事件；当前积压仍显示在上方。</p>':'')+analyticsTrend(result.daily)+'</div><details class="analytics-daily"><summary>查看每日数值</summary><div class="table-wrap"><table id="analytics-daily-table"><thead><tr><th>日期（北京时间）</th><th>新增工单</th><th>首次解决</th><th>平均首次解决时长</th></tr></thead><tbody>'+dayRows+'</tbody></table></div></details></section><section class="panel"><div class="panel-head"><h2>团队工作量与当前积压</h2><span class="section-note">仅当前积压提供明细入口</span></div><div class="table-wrap"><table id="analytics-teams-table"><thead><tr><th>团队</th><th>新增</th><th>首次解决</th><th>平均首次解决时长</th><th>当前积压</th><th>当前明细</th></tr></thead><tbody>'+teamRows+'</tbody></table></div><p class="analytics-scope-note subtitle">历史按事件当时的团队归属，转派不会改写历史指标。明细来自异步搜索；若搜索尚未同步，可能暂未显示全部当前积压工单。</p></section>';
  } catch(error) {
    if(run!==routeRun||!profile)return;
    const maintenance=['BUSINESS_WRITES_FROZEN','READ_SWITCH_MAINTENANCE'].includes(error.code);
    const message=maintenance?'系统正在维护，统计数据暂时不可用，请稍后重试。':error.code==='ANALYTICS_UNAVAILABLE'?'统计数据暂时不可用，请稍后重试。':errorText(error);
    document.querySelector('#analytics-freshness').textContent='本次统计读取失败，未取得可展示的新数据。';
    const data=document.querySelector('#analytics-data');data.className='panel';
    data.innerHTML='<div class="empty" role="alert"><strong>'+e(message)+'</strong><span>请稍后重新加载统计。</span><p><button id="analytics-retry">重新加载统计</button></p></div>';
    document.querySelector('#analytics-retry').onclick=()=>renderRoute();
  }
}
function creationKey() {return 'ticketpulse:create:'+profile.user.id;}
function createPage() {
  const key=creationKey();let draft=readDraft(key);
  if(!draft){draft={request_id:crypto.randomUUID(),title:'',description:'',category:'network',priority:'medium',locked:false};saveDraft(key,draft);}
  const fieldsDisabled=draft.locked?'readonly':'';
  shell('<div class="page-head"><div><div class="eyebrow">NEW REQUEST</div><h1>创建工单</h1><p class="subtitle">描述你遇到的问题，我们会安排相应团队受理。</p></div><a href="/workspace" data-nav>返回工作台</a></div><div class="create-layout"><section class="panel"><div class="panel-head"><h2>工单信息</h2><span class="section-note">创建人 · '+e(profile.user.name)+'</span></div><div class="panel-body"><form id="create-form"><div class="field"><label for="title">标题 <span class="required">*</span></label><input id="title" name="title" required minlength="5" maxlength="120" placeholder="简要说明遇到的问题" value="'+e(draft.title)+'" '+fieldsDisabled+'><small>5–120 个字符，简洁准确的标题有助于团队快速定位。</small></div><div class="field"><label for="description">问题描述 <span class="required">*</span></label><textarea id="description" name="description" required maxlength="5000" rows="7" placeholder="发生了什么？影响范围、出现时间，以及你已经尝试的操作。" '+fieldsDisabled+'>'+e(draft.description)+'</textarea><small>最多 5,000 个字符。</small></div><div class="form-grid"><div class="field"><label for="category">类别 <span class="required">*</span></label><select id="category" name="category" '+(draft.locked?'disabled':'')+'>'+profile.categories.map(c=>'<option value="'+e(c)+'" '+(draft.category===c?'selected':'')+'>'+categoryNames[c]+'</option>').join('')+'</select></div><div class="field"><label for="priority">优先级 <span class="required">*</span></label><select id="priority" name="priority" '+(draft.locked?'disabled':'')+'>'+profile.priorities.map(p=>'<option value="'+e(p)+'" '+(draft.priority===p?'selected':'')+'>'+priorityNames[p]+'</option>').join('')+'</select></div></div><div id="create-error" class="notice error" role="alert" '+(draft.locked?'':'hidden')+'>'+e(draft.locked?'上次提交结果待确认。请安全重试本次创建，表单内容和请求标识已保留。':'')+'</div><div class="form-footer"><span class="section-note">受理团队由系统自动确定</span><div class="form-actions"><button type="button" id="new-intent" '+(draft.locked?'disabled':'')+'>新建另一张</button><button class="primary" id="create-submit" type="submit">'+(draft.locked?'安全重试本次创建':'提交工单')+'</button></div></div></form></div></section><aside><div class="help-card"><h3>提交之后会发生什么？</h3><p>我们会根据类别，自动将工单交给合适的支持团队。</p><p>创建成功后，你可以在详情中查看负责团队、处理进展，并补充评论。</p><hr class="help-divider"><h3>帮助团队更快解决</h3><p>提供清楚的现象和影响范围。请勿在描述中填写密码或其他账号凭据。</p></div></aside></div>','创建工单');
  const form=document.querySelector('#create-form');
  function capture(){if(!draft.locked){for(const name of ['title','description','category','priority'])draft[name]=form.elements[name].value;saveDraft(key,draft);}}
  form.oninput=capture;form.onchange=capture;
  document.querySelector('#new-intent').onclick=()=>{if(draft.locked)return;removeDraft(key);createPage();};
  form.onsubmit=async event=>{
    event.preventDefault();if(operationBusy)return;capture();operationBusy=true;
    const button=document.querySelector('#create-submit');button.disabled=true;button.textContent='正在提交…';
    draft.locked=true;saveDraft(key,draft);
    for(const name of ['title','description'])form.elements[name].readOnly=true;
    for(const name of ['category','priority'])form.elements[name].disabled=true;
    document.querySelector('#new-intent').disabled=true;
    const {request_id,title,description,category,priority}=draft;
    const actor=profile.user.id;
    try {
      const result=await api('/tickets',{method:'POST',body:{request_id,title,description,category,priority}});
      removeDraft(key);if(profile?.user.id===actor){flash='工单已创建，系统已自动安排受理团队。';navigate('/tickets/'+result.ticket.id);}
    }catch(error){
      const unknown=['NETWORK_UNKNOWN','CREATE_RESULT_UNKNOWN','REQUEST_TIMEOUT','INTERNAL_ERROR'].includes(error.code);
      draft.locked=unknown;saveDraft(key,draft);
      if(profile?.user.id===actor&&location.pathname==='/tickets/new'){
        createPage();
        const el=document.querySelector('#create-error');el.hidden=false;el.textContent=unknown?'提交结果待确认，可安全重试本次创建。请保持原内容，重试不会重复创建工单。':errorText(error);
        const retry=document.querySelector('#create-submit');retry.disabled=false;retry.textContent=draft.locked?'安全重试本次创建':'提交工单';
      }
    }finally{operationBusy=false;}
  };
}
function inputKey(id,kind) {return 'ticketpulse:input:'+profile.user.id+':'+id+':'+kind;}
function saveCurrentInputs() {
  if(!profile)return;
  const id=detailState?.ticket.id;
  if(id){for(const name of ['comment','resolve','transfer','transfer-team','management-reopen','manual-close']){const field=document.querySelector('[data-draft="'+name+'"]');if(field)saveDraft(inputKey(id,name),field.value);}}
}
function participant(id) {return detailState?.participants?.find(p=>p.id===id)?.name || (id?'用户 '+id:'未分配');}
function teamName(id) {return detailState?.teams?.find(t=>t.id===id)?.name || '团队 '+id;}
async function loadDetail(id,run) {
  const detail=await api('/tickets/'+id);
  const options=await api('/tickets/'+id+'/options');
  if(run!==routeRun)return;
  if(options.version!==detail.ticket.version){throw Object.assign(new Error('工单正在更新，请刷新最新状态。'),{code:'VERSION_CONFLICT'});}
  detailState=detail;drawDetail(options);
}
function historyLine(h) {
  let extra='';const before=h.before,after=h.after;
  if(before&&before.current_team_id!==after.current_team_id)extra=teamName(before.current_team_id)+' → '+teamName(after.current_team_id);
  if(before&&before.assignee_id!==after.assignee_id)extra+=(extra?' · ':'')+participant(before.assignee_id)+' → '+participant(after.assignee_id);
  if(before&&before.status!==after.status)extra+=(extra?' · ':'')+statusNames[before.status]+' → '+statusNames[after.status];
  const note=h.action==='resolved'?after.resolution_note:h.reason;
  return '<li><div class="timeline-title"><strong>'+e(participant(h.operator_id))+'</strong> '+e(historyNames[h.action]||h.action)+'</div>'+(extra?'<div class="section-note">'+e(extra)+'</div>':'')+(note?'<div class="timeline-reason">'+e(note)+'</div>':'')+'<div class="timeline-time">'+e(date(h.occurred_at))+' · v'+h.version+' · '+e(teamName(h.responsible_team_id))+'</div></li>';
}
function drawDetail(options) {
  const d=detailState,t=d.ticket;
  const commentDraft=readDraft(inputKey(t.id,'comment'))||'';
  const row=(label,value)=>'<div class="info-row"><span class="label">'+label+'</span><span class="value">'+e(value)+'</span></div>';
  const comments=d.comments.length?d.comments.map(c=>'<article class="comment">'+avatar(participant(c.author_id),true)+'<div class="comment-main"><div class="comment-top"><span class="comment-name">'+e(participant(c.author_id))+'</span><time class="comment-date">'+e(date(c.created_at))+'</time></div><div class="comment-box text-body">'+e(c.content)+'</div></div></article>').join(''):'<div class="empty"><strong>还没有评论</strong><span>补充信息，让团队协作更顺畅。</span></div>';
  shell('<div class="page-head"><div><div class="detail-number">'+e(t.ticket_no)+' <span>·</span> <span id="ticket-version">v'+t.version+'</span></div><h1>工单详情</h1></div><div class="form-actions"><a href="'+e(workspacePath())+'" data-nav>返回列表</a><button id="detail-refresh" class="small">↻ 刷新最新状态</button></div></div><div id="detail-error" class="notice error" role="alert" hidden></div><div class="detail-layout"><div class="detail-left"><section class="panel"><div class="panel-body"><h2 class="detail-title">'+e(t.title)+'</h2><div class="detail-meta">'+badge(t.status,statusNames)+badge(t.priority,priorityNames)+'<span class="badge">'+e(categoryNames[t.category])+'</span></div><h3>问题描述</h3><div class="text-body" id="ticket-description">'+e(t.description)+'</div>'+(t.resolution_note?'<div class="info-divider"></div><h3>最近解决说明</h3><div class="text-body" id="resolution-note">'+e(t.resolution_note)+'</div>':'')+'</div></section><section class="panel"><div class="panel-head"><h2>沟通评论</h2><span class="section-note">'+d.comments.length+' 条评论</span></div><div class="panel-body"><div id="comment-list">'+comments+'</div><form id="comment-form" class="comment-compose"><div class="field"><label for="comment-content">补充评论</label><textarea id="comment-content" data-draft="comment" required maxlength="5000" placeholder="补充问题信息，或记录最新进展。">'+e(commentDraft)+'</textarea></div><div class="compose-footer"><span>所有有权查看此工单的人可见</span><button class="primary small" type="submit">发送评论</button></div></form></div></section><section class="panel"><div class="panel-head"><h2>操作历史</h2><span class="section-note">'+d.history.length+' 次操作 · 时间为北京时间</span></div><div class="panel-body"><ol class="timeline" id="history-list">'+d.history.slice().reverse().map(historyLine).join('')+'</ol></div></section></div><aside class="detail-right"><section class="panel"><div class="panel-head"><h2>工单信息</h2></div><div class="panel-body">'+row('创建人',participant(t.creator_id))+row('当前团队',d.current_team.name+(d.current_team.active?'':'（已停用）'))+row('当前负责人',d.assignee?d.assignee.name+(options.assignee_valid?'':'（已失效）'):'尚未分配')+row('初始受理团队',teamName(t.initial_team_id))+'<div class="info-divider"></div>'+row('创建时间',date(t.created_at))+row('更新时间',date(t.updated_at))+row('首次解决',date(t.first_resolved_at))+row('关闭时间',date(t.closed_at))+row('重开次数',String(t.reopen_count))+'</div></section><section class="panel"><div class="panel-head"><h2>执行操作</h2></div><div class="panel-body"><div class="action-buttons">'+options.actions.filter(a=>a!=='comments').map(a=>'<button class="small '+(a==='close'||a==='resolve'?'primary':'')+'" data-action="'+e(a)+'">'+e(actionNames[a])+'</button>').join('')+'</div>'+(options.actions.length===1?'<p class="subtitle">当前没有可执行的状态操作。</p>':'')+(!options.current_team_active?'<div class="notice error">负责团队已停用，待确认工单需要主管选择有效团队恢复。</div>':'')+'<div id="action-area"></div></div></section></aside></div>','工单详情');
  document.querySelector('#detail-refresh').onclick=()=>{saveCurrentInputs();renderRoute();};
  document.querySelector('#comment-content').oninput=event=>saveDraft(inputKey(t.id,'comment'),event.target.value);
  document.querySelector('#comment-form').onsubmit=async event=>{event.preventDefault();if(operationBusy)return;const input=document.querySelector('#comment-content');await performAction('comments',{content:input.value},'comment');};
  document.querySelectorAll('[data-action]').forEach(button=>button.onclick=()=>showActionForm(button.dataset.action,options));
}
function showActionForm(action,options) {
  saveCurrentInputs();const t=detailState.ticket;
  if(['claim','start','reopen','close'].includes(action)){performAction(action,{});return;}
  let controls='';
  if(action==='transfer')controls='<div class="field"><label for="target-assignee">新负责人</label><select id="target-assignee" required><option value="">选择当前团队处理人员</option>'+options.assignees.map(u=>'<option value="'+u.id+'">'+e(u.name)+'</option>').join('')+'</select></div>';
  if(action==='transfer-team'||action==='management-reopen')controls='<div class="field"><label for="target-team">'+(action==='management-reopen'?'恢复团队':'目标团队')+'</label><select id="target-team" required>'+(action==='management-reopen'&&options.current_team_active?'<option value="0">保留当前团队</option>':'<option value="">选择有效支持团队</option>')+options.teams.filter(team=>action!=='transfer-team'||team.id!==t.current_team_id).map(team=>'<option value="'+team.id+'">'+e(team.name)+'</option>').join('')+'</select></div>';
  const label=action==='resolve'?'解决说明':'操作原因';
  const saved=readDraft(inputKey(t.id,action))||'';
  document.querySelector('#action-area').innerHTML='<form class="action-form" id="action-form"><h3>'+e(actionNames[action])+'</h3>'+controls+'<div class="field"><label for="action-text">'+label+(action==='transfer'?'（可选）':'')+'</label><textarea id="action-text" data-draft="'+e(action)+'" '+(action==='transfer'?'':'required')+' maxlength="'+(action==='resolve'?5000:1000)+'" placeholder="'+(action==='resolve'?'说明处理结果与验证情况。':'说明本次操作的原因。')+'">'+e(saved)+'</textarea></div><div class="form-actions"><button class="primary small" type="submit">确认'+e(actionNames[action])+'</button><button class="small" id="cancel-action" type="button">取消</button></div></form>';
  document.querySelector('#action-text').oninput=event=>saveDraft(inputKey(t.id,action),event.target.value);
  document.querySelector('#cancel-action').onclick=()=>{saveCurrentInputs();document.querySelector('#action-area').replaceChildren();};
  document.querySelector('#action-form').onsubmit=event=>{
    event.preventDefault();if(operationBusy)return;
    const text=document.querySelector('#action-text').value;
    const fields=action==='resolve'?{resolution_note:text}:{reason:text};
    if(action==='transfer')fields.assignee_id=Number(document.querySelector('#target-assignee').value);
    if(action==='transfer-team'||action==='management-reopen')fields.team_id=Number(document.querySelector('#target-team').value);
    performAction(action,fields,action);
  };
}
async function performAction(action,fields,draftName) {
  if(operationBusy||!detailState)return;saveCurrentInputs();operationBusy=true;
  const id=detailState.ticket.id,version=detailState.ticket.version,actor=profile.user.id,run=routeRun;
  document.querySelectorAll('.detail-layout button').forEach(b=>b.disabled=true);
  try {
    await api('/tickets/'+action,{method:'POST',body:{ticket_id:id,expected_version:version,...fields}});
    if(draftName)removeDraft('ticketpulse:input:'+actor+':'+id+':'+draftName);
    if(profile?.user.id===actor&&run===routeRun&&location.pathname==='/tickets/'+id){
      flash=action==='comments'?'评论已发送。':'操作已保存，工单信息已更新。';
      await loadDetail(id,run);
    }
  }catch(error){
    if(profile&&location.pathname==='/tickets/'+id){
      if(error.status===403){pageError(error,()=>renderRoute());}
      else {const area=document.querySelector('#detail-error');if(area){area.hidden=false;area.textContent=errorText(error);}document.querySelectorAll('.detail-layout button').forEach(b=>b.disabled=false);}
    }
  }finally{operationBusy=false;}
}
async function renderRoute() {
  const run=++routeRun;operationBusy=false;detailState=null;
  if(!profile){if(location.pathname!=='/login')history.replaceState({},'','/login');loginPage();return;}
  if(location.pathname==='/'||location.pathname==='/login'){history.replaceState({},'','/workspace');}
  if(location.pathname==='/tickets/new'){createPage();return;}
  if(location.pathname==='/search'){await searchPage(run);return;}
  if(location.pathname==='/analytics'){await analyticsPage(run);return;}
  const match=location.pathname.match(/^\/tickets\/(\d+)$/);
  if(match){shell('<div class="loading">正在读取最新工单…</div>','工单详情');try{await loadDetail(Number(match[1]),run);}catch(error){if(run===routeRun&&profile)pageError(error,()=>renderRoute());}return;}
  if(location.pathname!=='/workspace')history.replaceState({},'','/workspace');
  await workspace(run);
}
document.addEventListener('click',event=>{
  const link=event.target.closest('a[data-nav]');
  const go=event.target.closest('[data-go]');
  const view=event.target.closest('[data-view]');
  if(link){event.preventDefault();saveCurrentInputs();navigate(link.getAttribute('href'));}
  else if(go){saveCurrentInputs();navigate(go.dataset.go);}
  else if(view){saveCurrentInputs();navigate('/workspace?view='+view.dataset.view);}
});
window.addEventListener('popstate',()=>{saveCurrentInputs();renderRoute();});
window.addEventListener('beforeunload',saveCurrentInputs);
window.addEventListener('focus',async()=>{if(!profile)return;const session=profile.csrf_token;try{const me=await api('/me');if(profile?.csrf_token===session)setSession(me);}catch(error){if(profile?.csrf_token===session)toast(errorText(error));}});
try {setSession(await api('/me',{auth:false}));}catch(error){const lastExpiry=readDraft('ticketpulse:last-expiry');if(error.code==='SESSION_EXPIRED'||(error.status===401&&lastExpiry&&new Date(lastExpiry)<=new Date()))history.replaceState({},'','/login?reason=expired');else if(error.status!==401)flash='暂时无法连接服务，请稍后重试。';}
renderRoute();
