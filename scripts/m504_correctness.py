#!/usr/bin/env python3
"""Independent current MySQL summaries vs HTTP search, without ES totals."""
import argparse
import hashlib
import http.client
import json
from pathlib import Path
import time
from urllib.parse import urlencode

import m504_environment as env
import m504_fixture as fixture
import m504_bench as bench

FIELDS=['id','ticket_no','title','priority','status','assignee_id','assignee_name','current_team_id','current_team_name','version','updated_at']

def expected(database, uid):
    where='t.organization_id=1'
    if uid in fixture.USERS.values():
        where+=' AND (t.creator_id='+str(uid)+' OR EXISTS(SELECT 1 FROM team_members m JOIN teams tm2 ON tm2.organization_id=m.organization_id AND tm2.id=m.team_id WHERE m.organization_id=t.organization_id AND m.team_id=t.current_team_id AND m.user_id='+str(uid)+' AND m.active=1 AND tm2.active=1))'
    query=("SELECT JSON_OBJECT('id',t.id,'ticket_no',t.ticket_no,'title',t.title,'priority',t.priority,'status',t.status,'assignee_id',t.assignee_id,'assignee_name',u.name,'current_team_id',t.current_team_id,'current_team_name',tm.name,'version',t.version,'updated_at',DATE_FORMAT(t.updated_at,'%Y-%m-%dT%H:%i:%s.%fZ')) FROM tickets t LEFT JOIN users u ON u.organization_id=t.organization_id AND u.id=t.assignee_id JOIN teams tm ON tm.organization_id=t.organization_id AND tm.id=t.current_team_id WHERE "+where+' ORDER BY t.id;')
    return [json.loads(row) for row in env.sql("SET time_zone='+00:00';USE "+database+';'+query,120).splitlines()]

def canonical(item):
    v={k:item[k] for k in FIELDS}
    # RFC3339 optional trailing fractional zeros are semantically equal.
    v['updated_at']=v['updated_at'].replace('.000000Z','Z')
    if '.' in v['updated_at']: v['updated_at']=v['updated_at'][:-1].rstrip('0')+'Z'
    return v

def walk(n,profile,query,folder,selective=True):
    value=fixture.fixtures();item=value['datasets'][str(n)]
    database=item['selective_database'] if selective else item['database']
    uid=fixture.USERS[profile] if selective else 5
    token='m504-profile-'+profile.lower() if selective else 'local-supervisor'
    source=expected(database,uid)
    # This controlled frozen fixture contains benchmark in every title and
    # description. Field filters are independently evaluated using source docs.
    raw={d['id']:d for d in fixture.documents(database)}
    def match(d):
        if 'status' in query and d['status'] not in query['status']: return False
        if 'priority' in query and d['priority'] not in query['priority']: return False
        if 'created_from' in query and d['created_at']<query['created_from']: return False
        if 'created_to' in query and d['created_at']>=query['created_to']: return False
        return True
    source=[s for s in source if match(raw[s['id']])]
    sort=query.get('sort','id');order=query.get('order','asc')
    field='priority_rank' if sort=='priority' else sort
    source.sort(key=lambda d:(raw[d['id']][field],d['id']),reverse=order=='desc')
    expected_by_id={d['id']:canonical(d) for d in source}
    expected_ids=[d['id'] for d in source]
    client=http.client.HTTPConnection('127.0.0.1',env.APP_PORT,timeout=12)
    values=dict(query);observed=[];samples=[];budget=None;start=time.perf_counter()
    cursor_store=[]
    try:
        while True:
            response,data=bench.request(client,urlencode(values,doseq=True),token)
            samples.append({**response,'page':len(samples)+1})
            if response['code']=='SEARCH_TOO_BROAD':budget={'page':len(samples),'returned_before':len(observed)};break
            if response['status']!=200:raise RuntimeError('Unexpected search error '+response['code'])
            if data['total'] is not None or data['total_relation']!='unavailable' or data['consistency']!='eventual_projection_current_mysql_authorization':raise RuntimeError('Search total/consistency contract changed')
            for result in data['items']:
                if result['id'] not in expected_by_id or canonical(result)!=expected_by_id[result['id']]:raise RuntimeError('Current source field/version/authorization mismatch')
                observed.append(result['id'])
            if data['has_more']:
                if not data.get('next_cursor') or len(data['items'])!=values['page_size']:raise RuntimeError('Malformed nonterminal page')
                if len(samples) in [1,10,100,500,1000]:cursor_store.append({'page':len(samples)+1,'cursor':data['next_cursor']})
                values['cursor']=data['next_cursor']
            else:
                if data.get('next_cursor'):raise RuntimeError('Terminal cursor present')
                break
            if len(samples)>max(1,n)+1:raise RuntimeError('Pagination did not terminate')
    finally:client.close()
    duplicates=len(observed)-len(set(observed))
    missing=len(set(expected_ids)-set(observed))
    if duplicates:raise RuntimeError('Repeated authorized ID')
    if observed!=expected_ids[:len(observed)]:raise RuntimeError('Pagination order or prefix differs from independent SQL')
    if budget is None and observed!=expected_ids:raise RuntimeError('Incomplete stationary cursor traversal')
    with (folder/'requests.jsonl').open('x') as output:
        for s in samples:output.write(json.dumps(s)+chr(10))
    (folder/'observed-ids.json').write_text(json.dumps(observed))
    (folder/'expected-ids.json').write_text(json.dumps(expected_ids))
    # Cursor opaque payload contains no bearer credentials; keep it private and
    # do not dump it into performance logs or documentation.
    private=env.RUNTIME/'deep-cursors';private.mkdir(exist_ok=True)
    path=private/(folder.name+'.json');path.write_text(json.dumps({'query':query,'profile':profile,'tickets':n,'selective':selective,'cursors':cursor_store}));path.chmod(0o600)
    result={'tickets':n,'profile':profile,'selective':selective,'query':query,'authorized_expected':len(source),
            'observed':len(observed),'pages':len(samples),'duplicate_ids':duplicates,'missing_ids':missing,
            'budget_exhausted':budget,'field_version_checks':'every returned summary equals independent current SQL',
            'order_matches':True,'complete':budget is None and missing==0,'seconds':time.perf_counter()-start,
            'page_latency_ms':{name:bench.percentile([s['ms'] for s in samples],q) for name,q in [('p50',.5),('p95',.95),('p99',.99)]},
            'deep_cursor_checkpoints':[c['page'] for c in cursor_store]}
    (folder/'result.json').write_text(json.dumps(result,indent=2))
    worker=fixture.fixtures()['workers']['app'];bench.join(folder,samples,env.ROOT/worker['diagnostics'])
    print(json.dumps({'case':folder.name,'pages':len(samples),'observed':len(observed),'expected':len(source),'budget':budget,'complete':result['complete']}),flush=True)
    return result

def suite(n,binary,label):
    fixture.stop_app();fixture.start_app(n,binary,True,label=label+'-app')
    folder=env.OUT/'correctness'/label;folder.mkdir(parents=True,exist_ok=False)
    results=[]
    for p in ['A','B','C','D']:
        q={'q':'benchmark','sort':'id','order':'asc','page_size':100 if p in ['A','B'] else 20 if p=='C' else 1}
        case=folder/(p+'-full-id');case.mkdir();results.append(walk(n,p,q,case))
    # All four supported sorts + filters/time window are checked continuously.
    for name,q in [('updated',{'sort':'updated_at','order':'desc'}),('created',{'sort':'created_at','order':'asc'}),('priority',{'sort':'priority','order':'desc','status':['pending','in_progress'],'priority':['high','medium']}),('time',{'sort':'created_at','order':'asc','created_from':'2026-11-03T00:00:00Z','created_to':'2026-11-07T00:00:00Z'})]:
        case=folder/('C-'+name);case.mkdir();results.append(walk(n,'C',{'q':'benchmark','page_size':20,**q},case))
    # Keep the normal D page20 failure explicit. No hidden budget relaxation.
    case=folder/'D-page20';case.mkdir();results.append(walk(n,'D',{'q':'benchmark','sort':'id','order':'asc','page_size':20},case))
    (folder/'summary.json').write_text(json.dumps(results,indent=2))
    return results

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tickets',type=int,choices=[10000,100000],default=10000)
    parser.add_argument('--binary',default='adaptive')
    parser.add_argument('--label',required=True)
    args=parser.parse_args();suite(args.tickets,args.binary,args.label)
