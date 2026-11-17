#!/usr/bin/env python3
"""Real current membership/transfer and search load with bounded live writes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import http.client
import json
import threading
import time
from urllib.parse import urlencode

import m504_environment as env
import m504_fixture as fixture
import m504_runtime as runtime
import m504_bench as bench

def api(method,path,token,body=None):
    client=http.client.HTTPConnection('127.0.0.1',env.APP_PORT,timeout=12)
    try:
        headers={'Authorization':'Bearer '+token,'Accept':'application/json'}
        data=json.dumps(body).encode() if body is not None else None
        if data:headers['Content-Type']='application/json'
        client.request(method,'/api/v1'+path,data,headers)
        response=client.getresponse();return response.status,json.loads(response.read())
    finally:client.close()

def authorized_source(database,uid):
    role=env.sql('USE '+database+';SELECT role FROM users WHERE id='+str(uid)+';').strip()
    where='t.organization_id=1'
    if role not in ['supervisor','admin']:
        where+=' AND (t.creator_id='+str(uid)
        if role=='agent':
            where+=' OR EXISTS(SELECT 1 FROM team_members m JOIN teams tm ON tm.organization_id=m.organization_id AND tm.id=m.team_id WHERE m.organization_id=t.organization_id AND m.team_id=t.current_team_id AND m.user_id='+str(uid)+' AND m.active=1 AND tm.active=1)'
        where+=')'
    query="SELECT JSON_OBJECT('id',t.id,'version',t.version,'current_team_id',t.current_team_id,'status',t.status,'priority',t.priority,'title',t.title) FROM tickets t WHERE "+where+';'
    return {d['id']:d for d in (json.loads(row) for row in env.sql('USE '+database+';'+query).splitlines())}

def load(suffix,name,c,token,uid,query,seconds=6,background=False):
    copy=fixture.fixtures()['write_copies'][suffix];source=authorized_source(copy['database'],uid)
    folder=env.OUT/'live'/name;folder.mkdir(parents=True,exist_ok=False)
    worker=fixture.fixtures()['workers']['app']
    monitor=bench.Monitor(folder);monitor.start();barrier=threading.Barrier(c+1);start=[0.0]
    writer_stop=threading.Event();writes=[]
    def write_load():
        client=http.client.HTTPConnection('127.0.0.1',env.APP_PORT,timeout=12)
        try:
            for i in range(1000):
                if writer_stop.is_set():break
                began=time.perf_counter()
                body={'request_id':name+'-'+str(i),'title':'benchmark M504 background '+str(i),
                      'description':'Synthetic isolated background load','category':'network','priority':'high'}
                client.request('POST','/api/v1/tickets',json.dumps(body).encode(),headers={'Authorization':'Bearer local-alice','Content-Type':'application/json'})
                response=client.getresponse();data=json.loads(response.read())
                record={'status':response.status,'ms':(time.perf_counter()-began)*1000,'code':data.get('error',{}).get('code','')}
                if response.status==201:
                    t=data['ticket'];source[t['id']]=t;record.update(id=t['id'],version=t['version'])
                writes.append(record)
                writer_stop.wait(max(0,.2-(time.perf_counter()-began))) # bounded target 5 HTTP creates/s
        finally:client.close()
    writer=threading.Thread(target=write_load,daemon=True)
    if background:writer.start()
    def run(number):
        client=http.client.HTTPConnection('127.0.0.1',env.APP_PORT,timeout=12);rows=[];barrier.wait()
        try:
            while time.perf_counter()<start[0]+seconds and not monitor.failure:
                row,data=bench.request(client,urlencode(query,doseq=True),token)
                if row['status']==200:
                    if data['total'] is not None:raise RuntimeError('Raw total exposed')
                    for item in data['items']:
                        if item['id'] not in source and background:
                            # A search can win the race to observe an acknowledged
                            # create. Verify that ID against current source SQL.
                            current=authorized_source(copy['database'],uid)
                            source.update(current)
                        if item['id'] not in source:raise RuntimeError('Unauthorized item')
                        expect=source[item['id']]
                        for key in ['id','version','current_team_id','status','priority','title']:
                            if item[key]!=expect[key]:raise RuntimeError('Current field/version mismatch')
                row['worker']=number;rows.append(row)
        finally:client.close()
        return rows
    try:
        with ThreadPoolExecutor(max_workers=c) as pool:
            futures=[pool.submit(run,i) for i in range(c)];start[0]=time.perf_counter();barrier.wait();rows=[r for future in futures for r in future.result()]
        elapsed=time.perf_counter()-start[0]
    finally:
        monitor.close();writer_stop.set()
        if background:writer.join(timeout=15)
    with (folder/'requests.jsonl').open('x') as output:
        for r in rows:output.write(json.dumps(r)+chr(10))
    a=bench.join(folder,rows,env.ROOT/worker['diagnostics'])
    success=[r for r in rows if r['status']==200]
    result={'fixture':suffix,'tickets':copy['tickets'],'concurrency':c,'query':query,'source_authorized':len(source),'requests':len(rows),'success':len(success),
        'success_qps':len(success)/elapsed,'latency_ms':{k:bench.percentile([r['ms'] for r in rows],p) for k,p in [('p50',.5),('p95',.95),('p99',.99)]},
        'codes':dict(Counter(r['code'] for r in rows if r['code'])),'unauthorized':0,'all_returned_versions_and_fields_current':True,'amplification':a,'safety_stop':monitor.failure,
        'background_write_target_tps':5 if background else 0,'background_completed_writes':sum(r['status']==201 for r in writes),
        'background_actual_tps':sum(r['status']==201 for r in writes)/elapsed,'background_write_errors':dict(Counter(r['code'] for r in writes if r['code'])),
        'load_model':'closed-loop HTTP search plus separate capped append-only create writer' if background else 'closed-loop HTTP search on stationary source'}
    if background:
        (folder/'writes.json').write_text(json.dumps(writes,indent=2))
    (folder/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps({'live':name,'qps':result['success_qps'],'codes':result['codes'],'authorized_check':True}),flush=True)
    if monitor.failure:raise RuntimeError('Live safety budget reached')
    return result

def permissions():
    suffix='permissions'
    copy=fixture.create_write_copy(10000,suffix);runtime.start(suffix);fixture.start_app(10000,'final-verified',False,label=suffix+'-app',write_copy=suffix)
    try:
        db=copy['database'];index=copy['index']
        # Dedicated account has no creator privilege; old IT membership is the
        # sole authority. Actual HTTP transfer records the next business version.
        env.sql('USE '+db+";INSERT INTO users(id,organization_id,name,role,active,auth_token_hash,created_at,updated_at) VALUES(505,1,'M504 live IT reader','agent',1,UNHEX(SHA2('m504-live-reader',256)),UTC_TIMESTAMP(6),UTC_TIMESTAMP(6));INSERT INTO team_members(organization_id,team_id,user_id,active,created_at) VALUES(1,10,505,1,UTC_TIMESTAMP(6));")
        id=int(env.sql('USE '+db+";SELECT id FROM tickets WHERE current_team_id=10 AND status IN ('pending','in_progress') ORDER BY id LIMIT 1;").strip())
        status,detail=api('GET','/tickets/'+str(id),'local-supervisor');ticket=detail['ticket']
        before=fixture.es('GET','/'+index+'/_doc/'+str(id))['_source']
        # Stop only the current experiment's ES consumer. CDC keeps writing to Kafka.
        workers=runtime.state();esworker=workers.pop('es')
        if not runtime.owned(esworker):raise RuntimeError('ES worker ownership mismatch')
        import os,signal
        os.kill(esworker['pid'],signal.SIGTERM)
        for _ in range(100):
            if not runtime.owned(esworker):break
            time.sleep(.1)
        runtime.STATE.write_text(json.dumps(workers))
        code,current=api('POST','/tickets/transfer-team','local-supervisor',{'ticket_id':id,'expected_version':ticket['version'],'team_id':20,'reason':'M504 real stale ES permission test'})
        if code!=200:raise RuntimeError('Real transfer failed')
        query={'ticket_no':ticket['ticket_no'],'page_size':20}
        for c in [10,50,200]:load(suffix,'transfer-stale-c'+str(c),c,'m504-live-reader',505,query,5)
        status,_=api('GET','/tickets/'+str(id),'m504-live-reader')
        if status!=403:raise RuntimeError('Old team detail leaked')
        stale=fixture.es('GET','/'+index+'/_doc/'+str(id))['_source']
        if stale['version']!=before['version'] or stale['current_team_id']!=10:raise RuntimeError('Expected actual consumer lag missing')
        runtime.stop()
        # Restart only owned worker binaries at the persisted checkpoint/group.
        folder=env.OUT/'environment/workers/permissions-resume';folder.mkdir(exist_ok=False)
        e=fixture.copy_connection(suffix);workers={}
        import subprocess
        for name,mode in [('capture','capture'),('es','consume')]:
            with (folder/(name+'.log')).open('x') as output:
                child=subprocess.Popen([str(env.RUNTIME/'ticketpulse-sync'),'-mode='+mode,'-server-id=350409'],env=e,cwd=env.ROOT,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
            workers[name]={'pid':child.pid,'mode':'-mode='+mode,'copy':suffix};runtime.STATE.write_text(json.dumps(workers))
        for _ in range(100):
            projected=fixture.es('GET','/'+index+'/_doc/'+str(id))['_source']
            if projected['version']==current['ticket']['version']:break
            time.sleep(.1)
        else:raise RuntimeError('Transfer projection recovery deadline')
        fixture.es('POST','/'+index+'/_refresh')
        for c in [10,50,200]:load(suffix,'transfer-current-c'+str(c),c,'local-device-agent',4,query,5)
        # Freeze source again and revoke/restore membership between requests.
        runtime.stop()
        page_query={'q':'benchmark','sort':'id','order':'asc','page_size':20}
        _,first=api('GET','/search/tickets?'+urlencode(page_query),'m504-live-reader')
        if not first['has_more']:raise RuntimeError('Membership cursor fixture insufficient')
        continued={**page_query,'cursor':first['next_cursor']}
        env.sql('USE '+db+';UPDATE team_members SET active=0 WHERE user_id=505 AND team_id=10;')
        for c in [10,50,200]:load(suffix,'membership-revoked-c'+str(c),c,'m504-live-reader',505,continued,5)
        env.sql('USE '+db+';UPDATE team_members SET active=1 WHERE user_id=505 AND team_id=10;')
        for c in [10,50,200]:load(suffix,'membership-restored-c'+str(c),c,'m504-live-reader',505,continued,5)
        path=env.OUT/'live/permission-summary.json'
        path.write_text(json.dumps({'ticket_id':id,'mysql_version':current['ticket']['version'],'stale_es_version':stale['version'],
            'old_team_denied':True,'current_team_summary_correct':True,'membership_cursor_revoked_and_restored':True,
            'concurrency':[10,50,200],'mysql_final_authority':True},indent=2))
    finally:fixture.stop_app();runtime.stop()

def background():
    for n in [10000,100000]:
        for variant in ['transport','final-verified']:
            suffix=('bg'+str(n)+'_'+('t' if variant=='transport' else 'f'))
            copy=fixture.create_write_copy(n,suffix,True)
            runtime.start(suffix);fixture.start_app(n,variant,False,label=suffix+'-app',write_copy=suffix)
            try:
                for c in [10,50,200]:
                    load(suffix,'background-'+str(n)+'-'+variant+'-c'+str(c),c,'local-supervisor',5,
                         {'q':'benchmark','sort':'updated_at','order':'desc','page_size':20},10,True)
                runtime.wait_caught_up(suffix)
                count=int(env.sql('USE '+copy['database']+';SELECT count(*) FROM tickets;').strip())
                requests=int(env.sql('USE '+copy['database']+';SELECT count(*) FROM ticket_create_requests;').strip())
                projected=fixture.es('GET','/'+copy['index']+'/_count')['count']
                if count!=n+requests or projected!=count:raise RuntimeError('Background write source/projection count mismatch')
                (env.OUT/'live'/('background-'+str(n)+'-'+variant+'-durability.json')).write_text(json.dumps({'base':n,'committed_creates':requests,'source_tickets':count,'projected':projected,'counts_match':True},indent=2))
            finally:fixture.stop_app();runtime.stop()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',nargs='?',choices=['permissions','background'],default='permissions');args=parser.parse_args()
    if args.action=='permissions':permissions()
    else:background()
