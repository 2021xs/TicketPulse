#!/usr/bin/env python3
"""True HTTP -> MySQL -> Binlog -> Kafka -> CK, independent PRD Gold."""
from datetime import datetime,timezone
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import analytics_fixture as fixture
import local_analytics as runtime

OUTPUT=fixture.OUTPUT

def call(path,token='local-supervisor',method='GET',body=None,expected=200):
    value=fixture.verified();headers={'Content-Type':'application/json'}
    if token:headers['Authorization']='Bearer '+token
    request=urllib.request.Request(value['url']+'/api/v1'+path,headers=headers,method=method,data=None if body is None else json.dumps(body).encode())
    try:
        with urllib.request.urlopen(request,timeout=12) as response:code=response.status;data=response.read()
    except urllib.error.HTTPError as error:code=error.code;data=error.read()
    if code!=expected:raise AssertionError(path+' HTTP='+str(code)+' want='+str(expected)+' '+data.decode())
    return json.loads(data)

def wait(label,fn,seconds=25):
    end=time.monotonic()+seconds;last=None
    while time.monotonic()<end:
        try:
            last=fn()
            if last:return last
        except (OSError,AssertionError):pass
        time.sleep(.15)
    raise AssertionError(label+' timed out: '+str(last))

def stats(query='start=2026-11-01&end=2026-11-04'):
    return call('/analytics?'+query)

def main():
    fixture.start();started=datetime.now(timezone.utc)
    gold=json.loads((fixture.ROOT/'tests/analytics/prd_gold.json').read_text())
    try:
        wait('historical + current caught up',lambda:stats()['sync']['caught_up'])
        result=stats()
        assert result['daily']==gold['daily'],(result['daily'],gold['daily'])
        assert result['overview']==gold['overview'],(result['overview'],gold['overview'])
        for team in result['teams']:
            if str(team['team_id']) in gold['teams']:
                for k,v in gold['teams'][str(team['team_id'])].items():assert team[k]==v,(team,k,v)
        filters={}
        for label,query,expected in [
            ('nov1-a','start=2026-11-01&end=2026-11-02&team_id=10',{'created':3,'first_resolved':1,'current_backlog':1}),
            ('nov2-a','start=2026-11-02&end=2026-11-03&team_id=10',{'created':2,'first_resolved':1,'current_backlog':1}),
            ('nov2-b','start=2026-11-02&end=2026-11-03&team_id=20',{'created':1,'first_resolved':1,'current_backlog':4}),
            ('nov3-a','start=2026-11-03&end=2026-11-04&team_id=10',{'created':2,'first_resolved':2,'current_backlog':1}),
            ('nov3-b','start=2026-11-03&end=2026-11-04&team_id=20',{'created':1,'first_resolved':1,'current_backlog':4}),
            ('future','start=2026-11-04&end=2026-11-05',{'created':0,'first_resolved':0,'current_backlog':5})]:
            r=stats(query)
            for k,v in expected.items():assert r['overview'][k]==v,(label,r)
            filters[label]=r['overview']
        backlog=wait('B current backlog in Search',lambda:call('/search/tickets?current_team_id=20&status=pending&status=in_progress&sort=id&order=asc')['items'])
        ids=[t['id'] for t in backlog];assert ids==[3,4,6,8],ids
        # Schema from MySQL is the authority even if the browser has a forged role.
        for token in ['local-alice','local-it-agent']:call('/analytics',token=token,expected=403)
        call('/analytics',token='',expected=401);call('/analytics?role=admin',expected=400)
        empty=call('/analytics?start=2026-11-01&end=2026-11-04',token='local-empty-admin')
        assert empty['data_state']=='empty' and empty['overview']['created']==0 and empty['overview']['average_first_resolution_seconds'] is None
        fixture.sql("UPDATE users SET active=FALSE WHERE id=5;")
        try:call('/analytics',expected=401)
        finally:fixture.sql("UPDATE users SET active=TRUE WHERE id=5;")
        # Independent durable CK ACK is paused while the real ES consumer proceeds.
        fixture.worker('ck','stop')
        original_version=call('/tickets/3',token='local-alice')['ticket']['version']
        payload={'ticket_id':3,'expected_version':original_version,'content':'M3 real HTTP / independent consumer / CK outage'}
        updated=call('/tickets/comments',token='local-alice',method='POST',body=payload)['ticket']
        assert updated['version']==original_version+1
        wait('ES progresses while CK paused',lambda:fixture.sql("SELECT COUNT(*) FROM sync_kafka_es_deliveries WHERE ticket_id=3 AND ticket_version="+str(original_version+1)+" AND status IN ('applied','duplicate_or_old');").strip()=='1')
        stale=stats();assert not stale['sync']['caught_up']
        fixture.worker('ck','start');wait('CK restarts at independent progress',lambda:stats()['sync']['caught_up'])
        assert stats()['overview']==gold['overview']
        # Native CK process really stops; business HTTP and real ES remain operational.
        outage_started=time.monotonic();runtime.stop()
        try:
            call('/analytics',expected=503)
            updated=call('/tickets/comments',token='local-alice',method='POST',body={'ticket_id':3,'expected_version':original_version+1,'content':'CK offline; MySQL remains writable'})['ticket']
            assert updated['version']==original_version+2
            wait('persistent retry while native CK offline',lambda:int(fixture.sql("SELECT COUNT(*) FROM sync_kafka_ck_deliveries WHERE status='retrying' AND ticket_id=3;").strip())>0)
        finally:runtime.start()
        wait('native CK recovery caught up',lambda:stats()['sync']['caught_up'],35)
        recovered=stats();assert recovered['overview']==gold['overview'];assert recovered['daily']==gold['daily']
        current=fixture.ck("SELECT ticket_id,tupleElement(argMax(tuple(version,current_team_id,status,first_resolved_at),tuple(version,complete)),1) AS latest_version,tupleElement(argMax(tuple(version,current_team_id,status,first_resolved_at),tuple(version,complete)),2) AS team,tupleElement(argMax(tuple(version,current_team_id,status,first_resolved_at),tuple(version,complete)),3) AS current_status FROM ticket_states GROUP BY ticket_id ORDER BY ticket_id FORMAT JSONEachRow")
        facts=fixture.ck("SELECT ticket_id,version,any(action) AS event_action,any(event_team_id) AS team,toString(any(occurred_at)) AS event_time FROM ticket_events WHERE action IN ('created','resolved') GROUP BY ticket_id,version ORDER BY ticket_id,version FORMAT JSONEachRow")
        versions=fixture.sql("SELECT id,ticket_no,version,status,current_team_id,reopen_count,first_resolved_at FROM tickets ORDER BY id;")
        output={'passed':True,'evidence':'RUNTIME','started_at':started.isoformat(),'finished_at':datetime.now(timezone.utc).isoformat(),'mysql_version':runtime.local.mysql('SELECT VERSION();').strip(),'clickhouse_version':fixture.ck('SELECT version()').strip(),'gold_file':'tests/analytics/prd_gold.json','actual':result,'filters':filters,'B_backlog_ids':ids,'empty_organization':empty,'independent_pause_status':stale['sync'],'native_ck_stop_and_restore':True,'outage_recovery_seconds':round(time.monotonic()-outage_started,3),'recovered':recovered,'latest_states':[json.loads(line) for line in current.splitlines()],'historical_metric_facts':[json.loads(line) for line in facts.splitlines()],'mysql_final_rows':versions,'m1_resolution_fields_preserved':True}
        (OUTPUT/'smoke-analytics-result.json').write_text(json.dumps(output,ensure_ascii=False,indent=2)+chr(10))
        print(json.dumps({'passed':True,'daily':recovered['daily'],'current_backlog':recovered['overview']['current_backlog'],'B_backlog_ids':ids,'native_ck_outage_recovery_seconds':output['outage_recovery_seconds']},ensure_ascii=False))
    finally:
        runtime.start()
        # Retain this owned fixture for Chrome; analytics_browser.py performs final cleanup.
        value=fixture.verified()
        if not value['pids'].get('ck'):fixture.worker('ck','start')
    return 0

if __name__=='__main__':raise SystemExit(main())
