#!/usr/bin/env python3
"""Real HTTP/Binlog/MQ/ES acceptance with owned native process failures."""
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import local_mysql as local
import local_kafka as runtime
import search_fixture as fixture

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'artifacts/m2-02'
records = []


def api(token, method, path, body=None, status=200):
    value = fixture.verified()
    payload = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(value['url']+'/api/v1'+path, data=payload, method=method,
                                    headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    try:
        response = urllib.request.urlopen(request,timeout=12)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        data = json.load(response)
        if response.code != status:
            raise AssertionError(method+' '+path+' returned '+str(response.code)+' '+json.dumps(data))
    return data


def action(token, kind, ticket, extra=None):
    body={'ticket_id':ticket['id'],'expected_version':ticket['version']}
    body.update(extra or {})
    result=api(token,'POST','/tickets/'+kind,body)['ticket']
    if result['version'] != ticket['version']+1:
        raise AssertionError('Version did not increment exactly once')
    return result


def create(request_id, title):
    return api('local-alice','POST','/tickets',{'request_id':request_id,'title':title,
                'description':'Real Binlog → persistent Kafka → Elasticsearch acceptance',
                'category':'network','priority':'high'},201)['ticket']


def wait(label, fn, deadline=30):
    end=time.monotonic()+deadline
    while time.monotonic()<end:
        try:
            if fn():
                return
        except (OSError,urllib.error.HTTPError):
            pass
        time.sleep(.08)
    raise AssertionError(label+' timed out')


def searchable(ticket, token='local-alice'):
    result=api(token,'GET','/search/tickets?ticket_no='+ticket['ticket_no'])
    return len(result['items'])==1 and result['items'][0]['version']==ticket['version']


def record(label, **evidence):
    records.append({'scenario':label,'passed':True,**evidence})
    print('PASS '+label,flush=True)


def status():
    value=fixture.verified()
    out=subprocess.check_output([str(ROOT/'bin/ticketpulse-sync'),'-mode=status'],cwd=ROOT,
                                env=runtime.environment(value['database']),text=True)
    return json.loads(out)


def main():
    fixture.start()
    began=datetime.now(timezone.utc)
    completed=False
    suffix=str(time.time_ns())
    try:
        ticket=create('smoke-'+suffix,'Smoke native chain '+suffix)
        wait('created ticket searchable',lambda:searchable(ticket))
        ticket=action('local-it-agent','claim',ticket)
        ticket=action('local-it-agent','start',ticket)
        ticket=action('local-it-agent','comments',ticket,{'content':'IT 已确认设备故障，需要设备团队接手。'})
        wait('comment version searchable',lambda:searchable(ticket))
        # Leave stale team A in real ES while the MySQL transaction commits team B.
        fixture.worker('stop')
        ticket=action('local-supervisor','transfer-team',ticket,{'team_id':20,'reason':'转设备团队处理网络适配器'})
        old=api('local-it-agent','GET','/search/tickets?ticket_no='+ticket['ticket_no'])
        assert old['items']==[] and old['total'] is None and not old['has_more']
        api('local-it-agent','GET','/tickets/'+str(ticket['id']),status=403)
        record('stale ES immediately obeys current MySQL permission',version=ticket['version'],no_results_no_total_no_cursor=True)
        fixture.worker('start')
        wait('new team searchable',lambda:searchable(ticket,'local-device-agent'))
        ticket=action('local-device-agent','claim',ticket)
        ticket=action('local-device-agent','start',ticket)
        ticket=action('local-device-agent','comments',ticket,{'content':'已更换网络适配器，进行验证。'})
        ticket=action('local-device-agent','resolve',ticket,{'resolution_note':'首次处理后恢复，等待确认。'})
        first=ticket['first_resolved_at']
        ticket=action('local-alice','reopen',ticket)
        ticket=action('local-device-agent','resolve',ticket,{'resolution_note':'补充驱动修复，复测通过。'})
        ticket=action('local-alice','close',ticket)
        wait('closed ticket searchable',lambda:searchable(ticket))
        detail=api('local-alice','GET','/tickets/'+str(ticket['id']))
        assert detail['ticket']['first_resolved_at']==first and detail['ticket']['reopen_count']==1
        assert len(detail['history'])==ticket['version'] and len(detail['comments'])==2
        assert [h['version'] for h in detail['history']]==list(range(1,ticket['version']+1))
        (OUTPUT/'collaboration-demo.json').write_text(json.dumps(detail,ensure_ascii=False,indent=2)+chr(10))
        record('cross-team HTTP lifecycle and comments',ticket_id=ticket['id'],final_version=ticket['version'],history_count=len(detail['history']),comment_count=2)

        runtime.legacy.stop('es')
        outage_began=time.monotonic()
        pending=create('es-outage-'+suffix,'ES outage created '+suffix)
        api('local-alice','GET','/search/tickets?q=outage',status=503)
        wait('ES retry persisted',lambda:fixture.sql("SELECT COUNT(*) FROM sync_kafka_es_deliveries WHERE ticket_id="+str(pending['id'])+" AND status='retrying';").strip()=='1')
        progress=status()
        assert progress['es_ack_pending']>0 or progress['es_pending']>0
        runtime.start()
        wait('native ES recovered projection',lambda:searchable(pending),45)
        record('native ES stopped: MySQL succeeds, search fails, retry recovers',outage_seconds=round(time.monotonic()-outage_began,3),created_ticket=pending['id'],unacknowledged_during_outage=True)

        # Native MQ outage: committed MySQL change must be resumed from saved Binlog position.
        before=status()
        runtime.stop()
        time.sleep(.3)
        pending=create('mq-outage-'+suffix,'MQ outage created '+suffix)
        transaction_position=local.mysql('SHOW BINARY LOG STATUS;').splitlines()[0].split()[1]
        time.sleep(3.5)
        position=int(fixture.sql("SELECT binlog_pos FROM sync_kafka_cdc_checkpoint WHERE source_name='tickets-v1';").strip())
        assert position<int(transaction_position)
        runtime.start()
        wait('native MQ restart resumed capture',lambda:searchable(pending),45)
        after=status()
        assert sum(p['log_end_offset'] for p in after['partitions'])>=sum(p['log_end_offset'] for p in before['partitions'])
        record('native Kafka restart preserves publications and resumes CDC',checkpoint_during_outage=position,unpublished_transaction_end=int(transaction_position),recovered_ticket=pending['id'])

        fixture.worker('stop')
        pending=create('worker-outage-'+suffix,'Worker restart created '+suffix)
        fixture.worker('start')
        wait('worker restart caught missing change',lambda:searchable(pending))
        record('whole CDC/ES worker process restart from durable progress',recovered_ticket=pending['id'])

        # Small sequential baseline, including ES's normal refresh interval; no synthetic load claim.
        latencies=[]
        for i in range(8):
            start=time.monotonic()
            sample=create('latency-'+suffix+'-'+str(i),'Latency sample '+suffix+' '+str(i))
            wait('latency sample visible',lambda:searchable(sample))
            latencies.append((time.monotonic()-start)*1000)
        samples=sorted(latencies)
        query_times=[]
        for _ in range(20):
            start=time.monotonic();api('local-supervisor','GET','/search/tickets?q=Latency&sort=updated_at&order=desc');query_times.append((time.monotonic()-start)*1000)
        metrics={'sequential_created_to_search_ms':latencies,'min_ms':min(samples),'median_ms':statistics.median(samples),'max_ms':max(samples),
                 'query_ms':{'samples':20,'median':statistics.median(query_times),'max':max(query_times)},
                 'scope':'8 sequential creates, 20 sequential queries, small local fixture, ES refresh_interval=1s; not a throughput benchmark'}
        record('small real synchronization/search timing baseline',**metrics)
        (OUTPUT/'timing.json').write_text(json.dumps(metrics,indent=2)+chr(10))
        completed=True
    finally:
        runtime.start()
        # Keep fixture for real browser verification; separate acceptance command cleans it afterward.
        try:
            fixture.worker('start')
        finally:
            report={'started_at':began.isoformat(),'finished_at':datetime.now(timezone.utc).isoformat(),'passed':completed,
                    'real_mysql':'8.4.5','real_kafka':'4.4.0','real_es':'8.17.5','scenarios':records}
            (OUTPUT/'smoke-search-result.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+chr(10))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
