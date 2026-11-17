#!/usr/bin/env python3
"""Bounded M5-04 workload and independently sampled search/SQL/ES evidence."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import math
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import threading
import time
from urllib.parse import urlencode
import urllib.request

import bench_http as historical
import m504_environment as env
import m504_fixture as fixture

OUT = env.OUT

def percentile(values, q):
    return historical.percentile(values, q)

def debug():
    with urllib.request.urlopen(fixture.APP_URL + '/debug/bench', timeout=3) as response:
        value = json.load(response)
    if value['diagnostic_write_failed']: raise RuntimeError('Search diagnostic evidence write failed')
    return value

class Monitor:
    def __init__(self, folder):
        self.folder, self.stop = folder, threading.Event()
        self.failure, self.samples = None, []
        self.monitor_errors = 0
        self.disk_sample_at = 0.0
        self.test_disk_bytes = 0
        self.thread = threading.Thread(target=self.run, daemon=True)

    def snapshot(self):
        worker = fixture.fixtures()['workers']['app']
        es = env.es_state()
        pids = [worker['pid'], es['server_pid'], env.mysql_runtime().state()['pid']]
        ps = subprocess.check_output(['ps', '-p', ','.join(map(str, pids)), '-o', 'pid=,rss=,%cpu=,time='], text=True)
        rss = sum(int(row.split()[1])*1024 for row in ps.splitlines())
        # Only node counters, never source documents, request query or auth headers.
        nodes = env.json_get('/_nodes/stats/indices/search,segments,refresh,merge,store,query_cache,request_cache,fielddata?filter_path=nodes.*.indices')
        runtime = env.json_get('/_nodes/stats/jvm,thread_pool,process,http,breaker?filter_path=nodes.*.jvm,nodes.*.thread_pool.search*,nodes.*.process,nodes.*.http.current_open,nodes.*.http.total_opened,nodes.*.breakers')
        node = next(iter(runtime['nodes'].values()))
        sql = env.sql("SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_running','Threads_connected','Queries','Slow_queries','Innodb_buffer_pool_reads','Innodb_buffer_pool_read_requests','Innodb_rows_read','Innodb_row_lock_waits'); SELECT count(*) FROM information_schema.innodb_trx;", 3)
        free = shutil.disk_usage(env.ROOT).free
        heap = node['jvm']['mem']['heap_used_percent']
        store = sum(n['indices']['store']['size_in_bytes'] for n in nodes['nodes'].values())
        if time.perf_counter()-self.disk_sample_at>5:
            usage=subprocess.check_output(['du','-sk',str(env.RUNTIME)],text=True)
            self.test_disk_bytes=int(usage.split()[0])*1024
            self.disk_sample_at=time.perf_counter()
        if free < 20*1024**3 or rss > 8*1024**3 or self.test_disk_bytes > 8*1024**3 or heap >= 90:
            self.failure = {'reason': 'safety_budget', 'free_disk': free, 'owned_rss': rss, 'es_heap_percent': heap, 'store_bytes': store, 'runtime_disk_bytes':self.test_disk_bytes}
            self.stop.set()
        return {'utc': env.stamp(), 'monotonic': time.perf_counter(), 'processes': ps, 'owned_rss_bytes': rss,
                'free_disk_bytes': free, 'owned_runtime_disk_bytes':self.test_disk_bytes, 'pool': debug(), 'es_indices': nodes, 'es_runtime': runtime, 'mysql_status': sql}

    def run(self):
        try:
            with (self.folder / 'resources.jsonl').open('x') as output:
                while not self.stop.is_set():
                    started = time.perf_counter()
                    try:
                        row = self.snapshot()
                        self.samples.append(row)
                        self.monitor_errors = 0
                    except (OSError, TimeoutError) as error:
                        # A monitoring socket may suffer the same ephemeral-port
                        # exhaustion as the app. Preserve it without claiming an
                        # ES sample; independently check local RSS/disk budgets.
                        self.monitor_errors += 1
                        row = {'utc':env.stamp(),'monitor_error':type(error).__name__,'consecutive':self.monitor_errors}
                        free=shutil.disk_usage(env.ROOT).free
                        if free < 20*1024**3 or self.monitor_errors >= 10:
                            self.failure={'reason':'monitor_unavailable','consecutive':self.monitor_errors}
                            self.stop.set()
                    output.write(json.dumps(row) + chr(10)); output.flush()
                    self.stop.wait(max(.1, 1-(time.perf_counter()-started)))
        except BaseException as error:
            self.failure = {'reason': 'monitor_failed', 'category': type(error).__name__}
            self.stop.set()

    def start(self): self.thread.start()
    def close(self): self.stop.set(); self.thread.join(timeout=10)

def workload():
    # Stable, frozen request distribution. Page20 remains in D and its budget
    # failures are visible; D page10 is a separate named workload.
    return [
        ('keyword', {'q': 'benchmark', 'sort': 'updated_at', 'order': 'desc', 'page_size': 20}),
        ('filters', {'q': 'benchmark', 'status': ['pending','in_progress'], 'priority': ['high','medium'], 'current_team_id':300, 'sort':'priority','order':'desc','page_size':20}),
        ('time', {'q': 'benchmark', 'created_from':'2026-11-03T00:00:00Z','created_to':'2026-11-07T00:00:00Z','sort':'created_at','order':'asc','page_size':20}),
        ('stable', {'q':'benchmark','sort':'id','order':'asc','page_size':20}),
    ]

def requests(profile, kind):
    if kind == 'page10':
        return [(name, {**values, 'page_size': 10}) for name, values in workload()]
    if kind == 'single': return workload()[:1]
    return workload()

def request(client, path, identity):
    start = time.perf_counter()
    try:
        client.request('GET', '/api/v1/search/tickets?' + path, headers={'Authorization':'Bearer '+identity, 'Accept':'application/json'})
        response = client.getresponse()
        raw = response.read()
        data = json.loads(raw)
        return {'status':response.status,'code':data.get('error',{}).get('code',''),
                'items':len(data.get('items',[])), 'request_id':response.getheader('X-Request-ID',''),
                'ms':(time.perf_counter()-start)*1000,'timeout':False,'transport_error':False}, data
    except (socket.timeout, TimeoutError):
        client.close()
        return {'status':0,'code':'CLIENT_TIMEOUT','items':0,'request_id':'','ms':(time.perf_counter()-start)*1000,'timeout':True,'transport_error':False}, {}
    except (OSError, http.client.HTTPException, ValueError):
        client.close()
        return {'status':0,'code':'CLIENT_TRANSPORT_ERROR','items':0,'request_id':'','ms':(time.perf_counter()-start)*1000,'timeout':False,'transport_error':True}, {}

def stage(folder, profile, concurrency, seconds, phase, kind, monitor, allowed):
    barrier = threading.Barrier(concurrency+1)
    start = [0.0]
    choices = requests(profile, kind)
    identity = 'm504-profile-' + profile.lower()
    rows = []
    def worker(number):
        client = http.client.HTTPConnection('127.0.0.1',env.APP_PORT,timeout=12)
        rng = random.Random(20261109 + number*1000003 + (999983 if phase=='warmup' else 0))
        result = []
        barrier.wait()
        try:
            sequence=0
            while time.perf_counter() < start[0]+seconds and not monitor.failure:
                sequence += 1
                name, values = choices[rng.randrange(len(choices))]
                sample,data = request(client, urlencode(values,doseq=True),identity)
                bad = sum(item['id'] not in allowed for item in data.get('items',[]))
                if bad: monitor.failure={'reason':'unauthorized_result','count':bad}; monitor.stop.set()
                sample.update(worker=number,sequence=sequence,phase=phase,query=name,unauthorized=bad,
                              offset=time.perf_counter()-start[0]-sample['ms']/1000)
                result.append(sample)
        finally: client.close()
        return result
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures=[pool.submit(worker,i) for i in range(concurrency)]
        start[0]=time.perf_counter(); barrier.wait()
        for f in futures: rows.extend(f.result())
    elapsed=time.perf_counter()-start[0]
    with (folder/(phase+'-requests.jsonl')).open('x') as output:
        for row in rows: output.write(json.dumps(row)+chr(10))
    success=[r for r in rows if r['status']==200 and not r['code']]
    codes=Counter(r['code'] for r in rows if r['code'])
    summary={'phase':phase,'admission_seconds':seconds,'completion_seconds':elapsed,'requests':len(rows),
             'success':len(success),'success_qps':len(success)/elapsed,'total_qps':len(rows)/elapsed,
             'codes':dict(codes),'statuses':dict(Counter(str(r['status']) for r in rows)),
             'latency_ms':{name:percentile([r['ms'] for r in rows],q) for name,q in [('p50',.5),('p95',.95),('p99',.99)]},
             'successful_latency_ms':{name:percentile([r['ms'] for r in success],q) for name,q in [('p50',.5),('p95',.95),('p99',.99)]},
             'returned_items':sum(r['items'] for r in success),'unauthorized':sum(r['unauthorized'] for r in rows)}
    return summary, rows

def join(folder, rows, diagnostics_path, flush=True):
    if flush:
        start=time.perf_counter()
        for attempt in range(45):
            try:
                debug();break
            except OSError:
                # Observed EADDRNOTAVAIL may also prevent a monitoring dial.
                # The finished load is not extended; wait for TIME_WAIT expiry
                # only to flush the already completed diagnostic records.
                if attempt==44:raise
                time.sleep(1)
        if time.perf_counter()-start>1:
            (folder/'post-load-flush-delay.json').write_text(json.dumps({'seconds':time.perf_counter()-start,'excluded_from_measurement':True}))
    ids={r['request_id'] for r in rows if r.get('request_id')}
    records=[]
    with diagnostics_path.open() as source:
        for line in source:
            d=json.loads(line)
            if d['request_id'] in ids: records.append(d)
    if len(records)!=len(ids): raise RuntimeError('Missing request-correlated diagnostics')
    with (folder/'search-diagnostics.jsonl').open('x') as output:
        for d in records: output.write(json.dumps(d)+chr(10))
    good=[d for d in records if d['outcome']=='success']
    returned=sum(d['returned'] for d in good)
    def stats(name, sample=records):
        vals=[d[name] for d in sample]
        return {'mean':sum(vals)/len(vals) if vals else None,'p50':percentile(vals,.5),'p95':percentile(vals,.95),'p99':percentile(vals,.99),'max':max(vals) if vals else None}
    result={'correlated':len(records),'http_request_ids':len(ids),
        'candidates_per_returned':sum(d['candidates'] for d in good)/returned if returned else None,
        'failed_candidates':sum(d['candidates'] for d in records if d['outcome']!='success'),
        'es_errors':dict(Counter(d.get('es_error_category','') for d in records if d.get('es_error_category'))),
        'timeout_locations':dict(Counter(d.get('timeout_at','') for d in records if d.get('timeout_at'))),
        'stats':{name:stats(name) for name in ['candidate_batches','candidates','mysql_queries','authorization_queries','connection_held_ms',
           'connection_acquire_and_begin_ms','identity_ms','freshness_ms','authorization_ms','es_ms','es_took_ms','version_rejected']},
        'successful_stats':{name:stats(name,good) for name in ['candidate_batches','candidates','mysql_queries','authorization_queries','connection_held_ms','authorization_ms','es_ms']}}
    (folder/'amplification.json').write_text(json.dumps(result,indent=2))
    return result

def run(n, profile, concurrency, label, seconds=6, warmup=2, kind='mix'):
    if concurrency not in [10,50,200] or not 0 < seconds <= 120 or not 0 <= warmup <= 10:
        raise RuntimeError('Finite 10/50/200 concurrency budget required')
    folder=OUT/'performance'/label
    folder.mkdir(parents=True,exist_ok=False)
    worker=fixture.fixtures()['workers']['app']
    if worker['dataset'] != n or not worker['selective']: raise RuntimeError('Wrong owned authorization fixture')
    buckets=json.loads((OUT/'datasets'/str(n)/'selectivity/buckets.json').read_text())
    allowed={int(i) for i,b in buckets.items() if b < fixture.PROFILES[profile]}
    config={'schema':'ticketpulse-m504-benchmark/v1','tickets':n,'profile':profile,'authorized_percentage':fixture.PROFILES[profile],
            'concurrency':concurrency,'duration_seconds':seconds,'warmup_seconds':warmup,'kind':kind,'seed':20261109,
            'queries':[(name,{k:v for k,v in query.items() if k!='cursor'},bool(query.get('cursor'))) for name,query in requests(profile,kind)],'load':'closed-loop HTTP keep-alive, completion including drain',
            'request_timeout_seconds':12,'api_timeout_seconds':10,'background_write_tps':0,
            'app_binary':worker['binary'],'diagnostics':worker['diagnostics'],'source':'frozen unchanged documents in derived authorization fixture'}
    (folder/'input.json').write_text(json.dumps(config,indent=2))
    monitor=Monitor(folder)
    before=debug(); monitor.start()
    try:
        warmed,_=stage(folder,profile,concurrency,warmup,'warmup',kind,monitor,allowed)
        measured_before=debug()
        measured,rows=stage(folder,profile,concurrency,seconds,'measurement',kind,monitor,allowed)
        after=debug()
    finally: monitor.close()
    amplification=join(folder,rows,env.ROOT/worker['diagnostics'])
    pool={'before':before,'after':after,'wait_count_delta':after['pool']['WaitCount']-before['pool']['WaitCount'],
          'wait_seconds_delta':(after['pool']['WaitDuration']-before['pool']['WaitDuration'])/1e9,
          'max_in_use':max((r['pool']['pool']['InUse'] for r in monitor.samples),default=None)}
    pool['measurement_before']=measured_before
    pool['measurement_wait_count_delta']=after['pool']['WaitCount']-measured_before['pool']['WaitCount']
    pool['measurement_wait_seconds_delta']=(after['pool']['WaitDuration']-measured_before['pool']['WaitDuration'])/1e9
    result={'config':config,'warmup':warmed,'measurement':measured,'amplification':amplification,'pool':pool,
            'safety_stop':monitor.failure,'resource_samples':len(monitor.samples)}
    (folder/'result.json').write_text(json.dumps(result,indent=2))
    print(json.dumps({'label':label,'success_qps':round(measured['success_qps'],2),'latency':measured['latency_ms'],
                     'codes':measured['codes'],'candidates_per_returned':amplification['candidates_per_returned'],
                     'auth_queries_mean':amplification['stats']['authorization_queries']['mean'],'safety_stop':monitor.failure}),flush=True)
    if monitor.failure: raise RuntimeError('Benchmark stopped by evidence/safety budget')
    return result

def reproduce(scenario, concurrency, label):
    folder=OUT/'reproduction'/label
    folder.mkdir(parents=True,exist_ok=False)
    old=OUT.parent/'m5-01/http'/('fixed-10000-team-filter-c200-r1' if scenario=='team-filter' else 'fixed-10000-mixed-rw-c50-r1')/'input.json'
    input=json.loads(old.read_text());metadata=input['metadata']
    worker=fixture.fixtures()['workers']['app']
    metadata['database']=(fixture.fixtures()['write_copies'][worker['write_copy']]['database'] if worker.get('write_copy') else fixture.fixtures()['datasets']['10000']['database'])
    (folder/'original-input.json').write_bytes(old.read_bytes())
    (folder/'metadata.json').write_text(json.dumps(metadata,indent=2))
    args=['--url',fixture.APP_URL,'--database',metadata['database'],'--tickets','10000','--seed',str(input['seed']),
          '--concurrency',str(concurrency),'--duration','8','--warmup','2','--timeout','5','--scenario',scenario,
          '--metadata',str(folder/'metadata.json'),'--output',str(folder/'http')]
    monitor=Monitor(folder);monitor.start()
    def tcp_evidence():
        with (folder/'tcp-states.jsonl').open('x') as output:
            while not monitor.stop.is_set():
                text=subprocess.check_output(['netstat','-an','-p','tcp'],text=True)
                all=[row for row in text.splitlines() if row.startswith('tcp')]
                rows=[row for row in all if '127.0.0.1.19202' in row]
                output.write(json.dumps({'utc':env.stamp(),'es':dict(Counter(row.split()[-1] for row in rows)),
                                         'all':dict(Counter(row.split()[-1] for row in all))})+chr(10));output.flush()
                monitor.stop.wait(1)
    tcp=threading.Thread(target=tcp_evidence,daemon=True);tcp.start()
    try:
        # Use the exact original route/random-selection implementation.
        with (folder/'driver.log').open('x') as log:
            process=subprocess.Popen(['python3',str(env.ROOT/'scripts/bench_http.py')]+args,cwd=env.ROOT,stdout=log,stderr=subprocess.STDOUT)
            deadline=time.perf_counter()+35
            while process.poll() is None:
                if monitor.failure or time.perf_counter()>deadline:
                    process.terminate();break
                monitor.stop.wait(.1)
            code=process.wait(timeout=15)
    finally:monitor.close();tcp.join(timeout=5)
    if code or monitor.failure:
        (folder/'failed.json').write_text(json.dumps({'exit_code':code,'monitor_failure':monitor.failure},indent=2))
        raise RuntimeError('Historical replay failed or safety budget reached')
    worker=fixture.fixtures()['workers']['app']
    rows=[json.loads(line) for line in (folder/'http/measurement-requests.jsonl').read_text().splitlines()]
    amplification=join(folder,rows,env.ROOT/worker['diagnostics'])
    result=json.loads((folder/'http/result.json').read_text())
    (folder/'comparison.json').write_text(json.dumps({'previous_input':str(old.relative_to(env.ROOT)),
        'same_generator_seed':True,'same_routes_and_role':True,'same_concurrency_and_duration':True,
        'differences':['new private ES/MySQL/app process','raised owned server file descriptor limit','M5-01 pool fix already present','diagnostic instrumentation'],
        'historical_root_cause':'UNKNOWN','observed_codes':result['measurement']['business_code_counts'],
        'search_amplification':amplification},indent=2))
    print(json.dumps({'historical_replay':label,'qps':result['measurement']['successful_qps'],'codes':result['measurement']['business_code_counts']}),flush=True)
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tickets',type=int,choices=[10000,100000],default=10000)
    parser.add_argument('--profile',choices=list(fixture.PROFILES),default='A')
    parser.add_argument('--concurrency',type=int,choices=[10,50,200],default=10)
    parser.add_argument('--duration',type=float,default=6)
    parser.add_argument('--warmup',type=float,default=2)
    parser.add_argument('--kind',choices=['mix','page10','single'],default='mix')
    parser.add_argument('--label',required=True)
    parser.add_argument('--historical',choices=['team-filter','mixed-rw'])
    args=parser.parse_args()
    if args.historical:reproduce(args.historical,args.concurrency,args.label)
    else:run(args.tickets,args.profile,args.concurrency,args.label,args.duration,args.warmup,args.kind)
