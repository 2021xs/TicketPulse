#!/usr/bin/env python3
"""Finite steady, deep-cursor, historical A/B and complete regression runs."""
import argparse
import http.client
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.parse import urlencode

import m504_environment as env
import m504_fixture as fixture
import m504_bench as bench
import m504_runtime as runtime

def steady():
    for variant in ['transport','final']:
        fixture.stop_app();fixture.start_app(100000,variant,True,label=variant+'-steady-app')
        for profile,kind in [('A','mix'),('C','mix'),('D','mix'),('D','page10')]:
            for c in [50,200]:
                bench.run(100000,profile,c,variant+'-steady-'+profile+'-'+kind+'-c'+str(c),60,5,kind)
    fixture.stop_app()

def deep():
    for variant,base in [('transport','transport'),('final','adaptive')]:
        fixture.stop_app();fixture.start_app(100000,variant,True,label=variant+'-deep-app')
        for profile in ['A','C','D']:
            # The existing private cursor checkpoint file is named by case.
            # Full suites wrote the latest variant; both use the unchanged key
            # and identical last-returned ordering. Bind only same query/user.
            saved=json.loads((env.RUNTIME/'deep-cursors'/(profile+'-full-id.json')).read_text())
            checkpoint=saved['cursors'][-1]
            # Cursors expire after one hour. Re-walk with production HTTP if a
            # long experiment has aged the private checkpoint; no forged token.
            client=http.client.HTTPConnection('127.0.0.1',env.APP_PORT,timeout=12)
            query=dict(saved['query'])
            token='m504-profile-'+profile.lower()
            for _ in range(checkpoint['page']-1):
                sample,response=bench.request(client,urlencode(query,doseq=True),token)
                if sample['status']!=200 or not response['has_more']:raise RuntimeError('Deep checkpoint traversal failed')
                query['cursor']=response['next_cursor']
            client.close()
            folder=env.OUT/'deep'/('input-'+variant+'-'+profile);folder.mkdir(parents=True,exist_ok=False)
            private=env.RUNTIME/'deep-inputs';private.mkdir(exist_ok=True)
            (private/(variant+'-'+profile+'.json')).write_text(json.dumps(query));(private/(variant+'-'+profile+'.json')).chmod(0o600)
            original=bench.requests
            bench.requests=lambda p,k:[('deep-page-'+str(checkpoint['page']),query)]
            try:
                for c in [10,50,200]:
                    bench.run(100000,profile,c,variant+'-deep-'+profile+'-c'+str(c),8,2,'deep')
            finally:bench.requests=original
    fixture.stop_app()

def historical(variant,suffix):
    fixture.stop_app()
    copy=fixture.create_write_copy(10000,suffix)
    runtime.start(suffix)
    fixture.start_app(10000,variant,False,label=suffix+'-app',write_copy=suffix)
    try:
        bench.reproduce('mixed-rw',50,suffix+'-mixed-rw-c50')
        runtime.wait_caught_up(suffix)
        folder=env.OUT/'reproduction'/(suffix+'-mixed-rw-c50')
        input=json.loads((folder/'http/input.json').read_text())
        rows=[json.loads(line) for phase in ['warmup','measurement'] for line in (folder/'http'/(phase+'-requests.jsonl')).read_text().splitlines()]
        creates=[r for r in rows if r['route']=='create' and r['status']==201]
        committed=int(env.sql('USE '+copy['database']+';SELECT count(*) FROM ticket_create_requests;').strip())
        if committed!=len(creates):raise RuntimeError('Acknowledged writes differ from committed source requests')
        (folder/'write-durability.json').write_text(json.dumps({'acknowledged_creates':len(creates),'committed_requests':committed,'same_routes_roles_seed':True,
            'warmup_writes_separate':True,'source_count':int(env.sql('USE '+copy['database']+';SELECT count(*) FROM tickets;').strip()),
            'projection_count':fixture.es('GET','/'+copy['index']+'/_count')['count'],'caught_up_last_source':True},indent=2))
    finally:fixture.stop_app();runtime.stop()

def regression(label='final'):
    fixture.stop_app();runtime.stop()
    folder=env.OUT/'regression'/label;folder.mkdir(parents=True,exist_ok=False)
    # A new CK runtime is used solely by original M3/M4 regression; no CK source
    # or M4 protocol is changed by this search task.
    import bench_ck_runtime as ck
    ck.RUNTIME,ck.STATE,ck.CONFIG=env.RUNTIME/'regression-ck',env.RUNTIME/'regression-ck/instance.json',env.RUNTIME/'regression-ck/config.xml'
    ck.PORT,ck.NATIVE_PORT,ck.OUTPUT=18126,19004,folder/'ck-runtime'
    original_config=ck.config_text
    ck.config_text=lambda:original_config().replace('<http_port>18124</http_port>','<http_port>18126</http_port>').replace('<tcp_port>19002</tcp_port>','<tcp_port>19004</tcp_port>')
    ck.start()
    mysql=env.mysql_runtime();owner=mysql.state()
    env.sql("CREATE USER IF NOT EXISTS 'm504_cdc'@'127.0.0.1' IDENTIFIED BY 'm504-local-replication'; GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'm504_cdc'@'127.0.0.1';")
    e=os.environ.copy();e.update(TICKETPULSE_TEST_DSN='root@unix('+str(env.MYSQL/'mysql.sock')+')/',
        TICKETPULSE_TEST_SERVER_UUID=owner['server_uuid'],TICKETPULSE_ES_URL=env.ES_URL,
        TICKETPULSE_CK_URL='http://127.0.0.1:18126',TICKETPULSE_KAFKA_BROKERS='127.0.0.1:19092',
        TICKETPULSE_CDC_ADDR='127.0.0.1:'+str(env.PORT),TICKETPULSE_CDC_USER='m504_cdc',TICKETPULSE_CDC_PASSWORD='m504-local-replication',
        TICKETPULSE_M4_EVIDENCE_DIR=str(folder/'m4'),TICKETPULSE_M503_EVIDENCE_DIR=str(folder/'m503'),
        TICKETPULSE_M504_EVIDENCE_DIR=str(folder/'m504'),TICKETPULSE_REBUILD_BINARY=str(env.RUNTIME/'rebuild-race'))
    commands=[('build',['go','build','-race','-o',e['TICKETPULSE_REBUILD_BINARY'],'./cmd/ticketpulse-rebuild']),
              ('integration',['go','test','-race','-tags=integration,m2,m3,m4','-count=1','-v','./internal/mysqlstore','./internal/syncpipe']),
              ('unit',['go','test','./...']),('race',['go','test','-race','./internal/search','./internal/httpapi','./internal/analytics','./internal/syncpipe','./internal/ticket']),
              ('vet',['go','vet','./...']),('python',['python3','scripts/bench_http_test.py'])]
    results=[]
    try:
        for name,command in commands:
            start=time.perf_counter()
            with (folder/(name+'.log')).open('x') as output:
                child=subprocess.run(command,cwd=env.ROOT,env=e,stdout=output,stderr=subprocess.STDOUT)
            results.append({'name':name,'command':command,'exit_code':child.returncode,'seconds':time.perf_counter()-start})
            (folder/'result.json').write_text(json.dumps(results,indent=2))
            print(json.dumps(results[-1]),flush=True)
            if child.returncode:raise RuntimeError('Regression failed: '+name)
        text=(folder/'integration.log').read_text()
        cases=[{'name':name,'result':status,'seconds':float(seconds)} for status,name,seconds in re.findall(r'^--- (PASS|FAIL): ([^ ]+) [(]([0-9.]+)s[)]$',text,re.M)]
        (folder/'test-summary.json').write_text(json.dumps({'passed':sum(c['result']=='PASS' for c in cases),'failed':sum(c['result']=='FAIL' for c in cases),'cases':cases},indent=2))
    finally:ck.stop()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['steady','deep','historical','regression']);parser.add_argument('--binary',default='final');parser.add_argument('--label',default='final')
    args=parser.parse_args()
    if args.action=='steady':steady()
    elif args.action=='deep':deep()
    elif args.action=='historical':historical(args.binary,args.label)
    else:regression(args.label)
