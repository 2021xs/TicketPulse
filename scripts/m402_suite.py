#!/usr/bin/env python3
"""Run the owned three-process fault matrix and milestone regressions."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import time

import m402_environment as env

def connection(folder):
    owner=env.mysql_runtime().state()
    env.sql("CREATE USER IF NOT EXISTS 'm402_cdc'@'127.0.0.1' IDENTIFIED BY 'm402-local-replication'; GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'm402_cdc'@'127.0.0.1';")
    e=os.environ.copy()
    e.update(TICKETPULSE_TEST_DSN='root@unix('+str(env.MYSQL/'mysql.sock')+')/',
        TICKETPULSE_TEST_SERVER_UUID=owner['server_uuid'],TICKETPULSE_ES_URL=env.ES_URL,
        TICKETPULSE_CK_URL=env.CK_URL,TICKETPULSE_KAFKA_BROKERS=env.BOOTSTRAP,
        TICKETPULSE_KAFKA_PARTITIONS='3',TICKETPULSE_KAFKA_REPLICATION_FACTOR='3',TICKETPULSE_KAFKA_MIN_INSYNC_REPLICAS='2',
        TICKETPULSE_CDC_ADDR='127.0.0.1:'+str(env.MYSQL_PORT),TICKETPULSE_CDC_USER='m402_cdc',TICKETPULSE_CDC_PASSWORD='m402-local-replication',
        TICKETPULSE_M402_CLUSTER_ID=env.cluster()['cluster_id'],TICKETPULSE_M402_EVIDENCE_DIR=str(folder/'faults'),
        TICKETPULSE_M4_EVIDENCE_DIR=str(folder/'m4'),TICKETPULSE_M503_EVIDENCE_DIR=str(folder/'m503'),
        TICKETPULSE_M504_EVIDENCE_DIR=str(folder/'m504'),TICKETPULSE_SYNC_BINARY=str(env.RUNTIME/'ticketpulse-sync'),
        TICKETPULSE_REBUILD_BINARY=str(env.RUNTIME/'ticketpulse-rebuild'))
    return e

def run(action,label,pattern='^TestM402'):
    folder=env.OUT/('matrix' if action=='matrix' else 'regression')/label
    folder.mkdir(parents=True,exist_ok=False)
    e=connection(folder)
    commands=[('build-sync',['go','build','-race','-o',e['TICKETPULSE_SYNC_BINARY'],'./cmd/ticketpulse-sync']),
              ('build-rebuild',['go','build','-race','-o',e['TICKETPULSE_REBUILD_BINARY'],'./cmd/ticketpulse-rebuild'])]
    if action=='matrix':
        commands.append(('faults',['go','test','-race','-tags=integration,m2,m3,m4,m402','-run='+pattern,'-count=1','-timeout=20m','-v','./internal/mysqlstore']))
    else:
        commands.extend([('integration',['go','test','-race','-tags=integration,m2,m3,m4','-count=1','-timeout=25m','-v','./internal/mysqlstore','./internal/syncpipe']),
            ('unit',['go','test','./...']),('race',['go','test','-race','./internal/search','./internal/httpapi','./internal/analytics','./internal/syncpipe','./internal/ticket']),
            ('vet',['go','vet','./...']),('python',['python3','scripts/bench_http_test.py'])])
    results=[]
    for name,command in commands:
        env.budget()
        start=time.perf_counter()
        with (folder/(name+'.log')).open('x') as output:
            child=subprocess.run(command,cwd=env.ROOT,env=e,stdout=output,stderr=subprocess.STDOUT)
        result={'name':name,'command':command,'exit_code':child.returncode,'seconds':time.perf_counter()-start,
            'finished_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'budget':env.budget()}
        results.append(result);(folder/'result.json').write_text(json.dumps(results,indent=2));print(json.dumps(result),flush=True)
        if child.returncode:raise RuntimeError('Owned test failed: '+name+'; raw log retained at '+str(folder/(name+'.log')))
    log=(folder/('faults.log' if action=='matrix' else 'integration.log')).read_text()
    cases=[{'name':name,'result':status,'seconds':float(seconds)} for status,name,seconds in re.findall(r'^--- (PASS|FAIL): ([^ ]+) [(]([0-9.]+)s[)]$',log,re.M)]
    (folder/'test-summary.json').write_text(json.dumps({'passed':sum(c['result']=='PASS' for c in cases),'failed':sum(c['result']=='FAIL' for c in cases),'cases':cases},indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['matrix','regression']);parser.add_argument('--label',default='initial');parser.add_argument('--pattern',default='^TestM402')
    args=parser.parse_args();run(args.action,args.label,args.pattern)
