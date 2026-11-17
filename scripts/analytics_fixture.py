#!/usr/bin/env python3
"""Disposable M3 MySQL database, real independent CDC/ES/CK processes and Web."""
from datetime import datetime, timezone
import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

import local_mysql as local
import local_search as search
import local_analytics as runtime
from search_fixture import owns

ROOT=Path(__file__).resolve().parents[1]
MARKER=ROOT/'.local/m301-fixture.json'
OUTPUT=ROOT/'artifacts/m3-01'
PORT=18083

def verified():
    owner=local.marker();local.verify(owner)
    value=json.loads(MARKER.read_text())
    if value.get('project')!=str(ROOT) or value.get('server_uuid')!=owner['server_uuid'] or not value['database'].startswith('ticketpulse_test_search_m3_'):
        raise RuntimeError('M3 fixture ownership mismatch')
    return value

def sql(statement):
    return local.mysql('USE '+verified()['database']+';'+chr(10)+statement)

def ck(query):
    value=verified()
    params=urllib.parse.urlencode({'database':value['database']+'_analytics_v1','wait_end_of_query':'1','output_format_json_quote_64bit_integers':'0'})
    with urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:18123/?'+params,data=query.encode()),timeout=8) as response:
        return response.read().decode()

def worker(component,action):
    value=verified();pids=value['pids'];pid=pids.get(component)
    if action=='stop':
        if pid and owns(pid,'ticketpulse-sync'):
            os.kill(pid,signal.SIGTERM)
            for _ in range(120):
                if not owns(pid,'ticketpulse-sync'):break
                time.sleep(.1)
            else:raise RuntimeError('Owned '+component+' worker has not stopped')
        pids[component]=None
    else:
        if pid and owns(pid,'ticketpulse-sync'):return
        mode={'cdc':'capture','es':'consume','ck':'ck'}[component]
        with (OUTPUT/('fixture-'+component+'.log')).open('a') as log:
            child=subprocess.Popen([str(ROOT/'bin/ticketpulse-sync'),'-mode='+mode,'-server-id=13001'],cwd=ROOT,env=runtime.environment(value['database']),stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        pids[component]=child.pid
    local.private_file(MARKER,json.dumps(value,indent=2))
    if action=='start':
        for _ in range(150):
            if child.poll() is not None:raise RuntimeError('M3 '+component+' worker exited; inspect its fixture log')
            ready=sql("SELECT COUNT(*) FROM sync_worker_state WHERE component='"+component+"' AND state='running' AND heartbeat_at>UTC_TIMESTAMP(6)-INTERVAL 5 SECOND;").strip()
            if ready=='1':return
            time.sleep(.1)
        raise RuntimeError('M3 '+component+' startup timeout')

def start():
    owner=local.marker();local.verify(owner);runtime.kafka.start();search.start_component('es',search.state());runtime.start()
    if MARKER.exists():
        value=verified();print(json.dumps({'url':value['url'],'database':value['database'],'existing':True}));return
    if search.occupied(PORT):raise RuntimeError('M3 fixture port occupied')
    name='ticketpulse_test_search_m3_'+secrets.token_hex(6)
    local.mysql('CREATE DATABASE '+name+' CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;')
    OUTPUT.mkdir(exist_ok=True)
    local.private_file(MARKER,json.dumps({'project':str(ROOT),'server_uuid':owner['server_uuid'],'database':name,'url':'http://127.0.0.1:'+str(PORT),'web_pid':None,'pids':{},'started_at':datetime.now(timezone.utc).isoformat()},indent=2))
    try:
        paths=['db/schema.sql','db/seed.sql','db/login_seed.sql','db/migrations/006_search_sync.sql','db/migrations/007_analytics_sync.sql','db/migrations/008_kafka_sync.sql','tests/analytics/prd_ten_tickets.sql']
        sql(chr(10).join((ROOT/path).read_text() for path in paths))
        # An isolated second organization makes the real no-data state observable.
        sql("INSERT INTO users(id,organization_id,name,role,active,auth_token_hash,created_at,updated_at) VALUES(21,2,'Empty Admin','admin',TRUE,UNHEX(SHA2('local-empty-admin',256)),UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)); INSERT INTO login_accounts(user_id,username,password_hash) SELECT 21,'empty-admin',password_hash FROM login_accounts WHERE user_id=6;")
        with (OUTPUT/'fixture-web.log').open('a') as log:
            child=subprocess.Popen([str(ROOT/'bin/ticketpulse'),'-listen','127.0.0.1:'+str(PORT)],cwd=ROOT,env=runtime.environment(name),stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        value=verified();value['web_pid']=child.pid;local.private_file(MARKER,json.dumps(value,indent=2))
        for _ in range(100):
            if child.poll() is not None:raise RuntimeError('M3 Web exited')
            try:
                with urllib.request.urlopen(value['url']+'/login',timeout=1):break
            except OSError:time.sleep(.1)
        for component in ['cdc','es','ck']:worker(component,'start')
        print(json.dumps({'url':value['url'],'database':name,'prd_ten_tickets':True,'independent_workers':True}))
    except BaseException:
        stop();raise

def stop():
    if not MARKER.exists():return
    value=verified()
    for component in ['cdc','es','ck']:worker(component,'stop')
    pid=value.get('web_pid')
    if pid and owns(pid,'ticketpulse'):
        os.kill(pid,signal.SIGTERM)
        for _ in range(80):
            if not owns(pid,'ticketpulse'):break
            time.sleep(.1)
    ck('DROP DATABASE '+value['database']+'_analytics_v1')
    request=urllib.request.Request('http://127.0.0.1:19200/'+value['database']+'_tickets_v1',method='DELETE')
    try:
        with urllib.request.urlopen(request,timeout=5):pass
    except urllib.error.HTTPError as error:
        if error.code!=404:raise
    subprocess.run([str(ROOT/'bin/ticketpulse-sync'),'-mode=cleanup-test'],cwd=ROOT,env=runtime.environment(value['database']),check=True)
    local.mysql('DROP DATABASE '+value['database']+';');MARKER.unlink()
    (OUTPUT/'acceptance-cleanup.json').write_text(json.dumps({'database':value['database'],'only_owned_fixture_removed':True,'business_database_preserved':True,'finished_at':datetime.now(timezone.utc).isoformat()},indent=2)+chr(10))
    print('Owned M3 fixture removed; demo data preserved')

def main():
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['start','stop','start-ck','stop-ck','start-es','stop-es','status','escape-team','restore-team','downgrade-supervisor','restore-supervisor'])
    args=parser.parse_args()
    if args.action=='start':start()
    elif args.action=='stop':stop()
    elif args.action in ['start-ck','stop-ck','start-es','stop-es']:
        action,component=args.action.split('-');worker(component,action)
    elif args.action=='status':
        subprocess.run([str(ROOT/'bin/ticketpulse-sync'),'-mode=analytics-status'],cwd=ROOT,env=runtime.environment(verified()['database']),check=True)
    elif args.action=='escape-team':sql("UPDATE teams SET name='IT <svg onload=alert(1)>' WHERE id=10;")
    elif args.action=='restore-team':sql("UPDATE teams SET name='IT 服务台' WHERE id=10;")
    elif args.action=='downgrade-supervisor':sql("UPDATE users SET role='agent' WHERE id=5;")
    else:sql("UPDATE users SET role='supervisor' WHERE id=5;")

if __name__=='__main__':main()
