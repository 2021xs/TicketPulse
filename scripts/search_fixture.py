#!/usr/bin/env python3
"""Owned disposable database, CDC worker and HTTP server for M2 acceptance."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time
import urllib.request
import urllib.error

import local_mysql as local
import local_kafka as search

ROOT = Path(__file__).resolve().parents[1]
MARKER = ROOT / '.local/m201-fixture.json'
OUTPUT = ROOT / 'artifacts/m2-02'
PORT = 18082


def verified():
    owner = local.marker()
    local.verify(owner)
    value = json.loads(MARKER.read_text())
    if value.get('project') != str(ROOT) or value.get('server_uuid') != owner['server_uuid'] or not value['database'].startswith('ticketpulse_test_search_'):
        raise RuntimeError('M2 fixture ownership mismatch')
    return value


def sql(statement):
    value = verified()
    return local.mysql('USE '+value['database']+';\n'+statement)


def owns(pid, name):
    try:
        cwd = subprocess.check_output(['lsof','-a','-p',str(pid),'-d','cwd','-Fn'],text=True)
        command = subprocess.check_output(['ps','-p',str(pid),'-o','command='],text=True)
        return 'n'+str(ROOT) in cwd.splitlines() and str(ROOT/'bin'/name) in command
    except subprocess.CalledProcessError:
        return False


def worker(action):
    value = verified()
    if action == 'stop':
        pid = value.get('sync_pid')
        if pid and owns(pid,'ticketpulse-sync'):
            os.kill(pid,signal.SIGTERM)
            for _ in range(100):
                if not owns(pid,'ticketpulse-sync'):
                    break
                time.sleep(.1)
            else:
                raise RuntimeError('Owned sync worker has not stopped')
        value['sync_pid'] = None
    else:
        if value.get('sync_pid') and owns(value['sync_pid'],'ticketpulse-sync'):
            return
        with (OUTPUT/'fixture-sync.log').open('a') as log:
            child = subprocess.Popen([str(ROOT/'bin/ticketpulse-sync'),'-mode=all','-server-id=12001'],cwd=ROOT,env=search.environment(value['database']),stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        value['sync_pid'] = child.pid
    local.private_file(MARKER,json.dumps(value,indent=2))
    if action == 'start':
        for _ in range(100):
            if child.poll() is not None:
                raise RuntimeError('M2 worker exited')
            count = sql("SELECT COUNT(*) FROM sync_worker_state WHERE component IN ('cdc','es') AND state='running' AND heartbeat_at>UTC_TIMESTAMP(6)-INTERVAL 5 SECOND;").strip()
            if count == '2':
                return
            time.sleep(.1)
        raise RuntimeError('M2 worker startup timeout')


def start():
    owner = local.marker()
    local.verify(owner)
    search.start();search.legacy.start_component('es',search.legacy.state())
    if MARKER.exists():
        value = verified()
        print(json.dumps({'url':value['url'],'database':value['database'],'existing':True}))
        return
    with socket.socket() as sock:
        if sock.connect_ex(('127.0.0.1',PORT)) == 0:
            raise RuntimeError('M2 fixture port occupied')
    name = 'ticketpulse_test_search_'+secrets.token_hex(6)
    local.mysql('CREATE DATABASE '+name+' CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;')
    OUTPUT.mkdir(exist_ok=True)
    local.private_file(MARKER,json.dumps({'project':str(ROOT),'server_uuid':owner['server_uuid'],'database':name,'url':'http://127.0.0.1:'+str(PORT),'web_pid':None,'sync_pid':None,'started_at':datetime.now(timezone.utc).isoformat()},indent=2))
    try:
        paths=['db/schema.sql','db/seed.sql','db/login_seed.sql','db/migrations/006_search_sync.sql','db/migrations/007_analytics_sync.sql','db/migrations/008_kafka_sync.sql']
        sql('\n'.join((ROOT/path).read_text() for path in paths))
        env = search.environment(name)
        with (OUTPUT/'fixture-web.log').open('a') as log:
            child = subprocess.Popen([str(ROOT/'bin/ticketpulse'),'-listen','127.0.0.1:'+str(PORT)],cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        value=verified();value['web_pid']=child.pid;local.private_file(MARKER,json.dumps(value,indent=2))
        for _ in range(100):
            if child.poll() is not None:
                raise RuntimeError('M2 Web server exited')
            try:
                with urllib.request.urlopen(value['url']+'/login',timeout=1):
                    break
            except OSError:
                time.sleep(.1)
        worker('start')
        print(json.dumps({'url':value['url'],'database':name,'created':True}))
    except BaseException:
        stop()
        raise


def stop():
    if not MARKER.exists():
        return
    value=verified();worker('stop')
    pid=value.get('web_pid')
    if pid and owns(pid,'ticketpulse'):
        os.kill(pid,signal.SIGTERM)
        for _ in range(80):
            if not owns(pid,'ticketpulse'):
                break
            time.sleep(.1)
    with (OUTPUT/'acceptance-final-status.json').open('w') as log:
        subprocess.run([str(ROOT/'bin/ticketpulse-sync'),'-mode=status'],cwd=ROOT,env=search.environment(value['database']),stdout=log)
    # Names are verified from the task marker; never touch other stream/index/database names.
    index=value['database']+'_tickets_v1'
    request=urllib.request.Request('http://127.0.0.1:19200/'+index,method='DELETE')
    try:
        with urllib.request.urlopen(request,timeout=5):
            pass
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
    subprocess.run([str(ROOT/'bin/ticketpulse-sync'),'-mode=cleanup-test'],cwd=ROOT,env=search.environment(value['database']),check=True)
    local.mysql('DROP DATABASE '+value['database']+';')
    MARKER.unlink()
    (OUTPUT/'acceptance-cleanup.json').write_text(json.dumps({'database':value['database'],'only_owned_fixture_removed':True,'business_database_preserved':True,'finished_at':datetime.now(timezone.utc).isoformat()},indent=2)+chr(10))
    print('Owned M2 fixture removed; existing business database preserved')


def mutate(action, value):
    if action in ['disable-user','enable-user']:
        sql('UPDATE users SET active='+('FALSE' if action=='disable-user' else 'TRUE')+' WHERE id='+str(value)+';')
    elif action in ['disable-team','enable-team']:
        sql('UPDATE teams SET active='+('FALSE' if action=='disable-team' else 'TRUE')+' WHERE id='+str(value)+';')
    elif action=='inspect':
        data=sql("SELECT HEX(CAST(JSON_OBJECT('id',id,'ticket_no',ticket_no,'title',title,'description',description,'version',version,'status',status,'assignee_id',assignee_id,'current_team_id',current_team_id,'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ')) AS CHAR)) FROM tickets WHERE id="+str(value)+';').strip()
        print(bytes.fromhex(data).decode())
        return
    print(json.dumps({'action':action,'fixture_only':True}))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('action',choices=['start','stop','stop-worker','start-worker','inspect','disable-user','enable-user','disable-team','enable-team'])
    parser.add_argument('value',type=int,nargs='?',default=0)
    args=parser.parse_args()
    if args.action=='start':start()
    elif args.action=='stop':stop()
    elif args.action.endswith('-worker'):worker(args.action.split('-')[0])
    else:mutate(args.action,args.value)


if __name__=='__main__':
    main()
