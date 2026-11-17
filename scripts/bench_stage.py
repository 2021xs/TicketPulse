#!/usr/bin/env python3
"""Owned M5 fixture lifecycle; workload and import rates remain separate."""
import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import time
import urllib.request

import local_mysql as local
import local_analytics as runtime

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / '.local/m5-fixture.json'
OUTPUT = ROOT / 'artifacts/m5-01'
PORT = 18085

def verified():
    owner = local.marker()
    local.verify(owner)
    value = json.loads(STATE.read_text())
    if value['project'] != str(ROOT) or value['source_uuid'] != owner['server_uuid'] or not value['database'].startswith('ticketpulse_test_m5_'):
        raise RuntimeError('M5 fixture ownership differs')
    return value

def environment(value=None):
    value = value or verified()
    env = runtime.environment(value['database'])
    env['TICKETPULSE_BENCH_DSN'] = env['TICKETPULSE_DSN']
    env['TICKETPULSE_TEST_SERVER_UUID'] = value['source_uuid']
    if value.get('ck_url'):env['TICKETPULSE_CK_URL'] = value['ck_url']
    return env

def sql(query):
    return local.mysql('USE ' + verified()['database'] + ';' + chr(10) + query)

def owned(pid, name):
    try:
        cmd = subprocess.check_output(['ps', '-p', str(pid), '-o', 'command='], text=True)
        cwd = subprocess.check_output(['lsof', '-a', '-p', str(pid), '-d', 'cwd', '-Fn'], text=True)
        return str(ROOT / 'bin' / name) in cmd and 'n' + str(ROOT) in cwd.splitlines()
    except subprocess.CalledProcessError:
        return False

def stop_workers():
    value = verified()
    for component, entry in value['pids'].items():
        if entry and owned(entry['pid'], entry['binary']):
            os.kill(entry['pid'], signal.SIGTERM)
            for _ in range(100):
                if not owned(entry['pid'], entry['binary']):break
                time.sleep(.1)
            else:raise RuntimeError('owned benchmark worker did not stop: ' + component)
    value['pids'] = {}
    local.private_file(STATE, json.dumps(value, indent=2))

def create():
    if STATE.exists():raise RuntimeError('M5 fixture already exists; reuse or explicitly clean it')
    owner = local.marker();local.verify(owner)
    if runtime.search.occupied(PORT):raise RuntimeError('benchmark port occupied')
    name = 'ticketpulse_test_m5_' + secrets.token_hex(5)
    local.mysql('CREATE DATABASE ' + name + ' CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;')
    value = {'project': str(ROOT), 'database': name, 'source_uuid': owner['server_uuid'], 'pids': {}, 'url': 'http://127.0.0.1:' + str(PORT), 'server_id': 51001 + secrets.randbelow(100000), 'created_at': time.time()}
    local.private_file(STATE, json.dumps(value, indent=2))
    paths = ['db/schema.sql', 'db/seed.sql', 'db/login_seed.sql', 'db/migrations/006_search_sync.sql', 'db/migrations/007_analytics_sync.sql', 'db/migrations/008_kafka_sync.sql', 'db/migrations/009_rebuild.sql']
    sql(chr(10).join((ROOT / p).read_text() for p in paths))
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / 'fixture.json').write_text(json.dumps({k:v for k,v in value.items() if k!='project'}, indent=2))
    print(json.dumps({'database':name,'url':value['url'],'server_id':value['server_id']}))

def start():
    value = verified();env = environment(value)
    jobs = [('cdc', 'ticketpulse-sync', ['-mode=capture', '-server-id=' + str(value['server_id'])]), ('es','ticketpulse-sync',['-mode=consume']),('ck','ticketpulse-sync',['-mode=ck']),('web','ticketpulse-bench-app',['-listen=127.0.0.1:'+str(PORT)])]
    for component,binary,args in jobs:
        if value['pids'].get(component):raise RuntimeError('benchmark component already recorded; stop before starting')
        with (OUTPUT / ('fixture-' + component + '.log')).open('a') as log:
            child = subprocess.Popen([str(ROOT/'bin'/binary)]+args,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        value['pids'][component]={'pid':child.pid,'binary':binary}
        local.private_file(STATE,json.dumps(value,indent=2))
        time.sleep(.25)
        if child.poll() is not None:raise RuntimeError(component+' exited; inspect M5 fixture log')
    for _ in range(100):
        try:
            with urllib.request.urlopen(value['url']+'/debug/bench',timeout=1) as response:json.loads(response.read());return
        except OSError:time.sleep(.1)
    raise RuntimeError('M5 application readiness timed out')

def cleanup():
    value=verified();stop_workers()
    # Enumerate and delete only exact names owned by this random fixture.
    with urllib.request.urlopen('http://127.0.0.1:19200/_cat/indices/'+value['database']+'*?format=json',timeout=10) as response:indices=json.loads(response.read())
    for row in indices:
        name=row['index']
        if not name.startswith(value['database']+'_'):raise RuntimeError('unexpected index outside benchmark namespace')
        with urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:19200/'+name,method='DELETE'),timeout=10):pass
    params=value.get('ck_url','http://127.0.0.1:18123')+'/'
    q="SELECT name FROM system.databases WHERE startsWith(name,'"+value['database']+"_') FORMAT JSONEachRow"
    with urllib.request.urlopen(urllib.request.Request(params,data=q.encode()),timeout=10) as response:databases=[json.loads(x)['name'] for x in response.read().decode().splitlines()]
    for name in databases:
        if not name.startswith(value['database']+'_'):raise RuntimeError('unexpected CK namespace')
        with urllib.request.urlopen(urllib.request.Request(params,data=('DROP DATABASE '+name).encode()),timeout=45):pass
    # Owned Topic deletion uses the pinned official CLI and exact test name.
    topic='ticketpulse.'+value['database']+'.tickets.v1'
    runtime.kafka.command('kafka-topics.sh',['--bootstrap-server','127.0.0.1:19092','--delete','--topic',topic],check=False)
    local.mysql('DROP DATABASE '+value['database']+';');STATE.unlink()
    (OUTPUT/'fixture-cleanup.json').write_text(json.dumps({'database':value['database'],'only_owned_fixture_removed':True,'completed_at':time.time()},indent=2))

def main():
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['create','start','stop','cleanup','seed','prepare','rebuild','check','status']);args,extra=parser.parse_known_args()
    if args.action=='create':create()
    elif args.action=='start':start()
    elif args.action=='stop':stop_workers()
    elif args.action=='cleanup':cleanup()
    else:
        value=verified();env=environment(value)
        binary={'seed':'ticketpulse-bench-seed','prepare':'ticketpulse-bench-prepare'}.get(args.action,'ticketpulse-rebuild')
        flags=[] if args.action in ['seed','prepare'] else ['-mode='+('run' if args.action=='rebuild' else args.action)]
        return subprocess.run([str(ROOT/'bin'/binary)]+flags+extra,cwd=ROOT,env=env).returncode
    return 0

if __name__=='__main__':raise SystemExit(main())
