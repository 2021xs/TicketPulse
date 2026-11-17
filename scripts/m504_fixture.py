#!/usr/bin/env python3
"""Frozen originals and separately derived, explicit authorization fixtures."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import secrets
import signal
import subprocess
import time
import urllib.error
import urllib.request

import m504_environment as env
import local_mysql as demo

ROOT, OUT, RUNTIME = env.ROOT, env.OUT, env.RUNTIME
MARKER = RUNTIME / 'fixtures.json'
APP_URL = 'http://127.0.0.1:' + str(env.APP_PORT)
PROFILES = {'A': 100, 'B': 50, 'C': 10, 'D': 1}
USERS = {'A': 501, 'B': 502, 'C': 503, 'D': 504}

def fixtures():
    value = json.loads(MARKER.read_text())
    if value['project'] != str(ROOT) or value['source_uuid'] != env.mysql_runtime().state()['server_uuid']:
        raise RuntimeError('M5-04 fixture ownership mismatch')
    return value

def save(value):
    demo.private_file(MARKER, json.dumps(value, indent=2))

def connection(n, selective=False, es_url=None):
    value = fixtures()
    item = value['datasets'][str(n)]
    database = item['selective_database'] if selective else item['database']
    index = item['selective_index'] if selective else item['index']
    e = os.environ.copy()
    e.update(TICKETPULSE_DSN='root@unix(' + str(env.MYSQL / 'mysql.sock') + ')/' + database,
             TICKETPULSE_BENCH_DSN='root@unix(' + str(env.MYSQL / 'mysql.sock') + ')/' + database,
             TICKETPULSE_TEST_SERVER_UUID=value['source_uuid'], TICKETPULSE_ES_URL=es_url or env.ES_URL,
             TICKETPULSE_SEARCH_INDEX=index, TICKETPULSE_SEARCH_CURSOR_KEY=value['cursor_key'])
    return e

def es(method, path, body=None, timeout=30):
    payload = body if isinstance(body, bytes) else json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(env.ES_URL + path, data=payload, method=method)
    req.add_header('Content-Type', 'application/x-ndjson' if path.endswith('_bulk') else 'application/json')
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return {} if method == 'HEAD' else json.load(response)

def schema(database):
    if not re.fullmatch('ticketpulse_test_m5_m504_[a-z0-9_]+',database) or len(database)>64:
        raise RuntimeError('Owned fixture schema required')
    if env.sql("SELECT count(*) FROM information_schema.schemata WHERE schema_name='" + database + "';").strip() != '0':
        raise RuntimeError('Refusing existing fixture schema')
    paths = ['db/schema.sql', 'db/seed.sql', 'db/login_seed.sql', 'db/migrations/006_search_sync.sql',
             'db/migrations/007_analytics_sync.sql', 'db/migrations/008_kafka_sync.sql', 'db/migrations/009_rebuild.sql']
    env.sql('CREATE DATABASE ' + database + ' CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;')
    env.sql('USE ' + database + ';' + chr(10) + chr(10).join((ROOT / p).read_text() for p in paths))

def documents(database):
    query = ("SELECT JSON_OBJECT('id',id,'organization_id',organization_id,'ticket_no',ticket_no,'title',title,"
             "'description',description,'status',status,'priority',priority,'priority_rank',FIELD(priority,'low','medium','high','urgent'),"
             "'category',category,'creator_id',creator_id,'assignee_id',assignee_id,'initial_team_id',initial_team_id,"
             "'current_team_id',current_team_id,'version',version,"
             "'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'),'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ')) "
             "FROM tickets ORDER BY id;")
    text = env.sql("SET time_zone='+00:00';USE " + database + ';' + query, 120)
    return [json.loads(row) for row in text.splitlines()]

def index_docs(index, docs):
    source = (ROOT / 'internal/search/elasticsearch.go').read_text()
    mapping = json.loads(source.split('const Mapping = ', 1)[1].split(chr(96))[1])
    try:
        es('HEAD', '/' + index)
    except urllib.error.HTTPError as error:
        if error.code != 404: raise
    else:
        raise RuntimeError('Refusing existing fixture index')
    es('PUT', '/' + index, mapping)
    digest = hashlib.sha256()
    for start in range(0, len(docs), 500):
        lines = []
        for doc in docs[start:start + 500]:
            raw = json.dumps(doc, sort_keys=True, separators=(',', ':')).encode()
            digest.update(raw + bytes([10]))
            lines.append(json.dumps({'index': {'_index': index, '_id': str(doc['id']), 'version': doc['version'], 'version_type': 'external'}}).encode())
            lines.append(raw)
        result = es('POST', '/_bulk', bytes([10]).join(lines) + bytes([10]))
        if result['errors']: raise RuntimeError('Owned fixture bulk returned item errors')
    es('POST', '/' + index + '/_refresh')
    count = es('GET', '/' + index + '/_count')['count']
    if count != len(docs): raise RuntimeError('Fixture projection count mismatch')
    return {'documents': count, 'sha256': digest.hexdigest(), 'mapping': mapping}

def create(n):
    if MARKER.exists():
        value = fixtures()
    else:
        value = {'project': str(ROOT), 'source_uuid': env.mysql_runtime().state()['server_uuid'],
                 'datasets': {}, 'workers': {}, 'cursor_key': secrets.token_hex(32)}
    if str(n) in value['datasets']: raise RuntimeError('Preserve existing fixture; no overwrite')
    database = 'ticketpulse_test_m5_m504_' + str(n)
    schema(database)
    value['datasets'][str(n)] = {'database': database, 'index': database + '_tickets_v1', 'tickets': n, 'seed': 20261109}
    save(value)
    folder = OUT / 'datasets' / str(n)
    folder.mkdir(parents=True, exist_ok=False)
    with (folder / 'seed.log').open('x') as log:
        result = subprocess.run([str(RUNTIME / 'search-fixture'), '-tickets=' + str(n), '-manifest=' + str(folder / 'manifest.json')],
                                cwd=ROOT, env=connection(n), stdout=log, stderr=subprocess.STDOUT)
    if result.returncode: raise RuntimeError('Seed failed; partial source preserved')
    manifest = json.loads((folder / 'manifest.json').read_text())
    oldpath = ROOT / 'artifacts/m5-01' / ('dataset-100000.json' if n == 100000 else 'dataset-dedicated-10000.json')
    old = json.loads(oldpath.read_text())
    if manifest['sha256'] != old['sha256']: raise RuntimeError('Frozen source bundle hash differs')
    docs = documents(database)
    projection = index_docs(value['datasets'][str(n)]['index'], docs)
    summaries = [{k: d[k] for k in ['id', 'version', 'current_team_id', 'creator_id', 'status', 'priority', 'category', 'created_at', 'updated_at']} for d in docs]
    (folder / 'current-summaries.json').write_text(json.dumps(summaries))
    (folder / 'projection.json').write_text(json.dumps(projection, indent=2))
    (folder / 'verification.json').write_text(json.dumps({'frozen_source_hash_matches': True,
        'baseline': str(oldpath.relative_to(ROOT)), 'counts': env.sql('USE ' + database + ';SELECT count(*) FROM tickets;SELECT count(*) FROM ticket_histories;SELECT count(*) FROM ticket_comments;'),
        'projection': {'count': projection['documents'], 'sha256': projection['sha256']}}, indent=2))
    print(json.dumps({'tickets': n, 'hash_matches': True, 'index': value['datasets'][str(n)]['index']}))

def create_write_copy(n=10000, suffix='writes', current_only=False):
    if not re.fullmatch('[a-z0-9_]{1,20}',suffix):raise RuntimeError('Explicit safe write copy suffix required')
    value=fixtures(); source=value['datasets'][str(n)]['database']
    database=source+'_'+suffix
    schema(database)
    for table in ['tickets'] if current_only else ['tickets','ticket_comments','ticket_histories']:
        cols=env.sql("SELECT column_name FROM information_schema.columns WHERE table_schema='"+source+"' AND table_name='"+table+"' ORDER BY ordinal_position;").splitlines()
        columns=','.join(cols)
        env.sql('USE '+database+';INSERT INTO '+table+'('+columns+') SELECT '+columns+' FROM '+source+'.'+table+';',120)
    env.sql('USE '+database+';ALTER TABLE ticket_create_requests AUTO_INCREMENT='+str(n+1000)+';')
    index=database+'_tickets_v1'
    projection=index_docs(index,documents(database))
    value.setdefault('write_copies',{})[suffix]={'database':database,'index':index,'tickets':n,'current_only':current_only}
    save(value)
    folder=OUT/'datasets'/str(n)/suffix;folder.mkdir(exist_ok=False)
    (folder/'copy.json').write_text(json.dumps({'source':source,'database':database,'index':index,'projection':projection,'original_untouched':True,
        'current_only':current_only,'meaning':'Append-only HTTP create load on unchanged current source copy; no recovery or historical completeness claim' if current_only else 'Full frozen business source clone'},indent=2))
    return value['write_copies'][suffix]

def copy_connection(suffix):
    value=fixtures(); item=value['write_copies'][suffix]
    e=connection(item['tickets'])
    dsn='root@unix('+str(env.MYSQL/'mysql.sock')+')/'+item['database']
    e.update(TICKETPULSE_DSN=dsn,TICKETPULSE_BENCH_DSN=dsn,TICKETPULSE_SYNC_DSN=dsn,TICKETPULSE_SEARCH_INDEX=item['index'],
             TICKETPULSE_KAFKA_BROKERS='127.0.0.1:19092',TICKETPULSE_CDC_ADDR='127.0.0.1:'+str(env.PORT),
             TICKETPULSE_CDC_USER='m504_cdc',TICKETPULSE_CDC_PASSWORD='m504-local-replication')
    return e

def selective(n):
    value = fixtures()
    item = value['datasets'][str(n)]
    if 'selective_database' in item: raise RuntimeError('Preserve existing selective fixture')
    source = item['database']
    database = source + '_auth'
    schema(database)
    # Read-only derived current-state fixture. Original source/histories remain
    # untouched. Bucket remapping changes only test team identities, preserving
    # query text, status, timestamps, versions and frozen document count.
    ids = list(range(1, n + 1))
    random.Random(20261109).shuffle(ids)
    buckets = {ticket_id: rank % 100 for rank, ticket_id in enumerate(ids)}
    teams = [(1000 + b * 3 + code, 'M504 bucket ' + str(b) + '/' + str(code)) for b in range(100) for code in range(3)]
    teams.append((300, 'M504 stale projection team'))
    env.sql('USE ' + database + ';INSERT INTO teams(id,organization_id,name,active,created_at,updated_at) VALUES ' +
            ','.join('(' + str(tid) + ",1,'" + name + "',1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))" for tid, name in teams) + ';')
    env.sql('USE ' + database + ';CREATE TABLE m504_fixture_buckets(id BIGINT PRIMARY KEY,bucket INT NOT NULL);')
    for start in range(0, n, 1000):
        env.sql('USE ' + database + ';INSERT INTO m504_fixture_buckets VALUES ' + ','.join('(' + str(i) + ',' + str(buckets[i]) + ')' for i in range(start + 1, min(n + 1, start + 1001))) + ';')
    columns = env.sql("SELECT column_name FROM information_schema.columns WHERE table_schema='" + source + "' AND table_name='tickets' ORDER BY ordinal_position;").splitlines()
    expressions = []
    for col in columns:
        if col in ['initial_team_id', 'current_team_id']:
            expressions.append('1000+b.bucket*3+CASE t.' + col + ' WHEN 10 THEN 0 WHEN 20 THEN 1 ELSE 2 END')
        else: expressions.append('t.' + col)
    env.sql('USE ' + database + ';INSERT INTO tickets(' + ','.join(columns) + ') SELECT ' + ','.join(expressions) + ' FROM ' + source + '.tickets t JOIN m504_fixture_buckets b ON b.id=t.id;')
    env.sql('USE ' + database + ';INSERT INTO users(id,organization_id,name,role,active,auth_token_hash,created_at,updated_at) VALUES ' +
            ','.join('(' + str(uid) + ",1,'M504 profile " + p + "','agent',1,UNHEX(SHA2('m504-profile-" + p.lower() + "',256)),UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))" for p, uid in USERS.items()) + ';')
    membership = []
    for profile, percentage in PROFILES.items():
        uid = USERS[profile]
        membership.append((300, uid))
        for b in range(percentage):
            membership.extend((1000 + b * 3 + code, uid) for code in range(3))
    for start in range(0, len(membership), 1000):
        env.sql('USE ' + database + ';INSERT INTO team_members(organization_id,team_id,user_id,active,created_at) VALUES ' +
                ','.join('(1,' + str(tid) + ',' + str(uid) + ',1,UTC_TIMESTAMP(6))' for tid, uid in membership[start:start + 1000]) + ';')
    docs = documents(database)
    for d in docs: d['current_team_id'] = 300
    index = database + '_tickets_v1'
    projection = index_docs(index, docs)
    item.update(selective_database=database, selective_index=index)
    save(value)
    folder = OUT / 'datasets' / str(n) / 'selectivity'
    folder.mkdir(exist_ok=False)
    (folder / 'buckets.json').write_text(json.dumps(buckets, sort_keys=True))
    counts = {}
    for p, uid in USERS.items():
        query = ("SELECT count(*) FROM tickets t WHERE t.organization_id=1 AND (t.creator_id=" + str(uid) +
                 " OR EXISTS(SELECT 1 FROM team_members m JOIN teams tm ON tm.id=m.team_id AND tm.organization_id=m.organization_id "
                 "WHERE m.organization_id=t.organization_id AND m.team_id=t.current_team_id AND m.user_id=" + str(uid) +
                 " AND m.active=TRUE AND tm.active=TRUE));")
        counts[p] = int(env.sql('USE ' + database + ';' + query).strip())
        if counts[p] != n * PROFILES[p] // 100: raise RuntimeError('Exact authorization ratio mismatch')
    (folder / 'derivation.json').write_text(json.dumps({'original_database': source, 'derived_database': database,
        'source_rows': n, 'es_candidates': projection['documents'], 'authorized_counts': counts,
        'percentages': PROFILES, 'bucket_seed': 20261109, 'users': USERS,
        'model': 'read-only fault fixture: shuffled IDs in 100 equal buckets; source current team remapped to bucket; ES current_team_id=300',
        'limitation': 'Synthetic same-version incorrect ES ownership isolates selectivity. Actual versioned transfer and consumer lag tested separately; no claimed CDC state or business TPS.',
        'original_source_untouched': True, 'projection_sha256': projection['sha256']}, indent=2))
    print(json.dumps({'tickets': n, 'authorization_counts': counts}))

def app_owned(worker):
    try:
        cmd = subprocess.check_output(['ps', '-p', str(worker['pid']), '-o', 'command='], text=True)
        return str(ROOT / worker['binary']) in cmd and '-listen=127.0.0.1:' + str(env.APP_PORT) in cmd
    except subprocess.CalledProcessError: return False

def start_app(n, binary='diagnostic-baseline', selective=False, es_url=None, label=None, write_copy=None):
    value = fixtures()
    if value['workers'] or env.occupied(env.APP_PORT): raise RuntimeError('Stop owned app before next experiment')
    path = RUNTIME / binary
    if not path.is_file() or path.parent != RUNTIME: raise RuntimeError('Explicit private benchmark binary required')
    folder = OUT / 'environment/apps' / (label or str(time.time_ns()))
    folder.mkdir(parents=True, exist_ok=False)
    with (folder / 'stderr.jsonl').open('x') as log:
        child = subprocess.Popen([str(path), '-listen=127.0.0.1:' + str(env.APP_PORT), '-diagnostics=' + str(folder / 'search.jsonl')],
                                 cwd=ROOT, env=copy_connection(write_copy) if write_copy else connection(n, selective, es_url), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    value['workers'] = {'app': {'pid': child.pid, 'binary': str(path.relative_to(ROOT)),
                              'dataset': n, 'selective': selective, 'write_copy':write_copy, 'diagnostics': str((folder / 'search.jsonl').relative_to(ROOT))}}
    save(value)
    for _ in range(100):
        if child.poll() is not None: raise RuntimeError('Owned app exited')
        try:
            with urllib.request.urlopen(APP_URL + '/debug/bench', timeout=1) as response:
                if not json.load(response)['diagnostic_write_failed']: return value['workers']['app']
        except OSError: pass
        time.sleep(.1)
    raise RuntimeError('Owned app readiness deadline')

def stop_app():
    value = fixtures()
    for worker in value['workers'].values():
        if app_owned(worker):
            os.kill(worker['pid'], signal.SIGTERM)
            for _ in range(100):
                if not app_owned(worker): break
                time.sleep(.1)
            else: raise RuntimeError('Owned app shutdown deadline')
        elif env.occupied(env.APP_PORT): raise RuntimeError('Refusing unowned app shutdown')
    value['workers'] = {}
    save(value)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['create', 'selective', 'start-app', 'stop-app', 'status'])
    parser.add_argument('--tickets', type=int, choices=[10000, 100000], default=10000)
    parser.add_argument('--binary', default='diagnostic-baseline')
    parser.add_argument('--selective', action='store_true')
    args = parser.parse_args()
    if args.action == 'create': create(args.tickets)
    elif args.action == 'selective': selective(args.tickets)
    elif args.action == 'start-app': print(json.dumps(start_app(args.tickets, args.binary, args.selective)))
    elif args.action == 'stop-app': stop_app()
    else: print(json.dumps({k: v for k, v in fixtures().items() if k != 'cursor_key'}))
