#!/usr/bin/env python3
"""Frozen v1 10k/100k fixtures on separately owned MySQL and ClickHouse."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.parse
import urllib.request

import m503_environment as ck
import m502_mysql as independent
import local_mysql as demo

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / '.local/m5-03-mysql'
OUT = ROOT / 'artifacts/m5-03'
MARKER = ROOT / '.local/m5-03-fixtures.json'


def mysql_runtime():
    independent.R, independent.STATE = RUNTIME, RUNTIME / 'instance.json'
    independent.ADMIN, independent.PORT = RUNTIME / 'admin.cnf', 33308
    independent.OUT = OUT / 'environment/mysql-runtime'
    return independent


def sql(query):
    m = mysql_runtime()
    value = m.state()
    if m.sql('SELECT @@server_uuid;', 5).strip() != value['server_uuid']:
        raise RuntimeError('M5-03 MySQL UUID mismatch')
    return m.sql(query)


def fixtures():
    value = json.loads(MARKER.read_text())
    if value['project'] != str(ROOT) or value['source_uuid'] != mysql_runtime().state()['server_uuid']:
        raise RuntimeError('Fixture ownership mismatch')
    return value


def ckquery(query, database=None):
    params = {'wait_end_of_query': '1', 'log_queries': '0', 'max_threads': '2'}
    if database:
        if not database.startswith('ticketpulse_m503_'):
            raise RuntimeError('M5-03 CK namespace required')
        params['database'] = database
    request = urllib.request.Request(ck.URL + '/?' + urllib.parse.urlencode(params), data=query.encode())
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode()


def environment(n):
    value = fixtures()
    item = value['datasets'][str(n)]
    env = os.environ.copy()
    dsn = 'root@unix(' + str(RUNTIME / 'mysql.sock') + ')/' + item['database']
    env.update(TICKETPULSE_DSN=dsn, TICKETPULSE_BENCH_DSN=dsn,
               TICKETPULSE_TEST_SERVER_UUID=value['source_uuid'], TICKETPULSE_CK_URL=ck.URL,
               TICKETPULSE_CK_DATABASE=item['ck_database'], TICKETPULSE_KAFKA_BROKERS='127.0.0.1:19092',
               TICKETPULSE_ES_URL='http://127.0.0.1:19200', TICKETPULSE_SEARCH_INDEX=item['database'] + '_tickets_v1')
    return env


def create(n, attempt=1):
    m = mysql_runtime()
    owner = m.state()
    sql('SELECT 1;')
    if MARKER.exists():
        value = fixtures()
    else:
        value = {'project': str(ROOT), 'source_uuid': owner['server_uuid'], 'datasets': {}, 'workers': {}}
    if str(n) in value['datasets'] and attempt == 1:
        raise RuntimeError('Fixture exists; preserve it and use another explicit namespace')
    name = 'ticketpulse_test_m5_m503_' + str(n) + '_v' + str(attempt)
    ckname = 'ticketpulse_m503_' + str(n) + ('_v' + str(attempt) if attempt > 1 else '')
    if sql("SELECT count(*) FROM information_schema.schemata WHERE schema_name='" + name + "';").strip() != '0':
        raise RuntimeError('Refusing existing source schema')
    if ckquery("SELECT count() FROM system.databases WHERE name='" + ckname + "'").strip() != '0':
        raise RuntimeError('Refusing existing CK schema')
    sql('CREATE DATABASE ' + name + ' CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;')
    paths = ['db/schema.sql', 'db/seed.sql', 'db/login_seed.sql', 'db/migrations/006_search_sync.sql',
             'db/migrations/007_analytics_sync.sql', 'db/migrations/008_kafka_sync.sql',
             'db/migrations/009_rebuild.sql']
    sql('USE ' + name + ';\n' + '\n'.join((ROOT / p).read_text() for p in paths))
    if str(n) in value['datasets']:
        value.setdefault('failed_datasets', {})[str(n) + '_before_v' + str(attempt)] = value['datasets'][str(n)]
    value['datasets'][str(n)] = {'database': name, 'ck_database': ckname, 'tickets': n,
                                 'port': 18086, 'created_at': ck.stamp(), 'seed': 20261109}
    demo.private_file(MARKER, json.dumps(value, indent=2))
    folder = OUT / 'datasets' / str(n)
    if attempt > 1:
        folder = folder / ('attempt-v' + str(attempt))
    folder.mkdir(parents=True)
    command = [str(ROOT / '.local/m5-03-ck-bench-seed'), '-tickets=' + str(n), '-manifest=' + str(folder / 'manifest.json')]
    with (folder / 'seed.log').open('x') as log:
        result = subprocess.run(command, cwd=ROOT, env=environment(n), stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError('Seed failed; partial fixture preserved')
    actual = json.loads((folder / 'manifest.json').read_text())['dataset']
    baseline = json.loads((ROOT / 'artifacts/m5-01' / ('dataset-100000.json' if n == 100000 else 'dataset-dedicated-10000.json')).read_text())
    if actual['sha256'] != baseline['sha256']:
        raise RuntimeError('Frozen generator bundle SHA256 differs')
    # Keep physical replay visible. It must have no effect on business counts.
    for table in ['ticket_events', 'ticket_states']:
        ckquery('INSERT INTO ' + table + ' SELECT * FROM ' + table + ' WHERE organization_id=1 AND ticket_id<=50', ckname)
    # Nonempty organization 2: deterministic 10% subset, independent keys and
    # mapped teams; this copy is only test data, never a new production model.
    team = lambda field: 'transform(' + field + ',[toUInt64(10),20,90],[toUInt64(200),210,290],' + field + ')'
    fcols = ['organization_id', 'ticket_id', 'version', 'business_event_id', 'transport_event_id', 'action',
             'occurred_at', 'event_team_id', 'initial_team_id', 'before_team_id', 'before_status', 'after_status',
             'assignee_id', 'created_at', 'first_resolved_at', 'ingested_at']
    scols = ['organization_id', 'ticket_id', 'version', 'complete', 'current_team_id', 'assignee_id', 'status',
             'initial_team_id', 'created_at', 'updated_at', 'first_resolved_at', 'ingested_at']
    for table, columns in [('ticket_events', fcols), ('ticket_states', scols)]:
        expr = []
        for field in columns:
            if field == 'organization_id':
                expr.append('toUInt64(2)')
            elif field == 'business_event_id':
                expr.append("concat('2:',toString(ticket_id),':',toString(version))")
            elif field in ['event_team_id', 'initial_team_id', 'before_team_id', 'current_team_id']:
                expr.append(team(field))
            else:
                expr.append(field)
        ckquery('INSERT INTO ' + table + ' (' + ','.join(columns) + ') SELECT ' + ','.join(expr)
                + ' FROM ' + table + ' WHERE organization_id=1 AND ticket_id<=' + str(n // 10), ckname)
    sql('USE ' + name + ";INSERT INTO teams(id,organization_id,name,active,created_at,updated_at) VALUES (210,2,'Other device',1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)),(290,2,'Other default',1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6));")
    details = {'frozen_bundle_matches': True, 'baseline_manifest': str((ROOT / 'artifacts/m5-01' / ('dataset-100000.json' if n == 100000 else 'dataset-dedicated-10000.json')).relative_to(ROOT)),
               'source_counts': sql('USE ' + name + ';SELECT count(*) FROM tickets;SELECT count(*) FROM ticket_histories;'),
               'organization_2': {'derivation': 'organization 1 ticket_id <=10%; team10/20/90 mapped to200/210/290', 'tickets': n // 10},
               'physical': ckquery("SELECT table,count() AS active_parts,sum(rows) AS rows,sum(bytes_on_disk) AS bytes FROM system.parts WHERE active AND database='" + ckname + "' GROUP BY table FORMAT JSONEachRow")}
    (folder / 'verification.json').write_text(json.dumps(details, indent=2) + '\n')
    print(json.dumps(details, indent=2))


def start_app(n, binary):
    value = fixtures()
    if value['workers']:
        raise RuntimeError('Stop owned benchmark app before starting another')
    if ck.occupied(18086):
        raise RuntimeError('Benchmark app port already occupied')
    path = ROOT / binary
    if not path.is_file() or path.parent != ROOT / '.local':
        raise RuntimeError('Explicit local benchmark binary required')
    folder = OUT / 'environment/app'
    folder.mkdir(parents=True, exist_ok=True)
    logpath = folder / (path.name + '-' + str(n) + '-' + str(time.time_ns()) + '.log')
    with logpath.open('x') as log:
        child = subprocess.Popen([str(path), '-listen=127.0.0.1:18086', '-diagnostic-log'],
                                 cwd=ROOT, env=environment(n), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    value['workers'] = {'app': {'pid': child.pid, 'binary': binary, 'dataset': n, 'log': str(logpath.relative_to(ROOT))}}
    demo.private_file(MARKER, json.dumps(value, indent=2))
    for _ in range(100):
        if child.poll() is not None:
            raise RuntimeError('Benchmark app exited; inspect its log')
        try:
            urllib.request.urlopen('http://127.0.0.1:18086/debug/bench', timeout=1).close()
            print(json.dumps(value['workers']))
            return
        except OSError:
            time.sleep(.1)
    raise RuntimeError('App readiness deadline')


def stop_app():
    value = fixtures()
    for worker in value['workers'].values():
        try:
            command = subprocess.check_output(['ps', '-p', str(worker['pid']), '-o', 'command='], text=True)
        except subprocess.CalledProcessError:
            continue
        if str(ROOT / worker['binary']) not in command or '-listen=127.0.0.1:18086' not in command:
            raise RuntimeError('Refusing unowned app process')
        os.kill(worker['pid'], signal.SIGTERM)
        for _ in range(100):
            if not ck.occupied(18086):
                break
            time.sleep(.1)
        else:
            raise RuntimeError('App shutdown deadline')
    value['workers'] = {}
    demo.private_file(MARKER, json.dumps(value, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['mysql-start', 'mysql-stop', 'create', 'start-app', 'stop-app', 'status'])
    parser.add_argument('--tickets', type=int, choices=[10000, 100000], default=10000)
    parser.add_argument('--binary', default='.local/m5-03-bench-app-original')
    parser.add_argument('--attempt', type=int, default=1)
    args = parser.parse_args()
    if args.action == 'mysql-start':
        mysql_runtime().start('reasonable')
    elif args.action == 'mysql-stop':
        mysql_runtime().stop()
    elif args.action == 'create':
        create(args.tickets, args.attempt)
    elif args.action == 'start-app':
        start_app(args.tickets, args.binary)
    elif args.action == 'stop-app':
        stop_app()
    else:
        print(json.dumps(fixtures(), indent=2))
