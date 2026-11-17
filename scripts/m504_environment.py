#!/usr/bin/env python3
"""Owned M5-04 MySQL / Elasticsearch runtimes. Demo ports are never mutated."""
import argparse
import datetime
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import socket
import subprocess
import time
import urllib.request

import local_mysql as demo
import local_search as pinned
import m502_mysql as mysql

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/m5-04'
RUNTIME = ROOT / '.local/m5-04'
MYSQL = RUNTIME / 'mysql'
ES = RUNTIME / 'elasticsearch'
PORT, ES_PORT, TRANSPORT_PORT, APP_PORT = 33309, 19202, 19302, 18087
ES_URL = 'http://127.0.0.1:' + str(ES_PORT)
STATE = ES / 'instance.json'

def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def occupied(port):
    with socket.socket() as connection:
        return connection.connect_ex(('127.0.0.1', port)) == 0

def mysql_runtime():
    mysql.R, mysql.STATE, mysql.ADMIN = MYSQL, MYSQL / 'instance.json', MYSQL / 'admin.cnf'
    mysql.PORT, mysql.OUT = PORT, OUT / 'environment/mysql-runtime'
    return mysql

def sql(query, timeout=60):
    m = mysql_runtime()
    value = m.state()
    if m.sql('SELECT @@server_uuid;', 3).strip() != value['server_uuid']:
        raise RuntimeError('M5-04 MySQL source UUID mismatch')
    return m.sql(query, timeout)

def es_state():
    value = json.loads(STATE.read_text())
    if (value.get('project') != str(ROOT) or value.get('config') != str(ES / 'config')
            or value.get('data') != str(ES / 'data') or value.get('port') != ES_PORT):
        raise RuntimeError('M5-04 Elasticsearch ownership mismatch')
    return value

def json_get(path, timeout=5):
    with urllib.request.urlopen(ES_URL + path, timeout=timeout) as response:
        return json.load(response)

def es_owned(pid):
    try:
        command = subprocess.check_output(['ps', '-p', str(pid), '-o', 'command='], text=True)
        return ('org.elasticsearch.bootstrap.Elasticsearch' in command
                and '-Djava.io.tmpdir=' + str(ES / 'tmp') in command
                and str(pinned.RUNTIME / pinned.PACKAGES['es'][0]) in command)
    except subprocess.CalledProcessError:
        return False

def es_start():
    distribution = pinned.RUNTIME / pinned.PACKAGES['es'][0]
    expected = pinned.PACKAGES['es'][3]
    if (distribution / '.ticketpulse-package').read_text().strip() != expected:
        raise RuntimeError('Unverified Elasticsearch distribution')
    if occupied(ES_PORT) or occupied(TRANSPORT_PORT):
        value = es_state()
        if value.get('server_pid') and es_owned(value['server_pid']):
            return
        node = next(iter(json_get('/_nodes/process,settings')['nodes'].values()))
        if not es_owned(node['process']['id']) or node['settings']['path']['data'] != str(ES / 'data'):
            raise RuntimeError('Unowned Elasticsearch port is occupied')
        value.update(server_pid=node['process']['id'], version=node['version'])
        demo.private_file(STATE, json.dumps(value, indent=2))
        # A prior readiness check may fail after the owned process started.
        # Record its effective settings before returning, preserving startup.
        folder = OUT / 'environment/elasticsearch-runtime' / str(time.time_ns())
        folder.mkdir(parents=True)
        nodes = json_get('/_nodes/process,jvm,settings,thread_pool,http')
        (folder / 'nodes.json').write_text(json.dumps(nodes, indent=2))
        (folder / 'elasticsearch.yml').write_text((ES / 'config/elasticsearch.yml').read_text())
        (folder / 'startup.json').write_text(json.dumps({'state': value, 'archive_sha512': expected,
            'heap_bytes': 512 * 1024**2, 'recovered_readiness_check': True,
            'resource_boundary': 'native macOS, no cgroup; MySQL64MiB, application pool20/10',
            'safety': {'min_free_disk_bytes': 20 * 1024**3, 'max_test_disk_bytes': 8 * 1024**3,
                       'max_total_owned_rss_bytes': 8 * 1024**3, 'max_es_heap_used_fraction': .90}}, indent=2))
        print(json.dumps({'es_pid': value['server_pid'], 'es_port': ES_PORT, 'adopted_owned_startup': True}))
        return
    if STATE.exists():
        value = es_state()
    else:
        if (ES / 'data').exists():
            raise RuntimeError('Refusing unowned Elasticsearch data directory')
        value = {'project': str(ROOT), 'data': str(ES / 'data'), 'config': str(ES / 'config'),
                 'port': ES_PORT, 'transport_port': TRANSPORT_PORT, 'created_at': stamp()}
    for directory in ['config', 'config/jvm.options.d', 'data', 'logs', 'tmp']:
        (ES / directory).mkdir(parents=True, exist_ok=True)
    for name in ['jvm.options', 'log4j2.properties']:
        source, target = distribution / 'config' / name, ES / 'config' / name
        content=source.read_text()
        if name=='jvm.options':
            # The ES launcher uses distribution cwd; give JVM dumps/GC logs
            # explicit private paths so this process never writes demo logs.
            content=content.replace('-XX:HeapDumpPath=data','-XX:HeapDumpPath='+str(ES/'data'))
            content=content.replace('-XX:ErrorFile=logs/hs_err_pid%p.log','-XX:ErrorFile='+str(ES/'logs/hs_err_pid%p.log'))
            content=content.replace('file=logs/gc.log','file='+str(ES/'logs/gc.log'))
            content=content.replace('-XX:ReplayDataFile=logs/replay_pid%p.log','-XX:ReplayDataFile='+str(ES/'logs/replay_pid%p.log'))
        if target.exists() and target.read_text() != content:
            raise RuntimeError('Existing private Elasticsearch base config differs')
        target.write_text(content)
    config = ('cluster.name: ticketpulse-m504\nnode.name: ticketpulse-m504\nnetwork.host: 127.0.0.1\n'
              'http.port: 19202\ntransport.port: 19302\ndiscovery.type: single-node\n'
              'xpack.security.enabled: false\nxpack.ml.enabled: false\n'
              'cluster.routing.allocation.disk.threshold_enabled: true\n'
              'path.data: ' + str(ES / 'data') + '\npath.logs: ' + str(ES / 'logs') + '\n')
    target = ES / 'config/elasticsearch.yml'
    if target.exists() and target.read_text() != config:
        raise RuntimeError('Existing Elasticsearch resource configuration differs')
    target.write_text(config)
    env = os.environ.copy()
    env.update(ES_PATH_CONF=str(ES / 'config'), ES_JAVA_OPTS='-Xms512m -Xmx512m',
               ES_TMPDIR=str(ES / 'tmp'))
    def file_budget():
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, hard), hard))
    with (ES / 'launch.log').open('a') as log:
        child = subprocess.Popen([str(distribution / 'bin/elasticsearch')], cwd=ROOT, env=env,
                                 stdout=log, stderr=subprocess.STDOUT, start_new_session=True, preexec_fn=file_budget)
    value.update(launcher_pid=child.pid, started_at=stamp())
    demo.private_file(STATE, json.dumps(value, indent=2))
    for _ in range(300):
        if child.poll() is not None:
            raise RuntimeError('Owned Elasticsearch exited; inspect private launch.log')
        try:
            health = json_get('/_cluster/health?wait_for_status=yellow&timeout=1s', 2)
            if health['status'] in ['yellow', 'green']:
                break
        except OSError:
            pass
        time.sleep(.2)
    else:
        raise RuntimeError('Owned Elasticsearch readiness deadline')
    nodes = json_get('/_nodes/process,jvm,settings,thread_pool,http')
    if len(nodes['nodes']) != 1:
        raise RuntimeError('Expected exactly one isolated node')
    node = next(iter(nodes['nodes'].values()))
    pid = node['process']['id']
    if not es_owned(pid) or node['settings']['path']['data'] != str(ES / 'data'):
        raise RuntimeError('Listening Elasticsearch process ownership not verified')
    value.update(server_pid=pid, version=node['version'])
    demo.private_file(STATE, json.dumps(value, indent=2))
    folder = OUT / 'environment/elasticsearch-runtime' / str(time.time_ns())
    folder.mkdir(parents=True)
    (folder / 'elasticsearch.yml').write_text(config)
    (folder / 'nodes.json').write_text(json.dumps(nodes, indent=2))
    (folder / 'startup.json').write_text(json.dumps({'state': value, 'archive_sha512': expected,
        'heap_bytes': 512 * 1024**2, 'mysql_buffer_bytes': 64 * 1024**2,
        'resource_boundary': 'native macOS processes, no cgroup; application pool 20 open/10 idle',
        'safety': {'min_free_disk_bytes': 20 * 1024**3, 'max_test_disk_bytes': 8 * 1024**3,
                   'max_total_owned_rss_bytes': 8 * 1024**3, 'max_es_heap_used_fraction': .90}}, indent=2))
    print(json.dumps({'es_pid': pid, 'es_port': ES_PORT, 'mysql_port': PORT, 'heap': 512 * 1024**2}))

def es_stop():
    value = es_state()
    pid = value.get('server_pid')
    if pid and es_owned(pid):
        os.kill(pid, signal.SIGTERM)
        for _ in range(300):
            if not es_owned(pid) and not occupied(ES_PORT):
                value.update(server_pid=None, stopped_at=stamp())
                demo.private_file(STATE, json.dumps(value, indent=2))
                return
            time.sleep(.1)
        raise RuntimeError('Owned Elasticsearch shutdown deadline')
    if occupied(ES_PORT):
        raise RuntimeError('Refusing to stop unowned Elasticsearch process')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'stop', 'status'])
    args = parser.parse_args()
    if args.action == 'start':
        m = mysql_runtime()
        if not occupied(PORT): m.start('baseline')
        es_start()
    elif args.action == 'stop':
        es_stop()
        mysql_runtime().stop()
    else:
        print(json.dumps({'mysql': mysql_runtime().state(), 'es': es_state()}))
