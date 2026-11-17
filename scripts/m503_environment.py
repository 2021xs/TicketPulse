#!/usr/bin/env python3
"""Owned M5-03 ClickHouse runtime; pinned 4 GiB, no previous evidence writes."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.request

import bench_ck_runtime as pinned
import local_mysql as mysql

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/m5-03'
RUNTIME = ROOT / '.local/m5-03-clickhouse'
STATE = RUNTIME / 'instance.json'
CONFIG = RUNTIME / 'config.xml'
PORT, NATIVE_PORT = 18125, 19003
URL = 'http://127.0.0.1:' + str(PORT)


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def occupied(port):
    with socket.socket() as connection:
        return connection.connect_ex(('127.0.0.1', port)) == 0


def configure_owner():
    pinned.RUNTIME, pinned.STATE, pinned.CONFIG = RUNTIME, STATE, CONFIG
    pinned.PORT, pinned.NATIVE_PORT = PORT, NATIVE_PORT


def state():
    value = json.loads(STATE.read_text())
    if (value.get('project') != str(ROOT) or value.get('data') != str(RUNTIME / 'data')
            or value.get('config') != str(CONFIG) or value.get('http_port') != PORT
            or value.get('native_port') != NATIVE_PORT):
        raise RuntimeError('M5-03 ClickHouse ownership mismatch')
    return value


def health():
    configure_owner()
    value = state()
    return {'url': URL, 'server_pid': pinned.server_pid(value.get('pid')), 'available': occupied(PORT)}


def query(sql):
    request = urllib.request.Request(URL + '/?wait_end_of_query=1&log_queries=0', data=sql.encode())
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.read().decode()


def start():
    configure_owner()
    binary = pinned.verify_binary()
    if occupied(PORT) or occupied(NATIVE_PORT):
        value = state()
        pid = pinned.server_pid(value.get('pid'))
        if not pid:
            raise RuntimeError('M5-03 ports occupied by an unowned process')
        print(json.dumps({'existing': True, 'server_pid': pid, 'url': URL}))
        return
    if STATE.exists():
        value = state()
    else:
        if (RUNTIME / 'data').exists():
            raise RuntimeError('Unowned M5-03 data directory; refusing startup')
        value = {'project': str(ROOT), 'data': str(RUNTIME / 'data'), 'config': str(CONFIG),
                 'http_port': PORT, 'native_port': NATIVE_PORT, 'pid': None,
                 'created_at': stamp(), 'verified_distribution': binary}
    RUNTIME.mkdir(parents=True, exist_ok=True)
    for name in ['data', 'logs', 'tmp', 'user-files', 'format-schemas']:
        (RUNTIME / name).mkdir(exist_ok=True)
    content = pinned.config_text().replace('<http_port>18124</http_port>', '<http_port>18125</http_port>')
    content = content.replace('<tcp_port>19002</tcp_port>', '<tcp_port>19003</tcp_port>')
    if CONFIG.exists() and CONFIG.read_text() != content:
        raise RuntimeError('Existing M5-03 resource configuration differs')
    CONFIG.write_text(content)
    mysql.private_file(STATE, json.dumps(value, indent=2))
    before = {'demo_web': occupied(18080), 'demo_ck': occupied(18123),
              'demo_config_sha256': pinned.checksum(pinned.source.RUNTIME / 'config.xml')}
    with (RUNTIME / 'launch.log').open('a') as log:
        child = subprocess.Popen([str(pinned.BINARY), 'server', '--config-file=' + str(CONFIG)],
                                 cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    value.update(pid=child.pid, started_at=stamp())
    mysql.private_file(STATE, json.dumps(value, indent=2))
    for _ in range(200):
        if child.poll() is not None:
            raise RuntimeError('M5-03 ClickHouse exited; inspect its launch.log')
        try:
            if query('SELECT 1').strip() == '1':
                break
        except OSError:
            time.sleep(.1)
    else:
        raise RuntimeError('M5-03 ClickHouse readiness deadline')
    pid = pinned.server_pid(child.pid)
    if not pid:
        raise RuntimeError('Listening M5-03 server ownership not verified')
    after = {'demo_web': occupied(18080), 'demo_ck': occupied(18123),
             'demo_config_sha256': pinned.checksum(pinned.source.RUNTIME / 'config.xml')}
    if before != after:
        raise RuntimeError('Demonstration runtime changed during CK startup')
    output = OUT / 'environment/ck-runtime' / datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    output.mkdir(parents=True)
    (output / 'config.xml').write_text(content)
    evidence = {'before': before, 'after': after, 'distribution': binary, 'server_pid': pid,
                'watchdog_pid': child.pid, 'url': URL, 'limits': {'server_memory_bytes': 4 * 1024**3,
                'query_memory_bytes': 512 * 1024**2, 'query_threads': 2, 'concurrent_queries': 64},
                'resource_boundary': 'native ClickHouse memory tracker, not a cgroup RSS limit',
                'config_sha256': hashlib.sha256(content.encode()).hexdigest(), 'captured_at': stamp()}
    (output / 'startup.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps(evidence, indent=2))


def stop():
    configure_owner()
    value = state()
    pid = pinned.server_pid(value.get('pid'))
    if pid:
        os.kill(pid, signal.SIGTERM)
        for _ in range(400):
            try:
                os.kill(pid, 0)
                alive = True
            except ProcessLookupError:
                alive = False
            if not alive and not occupied(PORT):
                value.update(pid=None, stopped_at=stamp())
                mysql.private_file(STATE, json.dumps(value, indent=2))
                print('Owned M5-03 ClickHouse stopped; all test and demonstration data retained')
                return
            time.sleep(.1)
        raise RuntimeError('Owned ClickHouse shutdown deadline')
    if occupied(PORT) or occupied(NATIVE_PORT):
        raise RuntimeError('Refusing to stop an unowned process')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'stop', 'status'])
    args = parser.parse_args()
    if args.action == 'start':
        start()
    elif args.action == 'stop':
        stop()
    else:
        configure_owner()
        value = state()
        print(json.dumps({'url': URL, 'server_pid': pinned.server_pid(value.get('pid')),
                          'available': occupied(PORT)}, indent=2))
