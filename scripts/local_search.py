#!/usr/bin/env python3
"""Owned Binlog/ES runtime helpers. Legacy NATS assets retained for rollback only."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import sys
import tarfile
import time
import urllib.request

import local_mysql as local

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / '.local' / 'm2-runtime.json'
RUNTIME = ROOT / '.local' / 'search'
PACKAGES = {
    'nats': ('nats-server-v2.15.2-darwin-arm64',
             'https://github.com/nats-io/nats-server/releases/download/v2.15.2/nats-server-v2.15.2-darwin-arm64.tar.gz',
             'sha256', 'cbd2219d01502c38733485ac684269da585ee479fa3b1377264beecdefdf9e9d'),
    'es': ('elasticsearch-8.17.5',
           'https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.17.5-darwin-aarch64.tar.gz',
           'sha512', '4ec9e41d8ebacba0af3f410420f525ae27971fca90e3c8742dc0f322f9e5bbfe4c7a96041330652916ecdf16fdc261bb1808b1412df1181232c52574a9bf9856'),
}
PORTS = {'nats': 14222, 'es': 19200}


def occupied(port):
    with socket.socket() as sock:
        return sock.connect_ex(('127.0.0.1', port)) == 0


def download(url, partial):
    with urllib.request.urlopen(urllib.request.Request(url, method='HEAD'), timeout=30) as response:
        size = int(response.headers['Content-Length'])
        ranges = response.headers.get('Accept-Ranges') == 'bytes'
    if not ranges:
        urllib.request.urlretrieve(url, partial)
        return
    prefix = partial.stat().st_size if partial.exists() else 0
    if prefix > size:
        raise RuntimeError('Partial archive exceeds package size')
    # Each range is a separate file. Interrupted downloads resume only verified complete ranges.
    parts = partial.parent / (partial.name + '-parts')
    parts.mkdir(exist_ok=True)
    chunks = [(start, min(start + 4*1024*1024, size)) for start in range(prefix, size, 4*1024*1024)]
    def fetch(chunk):
        start, end = chunk
        path = parts / str(start)
        if path.exists() and path.stat().st_size == end-start:
            return
        request = urllib.request.Request(url, headers={'Range': 'bytes=' + str(start) + '-' + str(end-1)})
        for attempt in range(4):
            try:
                with urllib.request.urlopen(request, timeout=45) as source, path.open('wb') as out:
                    if source.status != 206 or source.headers.get('Content-Range') != 'bytes '+str(start)+'-'+str(end-1)+'/'+str(size):
                        raise RuntimeError('Download range differs from request')
                    while True:
                        block = source.read(1024*1024)
                        if not block:
                            break
                        out.write(block)
                if path.stat().st_size != end-start:
                    raise RuntimeError('Incomplete download range')
                return
            except OSError:
                if attempt == 3:
                    raise
                time.sleep(1)
    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(fetch, chunks))
    with partial.open('ab') as out:
        for start, end in chunks:
            path = parts / str(start)
            with path.open('rb') as source:
                while True:
                    block = source.read(1024*1024)
                    if not block:
                        break
                    out.write(block)
            path.unlink()
    parts.rmdir()


def install(kind):
    name, url, algorithm, expected = PACKAGES[kind]
    dest = RUNTIME / name
    stamp = dest / '.ticketpulse-package'
    if stamp.is_file() and stamp.read_text().strip() == expected:
        return
    if dest.exists():
        raise RuntimeError('Unverified distribution directory exists: ' + name)
    archive = RUNTIME / (name + '.tar.gz')
    if not archive.is_file():
        print('Downloading ' + name, flush=True)
        partial = archive.with_suffix('.partial')
        download(url, partial)
        partial.rename(archive)
    digest = hashlib.new(algorithm)
    with archive.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    if digest.hexdigest() != expected:
        raise RuntimeError('Package checksum mismatch: ' + name)
    with tarfile.open(archive) as package:
        for member in package.getmembers():
            target = (RUNTIME / member.name).resolve()
            if not target.is_relative_to(dest.resolve()):
                raise RuntimeError('Unsafe package path')
            if member.issym() or member.islnk():
                base = target.parent if member.issym() else RUNTIME
                if not (base / member.linkname).resolve().is_relative_to(dest.resolve()):
                    raise RuntimeError('Unsafe package link')
            elif not member.isdir() and not member.isfile():
                raise RuntimeError('Unsupported package member')
        package.extractall(RUNTIME)
    stamp.write_text(expected + chr(10))


def state():
    value = json.loads(STATE.read_text())
    owner = local.marker()
    local.verify(owner)
    if value.get('project') != str(ROOT) or value.get('server_uuid') != owner['server_uuid']:
        raise RuntimeError('Search runtime ownership mismatch')
    return value


def enable_binlog():
    owner = local.marker()
    local.verify(owner)
    before = local.mysql('USE ticketpulse_local; SELECT COUNT(*) FROM tickets; SELECT COUNT(*) FROM ticket_histories; SELECT COUNT(*) FROM ticket_comments;')
    if local.mysql('SELECT @@log_bin;').strip() != '1':
        local.stop()
        local.start()
    local.verify(owner)
    settings = local.mysql('SELECT @@log_bin,@@binlog_format,@@binlog_row_image,@@binlog_row_metadata,@@server_id,@@sync_binlog;').strip().split()
    if settings != ['1', 'ROW', 'FULL', 'FULL', '1', '1']:
        raise RuntimeError('Binlog settings differ from the required ROW/FULL durable configuration')
    after = local.mysql('USE ticketpulse_local; SELECT COUNT(*) FROM tickets; SELECT COUNT(*) FROM ticket_histories; SELECT COUNT(*) FROM ticket_comments;')
    if after != before:
        raise RuntimeError('Business row counts changed during owned MySQL restart')
    print('MySQL Binlog enabled; UUID and ticket/history/comment counts preserved', flush=True)


def setup():
    if sys.platform != 'darwin' or os.uname().machine != 'arm64':
        raise RuntimeError('Native helper targets macOS arm64; use documented component endpoints elsewhere')
    owner = local.marker()
    local.verify(owner)
    RUNTIME.mkdir(parents=True, exist_ok=True)
    enable_binlog()
    local.mysql('USE ticketpulse_local;\n' + (ROOT / 'db/migrations/006_search_sync.sql').read_text())
    if not STATE.is_file():
        password = secrets.token_hex(24)
        local.mysql("CREATE USER 'ticketpulse_cdc'@'127.0.0.1' IDENTIFIED BY '" + password + "';" +
                    "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'ticketpulse_cdc'@'127.0.0.1';" +
                    "GRANT SELECT ON ticketpulse_local.* TO 'ticketpulse_cdc'@'127.0.0.1';")
        local.private_file(STATE, json.dumps({'project': str(ROOT), 'server_uuid': owner['server_uuid'],
                                           'cdc_user': 'ticketpulse_cdc', 'cdc_password': password,
                                           'pids': {}}, indent=2))

    value = state()
    if 'sync_dsn' not in value:
        password = secrets.token_hex(24)
        local.mysql("CREATE USER 'ticketpulse_sync'@'localhost' IDENTIFIED BY '"+password+"';"+
                    "GRANT REPLICATION CLIENT ON *.* TO 'ticketpulse_sync'@'localhost';"+
                    "GRANT SELECT ON ticketpulse_local.tickets TO 'ticketpulse_sync'@'localhost';"+
                    "GRANT SELECT ON ticketpulse_local.ticket_histories TO 'ticketpulse_sync'@'localhost';"+
                    ''.join("GRANT SELECT,INSERT,UPDATE ON ticketpulse_local."+table+" TO 'ticketpulse_sync'@'localhost';" for table in ['sync_cdc_checkpoint','sync_worker_state','sync_deliveries']))
        value['sync_dsn'] = 'ticketpulse_sync:'+password+'@unix('+local.SOCKET+')/ticketpulse_local'
        local.private_file(STATE,json.dumps(value,indent=2))
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(install, ['es']))
    for directory in ['es-data', 'es-logs']:
        (RUNTIME / directory).mkdir(exist_ok=True)

    config = RUNTIME / PACKAGES['es'][0] / 'config' / 'elasticsearch.yml'
    config.write_text('cluster.name: ticketpulse-m2\nnode.name: ticketpulse-local\nnetwork.host: 127.0.0.1\nhttp.port: 19200\ntransport.port: 19300\ndiscovery.type: single-node\nxpack.security.enabled: false\nxpack.ml.enabled: false\ncluster.routing.allocation.disk.threshold_enabled: false\npath.data: ' + str(RUNTIME / 'es-data') + '\npath.logs: ' + str(RUNTIME / 'es-logs') + '\n')
    print('Verified Elasticsearch 8.17.5 installed; legacy NATS files retained, no NATS startup', flush=True)
    start()


def start_component(kind, value):
    if occupied(PORTS[kind]):
        pid = value['pids'].get(kind)
        if not pid or not owns_process(pid, kind):
            raise RuntimeError('Component port is occupied by an unowned process: ' + kind)
        return
    if kind == 'nats':
        command = [str(RUNTIME / PACKAGES[kind][0] / 'nats-server'), '-c', str(RUNTIME / 'nats.conf')]
    else:
        command = [str(RUNTIME / PACKAGES[kind][0] / 'bin/elasticsearch')]
    env = os.environ.copy()
    if kind == 'es':
        env['ES_JAVA_OPTS'] = '-Xms512m -Xmx512m'
    with (RUNTIME / (kind + '.log')).open('a') as log:
        child = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    value['pids'][kind] = child.pid
    local.private_file(STATE, json.dumps(value, indent=2))
    for _ in range(300):
        if child.poll() is not None:
            raise RuntimeError(kind + ' exited; inspect .local/search/' + kind + '.log')
        try:
            url = 'http://127.0.0.1:18222/healthz' if kind == 'nats' else 'http://127.0.0.1:19200/_cluster/health?wait_for_status=yellow&timeout=1s'
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    print(kind + ' ready on 127.0.0.1:' + str(PORTS[kind]), flush=True)
                    return
        except OSError:
            time.sleep(.2)
    raise RuntimeError(kind + ' did not become ready within the startup deadline')


def owns_process(pid, kind):
    try:
        cwd = subprocess.check_output(['lsof', '-a', '-p', str(pid), '-d', 'cwd', '-Fn'], text=True)
        command = subprocess.check_output(['ps', '-p', str(pid), '-o', 'command='], text=True)
        directory = RUNTIME / PACKAGES[kind][0]
        directories = {'n'+str(ROOT), 'n'+str(directory)}
        return bool(directories.intersection(cwd.splitlines())) and str(directory) in command
    except subprocess.CalledProcessError:
        return False


def start():
    value = state()
    for kind in ['es']:
        start_component(kind, value)


def stop(kind):
    value = state()
    for component in (['nats', 'es'] if kind == 'all' else [kind]):
        pid = value['pids'].get(component)
        if pid and owns_process(pid, component):
            os.kill(pid, signal.SIGTERM)
            for _ in range(100):
                if not occupied(PORTS[component]):
                    break
                time.sleep(.1)
            if occupied(PORTS[component]):
                raise RuntimeError('Owned component has not stopped: ' + component)
        elif occupied(PORTS[component]):
            raise RuntimeError('Refusing to stop an unowned component')
    print('Requested owned components stopped; all data retained')


def environment(database='ticketpulse_local'):
    if not re.fullmatch(r'ticketpulse_(local|test_[a-z0-9_]+)', database):
        raise RuntimeError('Only owned local/test schemas are allowed')
    value = state()
    owner = local.marker()
    env = os.environ.copy()
    env.update({'TICKETPULSE_DSN': owner['dsn'] if database == 'ticketpulse_local' else owner['admin_dsn'] + database,
                'TICKETPULSE_SYNC_DSN': value['sync_dsn'] if database == 'ticketpulse_local' else owner['admin_dsn'] + database,
                'TICKETPULSE_CDC_ADDR': '127.0.0.1:33306', 'TICKETPULSE_CDC_USER': value['cdc_user'],
                'TICKETPULSE_CDC_PASSWORD': value['cdc_password'],
                'TICKETPULSE_ES_URL': 'http://127.0.0.1:19200',
                'TICKETPULSE_SEARCH_INDEX': database + '_tickets_v1'})
    return env


def execute(action, extra):
    import local_kafka
    env = local_kafka.environment()
    target = 'ticketpulse' if action == 'web' else 'ticketpulse-sync'
    if not (ROOT / 'bin' / target).is_file():
        subprocess.run(['go', 'build', '-o', 'bin/' + target, './cmd/' + target], cwd=ROOT, check=True)
    return subprocess.run([str(ROOT / 'bin' / target)] + extra, cwd=ROOT, env=env).returncode


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['setup', 'start', 'stop', 'web', 'sync', 'status'])
    parser.add_argument('--component', choices=['all', 'nats', 'es'], default='all')
    args, extra = parser.parse_known_args()
    if args.action == 'setup':
        setup()
    elif args.action == 'start':
        start()
    elif args.action == 'stop':
        stop(args.component)
    elif args.action == 'status':
        return execute('sync', ['-mode=status'] + extra)
    else:
        return execute(args.action, extra)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, OSError, subprocess.CalledProcessError) as error:
        print('TicketPulse search runtime: ' + str(error), file=sys.stderr)
        sys.exit(1)
