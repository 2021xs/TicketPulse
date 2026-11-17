#!/usr/bin/env python3
"""Disposable DEMO-02 resources. No retained source, offset or API operation.

Reuse the native distribution/identity helpers, redirect every mutable path
before starting, and load only a fresh synthetic source. No M4 control setup.
"""
import argparse
import contextlib
import datetime
import io
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import m403c3_environment as native

MARKER = ROOT / os.environ.get('DEMO02_MARKER', '.local/demo-02/current.json')
PORTS = dict(mysql=33317, kafka=19112, controller=19113, es=19211,
             es_transport=19313, ck=18134, ck_native=19012, api=18102)


def configure():
    if not MARKER.resolve().is_relative_to(ROOT / '.local/demo-02'):
        raise RuntimeError('Private marker must stay inside .local/demo-02')
    value = json.loads(MARKER.read_text())
    if not re.fullmatch(r'[0-9]{8}T[0-9]{6}\.[0-9]{6}Z', value['run']):
        raise RuntimeError('Invalid private test run')
    folder, output = ROOT / value['runtime'], ROOT / value['evidence']
    if folder.resolve() != ROOT / '.local/demo-02' / value['run'] or output.resolve() != ROOT / 'artifacts/demo-02' / value['run']:
        raise RuntimeError('Private paths differ')
    native.RUNTIME, native.OUT = folder, output
    native.MYSQL, native.ES = folder / 'mysql', folder / 'elasticsearch'
    native.CK, native.KAFKA = folder / 'clickhouse', folder / 'kafka'
    native.MYSQL_PORT, native.ES_PORT, native.ES_TRANSPORT = PORTS['mysql'], PORTS['es'], PORTS['es_transport']
    native.CK_PORT, native.CK_NATIVE = PORTS['ck'], PORTS['ck_native']
    native.KAFKA_PORT, native.CONTROLLER_PORT, native.APP_PORT = PORTS['kafka'], PORTS['controller'], PORTS['api']
    native.ES_URL = 'http://127.0.0.1:' + str(PORTS['es'])
    native.CK_URL = 'http://127.0.0.1:' + str(PORTS['ck'])
    def safe():
        if len(set(PORTS.values())) != len(PORTS) or any(p in [18080, 60555, 18101, 33306, 33315, 33316, 19092, 19108, 19110, 19200, 19209, 19210, 18123, 18132, 18133] for p in PORTS.values()):
            raise RuntimeError('Retained endpoints prohibited')
    native.safe_paths = safe
    safe()
    return folder, output


def new():
    if not MARKER.resolve().is_relative_to(ROOT / '.local/demo-02') or MARKER.exists():
        raise RuntimeError('Private marker outside test root or already exists')
    run = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    value = {'run': run, 'runtime': '.local/demo-02/' + run,
             'evidence': 'artifacts/demo-02/' + run}
    for key in ['runtime', 'evidence']:
        (ROOT / value[key]).mkdir(parents=True)
    MARKER.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(MARKER, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as output:
        json.dump(value, output, indent=2)
        output.write(chr(10))
    print(json.dumps(value))


def save(output, name, value):
    with (output / name).open('x') as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write(chr(10))


def identity(pid):
    parts = subprocess.check_output(['ps', '-p', str(pid), '-o', 'ppid=,lstart=,comm='], text=True).strip().split()
    if len(parts) < 7:
        raise RuntimeError('Process identity unavailable')
    path = Path(' '.join(parts[6:])).resolve()
    return {'pid': pid, 'parent_pid': int(parts[0]), 'started': ' '.join(parts[1:6]),
            'executable': str(path), 'sha256': native.sha(path)}


def environment():
    folder, _ = configure()
    native.verify()
    value = json.loads((folder / 'environment.json').read_text())
    if not re.fullmatch(r'ticketpulse_test_demo02_[0-9a-f]{16}', value['DEMO02_SCHEMA']) or value['TICKETPULSE_CDC_ADDR'] != '127.0.0.1:33317' or value['DEMO02_API'] != 'http://127.0.0.1:18102':
        raise RuntimeError('Private resource identity differs')
    env = {k: v for k, v in os.environ.items() if not k.startswith('TICKETPULSE_')}
    env.update(value)
    return env


def launch(label, binary, args, env):
    folder, output = configure()
    marker = folder / (label + '-process.json')
    if marker.exists():
        raise RuntimeError('No duplicate automatic launch')
    command = [str(folder / 'bin' / binary)] + args
    with (output / (label + '.log')).open('x') as log:
        child = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    value = {'identity': identity(child.pid), 'command': command, 'schema': env['DEMO02_SCHEMA']}
    native.private_file(marker, json.dumps(value, indent=2) + chr(10))
    return child, value


def start():
    folder, output = configure()
    if (folder / 'environment.json').exists():
        raise RuntimeError('Existing DEMO-02 run; observe instead of overwrite')
    native.budget()
    for name, port in PORTS.items():
        if native.occupied(port):
            raise RuntimeError('Unowned occupied endpoint: ' + name)
    mysql = native.mysql_runtime()
    with contextlib.redirect_stdout(io.StringIO()) as log:
        mysql.start('baseline')
    (output / 'mysql-start.json').write_text(log.getvalue())
    native.es_start()
    native.ck_start()
    native.kafka_start()
    proof = native.verify()
    schema = 'ticketpulse_test_demo02_' + secrets.token_hex(8)
    native.sql('CREATE DATABASE ' + schema + ' CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;')
    initialization = ['db/schema.sql', 'db/seed.sql', 'db/login_seed.sql',
                      'db/migrations/006_search_sync.sql', 'db/migrations/007_analytics_sync.sql',
                      'db/migrations/008_kafka_sync.sql', 'tests/analytics/prd_ten_tickets.sql']
    native.sql('USE ' + schema + ';' + chr(10).join((ROOT / p).read_text() for p in initialization))
    native.sql('USE ' + schema + ';ALTER TABLE ticket_create_requests AUTO_INCREMENT=100;')
    password = secrets.token_urlsafe(18)
    result = subprocess.run(['/usr/sbin/htpasswd', '-niBC', '12', 'test'], input=password + chr(10),
                            capture_output=True, text=True, check=True)
    hashed = result.stdout.strip().split(':', 1)[1]
    if not re.fullmatch(r'\$2[aby]\$12\$[./A-Za-z0-9]{53}', hashed):
        raise RuntimeError('Private login hash invalid')
    native.sql("USE " + schema + ";UPDATE login_accounts SET password_hash='" + hashed + "';")
    cdc_user, cdc_password = 'demo02_' + secrets.token_hex(5), secrets.token_hex(24)
    native.sql("CREATE USER '" + cdc_user + "'@'127.0.0.1' IDENTIFIED BY '" + cdc_password + "';"
               "GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO '" + cdc_user + "'@'127.0.0.1';")
    dsn = 'root@unix(' + str(native.MYSQL / 'mysql.sock') + ')/' + schema
    env = dict(TICKETPULSE_DSN=dsn, TICKETPULSE_SYNC_DSN=dsn,
               TICKETPULSE_CDC_ADDR='127.0.0.1:33317', TICKETPULSE_CDC_USER=cdc_user, TICKETPULSE_CDC_PASSWORD=cdc_password,
               TICKETPULSE_ES_URL=native.ES_URL, TICKETPULSE_SEARCH_INDEX=schema + '_tickets_v1',
               TICKETPULSE_CK_URL=native.CK_URL, TICKETPULSE_CK_DATABASE=schema + '_analytics_v1',
               TICKETPULSE_KAFKA_BROKERS='127.0.0.1:19112', TICKETPULSE_CURSOR_KEY=secrets.token_hex(32),
               TICKETPULSE_REQUIRE_WRITE_GATE='false', TICKETPULSE_TARGET_VERSION='',
               DEMO02_API='http://127.0.0.1:18102', DEMO02_SCHEMA=schema,
               DEMO02_PASSWORD=password, DEMO02_EVIDENCE=str(output))
    native.private_file(folder / 'environment.json', json.dumps(env, indent=2) + chr(10))
    binaries = folder / 'bin'
    binaries.mkdir()
    for name in ['ticketpulse', 'ticketpulse-sync']:
        command = ['go', 'build', '-o', str(binaries / name), './cmd/' + name]
        built = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        save(output, 'build-' + name + '.json', {'command': command, 'exit_code': built.returncode, 'stdout': built.stdout, 'stderr': built.stderr})
        if built.returncode:
            raise RuntimeError('Official binary build failed')
    env = environment()
    children, processes = [], []
    for mode in ['capture', 'consume', 'ck']:
        child, value = launch(mode, 'ticketpulse-sync', ['-mode=' + mode, '-server-id=15002'], env)
        children.append(child)
        processes.append(value)
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if any(child.poll() is not None for child in children):
            raise RuntimeError('Private worker exited; inspect preserved log')
        rows = native.sql('USE ' + schema + ";SELECT count(*) FROM sync_worker_state WHERE component IN ('cdc','es','ck') AND state='running';SELECT count(*) FROM sync_kafka_analytics_bootstrap;").splitlines()
        if rows == ['3', '1']:
            break
        time.sleep(.2)
    else:
        raise RuntimeError('Private worker startup deadline')
    api, value = launch('api', 'ticketpulse', ['-listen=127.0.0.1:18102', '-routing=configs/local.json'], env)
    processes.append(value)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if api.poll() is not None:
            raise RuntimeError('Private API exited')
        try:
            if native.get(env['DEMO02_API'] + '/readyz')['state'] == 'ready':
                break
        except OSError:
            pass
        time.sleep(.1)
    else:
        raise RuntimeError('Private API startup deadline')
    save(output, 'environment.json', {'state': 'READY', 'at': native.stamp(), 'ports': PORTS, 'source_schema': schema,
                                     'identity': proof, 'processes': processes, 'initialization': initialization,
                                     'web_sha256': native.sha(ROOT / 'web/app.js'), 'no_M4_control_initialization': True})
    print(json.dumps({'state': 'READY', 'api': env['DEMO02_API'], 'schema': schema}))


def stop():
    folder, output = configure()
    env = environment()
    stopped = []
    for label in ['api', 'ck', 'consume', 'capture']:
        value = json.loads((folder / (label + '-process.json')).read_text())
        expected = value['identity']
        actual = identity(expected['pid'])
        if actual['started'] != expected['started'] or actual['executable'] != expected['executable'] or actual['sha256'] != expected['sha256']:
            raise RuntimeError('Private process identity changed; no signal')
        os.kill(expected['pid'], signal.SIGTERM)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                os.kill(expected['pid'], 0)
            except ProcessLookupError:
                stopped.append({'label': label, 'identity': expected, 'actual_exit': True})
                break
            time.sleep(.1)
        else:
            raise RuntimeError('Private child exit unproven; no broad signal')
    with contextlib.redirect_stdout(io.StringIO()) as log:
        native.stop()
    (output / 'native-stop.log').write_text(log.getvalue())
    save(output, 'stopped.json', {'at': native.stamp(), 'processes': stopped, 'data_and_evidence_retained': True})
    print(json.dumps({'state': 'PRIVATE_PROCESSES_STOPPED', 'data_retained': True}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['new', 'start', 'status', 'stop'])
    args = parser.parse_args()
    try:
        if args.mode == 'new':
            new()
        elif args.mode == 'start':
            start()
        elif args.mode == 'stop':
            stop()
        else:
            environment()
            print(json.dumps({'state': 'PRIVATE_IDENTITY_VERIFIED'}))
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        # Do not expose a SQL string, login body, process environment or secret.
        print('DEMO-02 fixture failed: ' + type(error).__name__, file=sys.stderr)
        sys.exit(1)
