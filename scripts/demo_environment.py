#!/usr/bin/env python3
"""Small DEMO-01 native fixture; never uses the retained demo resources.

Reuse pinned distribution and process helpers. All mutable paths, credentials,
ports and evidence are redirected before calling any existing start helper.
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
import subprocess
import sys
import tarfile

import m403c3_environment as native

ROOT = Path(__file__).resolve().parents[1]
CURRENT = ROOT / '.local/demo-01/current.json'
PORTS = dict(mysql=33316, kafka=19110, controller=19111, es=19210,
             es_transport=19312, ck=18133, ck_native=19011, api=18101)


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def marker():
    value = json.loads(CURRENT.read_text())
    if not re.fullmatch(r'[0-9]{8}T[0-9]{6}\.[0-9]{6}Z', value['run']):
        raise RuntimeError('Invalid private DEMO run identity')
    for key, parent in [('runtime', '.local/demo-01'), ('evidence', 'artifacts/demo-01')]:
        if value[key] != parent + '/' + value['run']:
            raise RuntimeError('Private DEMO paths differ')
    return value


def configure():
    value = marker()
    runtime, output = ROOT / value['runtime'], ROOT / value['evidence']
    native.RUNTIME, native.OUT = runtime, output
    native.MYSQL, native.ES = runtime / 'mysql', runtime / 'elasticsearch'
    native.CK, native.KAFKA = runtime / 'clickhouse', runtime / 'kafka'
    native.MYSQL_PORT, native.ES_PORT, native.ES_TRANSPORT = PORTS['mysql'], PORTS['es'], PORTS['es_transport']
    native.CK_PORT, native.CK_NATIVE = PORTS['ck'], PORTS['ck_native']
    native.KAFKA_PORT, native.CONTROLLER_PORT, native.APP_PORT = PORTS['kafka'], PORTS['controller'], PORTS['api']
    native.ES_URL = 'http://127.0.0.1:' + str(PORTS['es'])
    native.CK_URL = 'http://127.0.0.1:' + str(PORTS['ck'])
    native.ENV = runtime / 'environment.json'
    def safe_paths():
        if runtime.resolve() != ROOT / '.local/demo-01' / value['run'] or output.resolve() != ROOT / 'artifacts/demo-01' / value['run']:
            raise RuntimeError('DEMO path ownership differs')
        if len(set(PORTS.values())) != len(PORTS):
            raise RuntimeError('DEMO ports overlap')
        if any(p in [18080, 60555, 33306, 19092, 19200, 18123, 33315, 19108, 19109, 19209, 18132] for p in PORTS.values()):
            raise RuntimeError('Retained endpoints prohibited')
    native.safe_paths = safe_paths
    safe_paths()
    return value, runtime, output


def save(folder, name, value):
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / name).open('x') as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write(chr(10))


def create_run():
    if CURRENT.exists():
        raise RuntimeError('A DEMO run already exists; inspect it without replacing')
    run = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    value = dict(run=run, runtime='.local/demo-01/' + run, evidence='artifacts/demo-01/' + run)
    for key in ['runtime', 'evidence']:
        (ROOT / value[key]).mkdir(parents=True)
    native.private_file(CURRENT, json.dumps(value, indent=2) + chr(10))
    print(json.dumps(value))


def packages():
    """Install immutable native distributions only; never call demo setup."""
    (ROOT / '.local').mkdir(exist_ok=True)
    native.distribution_mysql.install()
    native.distribution_es.RUNTIME.mkdir(parents=True, exist_ok=True)
    native.distribution_es.install('es')
    native.distribution_ck.source.install()
    kafka = native.distribution_kafka
    kafka.RUNTIME.mkdir(parents=True, exist_ok=True)
    for name, url, algorithm, expected, directory in kafka.PACKAGES:
        archive = kafka.RUNTIME / name
        if not archive.exists():
            temporary = archive.with_suffix('.partial')
            native.distribution_es.download(url, temporary)
            temporary.rename(archive)
        if native.sha(archive, algorithm) != expected:
            raise RuntimeError('Pinned distribution checksum differs')
        if directory.exists():
            continue
        with tarfile.open(archive) as package:
            for entry in package.getmembers():
                if not (kafka.RUNTIME / entry.name).resolve().is_relative_to(kafka.RUNTIME.resolve()):
                    raise RuntimeError('Archive path escapes distribution directory')
                if entry.issym() or entry.islnk():
                    base = (kafka.RUNTIME / entry.name).parent if entry.issym() else kafka.RUNTIME
                    if not (base / entry.linkname).resolve().is_relative_to(kafka.RUNTIME.resolve()):
                        raise RuntimeError('Archive link escapes distribution directory')
            package.extractall(kafka.RUNTIME)
    print(json.dumps({'state': 'PINNED_DISTRIBUTIONS_READY', 'service_setup_called': False}))


def start():
    value, runtime, output = configure()
    native.budget()
    for name, port in PORTS.items():
        if native.occupied(port) and not (runtime / 'environment.json').exists():
            raise RuntimeError('Unowned occupied port: ' + name + ':' + str(port))
    mysql = native.mysql_runtime()
    # Existing helper prints only settings; capture it inside this run's log.
    if not native.occupied(PORTS['mysql']):
        with contextlib.redirect_stdout(io.StringIO()) as log:
            mysql.start('baseline')
        (output / 'mysql-start.json').write_text(log.getvalue())
    native.es_start()
    native.ck_start()
    native.kafka_start()
    proof = native.verify()
    if native.ENV.exists():
        print(json.dumps({'state': 'OWNED_RUNTIME_VERIFIED', 'runtime': value['runtime']}))
        return
    schema = 'ticketpulse_test_demo_' + secrets.token_hex(8)
    if native.sql("SELECT count(*) FROM information_schema.schemata WHERE schema_name='" + schema + "'").strip() != '0':
        raise RuntimeError('Random schema already exists; no overwrite')
    password = secrets.token_hex(24)
    replication_user = 'demo_cdc_' + secrets.token_hex(5)
    native.sql("CREATE USER '" + replication_user + "'@'127.0.0.1' IDENTIFIED BY '" + password + "';"
               "GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO '" + replication_user + "'@'127.0.0.1';")
    native.sql('CREATE DATABASE ' + schema + ' CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;')
    initialization = ['db/schema.sql', 'db/seed.sql', 'db/login_seed.sql',
                      'db/migrations/006_search_sync.sql', 'db/migrations/007_analytics_sync.sql',
                      'db/migrations/008_kafka_sync.sql', 'tests/analytics/prd_ten_tickets.sql']
    native.sql('USE ' + schema + ';' + chr(10).join((ROOT / p).read_text() for p in initialization))
    native.sql('USE ' + schema + ';ALTER TABLE ticket_create_requests AUTO_INCREMENT=100;')
    login_password = secrets.token_urlsafe(18)
    hashed = subprocess.run(['/usr/sbin/htpasswd', '-niBC', '12', 'demo'],
                            input=login_password + chr(10), capture_output=True, text=True, check=True)
    bcrypt = hashed.stdout.strip().split(':', 1)[1]
    if not re.fullmatch(r'\$2[aby]\$12\$[./A-Za-z0-9]{53}', bcrypt):
        raise RuntimeError('Private login password hashing failed')
    native.sql("USE " + schema + ";UPDATE login_accounts SET password_hash='" + bcrypt + "';")
    dsn = 'root@unix(' + str(native.MYSQL / 'mysql.sock') + ')/' + schema
    env = {'TICKETPULSE_DSN': dsn, 'TICKETPULSE_SYNC_DSN': dsn, 'TICKETPULSE_CONTROL_DSN': dsn,
           'TICKETPULSE_CDC_ADDR': '127.0.0.1:' + str(PORTS['mysql']), 'TICKETPULSE_CDC_USER': replication_user,
           'TICKETPULSE_CDC_PASSWORD': password, 'TICKETPULSE_ES_URL': native.ES_URL,
           'TICKETPULSE_SEARCH_INDEX': schema + '_tickets_v1', 'TICKETPULSE_CK_URL': native.CK_URL,
           'TICKETPULSE_CK_DATABASE': schema + '_analytics_v1',
           'TICKETPULSE_KAFKA_BROKERS': '127.0.0.1:' + str(PORTS['kafka']),
           'TICKETPULSE_CURSOR_KEY': secrets.token_hex(32), 'TICKETPULSE_REQUIRE_WRITE_GATE': 'true',
           'TICKETPULSE_TARGET_VERSION': '', 'DEMO_SCHEMA': schema, 'DEMO_API': '127.0.0.1:' + str(PORTS['api']),
           'DEMO_RUNTIME': str(runtime), 'DEMO_EVIDENCE': str(output), 'DEMO_HANDOFF': 'demo_handoff',
           'DEMO_A_VERSION': 'active_original', 'DEMO_B_VERSION': 'candidate_v1', 'DEMO_JOB': 'demo_rebuild'}
    native.private_file(native.ENV, json.dumps(env, indent=2) + chr(10))
    native.private_file(runtime / 'login.json', json.dumps({'password': login_password, 'accounts': ['alice', 'it-agent', 'device-agent', 'supervisor']}) + chr(10))
    save(output, 'environment.json', {'at': stamp(), 'state': 'PRIVATE_RUNTIME_READY', 'ports': PORTS,
                                     'runtime': value['runtime'], 'schema': schema, 'topic': 'ticketpulse.' + schema + '.tickets.v1',
                                     'identity': proof, 'initialization': initialization,
                                     'ticket_count': 10, 'secrets_saved_only_in_private_runtime': True,
                                     'installation_009_to_015_pending': True})
    print(json.dumps({'state': 'PRIVATE_RUNTIME_READY', 'schema': schema, 'evidence': value['evidence']}))


def environment():
    _, runtime, _ = configure()
    native.verify()
    env = {k: v for k, v in os.environ.items() if not k.startswith('TICKETPULSE_')}
    env.update(json.loads((runtime / 'environment.json').read_text()))
    if env['DEMO_SCHEMA'] != env['TICKETPULSE_DSN'].split('/')[-1] or not re.fullmatch(r'ticketpulse_test_demo_[0-9a-f]{16}', env['DEMO_SCHEMA']):
        raise RuntimeError('Private schema/DSN differs')
    return env


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['new', 'packages', 'start', 'verify'])
    args = parser.parse_args()
    if args.mode == 'new':
        create_run()
    elif args.mode == 'packages':
        packages()
    elif args.mode == 'start':
        start()
    else:
        _, _, output = configure()
        save(output / 'observations', 'environment-' + str(datetime.datetime.now().timestamp()) + '.json', native.verify())
        print(json.dumps({'state': 'OWNED_RUNTIME_VERIFIED'}))


if __name__ == '__main__':
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        # Avoid echoing a SQL statement, environment or credentials on failure.
        print('DEMO environment failed: ' + type(error).__name__, file=sys.stderr)
        sys.exit(1)
