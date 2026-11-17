#!/usr/bin/env python3
"""Own one isolated MySQL instance. Never use machine-wide defaults or existing databases."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tarfile
import time
import urllib.request
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / '.local'
VERSION = '8.4.5'
ARCHIVE_SHA256 = 'b0d73b0c1f70b77f6cb3fe0e8373d50fa20ce436512545c9d14bb88c1bba8eba'
DIST = LOCAL / f'mysql-{VERSION}-macos15-arm64'
ARCHIVE = LOCAL / f'mysql-{VERSION}-macos15-arm64.tar.gz'
URL = f'https://cdn.mysql.com/archives/mysql-8.4/{ARCHIVE.name}'
DATA = LOCAL / 'mysql-data'
MARKER = LOCAL / 'instance.json'
ADMIN = LOCAL / 'mysql-admin.cnf'
PORT = 33306
SOCKET = str(LOCAL / 'mysql.sock')


def private_file(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as output:
        output.write(value)


def mysql(statement):
    return subprocess.run([str(DIST / 'bin/mysql'), f'--defaults-file={ADMIN}',
                           '--batch', '--skip-column-names'], input=statement, text=True,
                          check=True, capture_output=True).stdout


def marker():
    if not MARKER.is_file():
        raise RuntimeError('No owned instance; run setup first')
    value = json.loads(MARKER.read_text())
    if value.get('project') != str(ROOT) or value.get('datadir') != str(DATA):
        raise RuntimeError('Instance ownership mismatch; refusing to operate')
    return value


def verify(value):
    actual = mysql('SELECT @@server_uuid;').strip()
    if actual != value['server_uuid']:
        raise RuntimeError('MySQL identity mismatch; refusing to operate')


def install():
    if (DIST / 'bin/mysqld').is_file():
        return
    if sys.platform != 'darwin' or os.uname().machine != 'arm64':
        raise RuntimeError('This local helper targets macOS arm64; use compose.yaml on other platforms')
    if not ARCHIVE.is_file():
        print('Downloading official MySQL Community Server', flush=True)
        temporary = ARCHIVE.with_suffix('.partial')
        urllib.request.urlretrieve(URL, temporary)
        temporary.rename(ARCHIVE)
    if DIST.exists():
        raise RuntimeError('MySQL distribution directory already contains data; refusing to overwrite')
    digest = hashlib.sha256()
    with ARCHIVE.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    if digest.hexdigest() != ARCHIVE_SHA256:
        raise RuntimeError('MySQL archive checksum differs from the verified M1-01 package')
    with tarfile.open(ARCHIVE) as archive:
        for item in archive.getmembers():
            target = (LOCAL / item.name).resolve()
            if not target.is_relative_to(DIST.resolve()):
                raise RuntimeError('Unsafe archive path')
            if item.issym() or item.islnk():
                link_root = target.parent if item.issym() else LOCAL
                if not (link_root / item.linkname).resolve().is_relative_to(DIST.resolve()):
                    raise RuntimeError('Unsafe archive link')
            elif not item.isdir() and not item.isfile():
                raise RuntimeError('Unsupported archive member')
        archive.extractall(LOCAL) # Python 3.9 compatible; paths, links and checksum checked above.


def start_process(init_file=None):
    with socket.socket() as check:
        if check.connect_ex(('127.0.0.1', PORT)) == 0:
            raise RuntimeError(f'Port {PORT} is occupied; refusing to touch its instance')
    args = [str(DIST / 'bin/mysqld'), '--no-defaults', f'--basedir={DIST}',
            f'--datadir={DATA}', f'--socket={SOCKET}', f'--pid-file={LOCAL / "mysql.pid"}',
            f'--log-error={LOCAL / "mysql.log"}', '--bind-address=127.0.0.1',
            f'--port={PORT}', '--mysqlx=0', f'--log-bin={LOCAL / "ticketpulse-bin"}',
            '--server-id=1', '--binlog-format=ROW', '--binlog-row-image=FULL',
            '--binlog-row-metadata=FULL', '--binlog-expire-logs-seconds=604800', '--sync-binlog=1',
            '--innodb-buffer-pool-size=64M',
            '--innodb-flush-log-at-trx-commit=1', '--max-connections=64']
    if init_file:
        args.append(f'--init-file={init_file}')
    with (LOCAL / 'mysql-launch.log').open('a') as output:
        child = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    for _ in range(100):
        if child.poll() is not None:
            raise RuntimeError(f'MySQL exited; inspect {LOCAL / "mysql.log"}')
        try:
            mysql('SELECT 1;')
            return
        except subprocess.CalledProcessError:
            time.sleep(0.2)
    child.terminate()
    raise RuntimeError('MySQL did not start within 20 seconds')


def setup():
    LOCAL.mkdir(exist_ok=True)
    if MARKER.exists():
        start()
        migrate()
        print('Existing owned fixture database preserved and migrated')
        return
    for path in [DATA, ADMIN, LOCAL / 'mysql.sock', LOCAL / 'mysql.pid']:
        if path.exists():
            raise RuntimeError(f'{path} already exists without ownership; refusing to overwrite')
    install()
    root_password, app_password = secrets.token_hex(24), secrets.token_hex(24)
    DATA.mkdir()
    subprocess.run([str(DIST / 'bin/mysqld'), '--no-defaults', '--initialize-insecure',
                    f'--basedir={DIST}', f'--datadir={DATA}'], check=True)
    private_file(ADMIN, f'[client]\nuser=root\npassword={root_password}\nsocket={SOCKET}\n')
    init_file = LOCAL / 'mysql-first-start.sql'
    private_file(init_file, f"ALTER USER 'root'@'localhost' IDENTIFIED BY '{root_password}';\n")
    start_process(init_file)
    init_file.unlink()
    uuid = mysql('SELECT @@server_uuid;').strip()
    mysql(f"CREATE DATABASE ticketpulse_local CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;\n"
          f"CREATE USER 'ticketpulse'@'localhost' IDENTIFIED BY '{app_password}';\n"
          "GRANT SELECT, INSERT, UPDATE ON ticketpulse_local.* TO 'ticketpulse'@'localhost';\n")
    mysql('USE ticketpulse_local;\n' + (ROOT / 'db/schema.sql').read_text() + '\n' + (ROOT / 'db/seed.sql').read_text() + '\n' + (ROOT / 'db/login_seed.sql').read_text())
    value = {'project': str(ROOT), 'datadir': str(DATA), 'port': PORT, 'server_uuid': uuid,
             'dsn': f'ticketpulse:{app_password}@unix({SOCKET})/ticketpulse_local',
             'admin_dsn': f'root:{root_password}@unix({SOCKET})/'}
    private_file(MARKER, json.dumps(value, indent=2) + '\n')
    print(f'MySQL {VERSION} ready on 127.0.0.1:{PORT}; database=ticketpulse_local')


def start():
    value = marker()
    try:
        verify(value)
    except subprocess.CalledProcessError:
        start_process()
        verify(value)
    print(f'Owned MySQL ready on 127.0.0.1:{PORT}')


def migrate():
    value = marker()
    verify(value)
    tables = set(mysql("SELECT table_name FROM information_schema.tables WHERE table_schema='ticketpulse_local';").splitlines())
    cols = set(mysql("SELECT column_name FROM information_schema.columns WHERE table_schema='ticketpulse_local' AND table_name='ticket_histories';").splitlines())
    if 'ticket_comments' not in tables or not {'reason', 'comment_id'}.issubset(cols):
        raise RuntimeError('Migrations through 004 are required; inspect and apply earlier SQL migrations first')
    exists = tables.intersection({'login_accounts', 'user_sessions'})
    if exists and exists != {'login_accounts', 'user_sessions'}:
        raise RuntimeError('Migration 005 is partially applied; inspect actual tables before resuming')
    if not exists:
        mysql('USE ticketpulse_local;\n' + (ROOT / 'db/migrations/005_web_sessions.sql').read_text())
    mysql('USE ticketpulse_local;\n' + (ROOT / 'db/login_seed.sql').read_text())
    print('Web accounts and sessions ready; existing users, passwords and tickets preserved')


def stop():
    value = marker()
    verify(value)
    subprocess.run([str(DIST / 'bin/mysqladmin'), f'--defaults-file={ADMIN}', 'shutdown'], check=True)
    print('Owned MySQL stopped; all local data preserved')


def schema_status():
    value = marker()
    verify(value)
    tables = mysql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'ticketpulse_local';")
    counts = mysql('USE ticketpulse_local; SELECT COUNT(*) FROM tickets; SELECT COUNT(*) FROM ticket_histories; SELECT COUNT(*) FROM ticket_create_requests;')
    print('Owned database tables: ' + ', '.join(tables.split()))
    print('tickets/history/requests: ' + '/'.join(counts.split()))


def execute(action, arguments):
    value = marker()
    verify(value)
    env = os.environ.copy()
    if action == 'test':
        env['TICKETPULSE_TEST_DSN'] = value['admin_dsn']
        env['TICKETPULSE_TEST_SERVER_UUID'] = value['server_uuid']
        command = ['go', 'test', '-race', '-tags=integration', '-count=1', '-v', './...'] + arguments
        artifacts = ROOT / 'artifacts'
        artifacts.mkdir(exist_ok=True)
        began = datetime.now(timezone.utc)
        with (artifacts / 'integration.log').open('w') as log:
            child = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True)
            for line in child.stdout:
                log.write(line)
                print(line, end='', flush=True)
            code = child.wait()
        report = {'command': command, 'exit_code': code, 'started_at': began.isoformat(),
                  'finished_at': datetime.now(timezone.utc).isoformat(),
                  'mysql_version': mysql('SELECT VERSION();').strip(),
                  'server_uuid': value['server_uuid'], 'output': 'artifacts/integration.log'}
        (artifacts / 'integration-result.json').write_text(json.dumps(report, indent=2) + chr(10))
        return code
    else:
        env['TICKETPULSE_DSN'] = value['dsn']
        command = ['go', 'run', './cmd/ticketpulse'] + arguments
    return subprocess.run(command, cwd=ROOT, env=env).returncode


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['setup', 'migrate', 'start', 'stop', 'test', 'run', 'info', 'schema_status'])
    args, extra = parser.parse_known_args()
    if args.action in ['test', 'run']:
        return execute(args.action, extra)
    if extra:
        parser.error('Unexpected arguments')
    if args.action == 'info':
        value = marker()
        print(json.dumps({key: value[key] for key in ['project', 'datadir', 'port', 'server_uuid']}, indent=2))
    else:
        globals()[args.action]()
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, subprocess.CalledProcessError, OSError) as error:
        print(f'Local MySQL: {error}', file=sys.stderr)
        sys.exit(1)
