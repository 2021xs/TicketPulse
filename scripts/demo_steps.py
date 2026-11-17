#!/usr/bin/env python3
"""DEMO-01 operator convenience commands over the existing CLIs.

No automatic promote/rollback sequence. Every control step remains an explicit
ticketpulse-control invocation, with its original persistent safety checks.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time

import demo_environment as runtime

ROOT = runtime.ROOT


def display_json(value):
    if isinstance(value, dict):
        return {key: ('<redacted>' if key in ['worker_token', 'password', 'csrf_token', 'auth_token'] else display_json(item))
                for key, item in value.items()}
    if isinstance(value, list):
        return [display_json(item) for item in value]
    return value


def context():
    value, directory, output = runtime.configure()
    env = runtime.environment()
    # Synthetic fixture tokens come from the existing seed owner. Never print
    # them or copy them into the demo runbook and evidence.
    tokens = dict(re.findall(r"\((\d+), 1, [^\n]+?UNHEX\(SHA2\('([^']+)'", (ROOT / 'db/seed.sql').read_text()))
    env['TICKETPULSE_READ_PROBE_TOKEN'] = tokens['5']
    env['TICKETPULSE_READ_PERMISSION_TOKEN'] = tokens['2']
    return env, directory, output


def record(output, label, value):
    folder = output / 'commands'
    folder.mkdir(exist_ok=True)
    path = folder / (str(time.time_ns()) + '-' + label + '.json')
    with path.open('x') as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write(chr(10))
    return path


def invoke(target, args, env=None, display=True):
    current, directory, output = context()
    if env:
        current.update(env)
    command = [str(directory / 'bin' / target)] + args
    before = datetime.datetime.now(datetime.timezone.utc).isoformat()
    result = subprocess.run(command, cwd=ROOT, env=current, capture_output=True, text=True, timeout=330)
    public_stdout = result.stdout
    try:
        public_stdout = json.dumps(display_json(json.loads(result.stdout)), ensure_ascii=False, indent=2) + chr(10)
    except ValueError:
        pass
    raw = directory / 'raw-cli'
    raw.mkdir(exist_ok=True)
    runtime.native.private_file(raw / (str(time.time_ns()) + '-' + target + '.json'),
                                json.dumps({'stdout': result.stdout, 'stderr': result.stderr}, ensure_ascii=False) + chr(10))
    path = record(output, target, {'at': before, 'command': command, 'resource_schema': current['DEMO_SCHEMA'],
                                 'exit_code': result.returncode, 'stdout': public_stdout, 'stderr': result.stderr,
                                 'private_raw_retained': True})
    if result.returncode:
        print(json.dumps({'state': 'FAIL', 'evidence': str(path.relative_to(ROOT)), 'exit_code': result.returncode}), file=sys.stderr)
        raise RuntimeError('Official CLI refused; inspect preserved private evidence, keep freeze')
    if display:
        print(public_stdout, end='')
    return result


def target_environment(version):
    env, _, _ = context()
    id = env['DEMO_A_VERSION'] if version == 'A' else env['DEMO_B_VERSION']
    value = json.loads(invoke('ticketpulse-control', ['-mode=version', '-version=' + id], display=False).stdout)
    contract = value['contract']
    return {'TICKETPULSE_ES_URL': contract['es']['endpoint'], 'TICKETPULSE_SEARCH_INDEX': contract['es']['index'],
            'TICKETPULSE_CK_URL': contract['ck']['endpoint'], 'TICKETPULSE_CK_DATABASE': contract['ck']['database']}


def process_identity(pid):
    fields = subprocess.check_output(['ps', '-p', str(pid), '-o', 'ppid=,lstart=,comm='], text=True).strip().split()
    if len(fields) < 7:
        raise RuntimeError('Private process identity unavailable')
    return {'pid': pid, 'parent_pid': int(fields[0]), 'started': ' '.join(fields[1:6]), 'executable': ' '.join(fields[6:])}


def spawn(label, target, args, overrides=None):
    env, directory, output = context()
    env.update(overrides or {})
    marker = directory / (label + '-process.json')
    if marker.exists():
        raise RuntimeError('Process already recorded; no duplicate automatic launch')
    command = [str(directory / 'bin' / target)] + args
    log = (output / (label + '.log')).open('x')
    child = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    log.close()
    identity = process_identity(child.pid)
    value = {'identity': identity, 'command': command, 'schema': env['DEMO_SCHEMA'], 'automatic_restart': False}
    runtime.native.private_file(marker, json.dumps(value, indent=2) + chr(10))
    record(output, label, value)
    return value


def api(action, version=''):
    _, directory, output = context()
    request = {'Action': action, 'Version': version}
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(45)
        connection.connect(str(directory / 'launcher.sock'))
        connection.sendall(json.dumps(request).encode() + bytes([10]))
        with connection.makefile('r') as file:
            value = json.loads(file.readline())
    path = record(output, 'api-' + action, {'request': request, 'result': value, 'at': runtime.stamp()})
    if 'error' in value:
        raise RuntimeError('API lifecycle refused; inspect ' + str(path.relative_to(ROOT)))
    print(json.dumps(value, ensure_ascii=False, indent=2))
    return value


def build():
    _, directory, output = context()
    binary = directory / 'bin'
    if binary.exists():
        raise RuntimeError('Immutable binary directory already exists; no overwrite of running executables')
    binary.mkdir()
    for target in ['ticketpulse', 'ticketpulse-sync', 'ticketpulse-control', 'ticketpulse-rebuild', 'demo-launcher']:
        source = './scripts/demo_launcher' if target == 'demo-launcher' else './cmd/' + target
        command = ['go', 'build', '-o', str(binary / target), source]
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        path = record(output, 'build-' + target, {'command': command, 'exit_code': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr})
        if result.returncode:
            raise RuntimeError('Build failed; inspect ' + str(path.relative_to(ROOT)))
    print(json.dumps({'state': 'PRIVATE_BINARIES_BUILT', 'directory': str(binary.relative_to(ROOT))}))


def install():
    invoke('ticketpulse-rebuild', ['-mode=migrate'])
    for mode in ['migrate', 'migrate-handoff', 'migrate-read-control', 'migrate-read-runtime', 'migrate-read-rollback']:
        invoke('ticketpulse-control', ['-mode=' + mode])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['build', 'install', 'workers-a', 'workers-b', 'launcher',
                                       'api-start', 'api-stop', 'api-status', 'control', 'rebuild', 'sync', 'status', 'inspect'])
    parser.add_argument('--version', choices=['A', 'B'])
    parser.add_argument('--attempt', default='', help='explicit new process attempt label; preserves failed logs')
    args, extra = parser.parse_known_args()
    env, directory, output = context()
    if args.mode == 'build':
        build()
    elif args.mode == 'install':
        install()
    elif args.mode == 'workers-a':
        values = [spawn('a-' + mode, 'ticketpulse-sync', ['-mode=' + mode, '-server-id=15001']) for mode in ['capture', 'consume', 'ck']]
        print(json.dumps(values, indent=2))
    elif args.mode == 'workers-b':
        if args.attempt and not re.fullmatch(r'[a-z0-9_]{1,20}', args.attempt):
            raise RuntimeError('Invalid explicit attempt label')
        overrides = target_environment('B')
        suffix = '-' + args.attempt if args.attempt else ''
        values = [spawn('b-' + mode + suffix, 'ticketpulse-sync', ['-mode=' + mode, '-handoff=' + env['DEMO_HANDOFF']], overrides) for mode in ['consume', 'ck']]
        print(json.dumps(values, indent=2))
    elif args.mode == 'launcher':
        value = spawn('launcher', 'demo-launcher', ['-environment=' + str(directory / 'environment.json'), '-bin-root=' + str(directory / 'bin')])
        print(json.dumps(value, indent=2))
    elif args.mode.startswith('api-'):
        action = args.mode.split('-')[1]
        if action == 'start' and not args.version:
            raise RuntimeError('api-start requires --version A or B')
        version = env['DEMO_A_VERSION'] if args.version == 'A' else env['DEMO_B_VERSION'] if args.version == 'B' else ''
        api(action, version)
    elif args.mode == 'control':
        invoke('ticketpulse-control', extra)
    elif args.mode == 'rebuild':
        invoke('ticketpulse-rebuild', extra, target_environment(args.version) if args.version else None)
    elif args.mode == 'sync':
        if not any(value in ['-mode=status', '-mode=analytics-status'] for value in extra):
            raise RuntimeError('sync convenience mode only permits read-only status')
        invoke('ticketpulse-sync', extra, target_environment(args.version) if args.version else None)
    elif args.mode == 'status':
        invoke('ticketpulse-control', ['-mode=read-switch-status'] + extra)
    else:
        schema = env['DEMO_SCHEMA']
        sql = 'USE ' + schema + ';SELECT id,title,status,current_team_id,assignee_id,version FROM tickets ORDER BY id;SELECT ticket_id,version,action FROM ticket_histories ORDER BY ticket_id,version;SELECT ticket_id,version,content FROM ticket_comments ORDER BY id;'
        value = {'at': runtime.stamp(), 'schema': schema, 'mysql': runtime.native.sql(sql)}
        path = record(output, 'mysql-inspect', value)
        print(json.dumps({'state': 'MYSQL_OBSERVED', 'evidence': str(path.relative_to(ROOT))}))


if __name__ == '__main__':
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print('DEMO step failed: ' + str(error), file=sys.stderr)
        sys.exit(1)
