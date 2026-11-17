#!/usr/bin/env python3
"""Retain and independently observe one new C3 A-to-B-to-A service fixture.

This never targets a preceding fixture, demo instance, group or index. The
long-lived Go fixture is an explicit same-host launcher, without auto restart.
Its ready file is observed independently and is not counted as a completed test.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.request

import m403c3_environment as runtime
import m403c3_suite as suite

ROOT, OUT, RUNTIME = runtime.ROOT, runtime.OUT, runtime.RUNTIME
CURRENT = RUNTIME / 'retained-live.json'


def read(path):
    return json.loads(path.read_text())


def write(path, value):
    with path.open('x') as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write(chr(10))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def process(expected):
    pid = int(expected['pid'])
    command = ['ps', '-p', str(pid), '-o', 'ppid=,lstart=,comm=']
    fields = subprocess.check_output(command, text=True).strip().split()
    if len(fields) < 7:
        raise RuntimeError('Retained process identity is ambiguous')
    executable = Path(' '.join(fields[6:])).resolve()
    same = (' '.join(fields[1:6]) == expected['started']
            and str(executable) == expected['executable']
            and digest(executable) == expected['executable_sha256'])
    if not same or RUNTIME.resolve() not in executable.parents:
        raise RuntimeError('Retained process identity differs; no signal or mutation')
    return {'pid': pid, 'parent_pid': int(fields[0]), 'started': expected['started'],
            'executable': str(executable), 'sha256': digest(executable), 'same': True}


def http(base, path, token=''):
    request = urllib.request.Request(base + path)
    if token:
        request.add_header('Authorization', 'Bearer ' + token)
    with urllib.request.urlopen(request, timeout=15) as response:
        return {'path': path, 'status': response.status,
                'headers': {name: value for name, value in response.headers.items()
                            if name.lower().startswith('x-ticketpulse-')},
                'body': json.load(response)}


def observe():
    runtime.verify()
    marker = read(CURRENT)
    folder = ROOT / marker['evidence']
    retained = read(folder / 'verification/TestM403C3RetainedPrivateRoundtrip/retained-live.json')
    if not re.fullmatch(r'ticketpulse_test_[0-9a-f]+', retained['schema']):
        raise RuntimeError('Retained private schema is not a new fixture')
    if retained['topic'] != 'ticketpulse.' + retained['schema'] + '.tickets.v1':
        raise RuntimeError('Retained private Topic identity differs')
    processes = [process(retained['launcher'])] + [process(p) for p in retained['processes']]
    base = 'http://' + retained['api']
    ready = http(base, '/readyz')
    target = http(base, '/api/v1/read-target', 'local-supervisor')
    search = http(base, '/api/v1/search/tickets?sort=id&order=asc&page_size=100', 'local-supervisor')
    analysis = http(base, '/api/v1/analytics?start=2026-11-01&end=2026-11-04', 'local-supervisor')
    selection = retained['selection']
    if selection['version_id'] != 'active_original' or selection['read_revision'] != 3:
        raise RuntimeError('Retained roundtrip selection is not A revision 3')
    for item in [ready, target, search, analysis]:
        headers = {key.lower(): value for key, value in item['headers'].items()}
        if headers.get('x-ticketpulse-read-version') != 'active_original' or headers.get('x-ticketpulse-read-revision') != '3' or headers.get('x-ticketpulse-boot-id') != retained['api_boot_id']:
            raise RuntimeError('Live HTTP data/read identity differs')
    if target['body']['version_id'] != 'active_original' or target['body']['read_revision'] != 3:
        raise RuntimeError('Live authoritative API identity differs')
    freshness, status = search['body']['freshness'], analysis['body']['sync']
    for value, component in [(freshness, 'es'), (status, 'ck')]:
        if value['version_id'] != 'active_original' or value['read_revision'] != 3 or value['state_namespace'] != 'legacy' or value['consumer_group'] != retained['topic'] + '.' + component + '.v1':
            raise RuntimeError('Retained A data uses another target status')
    sql = runtime.sql('USE `' + retained['schema'] + '`; SELECT state,revision FROM sync_business_write_gate WHERE gate_id=1;SELECT version_id,read_revision FROM sync_projection_read_selection WHERE selector_id=1;')
    if sql.splitlines()[0].split()[0] != 'OPEN' or sql.splitlines()[1].split() != ['active_original', '3']:
        raise RuntimeError('Retained gate/selector is not the completed roundtrip')
    groups = [retained['topic'] + '.es.v1', retained['topic'] + '.ck.v1',
              retained['handoff']['es_group'], retained['handoff']['ck_group']]
    lines, states, positions = [], {}, {}
    for label, group in zip(['A_es', 'A_ck', 'B_es', 'B_ck'], groups):
        command = ['--bootstrap-server', '127.0.0.1:' + str(runtime.KAFKA_PORT), '--describe', '--group', group]
        description = runtime.kafka_command('kafka-consumer-groups.sh', command).stdout
        current = [line for line in description.splitlines() if line.split() and line.split()[0] == group]
        state = runtime.kafka_command('kafka-consumer-groups.sh', command + ['--state']).stdout
        if 'Stable' not in state:
            raise RuntimeError('Retained group is not stable: ' + label)
        states[label] = state
        positions[label] = {}
        for line in current:
            fields = line.split()
            if len(fields) < 7 or fields[1] != retained['topic']:
                raise RuntimeError('Retained group position unavailable')
            partition, committed, end, lag = map(int, fields[2:6])
            before = retained['progress']['groups'][label][retained['topic']][str(partition)]
            if before['Err'] is not None or committed < before['At'] or committed > end or lag != end-committed:
                raise RuntimeError('Retained group position changed incorrectly')
            positions[label][str(partition)] = {'committed': committed, 'end': end, 'lag': lag, 'member': fields[6]}
        lines.extend(current)
    if len(lines) != len(groups) * 3:
        raise RuntimeError('Retained groups do not have all three positions')
    # Reuse the public, read-only Reconcile command for both actual targets. It
    # does not reactivate an M4 job, change Offset, or emit a control marker.
    env = runtime.environment()
    env['TICKETPULSE_SYNC_DSN'] = env['TICKETPULSE_TEST_DSN'] + retained['schema']
    binary = Path(marker['binaries']) / 'ticketpulse-rebuild'
    reports = {}
    for label, version in zip(['A', 'B'], retained['contracts']):
        contract = version['contract']
        env.update(TICKETPULSE_ES_URL=contract['es']['endpoint'], TICKETPULSE_SEARCH_INDEX=contract['es']['index'],
                   TICKETPULSE_CK_URL=contract['ck']['endpoint'], TICKETPULSE_CK_DATABASE=contract['ck']['database'],
                   TICKETPULSE_TARGET_VERSION='')
        command = [str(binary), '-mode=check', '-target=both', '-batch-size=250', '-wait=50s', '-organization=1', '-start=2026-11-01', '-end=2026-11-04']
        result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=70)
        if result.returncode:
            raise RuntimeError('Retained ' + label + ' read-only reconcile failed: ' + result.stderr)
        reports[label] = json.loads(result.stdout)
        if reports[label]['state'] != 'consistent' or reports[label]['difference_count'] != 0:
            raise RuntimeError('Retained ' + label + ' full facts differ')
    value = {'at': runtime.stamp(), 'state': 'RETAINED_LIVE_VERIFIED', 'evidence': marker['evidence'],
             'processes': processes, 'selection': selection, 'gate_and_selection': sql,
             'http': {'ready': ready, 'target': target, 'search': search, 'analytics': analysis},
             'raw_group_describe': chr(10).join(lines), 'raw_group_states': states, 'positions': positions, 'A_B_reconcile': reports,
             'all_four_groups_retained': True, 'all_services_running': True, 'no_automatic_restart': True}
    observation = folder / ('observation-' + str(time.time_ns()) + '.json')
    write(observation, value)
    print(json.dumps({'state': value['state'], 'observation': str(observation.relative_to(ROOT)), 'api': retained['api']}, ensure_ascii=False))
    return value


def start():
    if CURRENT.exists():
        raise RuntimeError('An explicit retained fixture is already recorded; observe without replacing it')
    env = runtime.environment()
    date = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    folder = OUT / 'live' / date
    folder.mkdir(parents=True)
    binary_root = RUNTIME / 'live-bin' / date
    binary_root.mkdir(parents=True)
    write(folder / 'environment.json', runtime.verify())
    write(folder / 'source-at-run.json', suite.source_snapshot())
    env.update(TICKETPULSE_RETAIN_C3_RESOURCES='true', TICKETPULSE_C3_RETAIN_LIVE='true',
               TICKETPULSE_M403C3_EVIDENCE_DIR=str(folder / 'verification'),
               TICKETPULSE_M403C2_EVIDENCE_DIR=str(folder / 'forward'),
               TICKETPULSE_M403C1_EVIDENCE_DIR=str(folder / 'c1'), TICKETPULSE_M403B_EVIDENCE_DIR=str(folder / 'handoff'),
               TICKETPULSE_M4_EVIDENCE_DIR=str(folder / 'm4'))
    with (RUNTIME / 'suite.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for target in ['ticketpulse', 'ticketpulse-sync', 'ticketpulse-control', 'ticketpulse-rebuild']:
            suite.run(['go', 'build', '-race', '-o', str(binary_root / target), './cmd/' + target], folder, 'build-' + target, env)
        suite.run(['go', 'build', '-race', '-o', str(binary_root / 'ticketpulse-legacy-api'), './cmd/ticketpulse'], folder, 'build-legacy-api', env, cwd=RUNTIME / 'legacy-source')
        suite.run(['go', 'test', '-race', '-c', '-tags=integration,m2,m3,m4,m403b,m403c1,m403c2,m403c3', '-o', str(binary_root / 'ticketpulse-c3-live.test'), './internal/mysqlstore'], folder, 'build-live', env)
        for target, variable in [('ticketpulse', 'TICKETPULSE_API_BINARY'), ('ticketpulse-sync', 'TICKETPULSE_SYNC_BINARY'),
                                 ('ticketpulse-control', 'TICKETPULSE_CONTROL_BINARY'), ('ticketpulse-rebuild', 'TICKETPULSE_REBUILD_BINARY'),
                                 ('ticketpulse-legacy-api', 'TICKETPULSE_LEGACY_API_BINARY')]:
            env[variable] = str(binary_root / target)
        log = (folder / 'launcher.log').open('x')
        command = [str(binary_root / 'ticketpulse-c3-live.test'), '-test.count=1', '-test.timeout=0', '-test.v', '-test.run=^TestM403C3RetainedPrivateRoundtrip$']
        child = subprocess.Popen(command, cwd=ROOT / 'internal/mysqlstore', env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        log.close()
        write(CURRENT, {'pid': child.pid, 'evidence': str(folder.relative_to(ROOT)), 'binaries': str(binary_root), 'command': command})
        path = folder / 'verification/TestM403C3RetainedPrivateRoundtrip/retained-live.json'
        deadline = time.monotonic() + 180
        while not path.exists():
            if child.poll() is not None:
                write(folder / 'startup-failure.json', {'exit_code': child.returncode, 'log': str((folder / 'launcher.log').relative_to(ROOT))})
                raise RuntimeError('Retained fixture exited; inspect its preserved private log')
            runtime.budget()
            if time.monotonic() > deadline:
                raise RuntimeError('Retained fixture deadline; no process was signalled automatically')
            time.sleep(1)
        observe()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['start', 'observe'])
    args = parser.parse_args()
    if args.mode == 'start':
        start()
    else:
        observe()


if __name__ == '__main__':
    main()
