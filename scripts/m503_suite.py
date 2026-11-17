#!/usr/bin/env python3
"""Repeatable M5-03 matrices and bounded regression orchestration."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

import local_analytics as original
import local_mysql as demo
import m503_bench as bench
import m503_environment as runtime
import m503_fixture as fixture

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/m5-03'


def pause_merges():
    for item in fixture.fixtures()['datasets'].values():
        for table in ['ticket_events', 'ticket_states']:
            fixture.ckquery('SYSTEM STOP MERGES ' + item['ck_database'] + '.' + table)


def idle():
    for _ in range(30):
        raw = bench.control("SELECT count() AS active FROM system.processes WHERE current_database IN ('ticketpulse_m503_10000','ticketpulse_m503_100000_v2') FORMAT JSONEachRow")
        if json.loads(raw)['active'] == 0:
            return
        time.sleep(.5)
    raise RuntimeError('Queries did not drain within safety deadline')


def recover():
    fixture.stop_app()
    idle()
    runtime.stop()
    with (OUT / 'environment/restarts.log').open('a') as log:
        result = subprocess.run([sys.executable, 'scripts/m503_environment.py', 'start'],
                                cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError('Owned CK restart failed')
    pause_merges()


def call_bench(mode, query, n, concurrency, duration, warmup, run_id, folder, distribution='mixed'):
    idle()
    command = [sys.executable, 'scripts/m503_bench.py', '--mode', mode, '--query', query, '--tickets', str(n),
               '--concurrency', str(concurrency), '--duration', str(duration), '--warmup', str(warmup),
               '--distribution', distribution, '--run-id', run_id, '--output', folder]
    logpath = OUT / (folder + '.log')
    logpath.parent.mkdir(parents=True, exist_ok=True)
    with logpath.open('x') as log:
        result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    summary_path = OUT / folder / 'summary.json'
    if result.returncode:
        raise RuntimeError('Benchmark orchestration failed: ' + str(logpath.relative_to(ROOT)))
    summary = json.loads(summary_path.read_text())
    print(json.dumps({'run': run_id, 'requests': summary['requests'], 'qps': summary['successful_qps'],
                      'p95_ms': summary['p95_ms'], 'errors': summary['error_codes'], 'stop': summary['stop_reason']}), flush=True)
    return summary


def matrix(repeat):
    folder = OUT / 'ab' / ('repeat-' + str(repeat))
    folder.mkdir(parents=True, exist_ok=False)
    order = ['original', 'selected'] if repeat % 2 == 1 else ['selected', 'original']
    summaries = []
    for query in order:
        for mode in ['ck', 'http']:
            for n in [10000, 100000]:
                recover()
                if mode == 'http':
                    binary = '.local/m5-03-bench-app-' + ('original' if query == 'original' else 'final')
                    fixture.start_app(n, binary)
                for c in [10, 20, 50]:
                    name = 'ab' + str(repeat) + '-' + query + '-' + mode + '-' + str(n) + '-c' + str(c)
                    rel = str((folder / (query + '-' + mode + '-' + str(n) + '-c' + str(c))).relative_to(OUT))
                    j = call_bench(mode, query, n, c, 10, 2, name, rel)
                    summaries.append({'directory': rel, 'summary': j})
                    if j['stopped_for_safety']:
                        recover()
                        if mode == 'http':
                            fixture.start_app(n, binary)
                if mode == 'http':
                    fixture.stop_app()
    (folder / 'matrix.json').write_text(json.dumps({'order': order, 'runs': summaries}, indent=2) + '\n')


def diagnostics():
    folder = OUT / 'diagnostic-metrics'
    folder.mkdir(parents=True, exist_ok=False)
    summaries = []
    for metric in ['backlog', 'created', 'resolved', 'duration']:
        for c in [10, 20, 50]:
            recover()
            rel = 'diagnostic-metrics/' + metric + '-c' + str(c)
            j = call_bench('ck', metric, 100000, c, 6, 1, 'diag-' + metric + '-c' + str(c), rel)
            summaries.append({'metric': metric, 'summary': j})
    (folder / 'matrix.json').write_text(json.dumps(summaries, indent=2) + '\n')


def regression(attempt=1):
    recover()
    owner = demo.marker()
    demo.verify(owner)
    env = original.environment()
    env.update(TICKETPULSE_TEST_DSN=owner['admin_dsn'], TICKETPULSE_TEST_SERVER_UUID=owner['server_uuid'],
               TICKETPULSE_CK_URL=runtime.URL)
    folder = OUT / ('regression/final' if attempt == 1 else 'regression/final-attempt-' + str(attempt))
    folder.mkdir(parents=True, exist_ok=False)
    env['TICKETPULSE_M4_EVIDENCE_DIR'] = str(folder)
    env['TICKETPULSE_M503_EVIDENCE_DIR'] = str(folder)
    env['TICKETPULSE_REBUILD_BINARY'] = str(ROOT / '.local/m5-03-rebuild-race')
    commands = [('build', ['go', 'build', '-race', '-o', env['TICKETPULSE_REBUILD_BINARY'], './cmd/ticketpulse-rebuild']),
                ('integration', ['go', 'test', '-race', '-tags=integration,m2,m3,m4', '-count=1', '-v', './internal/mysqlstore', './internal/syncpipe']),
                ('unit', ['go', 'test', './...']),
                ('race', ['go', 'test', '-race', './internal/analytics', './internal/httpapi', './internal/syncpipe', './internal/ticket']),
                ('vet', ['go', 'vet', './...']),
                ('python', [sys.executable, '-m', 'py_compile', 'scripts/m503_bench.py', 'scripts/m503_correctness.py',
                            'scripts/m503_environment.py', 'scripts/m503_fixture.py', 'scripts/m503_queries.py', 'scripts/m503_suite.py'])]
    results = []
    for name, command in commands:
        start = time.perf_counter()
        with (folder / (name + '.log')).open('x') as log:
            child = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        results.append({'name': name, 'command': command, 'exit_code': child.returncode, 'seconds': time.perf_counter() - start})
        (folder / 'result.json').write_text(json.dumps(results, indent=2) + '\n')
        print(name, child.returncode, results[-1]['seconds'], flush=True)
        if child.returncode:
            raise RuntimeError('Required regression failed: ' + name)
    text = (folder / 'integration.log').read_text()
    cases = [{'name': name, 'result': status, 'seconds': float(seconds)}
             for status, name, seconds in re.findall(r'^--- (PASS|FAIL): ([^ ]+) [(]([0-9.]+)s[)]$', text, re.M)]
    (folder / 'test-summary.json').write_text(json.dumps({'passed': sum(x['result'] == 'PASS' for x in cases),
                                                        'failed': sum(x['result'] == 'FAIL' for x in cases),
                                                        'cases': cases}, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['matrix', 'diagnostics', 'regression'])
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--attempt', type=int, default=1)
    args = parser.parse_args()
    if args.action == 'matrix':
        matrix(args.repeat)
    elif args.action == 'diagnostics':
        diagnostics()
    else:
        regression(args.attempt)
