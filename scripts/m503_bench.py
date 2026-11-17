#!/usr/bin/env python3
"""Bounded M5-03 closed-loop CK/real HTTP load with raw per-request evidence."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import queue
import re
import shutil
import socket
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import urllib.error

import m503_environment as runtime
import m503_fixture as fixture
import m503_queries as queries

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/m5-03'

# A cyclic distribution is identical across variants; every HTTP sample asks
# for the complete dashboard. No metric subset endpoint is invented.
DISTRIBUTION = [
    {'name': 'org1-all-8d', 'org': 1, 'teams': [], 'start': '2026-11-01', 'end': '2026-11-09'},
    {'name': 'org1-it-8d', 'org': 1, 'teams': [10], 'start': '2026-11-01', 'end': '2026-11-09'},
    {'name': 'org1-device-1d', 'org': 1, 'teams': [20], 'start': '2026-11-03', 'end': '2026-11-04'},
    {'name': 'org1-all-1d', 'org': 1, 'teams': [], 'start': '2026-11-06', 'end': '2026-11-07'},
    {'name': 'org1-two-teams-3d', 'org': 1, 'teams': [10, 90], 'start': '2026-11-02', 'end': '2026-11-05'},
    {'name': 'org1-all-30d', 'org': 1, 'teams': [], 'start': '2026-11-01', 'end': '2026-12-01'},
    {'name': 'org2-all-8d', 'org': 2, 'teams': [], 'start': '2026-11-01', 'end': '2026-11-09'},
    {'name': 'org2-team-1d', 'org': 2, 'teams': [200], 'start': '2026-11-02', 'end': '2026-11-03'},
]


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    i = (len(values) - 1) * p
    a, b = math.floor(i), math.ceil(i)
    return round(values[a] + (values[b] - values[a]) * (i - a), 3)


def params_for(case):
    def utc(value):
        at = datetime.datetime.fromisoformat(value).replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
        return at.astimezone(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
    return {'org': str(case['org']), 'teams': json.dumps(case['teams']),
            'start': utc(case['start']), 'end': utc(case['end'])}


def control(sql):
    params = {'wait_end_of_query': '1', 'max_threads': '1', 'max_execution_time': '2',
              'max_memory_usage': str(32 << 20), 'log_queries': '0', 'output_format_json_quote_64bit_integers': '0'}
    request = urllib.request.Request(runtime.URL + '/?' + urllib.parse.urlencode(params), data=sql.encode())
    with urllib.request.urlopen(request, timeout=3) as response:
        return response.read().decode()


def collect_log(database, since, prefix=None):
    predicate = "startsWith(query_id,'" + prefix + "')" if prefix else "event_time_microseconds>=parseDateTime64BestEffort('" + since + "',6) AND startsWith(query,'WITH')"
    q = "SELECT event_time_microseconds,type,query_id,initial_query_id,query_duration_ms,read_rows,read_bytes,result_rows,result_bytes,memory_usage,exception_code,exception,ProfileEvents,Settings,query FROM system.query_log WHERE current_database='" + database + "' AND (" + predicate + " OR startsWith(initial_query_id,'" + (prefix or 'm503-unmatched') + "')) FORMAT JSONEachRow"
    # Log export is performed after load, with a larger result budget and finite
    # deadline. It is not included in measured load or query memory attribution.
    p = {'wait_end_of_query': '1', 'max_threads': '1', 'max_execution_time': '20',
         'max_memory_usage': str(128 << 20), 'max_block_size': '1024', 'log_queries': '0', 'output_format_json_quote_64bit_integers': '0'}
    req = urllib.request.Request(runtime.URL + '/?' + urllib.parse.urlencode(p), data=q.encode())
    with urllib.request.urlopen(req, timeout=25) as response:
        return response.read().decode()


def system_snapshot(database):
    q = "SELECT 'metric' AS kind,metric AS name,toFloat64(value) AS value FROM system.metrics WHERE metric IN ('Query','Merge','MemoryTracking','HTTPConnection','BackgroundMergesAndMutationsPoolTask') UNION ALL SELECT 'parts' AS kind,table AS name,toFloat64(count()) AS value FROM system.parts WHERE active AND database='" + database + "' GROUP BY table UNION ALL SELECT 'parts_rows' AS kind,table AS name,toFloat64(sum(rows)) AS value FROM system.parts WHERE active AND database='" + database + "' GROUP BY table UNION ALL SELECT 'parts_bytes' AS kind,table AS name,toFloat64(sum(bytes_on_disk)) AS value FROM system.parts WHERE active AND database='" + database + "' GROUP BY table FORMAT JSONEachRow"
    raw = control(q)
    return [json.loads(x) for x in raw.splitlines()]


class Monitor:
    def __init__(self, database, stop):
        self.database, self.stop = database, stop
        runtime.configure_owner()
        self.pid = runtime.pinned.server_pid(runtime.state().get('pid'))
        if not self.pid:
            raise RuntimeError('CK ownership could not be verified')
        self.done = threading.Event()
        self.samples = []
        self.reason = None
        self.disk_before = shutil.disk_usage(ROOT).used
        self.start = time.perf_counter()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.done.is_set():
            sample = {'utc': stamp(), 'elapsed_s': time.perf_counter() - self.start, 'pid': self.pid}
            try:
                raw = subprocess.check_output(['ps', '-p', str(self.pid), '-o', '%cpu=,rss='], text=True, timeout=1).split()
                sample.update(cpu_pct=float(raw[0]), rss_bytes=int(raw[1]) * 1024)
            except Exception as error:
                sample['os_probe_error'] = type(error).__name__
            try:
                raw = control("SELECT count() AS active_queries,sum(memory_usage) AS query_memory,sum(read_rows) AS read_rows,sum(read_bytes) AS read_bytes,max(elapsed) AS oldest_seconds FROM system.processes WHERE current_database='" + self.database + "' FORMAT JSONEachRow")
                sample['queries'] = json.loads(raw)
                raw = control("SELECT metric,value FROM system.metrics WHERE metric IN ('MemoryTracking','Merge','BackgroundMergesAndMutationsPoolTask') FORMAT JSONEachRow")
                sample['ck_metrics'] = {j['metric']: j['value'] for j in map(json.loads, raw.splitlines())}
            except Exception as error:
                sample['ck_probe_error'] = type(error).__name__
                if isinstance(error, urllib.error.HTTPError) and error.headers.get('X-ClickHouse-Exception-Code') == '241':
                    self.reason = 'ClickHouse memory-limit exception during resource probe'
            sample['disk_growth_bytes'] = shutil.disk_usage(ROOT).used - self.disk_before
            if sample.get('rss_bytes', 0) > 6 * 1024**3:
                self.reason = 'ClickHouse RSS exceeds 6 GiB safety budget'
            if sample.get('ck_metrics', {}).get('MemoryTracking', 0) >= 4 * 1024**3:
                self.reason = 'ClickHouse tracked memory reaches 4 GiB safety budget'
            if sample['disk_growth_bytes'] > 20 * 1024**3:
                self.reason = 'Run disk growth exceeds 20 GiB safety budget'
            self.samples.append(sample)
            if self.reason:
                self.stop.set()
                try:
                    control("KILL QUERY WHERE current_database='" + self.database + "' AND query_kind='Select' ASYNC")
                except Exception as error:
                    sample['cancel_error'] = type(error).__name__
                return
            self.done.wait(.5)


def summarize(rows, seconds, concurrency):
    good = [x for x in rows if x['ok']]
    latencies = [x['latency_ms'] for x in rows]
    return {'requests': len(rows), 'completed_successfully': len(good), 'errors': len(rows) - len(good),
            'completion_seconds': seconds, 'successful_qps': len(good) / seconds if seconds else 0,
            'offered_closed_loop_concurrency': concurrency,
            'p50_ms': percentile(latencies, .5), 'p95_ms': percentile(latencies, .95), 'p99_ms': percentile(latencies, .99),
            'success_p50_ms': percentile([x['latency_ms'] for x in good], .5),
            'success_p95_ms': percentile([x['latency_ms'] for x in good], .95),
            'success_p99_ms': percentile([x['latency_ms'] for x in good], .99),
            'status_counts': dict(Counter(str(x['status']) for x in rows)),
            'error_codes': dict(Counter(str(x['code']) for x in rows if not x['ok'])),
            'timeouts': sum(x['code'] in ['CLIENT_TIMEOUT', 'ANALYTICS_TIMEOUT', 159] for x in rows),
            'rejected': sum(x['status'] == 429 or x['code'] == 202 for x in rows),
            'queue_wait_ms': 'not instrumented; no added application queue',
            'by_distribution': {name: {'requests': len(v), 'success': sum(x['ok'] for x in v),
                                      'p95_ms': percentile([x['latency_ms'] for x in v], .95)}
                                for name in sorted({x['case'] for x in rows})
                                for v in [[x for x in rows if x['case'] == name]]}}


def load(args):
    value = fixture.fixtures()
    item = value['datasets'][str(args.tickets)]
    database = item['ck_database']
    output = OUT / args.output
    output.mkdir(parents=True, exist_ok=False)
    sql = queries.query(args.query) if args.mode == 'ck' else None
    stop = threading.Event()
    monitor = Monitor(database, stop)
    before = system_snapshot(database)
    started_utc = stamp()
    prefix = 'm503-' + args.run_id
    contract = vars(args).copy()
    if args.mode == 'http':
        app = value['workers'].get('app')
        if not app or app['dataset'] != args.tickets:
            raise RuntimeError('The measured HTTP app does not own the requested dataset')
        contract['app_binary'] = app['binary']
        contract['app_binary_sha256'] = hashlib.sha256((ROOT / app['binary']).read_bytes()).hexdigest()
        contract['app_log'] = app['log']
    contract['server_config_sha256'] = hashlib.sha256(runtime.CONFIG.read_bytes()).hexdigest()
    contract.update(database=database, distribution=DISTRIBUTION, started_at=started_utc,
                    ck_server_memory_bytes=4 * 1024**3, max_execution_time_s=4, client_timeout_s=6,
                    query_sha256=hashlib.sha256(sql.encode()).hexdigest() if sql else None,
                    qps_denominator='completion wall time including drain',
                    response_contract='HTTP always returns complete dashboard; individual CK queries are diagnostic only')
    (output / 'input.json').write_text(json.dumps(contract, indent=2) + '\n')
    if sql:
        (output / 'query.sql').write_text(sql + '\n')
    state = {'start': None, 'end': None}
    barrier = threading.Barrier(args.concurrency + 1)
    evidence = queue.Queue()
    def worker(index):
        conn = None
        barrier.wait()
        serial = 0
        while time.perf_counter() < state['end'] and not stop.is_set():
            case = DISTRIBUTION[(index + serial) % len(DISTRIBUTION)] if args.distribution == 'mixed' else DISTRIBUTION[args.case]
            qid = prefix + '-' + str(index) + '-' + str(serial)
            at = time.perf_counter()
            elapsed = at - state['start']
            sample = {'query_id': qid if args.mode == 'ck' else None, 'worker': index, 'serial': serial,
                      'case': case['name'], 'request_started_s': elapsed,
                      'phase': 'warmup' if elapsed < args.warmup else 'measurement'}
            try:
                if conn is None:
                    conn = http.client.HTTPConnection('127.0.0.1', 18125 if args.mode == 'ck' else 18086, timeout=6)
                if args.mode == 'ck':
                    p = {'database': database, 'max_threads': '2', 'max_execution_time': '4',
                         'wait_end_of_query': '1', 'output_format_json_quote_64bit_integers': '0',
                         'query_id': qid, 'log_comment': prefix}
                    p.update({'param_' + k: v for k, v in params_for(case).items()})
                    conn.request('POST', '/?' + urllib.parse.urlencode(p), body=sql.encode(), headers={'Connection': 'keep-alive'})
                else:
                    p = [('start', case['start']), ('end', case['end'])] + [('team_id', str(t)) for t in case['teams']]
                    token = 'local-supervisor' if case['org'] == 1 else 'local-other-admin'
                    conn.request('GET', '/api/v1/analytics?' + urllib.parse.urlencode(p),
                                 headers={'Authorization': 'Bearer ' + token, 'Connection': 'keep-alive'})
                response = conn.getresponse()
                raw = response.read()
                sample.update(status=response.status, response_bytes=len(raw),
                              ck_code=int(response.getheader('X-ClickHouse-Exception-Code', '0')),
                              ck_summary=json.loads(response.getheader('X-ClickHouse-Summary', '{}')))
                if args.mode == 'http':
                    data = json.loads(raw)
                    sample['code'] = data.get('error', {}).get('code', '')
                    if sample['code'] == 'ANALYTICS_UNAVAILABLE':
                        monitor.reason = 'Analytics unavailable under isolated load; stop and inspect CK memory errors'
                        stop.set()
                    if response.status == 200:
                        stable = {k: data[k] for k in ['start', 'end', 'timezone', 'overview', 'daily', 'teams', 'backlog_scope']}
                        sample['result_sha256'] = hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                        sample['overview'] = data['overview']
                else:
                    sample['code'] = sample['ck_code']
                    if response.status == 200:
                        data = [json.loads(x) for x in raw.decode().splitlines()]
                        sample['result_sha256'] = hashlib.sha256(json.dumps(data, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                    else:
                        sample['error_detail'] = raw.decode(errors='replace')[:2000]
                        if sample['ck_code'] == 241:
                            monitor.reason = 'ClickHouse memory limit reached in measured request'
                            stop.set()
                sample['ok'] = response.status == 200
                if response.will_close:
                    conn.close()
                    conn = None
            except (socket.timeout, TimeoutError):
                sample.update(status=0, code='CLIENT_TIMEOUT', ok=False)
                if conn:
                    conn.close()
                conn = None
            except Exception as error:
                sample.update(status=0, code=type(error).__name__, ok=False, error_detail=str(error)[:300])
                if conn:
                    conn.close()
                conn = None
            sample['latency_ms'] = (time.perf_counter() - at) * 1000
            sample['request_finished_s'] = time.perf_counter() - state['start']
            evidence.put(sample)
            serial += 1
        if conn:
            conn.close()
    monitor.thread.start()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        pending = [pool.submit(worker, i) for i in range(args.concurrency)]
        state['start'] = time.perf_counter()
        state['end'] = state['start'] + args.warmup + args.duration
        barrier.wait()
        for future in pending:
            future.result()
    finished = time.perf_counter()
    monitor.done.set()
    monitor.thread.join(timeout=5)
    rows = []
    while not evidence.empty():
        rows.append(evidence.get())
    measured = sorted([x for x in rows if x['phase'] == 'measurement'], key=lambda x: x['request_started_s'])
    warmup = sorted([x for x in rows if x['phase'] == 'warmup'], key=lambda x: x['request_started_s'])
    elapsed = max(0, finished - state['start'] - args.warmup)
    for filename, items in [('measurement-requests.jsonl', measured), ('warmup-requests.jsonl', warmup), ('resources.jsonl', monitor.samples)]:
        with (output / filename).open('x') as stream:
            for x in items:
                stream.write(json.dumps(x, separators=(',', ':')) + '\n')
    result = summarize(measured, elapsed, args.concurrency)
    result.update(stopped_for_safety=bool(monitor.reason), stop_reason=monitor.reason,
                  run_id=args.run_id, mode=args.mode, query=args.query, tickets=args.tickets,
                  admission_duration_s=args.duration, measurement_completed_at=stamp(),
                  peak_ck_rss_bytes=max((x.get('rss_bytes', 0) for x in monitor.samples), default=0),
                  peak_tracked_memory_bytes=max((x.get('ck_metrics', {}).get('MemoryTracking', 0) for x in monitor.samples), default=0),
                  peak_active_queries=max((x.get('queries', {}).get('active_queries', 0) for x in monitor.samples), default=0),
                  max_cpu_pct=max((x.get('cpu_pct', 0) for x in monitor.samples), default=0))
    result['throughput_bins'] = [{'from_s': i, 'to_s': min(i + 5, args.duration),
                                 'success_finished': sum(x['ok'] and args.warmup + i <= x['request_finished_s'] < args.warmup + i + 5 for x in measured),
                                 'error_finished': sum(not x['ok'] and args.warmup + i <= x['request_finished_s'] < args.warmup + i + 5 for x in measured)}
                                for i in range(0, math.ceil(args.duration), 5)]
    (output / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    (output / 'warmup-summary.json').write_text(json.dumps(summarize(warmup, args.warmup, args.concurrency), indent=2) + '\n')
    after = system_snapshot(database)
    (output / 'parts-metrics-before-after.json').write_text(json.dumps({'before': before, 'after': after}, indent=2) + '\n')
    # Query logs normally flush within 1 second. Flush only this owned runtime
    # after measurement, then persist every CK record including subqueries.
    control('SYSTEM FLUSH LOGS')
    raw = collect_log(database, started_utc, prefix if args.mode == 'ck' else None)
    (output / 'query-log.jsonl').write_text(raw)
    logs = [json.loads(x) for x in raw.splitlines()]
    terminal = [x for x in logs if x['type'] != 'QueryStart']
    finishes = [x for x in logs if x['type'] == 'QueryFinish']
    result['ck_log'] = {'records': len(logs), 'terminal_records': len(terminal),
                        'terminal_codes': dict(Counter(str(x['exception_code']) for x in terminal)),
                        'max_query_memory_bytes': max((x['memory_usage'] for x in terminal), default=0),
                        'p50_read_rows': percentile([x['read_rows'] for x in finishes], .5),
                        'p50_read_bytes': percentile([x['read_bytes'] for x in finishes], .5),
                        'cpu_user_us': sum(x['ProfileEvents'].get('UserTimeMicroseconds', 0) for x in terminal),
                        'cpu_system_us': sum(x['ProfileEvents'].get('SystemTimeMicroseconds', 0) for x in terminal)}
    (output / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ['run_id', 'requests', 'completed_successfully', 'successful_qps',
                                          'p95_ms', 'p99_ms', 'error_codes', 'stopped_for_safety',
                                          'peak_tracked_memory_bytes', 'peak_ck_rss_bytes', 'ck_log']}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['ck', 'http'], required=True)
    parser.add_argument('--query', default='original')
    parser.add_argument('--tickets', type=int, choices=[10000, 100000], required=True)
    parser.add_argument('--concurrency', type=int, choices=[10, 20, 50], required=True)
    parser.add_argument('--duration', type=float, default=10)
    parser.add_argument('--warmup', type=float, default=2)
    parser.add_argument('--distribution', choices=['mixed', 'single'], default='mixed')
    parser.add_argument('--case', type=int, choices=range(8), default=0)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--output', required=True, help='new relative directory under artifacts/m5-03')
    args = parser.parse_args()
    if not 0 < args.duration <= 180 or not 0 <= args.warmup <= 10 or not re.fullmatch(r'[a-z0-9_-]{1,50}', args.run_id):
        parser.error('bounded duration/warmup and safe run ID required')
    if Path(args.output).is_absolute() or '..' in Path(args.output).parts:
        parser.error('relative output path required')
    load(args)
