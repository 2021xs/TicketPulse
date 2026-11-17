#!/usr/bin/env python3
"""Full independent source/CK business-content audit and metric references."""
import argparse
from collections import Counter
import datetime
import hashlib
import json
import re
from pathlib import Path
import subprocess
import time
import urllib.parse
import urllib.request

import m503_bench as bench
import m503_fixture as fixture
import m503_queries as queries

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/m5-03/correctness'


def norm(value):
    if isinstance(value, str) and len(value) >= 19 and value[4] == '-' and value[7] == '-':
        try:
            value = re.sub(r'[.](\d{1,6})(?=Z|[+]00:00|$)', lambda m: '.' + m.group(1).ljust(6, '0'), value)
            return datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None).isoformat(timespec='microseconds')
        except ValueError:
            pass
    return value


def canonical(row):
    return {k: norm(v) for k, v in row.items()}


def encoded(row):
    return json.dumps(canonical(row), sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode() + b'\n'


def mysqlrows(sql, database):
    command = [str(fixture.demo.DIST / 'bin/mysql'), '--defaults-file=' + str(fixture.RUNTIME / 'admin.cnf'),
               '--batch', '--skip-column-names', '--quick']
    child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    child.stdin.write('USE ' + database + '; SET time_zone="+00:00"; ' + sql)
    child.stdin.close()
    for line in child.stdout:
        yield canonical(json.loads(bytes.fromhex(line.strip()).decode()))
    if child.wait() != 0:
        raise RuntimeError('Independent MySQL stream failed: ' + child.stderr.read())


def ckrows(sql, database):
    params = {'database': database, 'log_queries': '0', 'max_threads': '1', 'max_execution_time': '30',
              'max_memory_usage': str(256 << 20), 'output_format_json_quote_64bit_integers': '0'}
    req = urllib.request.Request(fixture.ck.URL + '/?' + urllib.parse.urlencode(params), data=(sql + ' FORMAT JSONEachRow').encode())
    with urllib.request.urlopen(req, timeout=40) as response:
        for line in response:
            yield canonical(json.loads(line))


def hashrows(rows):
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        digest.update(encoded(row))
        count += 1
    return {'rows': count, 'sha256': digest.hexdigest()}


def facts_sql():
    fields = ['organization_id', 'ticket_id', 'version', 'business_event_id', 'action', 'occurred_at',
              'event_team_id', 'initial_team_id', 'before_team_id', 'before_status', 'after_status',
              'assignee_id', 'created_at', 'first_resolved_at']
    expr = ['h.organization_id', 'h.ticket_id', 'h.version', "CONCAT(h.organization_id,':',h.ticket_id,':',h.version)",
            'h.action', "DATE_FORMAT(h.occurred_at,'%Y-%m-%d %H:%i:%s.%f')", 'h.responsible_team_id',
            "JSON_EXTRACT(h.after_state,'$.initial_team_id')", "JSON_EXTRACT(h.before_state,'$.current_team_id')",
            "COALESCE(JSON_UNQUOTE(JSON_EXTRACT(h.before_state,'$.status')),'')",
            "JSON_UNQUOTE(JSON_EXTRACT(h.after_state,'$.status'))", "JSON_EXTRACT(h.after_state,'$.assignee_id')",
            "DATE_FORMAT(t.created_at,'%Y-%m-%d %H:%i:%s.%f')", "JSON_EXTRACT(h.after_state,'$.first_resolved_at')"]
    mysql = 'SELECT HEX(JSON_OBJECT(' + ','.join("'" + k + "'," + x for k, x in zip(fields, expr)) + ')) FROM ticket_histories h FORCE INDEX (uk_history_ticket_version) STRAIGHT_JOIN tickets t ON t.id=h.ticket_id ORDER BY h.ticket_id,h.version;'
    ck = 'SELECT ' + ','.join(fields) + ' FROM ticket_events WHERE organization_id=1 ORDER BY organization_id,ticket_id,version'
    return fields, mysql, ck


def states_sql():
    fields = ['organization_id', 'ticket_id', 'version', 'complete', 'current_team_id', 'assignee_id',
              'status', 'initial_team_id', 'created_at', 'updated_at', 'first_resolved_at']
    expr = {k: k for k in fields}
    expr['ticket_id'], expr['complete'] = 'id', '1'
    for k in ['created_at', 'updated_at', 'first_resolved_at']:
        expr[k] = "DATE_FORMAT(" + k + ",'%Y-%m-%d %H:%i:%s.%f')"
    mysql = 'SELECT HEX(JSON_OBJECT(' + ','.join("'" + k + "'," + expr[k] for k in fields) + ')) FROM tickets ORDER BY id;'
    ck = 'SELECT ' + ','.join(fields) + ' FROM ticket_states WHERE organization_id=1 ORDER BY organization_id,ticket_id,version,complete'
    return fields, mysql, ck


def references(database):
    # MySQL independently picks MIN(version) for first resolution and uses the
    # persisted current row. No production CK SQL or metric function is called.
    q = """WITH first_version AS (
 SELECT ticket_id,MIN(version) AS version FROM ticket_histories
 WHERE action='resolved' AND JSON_UNQUOTE(JSON_EXTRACT(after_state,'$.status'))='awaiting_confirmation' GROUP BY ticket_id
) SELECT HEX(JSON_OBJECT('ticket_id',t.id,'organization_id',t.organization_id,
 'created_at',DATE_FORMAT(t.created_at,'%Y-%m-%d %H:%i:%s.%f'),'initial_team_id',t.initial_team_id,
 'current_team_id',t.current_team_id,'status',t.status,
 'resolved_at',DATE_FORMAT(h.occurred_at,'%Y-%m-%d %H:%i:%s.%f'),'resolved_team',h.responsible_team_id))
 FROM tickets t LEFT JOIN first_version f ON f.ticket_id=t.id
 LEFT JOIN ticket_histories h ON h.ticket_id=f.ticket_id AND h.version=f.version ORDER BY t.id;"""
    rows = list(mysqlrows(q, database))
    result = {}
    for ncase, case in enumerate(bench.DISTRIBUTION):
        use = rows if case['org'] == 1 else [x for x in rows if x['ticket_id'] <= len(rows) // 10]
        values = Counter()
        durations = Counter()
        values[('observed', '', 0)] = len(use)
        params = bench.params_for(case)
        start, end = norm(params['start']), norm(params['end'])
        teams = case['teams']
        def mapped(team):
            return {10: 200, 20: 210, 90: 290}.get(team, team) if case['org'] == 2 else team
        for row in use:
            created, resolved = row['created_at'], row['resolved_at']
            initial, current, event_team = mapped(row['initial_team_id']), mapped(row['current_team_id']), mapped(row['resolved_team'])
            if start <= created < end and (not teams or initial in teams):
                day = (datetime.datetime.fromisoformat(created) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d')
                values[('created', day, initial)] += 1
            if resolved and start <= resolved < end and (not teams or event_team in teams):
                day = (datetime.datetime.fromisoformat(resolved) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d')
                key = ('resolved', day, event_team)
                values[key] += 1
                durations[key] += (datetime.datetime.fromisoformat(resolved) - datetime.datetime.fromisoformat(created)).total_seconds()
            if row['status'] in ['pending', 'in_progress'] and (not teams or current in teams):
                values[('backlog', '', current)] += 1
        result[case['name']] = [{'metric': k[0], 'day': k[1], 'team_id': k[2], 'amount': v, 'duration': durations[k]} for k, v in sorted(values.items())]
    return q, result


def metrics(sql, database, case):
    params = {'database': database, 'wait_end_of_query': '1', 'max_threads': '2', 'max_execution_time': '4',
              'output_format_json_quote_64bit_integers': '0'}
    params.update({'param_' + k: v for k, v in bench.params_for(case).items()})
    req = urllib.request.Request(fixture.ck.URL + '/?' + urllib.parse.urlencode(params), data=sql.encode())
    with urllib.request.urlopen(req, timeout=6) as response:
        return [json.loads(x) for x in response.read().decode().splitlines()]


def audit(n, variant, attempt):
    item = fixture.fixtures()['datasets'][str(n)]
    database, ckdatabase = item['database'], item['ck_database']
    output = OUT / ('full-' + str(n))
    if attempt > 1:
        output = output / ('attempt-' + str(attempt))
    output.mkdir(parents=True, exist_ok=False)
    start = time.perf_counter()
    result = {'source_database': database, 'ck_database': ckdatabase, 'variant': variant, 'queries': {}}
    for name, sqls in [('facts', facts_sql()), ('states', states_sql())]:
        fields, mysql, ck = sqls
        result['queries'][name] = {'mysql': mysql, 'ck': ck}
        result['mysql_' + name] = hashrows(mysqlrows(mysql, database))
        stats = {'physical_rows': 0, 'duplicate_rows': 0, 'conflicting_keys': 0}
        def canonical_rows():
            prev_key, prev_content = None, None
            latest = None
            for row in ckrows(ck, ckdatabase):
                stats['physical_rows'] += 1
                key = (row['organization_id'], row['ticket_id'], row['version']) + ((row['complete'],) if name == 'states' else ())
                content = encoded(row)
                if key == prev_key:
                    stats['duplicate_rows'] += 1
                    if content != prev_content:
                        stats['conflicting_keys'] += 1
                        raise RuntimeError('Independent full-content equal-key conflict')
                    continue
                if name == 'facts':
                    yield row
                else:
                    if latest and latest['ticket_id'] != row['ticket_id']:
                        yield latest
                    latest = row
                prev_key, prev_content = key, content
            if name == 'states' and latest:
                yield latest
        result['ck_' + name] = hashrows(canonical_rows())
        result['ck_' + name + '_physical'] = stats
        result[name + '_same'] = result['mysql_' + name] == result['ck_' + name]
    query, expected = references(database)
    (output / 'independent-metric-reference.sql').write_text(query)
    (output / 'expected-metrics.json').write_text(json.dumps(expected, indent=2) + '\n')
    comparisons = []
    for case in bench.DISTRIBUTION:
        before = metrics(queries.query('original'), ckdatabase, case)
        after = metrics(queries.query(variant), ckdatabase, case)
        reference = expected[case['name']]
        comparisons.append({'case': case, 'before': before, 'after': after, 'independent': reference,
                            'same': before == after == reference})
    (output / 'metric-comparisons.json').write_text(json.dumps(comparisons, indent=2) + '\n')
    result.update(metric_comparisons_passed=all(x['same'] for x in comparisons), elapsed_seconds=time.perf_counter() - start)
    result['consistent'] = result['facts_same'] and result['states_same'] and result['metric_comparisons_passed']
    (output / 'audit.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
    if not result['consistent']:
        raise RuntimeError('Full independent audit did not match')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tickets', type=int, choices=[10000, 100000], default=100000)
    parser.add_argument('--query', default='checked-bufferless')
    parser.add_argument('--attempt', type=int, default=1)
    args = parser.parse_args()
    audit(args.tickets, args.query, args.attempt)
