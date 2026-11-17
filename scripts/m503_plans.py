#!/usr/bin/env python3
"""Sequential production/candidate/component SQL costs and query plans."""
import json
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request

import m503_bench as bench
import m503_fixture as fixture
import m503_queries as queries

OUT = Path(__file__).resolve().parents[1] / 'artifacts/m5-03/sql/final-plans'


def main():
    OUT.mkdir(parents=True, exist_ok=False)
    database = fixture.fixtures()['datasets']['100000']['ck_database']
    result = []
    for variant in ['original', 'selected', 'ordered-bounded', 'materialized', 'backlog', 'created', 'resolved', 'duration']:
        sql = queries.query(variant)
        folder = OUT / variant
        folder.mkdir()
        (folder / 'query.sql').write_text(sql + '\n')
        for case in [bench.DISTRIBUTION[0], bench.DISTRIBUTION[2], bench.DISTRIBUTION[6]]:
            params = {'database': database, 'wait_end_of_query': '1', 'max_threads': '2', 'max_execution_time': '4',
                      'output_format_json_quote_64bit_integers': '0', 'query_id': 'm503-plan-' + variant + '-' + str(time.time_ns())}
            params.update({'param_' + k: v for k, v in bench.params_for(case).items()})
            request = urllib.request.Request(fixture.ck.URL + '/?' + urllib.parse.urlencode(params), data=sql.encode())
            start = time.perf_counter()
            try:
                with urllib.request.urlopen(request, timeout=6) as response:
                    raw = response.read().decode()
                    summary = json.loads(response.headers.get('X-ClickHouse-Summary', '{}'))
                result.append({'variant': variant, 'case': case, 'query_id': params['query_id'],
                               'seconds': time.perf_counter() - start, 'summary': summary, 'ok': True})
                (folder / (case['name'] + '-rows.jsonl')).write_text(raw)
            except urllib.error.HTTPError as error:
                raw = error.read().decode()
                result.append({'variant': variant, 'case': case, 'error': raw, 'ok': False})
                (folder / (case['name'] + '-failure.log')).write_text(raw)
            if case['name'] == bench.DISTRIBUTION[0]['name']:
                for name, prefix in [('plan', 'EXPLAIN PLAN indexes=1 '), ('pipeline', 'EXPLAIN PIPELINE compact=0 ')]:
                    request = urllib.request.Request(fixture.ck.URL + '/?' + urllib.parse.urlencode(dict(params, query_id='m503-explain-' + str(time.time_ns()), log_queries='0')),
                                                     data=(prefix + sql.split('FORMAT JSONEachRow')[0]).encode())
                    with urllib.request.urlopen(request, timeout=6) as response:
                        (folder / (name + '.txt')).write_text(response.read().decode())
    bench.control('SYSTEM FLUSH LOGS')
    raw = bench.collect_log(database, bench.stamp(), 'm503-plan-')
    (OUT / 'query-log.jsonl').write_text(raw)
    logs = {j['query_id']: j for j in map(json.loads, raw.splitlines()) if j['type'] != 'QueryStart'}
    for row in result:
        if row.get('query_id') in logs:
            log = logs[row['query_id']]
            row['query_log'] = {k: log[k] for k in ['type', 'query_duration_ms', 'memory_usage', 'read_rows', 'read_bytes', 'ProfileEvents']}
    (OUT / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
    print([(x['variant'], x['case']['name'], x['summary'].get('memory_usage')) for x in result if x['ok']])


if __name__ == '__main__':
    main()
