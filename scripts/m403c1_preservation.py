#!/usr/bin/env python3
"""Read-only demo comparison, using the preserved M4 baseline. No test mutations."""
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import urllib.parse
import urllib.request

import local_mysql as mysql
import m403c1_environment as owned

ROOT, OUT = owned.ROOT, owned.OUT
BASE = ROOT / 'artifacts/m4-01/baseline-runtime'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    baseline = json.loads((BASE / 'demo-snapshot-summary.json').read_text())
    mysql.verify(mysql.marker())
    tables = list(baseline['mysql'])
    queries = []
    quote = chr(96)
    for table in tables:
        columns = baseline['mysql'][table]['column_names']
        pairs = ','.join("'" + c + "',HEX(CAST(" + quote + c + quote + ' AS BINARY))' for c in columns)
        queries.append("SELECT HEX(CAST(JSON_OBJECT('table_name','" + table + "','row',JSON_OBJECT(" + pairs + ")) AS CHAR)) FROM " + quote + table + quote + ' ORDER BY 1;')
    raw = mysql.mysql('USE ticketpulse_local;SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;START TRANSACTION READ ONLY;' + ''.join(queries) + 'COMMIT;')
    rows = {name: [] for name in tables}
    for line in raw.splitlines():
        value = json.loads(bytes.fromhex(line).decode())
        rows[value['table_name']].append(json.dumps(value['row'], sort_keys=True, separators=(',', ':')))
    result = {'at': owned.stamp(), 'baseline': str((BASE / 'demo-snapshot-summary.json').relative_to(ROOT)),
              'mysql': {}, 'es': {}, 'ck': {}, 'runtime': {}, 'read_only': True}
    for table, values in rows.items():
        payload = (chr(10).join(values) + chr(10)).encode()
        old = baseline['mysql'][table]
        result['mysql'][table] = {'rows': len(values), 'sha256': digest(payload),
                                 'same': len(values) == old['rows'] and digest(payload) == old['sha256']}
    # Identity/DDL observation confirms neither control migration was installed
    # on demo. This is a metadata SELECT, never a migration or reconciliation.
    control_tables = mysql.mysql("SELECT count(*) FROM information_schema.tables WHERE table_schema='ticketpulse_local' AND table_name IN ('sync_business_write_gate','sync_projection_versions','sync_projection_progress_bindings','sync_projection_control_log');").strip()
    result['demo_migration_011_tables'] = int(control_tables)
    handoff_tables = mysql.mysql("SELECT count(*) FROM information_schema.tables WHERE table_schema='ticketpulse_local' AND table_name IN ('sync_projection_handoffs','sync_projection_handoff_partitions','sync_projection_target_owners','sync_projection_handoff_log','sync_projection_deliveries','sync_projection_worker_state','sync_projection_watermarks');").strip()
    result['demo_migration_012_tables'] = int(handoff_tables)
    with urllib.request.urlopen('http://127.0.0.1:19200/ticketpulse_local_tickets_v1/_search?size=10000&version=true', timeout=10) as response:
        es = json.load(response)
    hits = sorted(es['hits']['hits'], key=lambda x: int(x['_id']))
    old = json.loads((BASE / 'es-demo-documents.json').read_text())
    result['es'] = {'documents': len(hits), 'same_fields_and_external_versions': hits == old,
                    'sha256': digest((json.dumps(hits, sort_keys=True, ensure_ascii=False, indent=2) + chr(10)).encode())}
    with urllib.request.urlopen('http://127.0.0.1:19200/ticketpulse_local_tickets_v1/_settings', timeout=5) as response:
        settings = json.load(response)
    result['es']['index_uuid'] = settings['ticketpulse_local_tickets_v1']['settings']['index']['uuid']
    params = urllib.parse.urlencode({'database': 'ticketpulse_local_analytics_v1', 'wait_end_of_query': '1', 'log_queries': '0'})
    for table in ['ticket_events', 'ticket_states']:
        query = 'SELECT * FROM ' + table + ' ORDER BY organization_id,ticket_id,version,ingested_at FORMAT JSONEachRow'
        with urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:18123/?' + params, data=query.encode()), timeout=10) as response:
            payload = response.read()
        current = [json.loads(line) for line in payload.splitlines()]
        old = [json.loads(line) for line in (BASE / ('ck-' + table + '.jsonl')).read_text().splitlines()]
        result['ck'][table] = {'physical_rows': len(current), 'same_all_physical_rows': current == old, 'sha256': digest(payload)}
    for name, port in [('mysql', 33306), ('kafka', 19092), ('es', 19200), ('ck', 18123), ('api', 18080)]:
        with socket.socket() as connection:
            result['runtime'][name] = {'port': port, 'reachable': connection.connect_ex(('127.0.0.1', port)) == 0}
    result['preserved'] = (all(v['same'] for v in result['mysql'].values()) and result['es']['same_fields_and_external_versions']
                           and all(v['same_all_physical_rows'] for v in result['ck'].values())
                           and int(control_tables) == 0 and int(handoff_tables) == 0
                           and all(v['reachable'] for v in result['runtime'].values()))
    result['demo_migration_013_tables'] = int(mysql.mysql("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='ticketpulse_local' AND table_name IN ('sync_projection_read_selection','sync_projection_read_switches');").strip())
    result['demo_cdc_checkpoint'] = mysql.mysql("USE ticketpulse_local;SELECT server_uuid,binlog_file,binlog_pos,bootstrap_complete FROM sync_kafka_cdc_checkpoint WHERE source_name='tickets-v1';").strip()
    result['demo_kafka_groups'] = owned.kafka_command('kafka-consumer-groups.sh', ['--bootstrap-server', '127.0.0.1:19092', '--describe', '--all-groups']).stdout
    result['preserved'] = result['preserved'] and result['demo_migration_013_tables'] == 0
    path = OUT / 'preservation' / str(__import__('time').time_ns())
    path.mkdir(parents=True)
    (path / 'demo.json').write_text(json.dumps(result, indent=2) + chr(10))
    print(json.dumps(result, indent=2))
    if not result['preserved']:
        raise RuntimeError('Demo differs from preserved baseline; investigate without changing it')


if __name__ == '__main__':
    main()
