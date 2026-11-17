#!/usr/bin/env python3
"""Real MySQL/Binlog/Kafka/ES tests; only randomly named test schemas are modified."""
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

import local_mysql as local
import local_kafka as search

ROOT = Path(__file__).resolve().parents[1]


def main():
    owner = local.marker()
    local.verify(owner)
    search.start();search.legacy.start_component('es',search.legacy.state())
    env = search.environment()
    env['TICKETPULSE_TEST_DSN'] = owner['admin_dsn']
    env['TICKETPULSE_TEST_SERVER_UUID'] = owner['server_uuid']
    command = ['go', 'test', '-race', '-tags=integration,m2', '-count=1', '-v',
               './internal/mysqlstore', '-run', '^TestM2'] + sys.argv[1:]
    output = ROOT / 'artifacts/m2-02'
    output.mkdir(exist_ok=True)
    started = datetime.now(timezone.utc)
    with (output / 'integration-search.log').open('w') as log:
        child = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, text=True)
        for line in child.stdout:
            print(line, end='', flush=True)
            log.write(line)
        code = child.wait()
    (output / 'integration-search-result.json').write_text(json.dumps({
        'command': command, 'exit_code': code, 'started_at': started.isoformat(),
        'finished_at': datetime.now(timezone.utc).isoformat(),
        'mysql_version': local.mysql('SELECT VERSION();').strip(),
        'kafka_version': '4.4.0', 'elasticsearch_version': '8.17.5',
        'real_binlog_real_kafka_real_es': True,
        'output': 'artifacts/m2-02/integration-search.log'}, indent=2) + chr(10))
    return code


if __name__ == '__main__':
    raise SystemExit(main())
