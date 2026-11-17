#!/usr/bin/env python3
"""Real isolated MySQL/Binlog/Kafka/ClickHouse correctness and recovery tests."""
from datetime import datetime,timezone
import json
from pathlib import Path
import subprocess
import sys
import urllib.request
import local_mysql as local
import local_analytics as runtime

ROOT=Path(__file__).resolve().parents[1]

def main():
    owner=local.marker();local.verify(owner);runtime.kafka.start();runtime.search.start_component('es',runtime.search.state());runtime.start()
    env=runtime.environment();env['TICKETPULSE_TEST_DSN']=owner['admin_dsn'];env['TICKETPULSE_TEST_SERVER_UUID']=owner['server_uuid']
    command=['go','test','-race','-tags=integration,m2,m3','-count=1','-v','./internal/mysqlstore','-run','^TestM3']+sys.argv[1:]
    output=ROOT/'artifacts/m3-01';output.mkdir(exist_ok=True);started=datetime.now(timezone.utc)
    with (output/'integration-analytics.log').open('w') as log:
        p=subprocess.Popen(command,cwd=ROOT,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        for line in p.stdout:print(line,end='',flush=True);log.write(line)
        code=p.wait()
    version=urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:18123/',data=b'SELECT version()')).read().decode().strip()
    (output/'integration-analytics-result.json').write_text(json.dumps({'command':command,'exit_code':code,'started_at':started.isoformat(),'finished_at':datetime.now(timezone.utc).isoformat(),'mysql_version':local.mysql('SELECT VERSION();').strip(),'clickhouse_version':version,'kafka_version':'4.4.0','elasticsearch_version':'8.17.5','real_binlog_kafka_clickhouse':True,'gold_source':'tests/analytics/prd_gold.json'},indent=2)+chr(10))
    return code

if __name__=='__main__':raise SystemExit(main())
