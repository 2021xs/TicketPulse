#!/usr/bin/env python3
"""Verified native ClickHouse plus existing owned MySQL/NATS/ES. Never reset business data."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import zipfile

import local_mysql as local
import local_search as search
import local_kafka as kafka

ROOT=Path(__file__).resolve().parents[1]
RUNTIME=ROOT/'.local/analytics'
STATE=RUNTIME/'instance.json'
VERSION='26.10.2.103'
CHECKSUM='992a893f01bba4419a50b50d2ce4557e9e0c2f9f7f3ae344dd5a4361cb81ba88'
URL='https://github.com/ClickHouse/ClickHouse/releases/download/v'+VERSION+'-stable/clickhouse-macos-aarch64.zip'
BINARY=RUNTIME/'clickhouse'
PORT=18123


def install():
    if sys.platform!='darwin' or os.uname().machine!='arm64':
        raise RuntimeError('Native helper supports macOS arm64; set ClickHouse endpoint for other environments')
    RUNTIME.mkdir(parents=True,exist_ok=True)
    archive=RUNTIME/('clickhouse-'+VERSION+'-macos-arm64.zip')
    if not archive.exists():
        print('Downloading official ClickHouse '+VERSION,flush=True)
        search.download(URL,archive.with_suffix('.partial'))
        archive.with_suffix('.partial').rename(archive)
    digest=hashlib.sha256()
    with archive.open('rb') as source:
        for block in iter(lambda:source.read(1024*1024),b''):
            digest.update(block)
    if digest.hexdigest()!=CHECKSUM:
        raise RuntimeError('ClickHouse archive differs from pinned Homebrew release checksum')
    if not BINARY.exists():
        with zipfile.ZipFile(archive) as package:
            if package.namelist()!=['clickhouse']:
                raise RuntimeError('Unexpected native archive content')
            package.extractall(RUNTIME)
        BINARY.chmod(0o755)
    print('Verified native ClickHouse '+VERSION,flush=True)


def state():
    value=json.loads(STATE.read_text())
    owner=local.marker();local.verify(owner)
    if value.get('project')!=str(ROOT) or value.get('server_uuid')!=owner['server_uuid'] or value.get('data')!=str(RUNTIME/'data'):
        raise RuntimeError('ClickHouse ownership mismatch')
    return value


def setup():
    owner=local.marker();local.verify(owner)
    kafka.start();search.start_component('es',search.state())
    install()
    for name in ['data','logs','tmp','user-files','format-schemas']:
        (RUNTIME/name).mkdir(exist_ok=True)
    if not STATE.exists():
        local.private_file(STATE,json.dumps({'project':str(ROOT),'server_uuid':owner['server_uuid'],'data':str(RUNTIME/'data'),'pid':None},indent=2))
    state()
    # Minimal native single-node configuration; only loopback HTTP/native ports.
    config='''<clickhouse>
      <logger><level>warning</level><log>LOG/server.log</log><errorlog>LOG/error.log</errorlog><size>10M</size><count>2</count></logger>
      <listen_host>127.0.0.1</listen_host><http_port>18123</http_port><tcp_port>19001</tcp_port>
      <path>DATA/</path><tmp_path>TMP/</tmp_path><user_files_path>USER/</user_files_path><format_schema_path>FORMAT/</format_schema_path>
      <timezone>UTC</timezone><max_connections>64</max_connections><max_concurrent_queries>16</max_concurrent_queries>
      <max_server_memory_usage>1073741824</max_server_memory_usage>
      <background_pool_size>4</background_pool_size><background_schedule_pool_size>4</background_schedule_pool_size>
      <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
      <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
      <mark_cache_size>16777216</mark_cache_size><uncompressed_cache_size>16777216</uncompressed_cache_size>
      <merge_tree><number_of_free_entries_in_pool_to_execute_mutation>2</number_of_free_entries_in_pool_to_execute_mutation><number_of_free_entries_in_pool_to_lower_max_size_of_merge>2</number_of_free_entries_in_pool_to_lower_max_size_of_merge><number_of_free_entries_in_pool_to_execute_optimize_entire_partition>2</number_of_free_entries_in_pool_to_execute_optimize_entire_partition></merge_tree>
      <profiles><default><max_threads>2</max_threads><max_memory_usage>536870912</max_memory_usage><async_insert>0</async_insert></default></profiles>
      <users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>
      <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
    </clickhouse>'''
    for placeholder,directory in [('LOG','logs'),('DATA','data'),('TMP','tmp'),('USER','user-files'),('FORMAT','format-schemas')]:
        config=config.replace(placeholder,str(RUNTIME/directory))
    (RUNTIME/'config.xml').write_text(config)
    local.mysql('USE ticketpulse_local;\n'+(ROOT/'db/migrations/007_analytics_sync.sql').read_text())
    local.mysql("GRANT SELECT,INSERT,UPDATE ON ticketpulse_local.sync_source_watermark TO 'ticketpulse_sync'@'localhost';"+
                "GRANT SELECT,INSERT,UPDATE ON ticketpulse_local.sync_consumer_watermarks TO 'ticketpulse_sync'@'localhost';"+
                "GRANT SELECT,INSERT,UPDATE ON ticketpulse_local.sync_analytics_bootstrap TO 'ticketpulse_sync'@'localhost';"+
                "GRANT SELECT,INSERT,UPDATE ON ticketpulse_local.sync_ck_deliveries TO 'ticketpulse_sync'@'localhost';")
    start()


def server_pid(pid):
    try:
        cwd=subprocess.check_output(['lsof','-a','-p',str(pid),'-d','cwd','-Fn'],text=True)
        command=subprocess.check_output(['ps','-p',str(pid),'-o','command='],text=True)
        if 'n'+str(ROOT) not in cwd.splitlines():return None
        if str(BINARY)+' server' in command and '--config-file='+str(RUNTIME/'config.xml') in command:return pid
        # Official native ClickHouse keeps a watchdog parent; the listening server
        # is its child. Verify both executable ownership and the exact config.
        if not command.strip().startswith('clickhouse-watchdog'):return None
        executable=subprocess.check_output(['lsof','-a','-p',str(pid),'-d','txt','-Fn'],text=True)
        if 'n'+str(BINARY) not in executable.splitlines():return None
        children=subprocess.check_output(['pgrep','-P',str(pid)],text=True).split()
        for child in children:
            found=server_pid(int(child))
            if found:return found
        return None
    except subprocess.CalledProcessError:
        return None


def owns(pid):
    return server_pid(pid) is not None


def start():
    value=state()
    if search.occupied(PORT):
        if not value.get('pid') or not owns(value['pid']):
            raise RuntimeError('ClickHouse port occupied by an unowned process')
        return
    with (RUNTIME/'launch.log').open('a') as log:
        child=subprocess.Popen([str(BINARY),'server','--config-file='+str(RUNTIME/'config.xml')],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    value['pid']=child.pid;local.private_file(STATE,json.dumps(value,indent=2))
    for _ in range(200):
        if child.poll() is not None:
            raise RuntimeError('ClickHouse exited; inspect .local/analytics/launch.log and logs/error.log')
        try:
            with urllib.request.urlopen('http://127.0.0.1:'+str(PORT)+'/ping',timeout=1) as r:
                if r.read().strip()==b'Ok.':
                    print('ClickHouse ready on 127.0.0.1:'+str(PORT),flush=True)
                    return
        except OSError:
            time.sleep(.1)
    raise RuntimeError('ClickHouse readiness timeout')


def stop():
    value=state();pid=value.get('pid')
    owned_server=server_pid(pid) if pid else None
    if owned_server:
        os.kill(owned_server,signal.SIGTERM)
        # Listening sockets close before ClickHouse releases its data-directory
        # lock. Wait for the actual owned server, so immediate restart is safe.
        for _ in range(400):
            try:os.kill(owned_server,0);alive=True
            except ProcessLookupError:alive=False
            if not alive and not search.occupied(PORT):
                value['pid']=None;local.private_file(STATE,json.dumps(value,indent=2))
                print('Owned ClickHouse stopped; all data preserved',flush=True)
                return
            time.sleep(.1)
        raise RuntimeError('ClickHouse stop timeout')
    if search.occupied(PORT):raise RuntimeError('Refusing to stop unowned ClickHouse')


def environment(database='ticketpulse_local'):
    env=kafka.environment(database)
    env['TICKETPULSE_CK_URL']='http://127.0.0.1:'+str(PORT)
    env['TICKETPULSE_CK_DATABASE']=database+'_analytics_v1'
    return env


def execute(target,arguments):
    return subprocess.run([str(ROOT/'bin'/target)]+arguments,cwd=ROOT,env=environment()).returncode


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('action',choices=['setup','start','stop','sync','web','status','snapshot','replay'])
    args,extra=parser.parse_known_args()
    if args.action=='setup':setup()
    elif args.action=='start':start()
    elif args.action=='stop':stop()
    elif args.action=='web':return execute('ticketpulse',extra)
    elif args.action=='sync':return execute('ticketpulse-sync',['-mode=all']+extra)
    elif args.action=='status':return execute('ticketpulse-sync',['-mode=analytics-status']+extra)
    elif args.action=='snapshot':return execute('ticketpulse-sync',['-mode=analytics-snapshot']+extra)
    else:return execute('ticketpulse-sync',['-mode=replay','-consumer=ck']+extra)
    return 0


if __name__=='__main__':
    try:
        sys.exit(main())
    except (OSError,RuntimeError,subprocess.CalledProcessError) as error:
        print('TicketPulse analytics: '+str(error),file=sys.stderr)
        sys.exit(1)
