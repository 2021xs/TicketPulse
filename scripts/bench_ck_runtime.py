#!/usr/bin/env python3
"""Owned M5-only ClickHouse 18124/19002. Never changes the existing 18123 server."""
import argparse
import datetime
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
import local_analytics as source
import local_mysql as local

ROOT=Path(__file__).resolve().parents[1]
RUNTIME=ROOT/'.local/m5-clickhouse'
STATE=RUNTIME/'instance.json'
CONFIG=RUNTIME/'config.xml'
BINARY=source.BINARY
PORT=18124
NATIVE_PORT=19002
OUTPUT=ROOT/'artifacts/m5-01/environment/independent-clickhouse'


def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def occupied(port):
    with socket.socket() as connection:return connection.connect_ex(('127.0.0.1',port))==0

def checksum(path):
    digest=hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda:handle.read(1024*1024),b''):digest.update(block)
    return digest.hexdigest()

def verify_binary():
    archive=source.RUNTIME/('clickhouse-'+source.VERSION+'-macos-arm64.zip')
    if not BINARY.is_file() or not archive.is_file():raise RuntimeError('Existing verified ClickHouse distribution is required; no download or reinstall')
    if checksum(archive)!=source.CHECKSUM:raise RuntimeError('Existing ClickHouse release archive checksum differs')
    with zipfile.ZipFile(archive) as package:
        if package.namelist()!=['clickhouse']:raise RuntimeError('Unexpected pinned distribution members')
        digest=hashlib.sha256()
        with package.open('clickhouse') as handle:
            for block in iter(lambda:handle.read(1024*1024),b''):digest.update(block)
    actual=checksum(BINARY)
    if actual!=digest.hexdigest():raise RuntimeError('ClickHouse executable differs from verified release archive')
    return {'version':source.VERSION,'archive_sha256':source.CHECKSUM,'binary_sha256':actual,'binary':str(BINARY.relative_to(ROOT))}

def state():
    value=json.loads(STATE.read_text())
    if value.get('project')!=str(ROOT) or value.get('data')!=str(RUNTIME/'data') or value.get('config')!=str(CONFIG) or value.get('http_port')!=PORT or value.get('native_port')!=NATIVE_PORT:
        raise RuntimeError('M5 ClickHouse instance ownership mismatch')
    return value

def server_pid(pid):
    if not isinstance(pid,int) or pid<1:return None
    try:
        cwd=subprocess.check_output(['lsof','-a','-p',str(pid),'-d','cwd','-Fn'],text=True,timeout=2)
        command=subprocess.check_output(['ps','-p',str(pid),'-o','command='],text=True,timeout=2)
        if 'n'+str(ROOT) not in cwd.splitlines():return None
        if str(BINARY)+' server' in command and '--config-file='+str(CONFIG) in command:return pid
        if not command.strip().startswith('clickhouse-watchdog'):return None
        executable=subprocess.check_output(['lsof','-a','-p',str(pid),'-d','txt','-Fn'],text=True,timeout=2)
        if 'n'+str(BINARY) not in executable.splitlines():return None
        children=subprocess.check_output(['pgrep','-P',str(pid)],text=True,timeout=2).split()
        for child in children:
            found=server_pid(int(child))
            if found:return found
    except (subprocess.CalledProcessError,subprocess.TimeoutExpired,ValueError):return None
    return None

def config_text():
    content='''<clickhouse>
 <logger><level>warning</level><log>LOG/server.log</log><errorlog>LOG/error.log</errorlog><size>10M</size><count>2</count></logger>
 <listen_host>127.0.0.1</listen_host><http_port>18124</http_port><tcp_port>19002</tcp_port>
 <path>DATA/</path><tmp_path>TMP/</tmp_path><user_files_path>USER/</user_files_path><format_schema_path>FORMAT/</format_schema_path>
 <timezone>UTC</timezone><max_connections>128</max_connections><max_concurrent_queries>64</max_concurrent_queries>
 <max_server_memory_usage>4294967296</max_server_memory_usage>
 <background_pool_size>4</background_pool_size><background_schedule_pool_size>4</background_schedule_pool_size>
 <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
 <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
 <mark_cache_size>16777216</mark_cache_size><uncompressed_cache_size>16777216</uncompressed_cache_size>
 <merge_tree><number_of_free_entries_in_pool_to_execute_mutation>2</number_of_free_entries_in_pool_to_execute_mutation><number_of_free_entries_in_pool_to_lower_max_size_of_merge>2</number_of_free_entries_in_pool_to_lower_max_size_of_merge><number_of_free_entries_in_pool_to_execute_optimize_entire_partition>2</number_of_free_entries_in_pool_to_execute_optimize_entire_partition></merge_tree>
 <profiles><default><max_threads>2</max_threads><max_memory_usage>536870912</max_memory_usage><async_insert>0</async_insert><log_queries>1</log_queries><log_query_threads>1</log_query_threads><log_queries_probability>1</log_queries_probability></default></profiles>
 <users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>
 <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
 <query_log><database>system</database><table>query_log</table><flush_interval_milliseconds>1000</flush_interval_milliseconds></query_log>
 <query_thread_log><database>system</database><table>query_thread_log</table><flush_interval_milliseconds>1000</flush_interval_milliseconds></query_thread_log>
</clickhouse>'''
    for placeholder,name in [('LOG','logs'),('DATA','data'),('TMP','tmp'),('USER','user-files'),('FORMAT','format-schemas')]:content=content.replace(placeholder,str(RUNTIME/name))
    return content

def query(statement):
    with urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:'+str(PORT)+'/?max_execution_time=2&wait_end_of_query=1',data=statement.encode()),timeout=3) as response:return response.read(1024*1024).decode()

def start():
    if sys.platform!='darwin' or os.uname().machine!='arm64':raise RuntimeError('Verified native runtime supports macOS arm64 only')
    binary=verify_binary()
    if occupied(PORT) or occupied(NATIVE_PORT):
        if not STATE.is_file():raise RuntimeError('M5 ports occupied without owned marker')
        value=state();pid=server_pid(value.get('pid'))
        if not pid:raise RuntimeError('M5 ports occupied by unowned process')
        print(json.dumps({'state':'running','server_pid':pid,'url':'http://127.0.0.1:'+str(PORT),'existing':True}));return
    if STATE.is_file():value=state()
    else:
        if (RUNTIME/'data').exists() and any((RUNTIME/'data').iterdir()):raise RuntimeError('Existing M5 data without marker; refusing startup')
        value={'project':str(ROOT),'data':str(RUNTIME/'data'),'config':str(CONFIG),'http_port':PORT,'native_port':NATIVE_PORT,'pid':None,'created_at':stamp(),'verified_distribution':binary}
    RUNTIME.mkdir(parents=True,exist_ok=True)
    for name in ['data','logs','tmp','user-files','format-schemas']:(RUNTIME/name).mkdir(exist_ok=True)
    CONFIG.write_text(config_text());local.private_file(STATE,json.dumps(value,indent=2))
    OUTPUT.mkdir(parents=True,exist_ok=True)
    (OUTPUT/'config.xml').write_text(CONFIG.read_text())
    before={'demo_18123_available':occupied(18123),'demo_18080_available':occupied(18080),'source_config_sha256':checksum(source.RUNTIME/'config.xml'),'m5_config_sha256':checksum(CONFIG),'distribution':binary,'limits':{'server_memory_bytes':4*1024**3,'concurrent_queries':64,'query_threads':2,'query_memory_bytes':512*1024**2},'started_at':stamp()}
    with (RUNTIME/'launch.log').open('a') as log:child=subprocess.Popen([str(BINARY),'server','--config-file='+str(CONFIG)],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    value['pid']=child.pid;value['started_at']=stamp();local.private_file(STATE,json.dumps(value,indent=2))
    for _ in range(200):
        if child.poll() is not None:raise RuntimeError('M5 server exited; inspect .local/m5-clickhouse/launch.log')
        try:
            if query('SELECT 1').strip()=='1':break
        except OSError:time.sleep(.1)
    else:raise RuntimeError('M5 server startup timeout')
    pid=server_pid(child.pid)
    if not pid:raise RuntimeError('M5 listening process ownership could not be verified')
    # Wait for the new runtime system log table provisioning after one real query.
    log_tables=[]
    for _ in range(30):
        log_tables=query("SELECT name FROM system.tables WHERE database='system' AND name IN ('query_log','query_thread_log') ORDER BY name FORMAT TabSeparated").splitlines()
        if len(log_tables)==2:break
        time.sleep(.1)
    profile=query("SELECT name,value FROM system.settings WHERE name IN ('max_threads','max_memory_usage','log_queries','log_query_threads') ORDER BY name FORMAT JSONEachRow")
    after={'source_config_sha256':checksum(source.RUNTIME/'config.xml'),'demo_18123_available':occupied(18123),'demo_18080_available':occupied(18080)}
    if before['source_config_sha256']!=after['source_config_sha256']:raise RuntimeError('Existing demonstration CK config changed unexpectedly')
    evidence={'before':before,'after':after,'server_pid':pid,'watchdog_pid':child.pid,'http_url':'http://127.0.0.1:18124','native_port':19002,'system_log_tables':log_tables,'profile_settings':[json.loads(line) for line in profile.splitlines()],'finished_at':stamp(),'existing_demo_untouched':True}
    (OUTPUT/'startup.json').write_text(json.dumps(evidence,indent=2)+'\n');print(json.dumps(evidence,indent=2))

def stop():
    value=state();pid=server_pid(value.get('pid'))
    if pid:
        os.kill(pid,signal.SIGTERM)
        for _ in range(400):
            try:os.kill(pid,0);alive=True
            except ProcessLookupError:alive=False
            if not alive and not occupied(PORT):
                value['pid']=None;value['stopped_at']=stamp();local.private_file(STATE,json.dumps(value,indent=2));print('Owned M5 ClickHouse stopped; data and original18123 retained');return
            time.sleep(.1)
        raise RuntimeError('Owned M5 ClickHouse stop timeout')
    if occupied(PORT) or occupied(NATIVE_PORT):raise RuntimeError('Refusing to stop unowned M5 port process')
    print('Owned M5 ClickHouse already stopped; data retained')

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['start','stop','status']);args=parser.parse_args()
    if args.action=='start':start()
    elif args.action=='stop':stop()
    else:
        value=state();print(json.dumps({'url':'http://127.0.0.1:18124','server_pid':server_pid(value.get('pid')),'http_available':occupied(PORT),'native_available':occupied(NATIVE_PORT),'data':'.local/m5-clickhouse/data'},indent=2))
if __name__=='__main__':
    try:main()
    except (OSError,RuntimeError,subprocess.CalledProcessError) as error:print('TicketPulse M5 ClickHouse: '+str(error),file=sys.stderr);sys.exit(1)
