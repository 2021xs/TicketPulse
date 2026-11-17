#!/usr/bin/env python3
"""Lightweight read-only M5 metrics: collect(database=None, es_index=None,
ck_database=None, pids=None) -> dict. No logging/config changes or load.
compare(previous,current) returns sample deltas; CLI interval>=3s/duration<=60s.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
import os
from pathlib import Path
import re
import shutil
import socket
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import local_mysql as local
ROOT=Path(__file__).resolve().parents[1]
DEADLINE=2.0
RESPONSE_BUDGET=2*1024*1024

def unknown(reason):return {'state':'UNKNOWN','reason':reason}
def identifier(value,limit=128,es=False):
    pattern=r'[a-z0-9][a-z0-9_-]*' if es else r'[a-z0-9_]+'
    if not isinstance(value,str) or len(value)>limit or not re.fullmatch(pattern,value):raise ValueError('invalid metrics namespace')
    return value

def run(args,timeout=DEADLINE):
    started=time.monotonic()
    try:p=subprocess.run(args,cwd=ROOT,capture_output=True,text=True,timeout=timeout)
    except subprocess.TimeoutExpired:return unknown('command deadline exceeded')
    except OSError:return unknown('command unavailable')
    if p.returncode:return unknown('command failed with exit '+str(p.returncode))
    if len(p.stdout.encode())>RESPONSE_BUDGET:return unknown('command response exceeds bounded budget')
    return {'state':'OK','value':p.stdout,'probe_ms':round((time.monotonic()-started)*1000,3)}

def mysql_query(statement):
    started=time.monotonic()
    try:
        p=subprocess.run([str(local.DIST/'bin/mysql'),'--defaults-file='+str(local.ADMIN),'--connect-timeout=1','--batch','--skip-column-names'],cwd=ROOT,input='SET SESSION MAX_EXECUTION_TIME=750; SET SESSION lock_wait_timeout=1; '+statement,text=True,capture_output=True,timeout=DEADLINE)
    except subprocess.TimeoutExpired:return unknown('MySQL probe deadline exceeded')
    except OSError:return unknown('owned MySQL client unavailable')
    if p.returncode:
        match=re.search(r'ERROR (\d+)',p.stderr)
        return unknown('MySQL probe failed'+(' with code '+match.group(1) if match else ''))
    if len(p.stdout.encode())>RESPONSE_BUDGET:return unknown('MySQL result exceeds bounded budget')
    return {'state':'OK','value':p.stdout,'probe_ms':round((time.monotonic()-started)*1000,3)}

def json_rows(statement):
    result=mysql_query(statement)
    if result['state']!='OK':return result
    try:result['value']=[json.loads(line) for line in result['value'].splitlines() if line]
    except (ValueError,TypeError):return unknown('MySQL structured response unavailable')
    return result

def mysql_metrics(database):
    owner=local.marker();identity=mysql_query('SELECT @@server_uuid;')
    if identity['state']!='OK' or identity['value'].strip()!=owner['server_uuid']:return unknown('MySQL ownership/UUID not verified')
    queries={
      'settings':"SELECT JSON_OBJECT('version',VERSION(),'performance_schema',@@performance_schema,'slow_query_log',@@slow_query_log,'long_query_time_seconds',@@long_query_time,'log_output',@@log_output,'innodb_buffer_pool_bytes',@@innodb_buffer_pool_size,'max_connections',@@max_connections,'sync_binlog',@@sync_binlog,'innodb_flush_log_at_trx_commit',@@innodb_flush_log_at_trx_commit);",
      'statement_consumers':"SELECT JSON_OBJECT('name',NAME,'enabled',ENABLED) FROM performance_schema.setup_consumers WHERE NAME LIKE '%statement%';",
      'global_counters':"SELECT JSON_OBJECT('name',VARIABLE_NAME,'value',VARIABLE_VALUE) FROM performance_schema.global_status WHERE VARIABLE_NAME IN ('Questions','Queries','Slow_queries','Threads_connected','Threads_running','Connections','Aborted_clients','Aborted_connects','Innodb_buffer_pool_reads','Innodb_buffer_pool_read_requests','Innodb_buffer_pool_pages_dirty','Innodb_data_reads','Innodb_data_writes','Innodb_data_read','Innodb_data_written','Innodb_os_log_written','Innodb_log_waits','Innodb_row_lock_current_waits','Innodb_row_lock_time','Innodb_row_lock_waits','Created_tmp_disk_tables','Created_tmp_tables','Sort_merge_passes');",
      'transactions':"SELECT JSON_OBJECT('active',COUNT(*),'oldest_age_seconds',COALESCE(MAX(TIMESTAMPDIFF(SECOND,trx_started,NOW())),0),'rows_locked',COALESCE(SUM(trx_rows_locked),0),'rows_modified',COALESCE(SUM(trx_rows_modified),0)) FROM information_schema.innodb_trx; SELECT JSON_OBJECT('history_list_length',COUNT,'enabled',STATUS) FROM information_schema.innodb_metrics WHERE NAME='trx_rseg_history_len';",
      'lock_waits':"SELECT JSON_OBJECT('lock_waits',COUNT(*)) FROM performance_schema.data_lock_waits;",
      'tables':"SELECT JSON_OBJECT('table',table_name,'approx_rows',table_rows,'data_bytes',data_length,'index_bytes',index_length,'data_free_bytes',data_free) FROM information_schema.tables WHERE table_schema='"+database+"' AND table_type='BASE TABLE' ORDER BY table_name;",
      'index_io':"SELECT JSON_OBJECT('table',OBJECT_NAME,'index',INDEX_NAME,'reads',COUNT_READ,'read_ms',SUM_TIMER_READ/1000000000,'writes',COUNT_WRITE,'write_ms',SUM_TIMER_WRITE/1000000000) FROM performance_schema.table_io_waits_summary_by_index_usage WHERE OBJECT_SCHEMA='"+database+"' ORDER BY SUM_TIMER_WAIT DESC LIMIT 20;",
      'statement_latency':"SELECT JSON_OBJECT('digest',DIGEST,'normalized_statement',LEFT(DIGEST_TEXT,512),'count',COUNT_STAR,'total_ms',SUM_TIMER_WAIT/1000000000,'avg_ms',AVG_TIMER_WAIT/1000000000,'max_ms',MAX_TIMER_WAIT/1000000000,'p95_ms',QUANTILE_95/1000000000,'rows_examined',SUM_ROWS_EXAMINED,'rows_sent',SUM_ROWS_SENT,'tmp_disk_tables',SUM_CREATED_TMP_DISK_TABLES,'no_index_used',SUM_NO_INDEX_USED) FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME='"+database+"' ORDER BY SUM_TIMER_WAIT DESC LIMIT 15;",
      'cdc_checkpoint':"SELECT JSON_OBJECT('source',source_name,'server_uuid',server_uuid,'binlog_file',binlog_file,'binlog_pos',binlog_pos,'bootstrap_complete',bootstrap_complete,'updated_at',updated_at) FROM "+database+".sync_kafka_cdc_checkpoint;",
      'worker_progress':"SELECT JSON_OBJECT('component',component,'state',state,'heartbeat_at',heartbeat_at,'last_event_at',last_event_at,'has_error',last_error IS NOT NULL) FROM "+database+".sync_worker_state; SELECT JSON_OBJECT('component',component,'partition',partition_id,'offset',record_offset,'issued_at',issued_at,'applied_at',applied_at) FROM "+database+".sync_kafka_watermarks ORDER BY component,partition_id;"
    }
    with ThreadPoolExecutor(max_workers=4) as pool:values=dict(zip(queries,pool.map(json_rows,queries.values())))
    return {'state':'OK','database':database,'observations':values,'notes':['Statement latency cumulative since reset; no raw SQL or credentials captured.','Table row estimates approximate; exact large table scans excluded.','Locks/transactions instance-wide; index/digest queries namespace-filtered.']}

class Reader:
    def __init__(self,data):self.data=data;self.pos=0
    def take(self,n):
        value=self.data[self.pos:self.pos+n];self.pos+=n
        if len(value)!=n:raise ValueError('Kafka truncated response')
        return value
    def integer(self,fmt):return struct.unpack(fmt,self.take(struct.calcsize(fmt)))[0]
    def i16(self):return self.integer('>h')
    def i32(self):return self.integer('>i')
    def i64(self):return self.integer('>q')
    def string(self):
        n=self.i16()
        return None if n<0 else self.take(n).decode()

def kstring(value):
    b=value.encode();return struct.pack('>h',len(b))+b

def recv_exact(connection,n):
    chunks=[];received=0
    while received<n:
        block=connection.recv(n-received)
        if not block:raise ValueError('Kafka connection closed')
        chunks.append(block);received+=len(block)
    return b''.join(chunks)

def kafka_request(key,version,body):
    payload=struct.pack('>hhi',key,version,1)+kstring('tp-m5-metrics')+body
    with socket.create_connection(('127.0.0.1',19092),timeout=1) as connection:
        connection.settimeout(1);connection.sendall(struct.pack('>i',len(payload))+payload)
        n=struct.unpack('>i',recv_exact(connection,4))[0]
        if n<4 or n>RESPONSE_BUDGET:raise ValueError('Kafka response budget')
        reader=Reader(recv_exact(connection,n))
        if reader.i32()!=1:raise ValueError('Kafka correlation mismatch')
        return reader

def topic_partitions(topic):
    reader=kafka_request(3,0,struct.pack('>i',1)+kstring(topic))
    for _ in range(reader.i32()):reader.i32();reader.string();reader.i32()
    found=[]
    for _ in range(reader.i32()):
        error=reader.i16();name=reader.string();partitions=[]
        for _ in range(reader.i32()):
            pe=reader.i16();pid=reader.i32();leader=reader.i32()
            replicas=[reader.i32() for _ in range(reader.i32())];isr=[reader.i32() for _ in range(reader.i32())]
            partitions.append({'partition':pid,'leader':leader,'replicas':replicas,'isr':isr,'error_code':pe})
        if name==topic:
            if error:raise ValueError('Kafka topic metadata error')
            found=partitions
    if not found or len(found)>64:raise ValueError('Kafka partition metadata unavailable')
    return sorted(found,key=lambda p:p['partition'])

def offsets(topic,parts,timestamp):
    body=struct.pack('>ii',-1,1)+kstring(topic)+struct.pack('>i',len(parts))
    for p in parts:body+=struct.pack('>iq',p['partition'],timestamp)
    reader=kafka_request(2,1,body);result={}
    for _ in range(reader.i32()):
        name=reader.string()
        for _ in range(reader.i32()):
            part=reader.i32();error=reader.i16();reader.i64();offset=reader.i64()
            if name==topic:
                if error:raise ValueError('Kafka offset error')
                result[part]=offset
    return result

def group_offsets(topic,parts,group):
    body=kstring(group)+struct.pack('>i',1)+kstring(topic)+struct.pack('>i',len(parts))
    for p in parts:body+=struct.pack('>i',p['partition'])
    reader=kafka_request(9,1,body);result={}
    for _ in range(reader.i32()):
        name=reader.string()
        for _ in range(reader.i32()):
            part=reader.i32();offset=reader.i64();reader.string();error=reader.i16()
            if name==topic:
                if error:raise ValueError('Kafka group offset error')
                result[part]=offset
    return result

def kafka_metrics(database):
    topic='ticketpulse.'+database+'.tickets.v1';started=time.monotonic()
    try:
        parts=topic_partitions(topic);earliest=offsets(topic,parts,-2);latest=offsets(topic,parts,-1);groups={}
        for component in ['es','ck']:
            group=topic+'.'+component+'.v1';committed=group_offsets(topic,parts,group)
            groups[component]={'group':group,'partitions':[{'partition':p['partition'],'start':earliest.get(p['partition']),'end':latest.get(p['partition']),'committed':committed.get(p['partition']),'lag':None if committed.get(p['partition'],-1)<0 else max(0,latest[p['partition']]-committed[p['partition']]),'offset_state':'UNKNOWN' if committed.get(p['partition'],-1)<0 else 'OK'} for p in parts]}
        return {'state':'OK','topic':topic,'partitions':parts,'end_offsets':latest,'groups':groups,'probe_ms':round((time.monotonic()-started)*1000,3),'broker_byte_rates':unknown('No JMX/metrics endpoint; records/s uses offset sample deltas.'),'retention':unknown('Capture official topic retention CLI once; omitted per3s.'),'notes':['End and committed are sequential observations; committed can advance past sampled end. Lag is clamped at0, not a continuous cross-partition watermark.']}
    except (OSError,ValueError,struct.error,UnicodeError):return unknown('Kafka read-only metadata/offset probe unavailable within deadline')

def http_json(url,body=None):
    started=time.monotonic()
    try:
        with urllib.request.urlopen(urllib.request.Request(url,data=body),timeout=DEADLINE) as response:data=response.read(RESPONSE_BUDGET+1)
        if len(data)>RESPONSE_BUDGET:return unknown('HTTP response budget exceeded')
        value=json.loads(data)
    except urllib.error.HTTPError as error:return unknown('HTTP probe status '+str(error.code))
    except (OSError,ValueError):return unknown('HTTP probe unavailable within deadline')
    return {'state':'OK','value':value,'probe_ms':round((time.monotonic()-started)*1000,3)}

def es_metrics(index):
    filtered='_nodes,nodes.*.name,nodes.*.timestamp,nodes.*.indices.indexing,nodes.*.indices.search,nodes.*.indices.merges,nodes.*.indices.refresh,nodes.*.indices.segments,nodes.*.process.cpu,nodes.*.process.mem,nodes.*.jvm.mem,nodes.*.jvm.gc,nodes.*.fs.total,nodes.*.fs.io_stats,nodes.*.thread_pool.search,nodes.*.thread_pool.write'
    return {'state':'OK','index':index,'nodes':http_json('http://127.0.0.1:19200/_nodes/stats/indices,process,jvm,fs,thread_pool?filter_path='+urllib.parse.quote(filtered,safe=',.*')),'index_stats':http_json('http://127.0.0.1:19200/'+index+'/_stats/docs,store,indexing,search,merge,refresh,segments?filter_path=_shards,_all.total')}

def ck_endpoint():
    endpoint=os.environ.get('TICKETPULSE_BENCH_CK_URL','http://127.0.0.1:18123')
    if endpoint not in ['http://127.0.0.1:18123','http://127.0.0.1:18124']:raise ValueError('metrics CK URL must be owned loopback18123 or18124')
    return endpoint

def ck_rows(query,database=None,profile_defaults=False):
    params={'max_execution_time':'1','max_threads':'1','max_memory_usage':'33554432','log_queries':'0','wait_end_of_query':'1','output_format_json_quote_64bit_integers':'0'}
    if profile_defaults:params={'max_execution_time':'1','wait_end_of_query':'1','output_format_json_quote_64bit_integers':'0'}
    if database:params['database']=database
    started=time.monotonic()
    try:
        with urllib.request.urlopen(urllib.request.Request(ck_endpoint()+'/?'+urllib.parse.urlencode(params),data=(query+' FORMAT JSONEachRow').encode()),timeout=DEADLINE) as response:data=response.read(RESPONSE_BUDGET+1)
        if len(data)>RESPONSE_BUDGET:return unknown('ClickHouse response budget exceeded')
        value=[json.loads(line) for line in data.decode().splitlines() if line]
    except urllib.error.HTTPError as error:return unknown('ClickHouse probe HTTP '+str(error.code))
    except (OSError,ValueError):return unknown('ClickHouse probe unavailable within deadline')
    return {'state':'OK','value':value,'probe_ms':round((time.monotonic()-started)*1000,3)}

def ck_metrics(database):
    queries={
      'settings':"SELECT name,value FROM system.settings WHERE name IN ('log_queries','log_query_threads','max_threads','max_memory_usage')",
      'parts':"SELECT table,count() AS active_parts,sum(rows) AS physical_rows,sum(bytes_on_disk) AS disk_bytes,sum(data_compressed_bytes) AS compressed_bytes,sum(data_uncompressed_bytes) AS uncompressed_bytes FROM system.parts WHERE active AND database='"+database+"' GROUP BY table ORDER BY table",
      'merges':"SELECT table,elapsed,progress,total_size_bytes_compressed,bytes_read_uncompressed,bytes_written_uncompressed,memory_usage,num_parts FROM system.merges WHERE database='"+database+"' LIMIT 20",
      'processes':"SELECT count() AS active_queries,sum(elapsed) AS elapsed_seconds,sum(read_rows) AS read_rows,sum(read_bytes) AS read_bytes,sum(memory_usage) AS memory_bytes FROM system.processes WHERE current_database='"+database+"'",
      'metrics':"SELECT metric,value FROM system.metrics WHERE metric IN ('Query','Merge','MemoryTracking','BackgroundMergesAndMutationsPoolTask','BackgroundMergesAndMutationsPoolSize','TCPConnection','HTTPConnection')",
      'events':"SELECT event,value FROM system.events WHERE event IN ('Query','SelectQuery','InsertQuery','InsertedRows','InsertedBytes','SelectedRows','SelectedBytes','RealTimeMicroseconds','UserTimeMicroseconds','SystemTimeMicroseconds','OSReadBytes','OSWriteBytes','DelayedInserts','RejectedInserts','Merge')",
      'log_tables':"SELECT name FROM system.tables WHERE database='system' AND name IN ('query_log','query_thread_log','part_log','metric_log')"
    }
    with ThreadPoolExecutor(max_workers=4) as pool:values=dict(zip(queries,pool.map(ck_rows,queries.values())))
    values['profile_settings']=ck_rows(queries['settings'],profile_defaults=True)
    values['settings']['scope']='collector session effective settings; overrides max_threads/memory/log_queries'
    available=[x['name'] for x in values['log_tables'].get('value',[])] if values['log_tables']['state']=='OK' else []
    values['query_log']=ck_rows("SELECT count() AS finished_queries,quantile(0.5)(query_duration_ms) AS p50_ms,quantile(0.95)(query_duration_ms) AS p95_ms,max(query_duration_ms) AS max_ms,sum(read_rows) AS read_rows,sum(read_bytes) AS read_bytes,max(memory_usage) AS peak_memory FROM system.query_log WHERE event_time>now()-INTERVAL 60 SECOND AND current_database='"+database+"' AND type='QueryFinish'") if 'query_log' in available else unknown('system.query_log absent; log_queries=1 alone does not provision it.')
    if 'query_log' in available:
        values['query_log_errors']=ck_rows("SELECT type,exception_code,count() AS queries,max(query_duration_ms) AS max_ms FROM system.query_log WHERE event_time>now()-INTERVAL 60 SECOND AND current_database='"+database+"' AND type IN ('ExceptionBeforeStart','ExceptionWhileProcessing') GROUP BY type,exception_code LIMIT 20")
    else:values['query_log_errors']=unknown('system.query_log absent')
    values['query_thread_log']=ck_rows("SELECT count() AS thread_records,uniqExact(query_id) AS queries,sum(read_rows) AS read_rows,sum(read_bytes) AS read_bytes,max(peak_memory_usage) AS thread_peak_memory FROM system.query_thread_log WHERE event_time>now()-INTERVAL 60 SECOND AND current_database='"+database+"'") if 'query_thread_log' in available else unknown('system.query_thread_log absent')
    return {'state':'OK','database':database,'endpoint':ck_endpoint(),'observations':values,'notes':['Collector sets log_queries=0 for its session only; shared settings unchanged.','system.parts gives physical metadata rows, not canonical business counts.','No SYSTEM FLUSH LOGS, OPTIMIZE, exact large count or mutation.']}

def vm_fields(text):
    values={}
    for line in text.splitlines():
        match=re.match(r'([^:]+):\s+(\d+)\.',line)
        if match:values[match.group(1).strip(' "')]=int(match.group(2))
    page=re.search(r'page size of (\d+) bytes',text)
    return {'page_size':int(page.group(1)) if page else None,'counters':values}

def detected_pids():
    result={};pidfile=ROOT/'.local/mysql.pid'
    if pidfile.is_file():
        try:result['mysql']=int(pidfile.read_text().strip())
        except ValueError:pass
    for name,path,key in [('es',ROOT/'.local/search/instance.json','pids'),('kafka',ROOT/'.local/kafka/instance.json','pid'),('ck',ROOT/'.local/analytics/instance.json','pid')]:
        try:
            value=json.loads(path.read_text());pid=value.get(key)
            if isinstance(pid,dict):pid=pid.get(name)
            if pid:result[name]=int(pid)
        except (OSError,ValueError,TypeError):pass
    rows=run(['ps','-axo','pid=,comm='])
    if rows['state']=='OK':
        for line in rows['value'].splitlines():
            split=line.strip().split(None,1)
            if len(split)==2 and (str(ROOT/'bin/') in split[1] or split[1].endswith('/ticketpulse') or split[1].endswith('/ticketpulse-sync')):result['ticketpulse_'+split[0]]=int(split[0])
    # Official CK watchdog and ES launcher may not be the listening server.
    for name,port in [('mysql',33306),('kafka',19092),('es',19200),('ck',int(urllib.parse.urlparse(ck_endpoint()).port)),('web',18080)]:
        owners=run(['lsof','-nP','-iTCP:'+str(port),'-sTCP:LISTEN','-t'])
        if owners['state']=='OK':
            ids=sorted(set(int(v) for v in owners['value'].split() if v.isdigit()))
            for i,pid in enumerate(ids):result[name+('_'+str(i) if len(ids)>1 else '')]=pid
    # Report each PID once, preferring the listening component label.
    preferred={pid:name for name,pid in result.items() if name in ['mysql','kafka','es','ck','web']}
    return {name:pid for name,pid in result.items() if pid not in preferred or preferred[pid]==name}

def process_metrics(pids):
    if pids is None:pids=detected_pids()
    elif not isinstance(pids,dict):pids={str(pid):pid for pid in pids}
    result={}
    for name,pid in pids.items():
        if not isinstance(pid,int) or pid<1:result[str(name)]=unknown('invalid PID');continue
        row=run(['ps','-p',str(pid),'-o','pid=,ppid=,%cpu=,rss=,vsz=,etime='])
        if row['state']!='OK':result[str(name)]=row;continue
        values=row['value'].strip().split()
        try:result[str(name)]={'state':'OK','pid':int(values[0]),'ppid':int(values[1]),'cpu_percent':float(values[2]),'rss_bytes':int(values[3])*1024,'vsz_bytes':int(values[4])*1024,'elapsed':values[5]}
        except (ValueError,IndexError):result[str(name)]=unknown('PID absent or response unavailable')
    return result

def host_metrics(pids):
    usage=shutil.disk_usage(ROOT);vm=run(['vm_stat']);iostat=run(['iostat','-d','-I','-c','1']);pressure=run(['memory_pressure','-Q'])
    if vm['state']=='OK':vm['value']=vm_fields(vm['value'])
    return {'state':'OK','platform':sys.platform,'cpu_count':os.cpu_count(),'load_average':list(os.getloadavg()),'disk':{'total_bytes':usage.total,'used_bytes':usage.used,'free_bytes':usage.free},'vm':vm,'memory_pressure':pressure,'disk_io_cumulative':iostat,'processes':process_metrics(pids),'notes':['ps CPU sampling, not request CPU attribution.','vm_stat paging counters cumulative; compare() gives new paging.','Disk I/O instance-wide; database attribution unavailable.']}

def demo_health():
    started=time.monotonic()
    try:
        with urllib.request.urlopen('http://127.0.0.1:18080/login',timeout=1) as r:r.read(1024);status=r.status
        return {'state':'OK','http_status':status,'latency_ms':round((time.monotonic()-started)*1000,3)}
    except (OSError,ValueError):return unknown('demo HTTP page unavailable within 1s')

def collect(database=None,es_index=None,ck_database=None,pids=None):
    database=identifier(database or 'ticketpulse_local',64);es_index=identifier(es_index or database+'_tickets_v1',201,True);ck_database=identifier(ck_database or database+'_analytics_v1',128)
    if database!='ticketpulse_local' and not database.startswith('ticketpulse_test_'):raise ValueError('only owned demo or isolated database permitted')
    started=time.monotonic();functions={'host':lambda:host_metrics(pids),'mysql':lambda:mysql_metrics(database),'kafka':lambda:kafka_metrics(database),'es':lambda:es_metrics(es_index),'ck':lambda:ck_metrics(ck_database),'demo_http':demo_health}
    result={'schema':'ticketpulse-bench-metrics/v1','sampled_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'monotonic_seconds':started,'database':database,'es_index':es_index,'ck_database':ck_database}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures={key:pool.submit(fn) for key,fn in functions.items()}
        for key,future in futures.items():
            try:result[key]=future.result()
            except Exception as error:result[key]=unknown('probe failed: '+type(error).__name__)
    result['collection_ms']=round((time.monotonic()-started)*1000,3)
    result['budget_notes']={'extra_disk_bytes':20*1024**3,'new_process_rss_bytes':8*1024**3,'stage_seconds':1800,'total_seconds':5400,'stop_conditions':['new swapouts/pageouts or worsening memory pressure','new RSS >8GiB','extra disk >20GiB','shared demo HTTP error','stage>30min or total>90min']}
    return result

def compare(previous,current):
    seconds=current['monotonic_seconds']-previous['monotonic_seconds'];result={'interval_seconds':seconds}
    if seconds<=0:return result
    previous_end=previous.get('kafka',{}).get('end_offsets',{});current_end=current.get('kafka',{}).get('end_offsets',{});rates={}
    for p,end in current_end.items():
        old=previous_end.get(p,previous_end.get(str(p)))
        if old is not None:rates[str(p)]=(end-old)/seconds if end>=old else unknown('topic reset/offset decrease between samples')
    result['kafka_partition_records_per_second']=rates
    oldvm=previous.get('host',{}).get('vm',{}).get('value',{}).get('counters',{});newvm=current.get('host',{}).get('vm',{}).get('value',{}).get('counters',{})
    result['new_vm_pages']={k:newvm[k]-oldvm[k] for k in ['Swapouts','Swapins','Pageouts','Pageins'] if k in newvm and k in oldvm}
    oldprocess=previous.get('host',{}).get('processes',{});newprocess=current.get('host',{}).get('processes',{})
    result['process_rss_delta_bytes']={k:v['rss_bytes']-oldprocess[k]['rss_bytes'] for k,v in newprocess.items() if v.get('state')=='OK' and oldprocess.get(k,{}).get('state')=='OK'}
    result['disk_used_delta_bytes']=current.get('host',{}).get('disk',{}).get('used_bytes',0)-previous.get('host',{}).get('disk',{}).get('used_bytes',0)
    def counters(sample,component,section,key):
        values=sample.get(component,{}).get('observations',{}).get(section,{}).get('value',[])
        return {v[key]:float(v['value']) for v in values if key in v and 'value' in v}
    for label,component,section,key in [('mysql_counters_per_second','mysql','global_counters','name'),('ck_events_per_second','ck','events','event')]:
        a=counters(previous,component,section,key);b=counters(current,component,section,key)
        result[label]={k:(v-a[k])/seconds for k,v in b.items() if k in a and v>=a[k]}
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--database');parser.add_argument('--es-index');parser.add_argument('--ck-database');parser.add_argument('--ck-url',help='owned18123 or18124 endpoint; overrides TICKETPULSE_BENCH_CK_URL');parser.add_argument('--pids',help='JSON label->PID object or PID list');parser.add_argument('--interval',type=float,default=3);parser.add_argument('--duration',type=float,default=0);parser.add_argument('--output',help='new JSONL file; existing evidence never overwritten');args=parser.parse_args()
    if args.interval<3 or args.duration<0 or args.duration>60:parser.error('interval >=3s and duration 0–60s required')
    if args.ck_url:os.environ['TICKETPULSE_BENCH_CK_URL']=args.ck_url
    ck_endpoint()
    pids=json.loads(args.pids) if args.pids else None;destination=open(args.output,'x') if args.output else sys.stdout;end=time.monotonic()+args.duration;previous=None
    try:
        while True:
            if previous is not None and time.monotonic()>=end:break
            began=time.monotonic();value=collect(args.database,args.es_index,args.ck_database,pids)
            if previous:value['delta']=compare(previous,value)
            destination.write(json.dumps(value,ensure_ascii=False,separators=(',',':'))+'\n');destination.flush();previous=value;remaining=end-time.monotonic()
            if args.duration==0 or remaining<=0:break
            time.sleep(min(max(0,args.interval-(time.monotonic()-began)),remaining))
    finally:
        if destination is not sys.stdout:destination.close()
if __name__=='__main__':main()
