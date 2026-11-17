#!/usr/bin/env python3
"""Owned Kafka4.4 three combined nodes plus isolated MySQL/ES/CK. No demo mutations."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import socket
import subprocess
import time
import urllib.request

import local_kafka as pinned
import local_search as es_release
import local_mysql as mysql_release
import m502_mysql as mysql
import bench_ck_runtime as ck

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'artifacts/m4-02'
RUNTIME=ROOT/'.local/m4-02'
CLUSTER=RUNTIME/'kafka'
MARKER=CLUSTER/'cluster.json'
BROKERS={401:19094,402:19096,403:19098}
CONTROLLERS={n:p+1 for n,p in BROKERS.items()}
BOOTSTRAP=','.join('127.0.0.1:'+str(p) for p in BROKERS.values())
MYSQL=RUNTIME/'mysql';MYSQL_PORT=33310
ES=RUNTIME/'es';ES_PORT=19204;ES_TRANSPORT=19304;ES_URL='http://127.0.0.1:'+str(ES_PORT)
CK=RUNTIME/'ck';CK_PORT=18127;CK_NATIVE=19005;CK_URL='http://127.0.0.1:'+str(CK_PORT)
APP_PORT=18088

def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def checksum(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1<<20),b''):h.update(block)
    return h.hexdigest()
def occupied(port):
    with socket.socket() as connection:
        connection.settimeout(1);return connection.connect_ex(('127.0.0.1',port))==0
def private(path,value):mysql_release.private_file(path,json.dumps(value,indent=2))
def nofile():
    soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE,(min(65536,hard),hard))
def java_env(node=None):
    e=os.environ.copy();e.update(JAVA_HOME=str(pinned.JAVA),KAFKA_HEAP_OPTS='-Xms128m -Xmx512m',LOG_DIR=str(CLUSTER/('node-'+str(node) if node else 'cli')/'logs'))
    return e
def cli(name,args,timeout=15,check=True):
    p=subprocess.run([str(pinned.KAFKA/'bin'/name)]+args,env=java_env(),cwd=ROOT,capture_output=True,text=True,timeout=timeout)
    if check and p.returncode:raise RuntimeError('Kafka CLI failed: '+name+' '+p.stderr[-500:])
    return p
def cluster():
    v=json.loads(MARKER.read_text())
    if v['project']!=str(ROOT) or v['runtime']!=str(CLUSTER) or v['brokers']!={str(k):v for k,v in BROKERS.items()}:raise RuntimeError('M402 Kafka ownership mismatch')
    return v
def node_owned(node,pid):
    try:
        cmd=subprocess.check_output(['ps','-p',str(pid),'-o','command='],text=True)
        return 'kafka.Kafka' in cmd and str(CLUSTER/('node-'+str(node))/'server.properties') in cmd and str(pinned.JAVA) in cmd
    except subprocess.CalledProcessError:return False
def node_pid(node,value):
    entry=value['nodes'][str(node)]
    if entry.get('pid') and node_owned(node,entry['pid']):return entry['pid']
    # Kafka's shell launcher execs Java asynchronously. Recover only a listener
    # whose exact private config and pinned JDK prove ownership, never a port
    # alone. This also prevents a second launch during exec/readiness.
    listeners=[]
    for port in [BROKERS[node],CONTROLLERS[node]]:
        result=subprocess.run(['lsof','-nP','-iTCP:'+str(port),'-sTCP:LISTEN','-t'],capture_output=True,text=True)
        listeners.extend(int(pid) for pid in result.stdout.split())
    owned={pid for pid in listeners if node_owned(node,pid)}
    if len(owned)>1:raise RuntimeError('Multiple owned listeners for one Kafka node')
    if owned:
        pid=owned.pop();old=entry.get('pid');entry['pid']=pid;private(MARKER,value)
        path=OUT/'environment/kafka/process-ownership-recovery.jsonl'
        with path.open('a') as output:output.write(json.dumps({'at':stamp(),'node':node,'old_marker_pid':old,'verified_listener_pid':pid,'exact_config_and_JDK_verified':True})+chr(10))
        return pid
    return None
def budget():
    free=shutil.disk_usage(ROOT).free
    disk=0;changing_files=False
    if RUNTIME.exists():
        measurement=subprocess.run(['du','-sk',str(RUNTIME)],capture_output=True,text=True)
        # CK atomically removes insert/merge temp directories while du walks.
        # Keep its completed total for ENOENT; other measurement failures stop
        # the owned test rather than disabling budget checks.
        changing_files=measurement.returncode!=0
        errors=measurement.stderr.splitlines()
        if not measurement.stdout.split() or (changing_files and any('No such file or directory' not in line for line in errors)):
            raise RuntimeError('Independent disk budget cannot be measured: '+measurement.stderr[-300:])
        disk=int(measurement.stdout.split()[0])*1024
    pids=[]
    if MARKER.exists():pids=[n['pid'] for n in cluster()['nodes'].values() if n.get('pid')]
    rss=0
    if pids:
        p=subprocess.run(['ps','-p',','.join(map(str,pids)),'-o','rss='],capture_output=True,text=True)
        rss=sum(int(x)*1024 for x in p.stdout.split())
    if free<20*1024**3 or disk>8*1024**3 or rss>6*1024**3:raise RuntimeError('Independent Kafka safety budget reached')
    return {'free_disk_bytes':free,'owned_disk_bytes':disk,'kafka_rss_bytes':rss,'disk_walk_saw_deleted_temporary_files':changing_files}

def config(node):
    home=CLUSTER/('node-'+str(node))
    return chr(10).join([
        'process.roles=broker,controller','node.id='+str(node),
        'controller.quorum.bootstrap.servers='+','.join('127.0.0.1:'+str(p) for p in CONTROLLERS.values()),
        'listeners=PLAINTEXT://127.0.0.1:'+str(BROKERS[node])+',CONTROLLER://127.0.0.1:'+str(CONTROLLERS[node]),
        'advertised.listeners=PLAINTEXT://127.0.0.1:'+str(BROKERS[node])+',CONTROLLER://127.0.0.1:'+str(CONTROLLERS[node]),
        'controller.listener.names=CONTROLLER','listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
        'inter.broker.listener.name=PLAINTEXT','log.dirs='+str(home/'data'),'metadata.log.dir='+str(home/'metadata'),
        'num.partitions=3','default.replication.factor=3','min.insync.replicas=2',
        'offsets.topic.replication.factor=3','offsets.topic.num.partitions=3','offsets.commit.required.acks=-1',
        'transaction.state.log.replication.factor=3','transaction.state.log.min.isr=2',
        'group.initial.rebalance.delay.ms=0','num.network.threads=2','num.io.threads=4','num.replica.fetchers=1','background.threads=4',
        'replica.lag.time.max.ms=10000','broker.heartbeat.interval.ms=1000','broker.session.timeout.ms=9000',
        'log.retention.ms=-1','log.retention.bytes=-1','log.segment.bytes=16777216',
        'log.flush.interval.messages=1','log.flush.interval.ms=1000','auto.create.topics.enable=false',
        'unclean.leader.election.enable=false','delete.topic.enable=true'])+chr(10)

def kafka_setup():
    if MARKER.exists():cluster();return
    if CLUSTER.exists() and any(CLUSTER.iterdir()):raise RuntimeError('Refusing unowned Kafka runtime')
    # Verified pinned distributions are reused read-only; no install/setup of demo.
    for archive,_,algorithm,expected,_ in pinned.PACKAGES:
        h=hashlib.new(algorithm)
        with (pinned.RUNTIME/archive).open('rb') as source:
            for block in iter(lambda:source.read(1<<20),b''):h.update(block)
        if h.hexdigest()!=expected:raise RuntimeError('Pinned Kafka/JDK archive checksum differs')
    CLUSTER.mkdir(parents=True)
    cid=cli('kafka-storage.sh',['random-uuid']).stdout.strip()
    ids={n:cli('kafka-storage.sh',['random-uuid']).stdout.strip() for n in BROKERS}
    initial=','.join(str(n)+'@127.0.0.1:'+str(CONTROLLERS[n])+':'+ids[n] for n in BROKERS)
    value={'project':str(ROOT),'runtime':str(CLUSTER),'cluster_id':cid,'brokers':BROKERS,'controllers':CONTROLLERS,'initial_controllers':initial,'nodes':{},'created_at':stamp(),'status':'FORMATTED'}
    for n in BROKERS:
        home=CLUSTER/('node-'+str(n));home.mkdir()
        (home/'logs').mkdir();path=home/'server.properties';path.write_text(config(n))
        v=cli('kafka-storage.sh',['format','--initial-controllers',initial,'-t',cid,'-c',str(path)],30)
        folder=OUT/'environment/kafka'/('node-'+str(n));folder.mkdir(parents=True)
        (folder/'server.properties').write_text(config(n));(folder/'format.txt').write_text(v.stdout+v.stderr)
        value['nodes'][str(n)]={'id':n,'pid':None,'config':str(path),'data':str(home/'data'),'metadata':str(home/'metadata'),'directory_id':ids[n]}
    private(MARKER,value)
    (OUT/'environment/kafka/cluster.json').write_text(json.dumps(value,indent=2))

def node_start(node):
    v=cluster();entry=v['nodes'][str(node)]
    if node_pid(node,v):return
    if entry.get('pid'):
        for _ in range(200):
            try:command=subprocess.check_output(['ps','-p',str(entry['pid']),'-o','command='],text=True)
            except subprocess.CalledProcessError:break
            if node_owned(node,entry['pid']):return
            if str(entry['config']) not in command or 'kafka-' not in command:break
            time.sleep(.1)
        if node_pid(node,v):return
    if occupied(BROKERS[node]) or occupied(CONTROLLERS[node]):raise RuntimeError('Unowned occupied Kafka node port')
    path=Path(entry['config'])
    if path.read_text()!=config(node):raise RuntimeError('Pinned node configuration changed')
    budget()
    with (path.parent/'launch.log').open('a') as output:
        child=subprocess.Popen([str(pinned.KAFKA/'bin/kafka-server-start.sh'),str(path)],env=java_env(node),cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,start_new_session=True,preexec_fn=nofile)
    entry['pid']=child.pid;entry['started_at']=stamp();private(MARKER,v)
    for _ in range(200):
        if node_owned(node,child.pid):return
        if child.poll() is not None:raise RuntimeError('Owned Kafka launcher exited; inspect private launch.log')
        time.sleep(.1)
    raise RuntimeError('Owned Kafka exec deadline; refusing duplicate launch')

def node_stop(node,hard=False):
    v=cluster();entry=v['nodes'][str(node)];pid=node_pid(node,v)
    if pid and node_owned(node,pid):
        os.kill(pid,signal.SIGKILL if hard else signal.SIGTERM)
        for _ in range(200):
            if not node_owned(node,pid) and not occupied(BROKERS[node]):break
            time.sleep(.1)
        else:raise RuntimeError('Owned node shutdown deadline')
        entry['pid']=None;entry['stopped_at']=stamp();private(MARKER,v)
    elif occupied(BROKERS[node]):raise RuntimeError('Refusing to stop unowned Kafka')

def kafka_start():
    kafka_setup()
    for node in BROKERS:node_start(node)
    for _ in range(100):
        if all(occupied(port) for port in BROKERS.values()):
            r=cli('kafka-broker-api-versions.sh',['--bootstrap-server',BOOTSTRAP],10,False)
            if r.returncode==0 and all('(id: '+str(n)+' ' in r.stdout for n in BROKERS):
                folder=OUT/'environment/kafka/readiness'/str(time.time_ns());folder.mkdir(parents=True)
                (folder/'api-versions.txt').write_text(r.stdout+r.stderr)
                q=cli('kafka-metadata-quorum.sh',['--bootstrap-server',BOOTSTRAP,'describe','--status'])
                (folder/'quorum-status.txt').write_text(q.stdout+q.stderr)
                for name,data in [('api-versions.txt',r.stdout+r.stderr),('quorum-status.txt',q.stdout+q.stderr)]:
                    path=OUT/'environment/kafka'/name
                    if not path.exists():path.write_text(data)
                return
        time.sleep(.2)
    raise RuntimeError('Three-broker readiness deadline')

def mysql_runtime():
    mysql.R,mysql.STATE,mysql.ADMIN=MYSQL,MYSQL/'instance.json',MYSQL/'admin.cnf'
    mysql.PORT,mysql.OUT=MYSQL_PORT,OUT/'environment/mysql'
    return mysql
def sql(query,timeout=30):
    m=mysql_runtime();v=m.state()
    if m.sql('SELECT @@server_uuid;',3).strip()!=v['server_uuid']:raise RuntimeError('M402 MySQL UUID differs')
    return m.sql(query,timeout)

def es_owned(pid):
    try:
        cmd=subprocess.check_output(['ps','-p',str(pid),'-o','command='],text=True)
        return 'org.elasticsearch.bootstrap.Elasticsearch' in cmd and '-Djava.io.tmpdir='+str(ES/'tmp') in cmd
    except subprocess.CalledProcessError:return False
def es_start():
    marker=ES/'instance.json'
    if marker.exists():
        state=json.loads(marker.read_text())
        if state['data']!=str(ES/'data'):raise RuntimeError('ES ownership mismatch')
        if state.get('pid') and es_owned(state['pid']):return
    else:
        if (ES/'data').exists():raise RuntimeError('Unowned ES data')
        state={'project':str(ROOT),'data':str(ES/'data')}
    if occupied(ES_PORT):raise RuntimeError('Unowned ES port')
    dist=es_release.RUNTIME/es_release.PACKAGES['es'][0]
    if (dist/'.ticketpulse-package').read_text().strip()!=es_release.PACKAGES['es'][3]:raise RuntimeError('ES distribution not verified')
    for name in ['data','tmp','logs','config','config/jvm.options.d']:(ES/name).mkdir(parents=True,exist_ok=True)
    for name in ['jvm.options','log4j2.properties']:
        text=(dist/'config'/name).read_text()
        text=text.replace('file=logs/gc.log','file='+str(ES/'logs/gc.log')).replace('-XX:HeapDumpPath=data','-XX:HeapDumpPath='+str(ES/'data')).replace('-XX:ErrorFile=logs/hs_err_pid%p.log','-XX:ErrorFile='+str(ES/'logs/hs_err_pid%p.log'))
        (ES/'config'/name).write_text(text)
    config='cluster.name: ticketpulse-m402'+chr(10)+'node.name: ticketpulse-m402'+chr(10)+'network.host: 127.0.0.1'+chr(10)+'http.port: '+str(ES_PORT)+chr(10)+'transport.port: '+str(ES_TRANSPORT)+chr(10)+'discovery.type: single-node'+chr(10)+'xpack.security.enabled: false'+chr(10)+'xpack.ml.enabled: false'+chr(10)+'path.data: '+str(ES/'data')+chr(10)+'path.logs: '+str(ES/'logs')+chr(10)
    (ES/'config/elasticsearch.yml').write_text(config)
    e=os.environ.copy();e.update(ES_PATH_CONF=str(ES/'config'),ES_JAVA_OPTS='-Xms512m -Xmx512m',ES_TMPDIR=str(ES/'tmp'))
    with (ES/'launch.log').open('a') as log:child=subprocess.Popen([str(dist/'bin/elasticsearch')],cwd=ROOT,env=e,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,preexec_fn=nofile)
    state['launcher_pid']=child.pid;private(marker,state)
    for _ in range(200):
        if child.poll() is not None:raise RuntimeError('Owned ES startup failure')
        try:
            with urllib.request.urlopen(ES_URL+'/_nodes/process,jvm,settings',timeout=2) as response:nodes=json.load(response)
            node=next(iter(nodes['nodes'].values()));pid=node['process']['id']
            if not es_owned(pid) or node['settings']['path']['data']!=str(ES/'data'):raise RuntimeError('Listening ES ownership mismatch')
            state['pid']=pid;private(marker,state)
            folder=OUT/'environment/es';folder.mkdir(exist_ok=True)
            (folder/'nodes.json').write_text(json.dumps(nodes,indent=2));(folder/'elasticsearch.yml').write_text(config);return
        except OSError:time.sleep(.2)
    raise RuntimeError('Owned ES readiness deadline')
def es_stop():
    marker=ES/'instance.json'
    if not marker.exists():return
    v=json.loads(marker.read_text());pid=v.get('pid')
    if pid and es_owned(pid):
        os.kill(pid,signal.SIGTERM)
        for _ in range(200):
            if not es_owned(pid) and not occupied(ES_PORT):break
            time.sleep(.1)
        else:raise RuntimeError('ES shutdown deadline')
        v['pid']=None;private(marker,v)
    elif occupied(ES_PORT):raise RuntimeError('Unowned ES port')
def ck_runtime():
    ck.RUNTIME,ck.STATE,ck.CONFIG=CK,CK/'instance.json',CK/'config.xml';ck.PORT,ck.NATIVE_PORT=CK_PORT,CK_NATIVE;ck.OUTPUT=OUT/'environment/ck'
    if not getattr(ck,'m402_configured',False):
        original=ck.config_text
        ck.config_text=lambda:original().replace('<http_port>18124</http_port>','<http_port>'+str(CK_PORT)+'</http_port>').replace('<tcp_port>19002</tcp_port>','<tcp_port>'+str(CK_NATIVE)+'</tcp_port>')
        ck.m402_configured=True
    return ck

def start():
    RUNTIME.mkdir(exist_ok=True,parents=True)
    if not occupied(MYSQL_PORT):mysql_runtime().start('baseline')
    kafka_start();es_start();ck_runtime().start()
    (OUT/'environment/budgets.json').write_text(json.dumps({'kafka_heap_max_bytes_per_node':512*1024**2,'kafka_heap_min_bytes_per_node':128*1024**2,
        'test_disk_limit_bytes':8*1024**3,'min_free_disk_bytes':20*1024**3,'kafka_rss_limit_bytes':6*1024**3,
        'es_heap_bytes':512*1024**2,'ck_memory_tracker_bytes':4*1024**3,'mysql_buffer_pool_bytes':64*1024**2,
        'fault_domain':'same Mac separate native processes, same physical disk and network'},indent=2))
    print(json.dumps({'bootstrap':BOOTSTRAP,'cluster_id':cluster()['cluster_id'],'process_level_only':True}))
def stop():
    # Final teardown can remove controller majority before the last node's
    # graceful shutdown finishes. Signal all owned nodes, then enforce one
    # bounded cleanup deadline; fault tests still use node_stop separately.
    value=cluster();pids={}
    for n in BROKERS:
        pid=node_pid(n,value)
        if pid:
            pids[n]=pid;os.kill(pid,signal.SIGTERM)
        elif occupied(BROKERS[n]) or occupied(CONTROLLERS[n]):raise RuntimeError('Unowned port during final Kafka teardown')
    deadline=time.monotonic()+20
    while time.monotonic()<deadline and any(node_owned(n,pid) for n,pid in pids.items()):time.sleep(.1)
    killed=[]
    for n,pid in pids.items():
        if node_owned(n,pid):os.kill(pid,signal.SIGKILL);killed.append(n)
    for _ in range(100):
        if not any(occupied(port) for port in list(BROKERS.values())+list(CONTROLLERS.values())):break
        time.sleep(.1)
    else:raise RuntimeError('Owned Kafka final teardown failed')
    for n in BROKERS:value['nodes'][str(n)]['pid']=None
    private(MARKER,value)
    (OUT/'environment/kafka/final-teardown.json').write_text(json.dumps({'at':stamp(),'graceful_seconds_budget':20,'forced_owned_nodes':killed,'all_private_ports_closed':True},indent=2))
    es_stop();ck_runtime().stop();mysql_runtime().stop()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['start','stop','status','node-start','node-stop']);parser.add_argument('--node',type=int,choices=list(BROKERS));parser.add_argument('--sigkill',action='store_true');args=parser.parse_args()
    if args.action=='start':start()
    elif args.action=='stop':stop()
    elif args.action=='status':print(json.dumps({'cluster':cluster(),'budget':budget()},indent=2))
    elif args.action=='node-start':node_start(args.node)
    else:node_stop(args.node,args.sigkill)
