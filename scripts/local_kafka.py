#!/usr/bin/env python3
"""Owned Apache Kafka 4.4.0 KRaft broker; keeps both Kafka and MySQL data."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tarfile
import time
import urllib.request

import local_mysql as local
import local_search as legacy

ROOT=Path(__file__).resolve().parents[1]
RUNTIME=ROOT/'.local/kafka'
STATE=RUNTIME/'instance.json'
KAFKA=RUNTIME/'kafka_2.13-4.4.0'
JAVA=RUNTIME/'jdk-21.0.13+9/Contents/Home'
PORT=19092
PACKAGES=[('kafka_2.13-4.4.0.tgz','https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz','sha512','f5ceba8a265ab6bb2d68362b1f6859a95e15d60ef92ba83b43fdf065b557dc18e98d6383c15b9977320e25b3a2c8b6f11decb17b0fa6c0f163423793a0eb95cb',KAFKA),('OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.13_9.tar.gz','https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.13_9.tar.gz','sha256','bf036b35b071bfb2f54f2c0af8ee8d88afa95ee6eb3dce02c0f94cfc2524d04a',JAVA)]

def java_env():
    env=os.environ.copy();env['JAVA_HOME']=str(JAVA);env['KAFKA_HEAP_OPTS']='-Xms128m -Xmx512m';env['LOG_DIR']=str(RUNTIME/'logs');return env

def state():
    owner=local.marker();local.verify(owner);value=json.loads(STATE.read_text())
    if value.get('project')!=str(ROOT) or value.get('server_uuid')!=owner['server_uuid'] or value.get('data')!=str(RUNTIME/'data'):raise RuntimeError('Kafka ownership mismatch')
    return value

def owns(pid):
    try:
        cwd=subprocess.check_output(['lsof','-a','-p',str(pid),'-d','cwd','-Fn'],text=True)
        cmd=subprocess.check_output(['ps','-p',str(pid),'-o','command='],text=True)
        return 'n'+str(ROOT) in cwd.splitlines() and 'kafka.Kafka' in cmd and str(RUNTIME/'server.properties') in cmd and str(JAVA) in cmd
    except subprocess.CalledProcessError:return False

def command(name,args,check=True):
    return subprocess.run([str(KAFKA/'bin'/name)]+args,cwd=ROOT,env=java_env(),check=check,text=True,capture_output=True)

def setup():
    if sys.platform!='darwin' or os.uname().machine!='arm64':raise RuntimeError('Native helper targets macOS arm64; use configured Kafka endpoint elsewhere')
    owner=local.marker();local.verify(owner);RUNTIME.mkdir(parents=True,exist_ok=True)
    if not legacy.STATE.is_file() or not (legacy.RUNTIME/legacy.PACKAGES['es'][0]).is_dir():legacy.setup()
    for name,url,algorithm,checksum,directory in PACKAGES:
        archive=RUNTIME/name
        if not archive.exists():legacy.download(url,archive.with_suffix('.partial'));archive.with_suffix('.partial').rename(archive)
        h=hashlib.new(algorithm)
        with archive.open('rb') as src:
            for block in iter(lambda:src.read(1048576),b''):h.update(block)
        if h.hexdigest()!=checksum:raise RuntimeError('Official release checksum mismatch: '+name)
        if not directory.exists():
            with tarfile.open(archive) as package:
                for entry in package.getmembers():
                    if not str((RUNTIME/entry.name).resolve()).startswith(str(RUNTIME.resolve())+'/'):raise RuntimeError('Archive path escapes owned runtime')
                package.extractall(RUNTIME)
    for name in ['data','logs']:(RUNTIME/name).mkdir(exist_ok=True)
    config='\n'.join(['process.roles=broker,controller','node.id=1','controller.quorum.bootstrap.servers=127.0.0.1:19093','listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093','advertised.listeners=PLAINTEXT://127.0.0.1:19092','controller.listener.names=CONTROLLER','listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT','inter.broker.listener.name=PLAINTEXT','log.dirs='+str(RUNTIME/'data'),'num.partitions=3','default.replication.factor=1','offsets.topic.replication.factor=1','offsets.topic.num.partitions=3','transaction.state.log.replication.factor=1','transaction.state.log.min.isr=1','min.insync.replicas=1','group.initial.rebalance.delay.ms=0','num.network.threads=2','num.io.threads=4','num.replica.fetchers=1','log.retention.ms=-1','log.retention.bytes=-1','log.flush.interval.messages=1','log.flush.interval.ms=1000','auto.create.topics.enable=false','unclean.leader.election.enable=false','delete.topic.enable=true'])+chr(10)
    (RUNTIME/'server.properties').write_text(config)
    if not STATE.exists():
        cluster=command('kafka-storage.sh',['random-uuid']).stdout.strip()
        local.private_file(STATE,json.dumps({'project':str(ROOT),'server_uuid':owner['server_uuid'],'data':str(RUNTIME/'data'),'cluster_id':cluster,'pid':None},indent=2))
    value=state()
    if not (RUNTIME/'data/meta.properties').exists():
        result=command('kafka-storage.sh',['format','--standalone','-t',value['cluster_id'],'-c',str(RUNTIME/'server.properties')]);print(result.stdout.strip(),flush=True)
    # Explicit sync-only migration; never reset a business row or legacy checkpoint.
    local.mysql('USE ticketpulse_local;'+chr(10)+chr(10).join((ROOT/'db/migrations'/p).read_text() for p in ['006_search_sync.sql','007_analytics_sync.sql','008_kafka_sync.sql']))
    tables=['sync_kafka_cdc_checkpoint','sync_kafka_pipeline','sync_kafka_es_deliveries','sync_kafka_ck_deliveries','sync_kafka_watermarks','sync_kafka_analytics_bootstrap','sync_source_watermark']
    local.mysql(''.join("GRANT SELECT,INSERT,UPDATE ON ticketpulse_local."+t+" TO 'ticketpulse_sync'@'localhost';" for t in tables))
    start();print('Verified Apache Kafka 4.4.0 / Temurin 21.0.13; single-node KRaft, loopback only',flush=True)

def start():
    legacy.start_component('es',legacy.state())
    value=state()
    if legacy.occupied(PORT):
        if not value.get('pid') or not owns(value['pid']):raise RuntimeError('Kafka port is occupied by an unowned process')
        return
    with (RUNTIME/'launch.log').open('a') as log:
        child=subprocess.Popen([str(KAFKA/'bin/kafka-server-start.sh'),str(RUNTIME/'server.properties')],cwd=ROOT,env=java_env(),stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    value['pid']=child.pid;local.private_file(STATE,json.dumps(value,indent=2))
    for _ in range(200):
        if child.poll() is not None:raise RuntimeError('Kafka exited; inspect .local/kafka/launch.log')
        if legacy.occupied(PORT):
            result=command('kafka-broker-api-versions.sh',['--bootstrap-server','127.0.0.1:'+str(PORT)],check=False)
            if result.returncode==0:print('Kafka ready at 127.0.0.1:'+str(PORT),flush=True);return
        time.sleep(.15)
    raise RuntimeError('Kafka readiness timeout')

def stop():
    value=state();pid=value.get('pid')
    if pid and owns(pid):
        os.kill(pid,signal.SIGTERM)
        for _ in range(300):
            if not owns(pid) and not legacy.occupied(PORT):value['pid']=None;local.private_file(STATE,json.dumps(value,indent=2));print('Owned Kafka stopped; all log and business data preserved',flush=True);return
            time.sleep(.1)
        raise RuntimeError('Kafka stop timeout')
    if legacy.occupied(PORT):raise RuntimeError('Refusing to stop unowned Kafka')

def environment(database='ticketpulse_local'):
    env=legacy.environment(database);env.pop('TICKETPULSE_NATS_URL',None)
    env['TICKETPULSE_KAFKA_BROKERS']='127.0.0.1:'+str(PORT);env.setdefault('TICKETPULSE_KAFKA_PARTITIONS','3');return env

def execute(target,args):return subprocess.run([str(ROOT/'bin'/target)]+args,cwd=ROOT,env=environment()).returncode

def main():
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['setup','start','stop','status','sync','web','migrate']);args,extra=parser.parse_known_args()
    if args.action=='setup':setup()
    elif args.action=='start':start()
    elif args.action=='stop':stop()
    elif args.action=='sync':return execute('ticketpulse-sync',['-mode=all']+extra)
    elif args.action=='web':return execute('ticketpulse',extra)
    else:return execute('ticketpulse-sync',['-mode='+('migrate-kafka' if args.action=='migrate' else 'status')]+extra)
    return 0

if __name__=='__main__':
    try:sys.exit(main())
    except (OSError,RuntimeError,subprocess.CalledProcessError) as e:print('TicketPulse Kafka: '+str(e),file=sys.stderr);sys.exit(1)
