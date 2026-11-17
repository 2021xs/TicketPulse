#!/usr/bin/env python3
"""Capture live source excerpts, pinned distributions and ended Broker logs."""
import argparse
import datetime
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile

import m402_environment as env
import local_kafka as kafka

FILES=['internal/syncpipe/queue.go','internal/syncpipe/capture.go','internal/syncpipe/consumer.go',
       'internal/syncpipe/analytics_consumer.go','internal/syncpipe/kafka_identity.go','internal/syncpipe/retry.go',
       'internal/syncpipe/linkerrors/errors.go','internal/syncpipe/snapshot.go','internal/syncpipe/analytics_snapshot.go',
       'internal/syncpipe/watermark.go','internal/syncpipe/rebuild_snapshot.go','internal/syncpipe/rebuild_tail.go',
       'internal/syncpipe/rebuild_target.go','cmd/ticketpulse-sync/main.go','db/migrations/008_kafka_sync.sql','go.mod']

def investigation():
    folder=env.OUT/'source';folder.mkdir(parents=True,exist_ok=False)
    with tarfile.open(env.OUT/'pre-m4-02-source.tar.gz') as archive:
        for name in FILES:
            old=archive.extractfile(name).read().decode()
            path=folder/'original'/name;path.parent.mkdir(parents=True,exist_ok=True)
            path.write_text(chr(10).join(f'{n:4} {line}' for n,line in enumerate(old.splitlines(),1))+chr(10))
            new=(env.ROOT/name).read_text();path=folder/'current'/name;path.parent.mkdir(parents=True,exist_ok=True)
            path.write_text(chr(10).join(f'{n:4} {line}' for n,line in enumerate(new.splitlines(),1))+chr(10))
    distributions=[]
    for name,url,algorithm,expected,marker in kafka.PACKAGES:
        path=kafka.RUNTIME/name;h=hashlib.new(algorithm)
        with path.open('rb') as source:
            for block in iter(lambda:source.read(1<<20),b''):h.update(block)
        actual=h.hexdigest();distributions.append({'archive':path.relative_to(env.ROOT).as_posix(),'source_url':url,'algorithm':algorithm,'expected':expected,'actual':actual,'verified':actual==expected})
    if not all(r['verified'] for r in distributions):raise RuntimeError('Pinned Kafka/JDK checksum changed')
    (env.OUT/'environment/distributions.json').write_text(json.dumps(distributions,indent=2)+chr(10))
    for node in env.BROKERS:
        target=env.OUT/'environment/kafka'/('node-'+str(node))
        for location in ['data/meta.properties','metadata/meta.properties']:
            path=env.CLUSTER/('node-'+str(node))/location
            (target/(location.replace('/','-'))).write_bytes(path.read_bytes())
    (folder/'investigation-manifest.json').write_text(json.dumps({'captured_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'read_from':'immutable start-of-round source + current live files','files':FILES},indent=2)+chr(10))

def logs():
    folder=env.OUT/'broker-logs';folder.mkdir(parents=True,exist_ok=False)
    result=[]
    for node in env.BROKERS:
        entry=env.cluster()['nodes'][str(node)]
        if entry.get('pid') and env.node_owned(node,entry['pid']):raise RuntimeError('Capture logs only after owned Brokers stop')
        target=folder/('node-'+str(node));target.mkdir()
        home=env.CLUSTER/('node-'+str(node))
        candidates=[home/'launch.log']+list((home/'logs').glob('*.log'))+list((home/'logs').glob('*.log.*'))
        for path in sorted(set(candidates)):
            if not path.is_file():continue
            dest=target/(path.name+'.gz');h=hashlib.sha256();length=0
            with path.open('rb') as source,dest.open('xb') as output,gzip.GzipFile(filename='',mode='wb',fileobj=output,mtime=0) as zipped:
                for block in iter(lambda:source.read(1<<20),b''):h.update(block);length+=len(block);zipped.write(block)
            verified=hashlib.sha256()
            with gzip.open(dest,'rb') as source:
                for block in iter(lambda:source.read(1<<20),b''):verified.update(block)
            if verified.hexdigest()!=h.hexdigest():raise RuntimeError('Broker log compression differs')
            result.append({'node':node,'raw_path':path.relative_to(env.ROOT).as_posix(),'artifact':dest.relative_to(env.OUT).as_posix(),'raw_bytes':length,'raw_sha256':h.hexdigest()})
    (folder/'manifest.json').write_text(json.dumps(result,indent=2)+chr(10));print(json.dumps({'broker_log_files':len(result),'raw_log_bytes':sum(r['raw_bytes'] for r in result)}))

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['investigation','logs']);args=parser.parse_args()
    if args.action=='investigation':investigation()
    else:logs()
