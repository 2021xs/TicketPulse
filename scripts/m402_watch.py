#!/usr/bin/env python3
"""Read-only resource sampler; stop only the owned test on its safety budget."""
import argparse
import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import time

import m402_environment as env

def sample():
    value={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'budget':env.budget(),'processes':[]}
    ids=[n.get('pid') for n in env.cluster()['nodes'].values() if n.get('pid')]
    for marker in [env.MYSQL/'instance.json',env.ES/'instance.json',env.CK/'instance.json']:
        if marker.exists():
            state=json.loads(marker.read_text())
            ids.extend(state[k] for k in ['pid','launcher_pid'] if state.get(k))
    # Account for a ClickHouse watchdog child without dumping full commands.
    output=subprocess.run(['ps','-ax','-o','pid=,ppid=,rss=,%cpu=,command='],capture_output=True,text=True,check=True).stdout
    for line in output.splitlines():
        fields=line.strip().split(None,4)
        if len(fields)<5:continue
        pid,parent,rss,cpu,command=fields
        if int(pid) not in ids and int(parent) not in ids and (str(env.RUNTIME) not in command or 'm402_watch.py' in command):continue
        component='kafka' if 'kafka.Kafka' in command else 'es' if 'org.elasticsearch' in command else 'ck' if 'clickhouse' in command else 'mysql' if 'mysqld' in command else 'worker'
        value['processes'].append({'pid':int(pid),'parent_pid':int(parent),'rss_bytes':int(rss)*1024,'cpu_percent':float(cpu),'component':component})
    return value

def run(label,seconds):
    path=env.OUT/'resources'/(label+'.jsonl');path.parent.mkdir(parents=True,exist_ok=True)
    end=time.monotonic()+seconds
    with path.open('x') as output:
        while time.monotonic()<end:
            try:value=sample()
            except RuntimeError as error:
                output.write(json.dumps({'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'safety_budget_reached':str(error)})+chr(10));output.flush()
                # Failure injection and all stop operations remain ownership
                # checked; no demo PID is eligible. Stop the private Brokers
                # and components to halt resource growth immediately.
                env.stop()
                raise
            output.write(json.dumps(value)+chr(10));output.flush();time.sleep(2)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--label',required=True);parser.add_argument('--seconds',type=int,default=3600)
    args=parser.parse_args();run(args.label,args.seconds)
