#!/usr/bin/env python3
"""Private CDC/ES consumers for background writes, preserving all old data."""
import json
import os
import signal
import subprocess
import time

import m504_environment as env
import m504_fixture as fixture

STATE=env.RUNTIME/'workers.json'

def state():
    if not STATE.exists():return {}
    return json.loads(STATE.read_text())

def owned(worker):
    try:
        command=subprocess.check_output(['ps','-p',str(worker['pid']),'-o','command='],text=True)
        return str(env.RUNTIME/'ticketpulse-sync') in command and worker['mode'] in command
    except subprocess.CalledProcessError:return False

def start(suffix):
    if state():raise RuntimeError('Private sync workers already registered')
    e=fixture.copy_connection(suffix)
    env.sql("CREATE USER IF NOT EXISTS 'm504_cdc'@'127.0.0.1' IDENTIFIED BY 'm504-local-replication'; GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'm504_cdc'@'127.0.0.1';")
    folder=env.OUT/'environment/workers'/suffix;folder.mkdir(parents=True,exist_ok=False)
    with (folder/'checkpoint.json').open('x') as stdout, (folder/'checkpoint.log').open('x') as stderr:
        subprocess.run([str(env.RUNTIME/'search-fixture-next'),'-initialize-cdc'],env=e,cwd=env.ROOT,stdout=stdout,stderr=stderr,check=True)
    workers={}
    for name,mode in [('capture','capture'),('es','consume')]:
        log=folder/(name+'.log')
        with log.open('x') as output:
            child=subprocess.Popen([str(env.RUNTIME/'ticketpulse-sync'),'-mode='+mode,'-server-id=350409'],env=e,cwd=env.ROOT,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
        workers[name]={'pid':child.pid,'mode':'-mode='+mode,'copy':suffix,'log':str(log.relative_to(env.ROOT))}
        STATE.write_text(json.dumps(workers))
        time.sleep(.3)
        if child.poll() is not None:raise RuntimeError('Private sync exited')
    database=fixture.fixtures()['write_copies'][suffix]['database']
    for _ in range(100):
        rows=env.sql('USE '+database+";SELECT component,state FROM sync_worker_state WHERE component IN ('es','cdc') ORDER BY component;")
        if 'cdc\trunning' in rows and 'es\trunning' in rows:return workers
        time.sleep(.1)
    raise RuntimeError('Private worker readiness deadline')

def stop():
    workers=state()
    for worker in workers.values():
        if owned(worker):
            os.kill(worker['pid'],signal.SIGTERM)
            for _ in range(100):
                if not owned(worker):break
                time.sleep(.1)
            else:raise RuntimeError('Owned sync shutdown timeout')
    STATE.write_text('{}')

def wait_caught_up(suffix,seconds=30):
    database=fixture.fixtures()['write_copies'][suffix]['database'];index=fixture.fixtures()['write_copies'][suffix]['index']
    for _ in range(seconds*2):
        source=fixture.documents(database)
        # Dedicated write copies are bounded to 10k plus finite HTTP writes.
        last=source[-1]
        try:document=fixture.es('GET','/'+index+'/_doc/'+str(last['id']))['_source']
        except Exception:document={}
        if document.get('version')==last['version']:
            fixture.es('POST','/'+index+'/_refresh');return
        time.sleep(.5)
    raise RuntimeError('Last source projection not caught up')
