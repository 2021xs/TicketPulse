#!/usr/bin/env python3
"""M5-02 worker/rebuild harness on independent UUID-verified MySQL."""
import argparse,json,os,signal,subprocess,time,urllib.request
from pathlib import Path
import m502_mysql as mysql
import bench_stage as bench
ROOT=mysql.ROOT;OUT=ROOT/'artifacts/m5-02';WORKER=mysql.R/'workers.json'
def verified():
 v=mysql.state();actual=mysql.sql('SELECT @@server_uuid;').strip()
 if actual!=v['server_uuid']:raise RuntimeError('source UUID mismatch')
 return {'project':str(ROOT),'database':v['database'],'source_uuid':v['server_uuid'],'ck_url':'http://127.0.0.1:18124','pids':json.loads(WORKER.read_text()) if WORKER.exists() else {},'url':'http://127.0.0.1:18085','server_id':350207}
def env():
 v=verified();e=os.environ.copy();dsn='root@unix('+str(mysql.R/'mysql.sock')+')/'+v['database'];e.update({'TICKETPULSE_DSN':dsn,'TICKETPULSE_BENCH_DSN':dsn,'TICKETPULSE_SYNC_DSN':dsn,'TICKETPULSE_TEST_SERVER_UUID':v['source_uuid'],'TICKETPULSE_ES_URL':'http://127.0.0.1:19200','TICKETPULSE_SEARCH_INDEX':v['database']+'_tickets_v1','TICKETPULSE_CK_URL':v['ck_url'],'TICKETPULSE_CK_DATABASE':v['database']+'_analytics_v1','TICKETPULSE_KAFKA_BROKERS':'127.0.0.1:19092','TICKETPULSE_CDC_ADDR':'127.0.0.1:33307','TICKETPULSE_CDC_USER':'m502_cdc','TICKETPULSE_CDC_PASSWORD':'m502-local-replication'});return e

def stop():
 v=verified()
 for x in v['pids'].values():
  if bench.owned(x['pid'],x['binary']):
   os.kill(x['pid'],signal.SIGTERM)
   for _ in range(100):
    if not bench.owned(x['pid'],x['binary']):break
    time.sleep(.1)
 WORKER.write_text('{}')

def start():
 v=verified();e=env();jobs=[('cdc','ticketpulse-sync',['-mode=capture','-server-id=350207']),('es','ticketpulse-sync',['-mode=consume']),('ck','ticketpulse-sync',['-mode=ck']),('web','ticketpulse-bench-app',['-listen=127.0.0.1:18085'])];pids={}
 if v['pids']:raise RuntimeError('stop before start')
 for name,binary,args in jobs:
  with (OUT/('worker-'+name+'.log')).open('a') as log:p=subprocess.Popen([str(ROOT/'bin'/binary)]+args,cwd=ROOT,env=e,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
  pids[name]={'pid':p.pid,'binary':binary};WORKER.write_text(json.dumps(pids));time.sleep(.3)
  if p.poll() is not None:raise RuntimeError(name+' worker exited')
 for _ in range(100):
  try:urllib.request.urlopen(v['url']+'/debug/bench',timeout=1).close();return
  except OSError:time.sleep(.1)
 raise RuntimeError('readiness failed')
def run(job,folder,kill_at=0,window=1200):
 v=verified();e=env();o=Path(folder);o.mkdir(parents=True,exist_ok=False);base=[str(ROOT/'bin/ticketpulse-rebuild'),'-job='+job,'-batch-size=500','-wait=5m','-organization=1','-start=2026-11-01','-end=2026-11-09'];trace=o/'page-trace.jsonl';start_at=time.time();start=time.monotonic()
 def call(mode):return subprocess.run(base+['-mode='+mode],env=e,capture_output=True,text=True,timeout=15)
 create=call('create');o.joinpath('create.json').write_text(create.stdout);o.joinpath('create.log').write_text(create.stderr)
 if create.returncode:raise RuntimeError('create failed')
 cmd=base+['-mode=run','-page-trace='+str(trace)];last=None;killed=False
 with o.joinpath('stdout.json').open('w') as stdout,o.joinpath('stderr.log').open('w') as stderr,o.joinpath('progress.jsonl').open('w') as progress:
  p=subprocess.Popen(cmd,env=e,stdout=stdout,stderr=stderr)
  while p.poll() is None:
   r=call('status');j=json.loads(r.stdout) if not r.returncode else {'problem':r.stderr};elapsed=time.monotonic()-start
   try:trx=mysql.sql("SELECT JSON_OBJECT('active',count(*),'seconds',COALESCE(max(timestampdiff(second,trx_started,now())),0),'modified',COALESCE(sum(trx_rows_modified),0)) FROM information_schema.innodb_trx; SELECT COUNT FROM information_schema.innodb_metrics WHERE NAME='trx_rseg_history_len';",5)
   except Exception:trx='UNKNOWN'
   row={'elapsed':elapsed,'utc':time.time(),'job':j,'mysql_trx':trx};progress.write(json.dumps(row)+chr(10));progress.flush();last=j
   if kill_at and j.get('applied',0)>=kill_at:p.kill();killed=True;break
   if elapsed>window:p.send_signal(signal.SIGTERM);break
   time.sleep(2)
  code=p.wait(timeout=20)
 o.joinpath('status.json').write_text(call('status').stdout);o.joinpath('job-logs.jsonl').write_text(call('logs').stdout);o.joinpath('result.json').write_text(json.dumps({'command':cmd,'exit_code':code,'sigkill':killed,'start_utc':start_at,'seconds':time.monotonic()-start,'last_observation':last},indent=2));print(code,time.monotonic()-start)
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','start','stop','run','audit','check','environment']);p.add_argument('--job',default='m502');p.add_argument('--output');p.add_argument('--kill-at',type=int,default=0);a=p.parse_args()
 if a.action=='prepare':raise SystemExit(subprocess.run([str(ROOT/'bin/ticketpulse-bench-prepare'),'-batch=500','-deadline=10m'],env=env()).returncode)
 elif a.action=='start':start()
 elif a.action=='stop':stop()
 elif a.action=='run':run(a.job,a.output,a.kill_at)
 elif a.action=='audit':
  import bench_audit as audit
  original_rows=audit.ckrows
  def canonical_rows(query,url,db):
   facts=query.startswith('SELECT organization_id,ticket_id,version,business_event_id,action,')
   previous_key=None;previous_value=None
   for row in original_rows(query,url,db):
    if facts:
     key=(row['organization_id'],row['ticket_id'],row['version']);value=audit.norm(row)
     if previous_key is not None and key<previous_key:raise RuntimeError('independent source ordering failed')
     if key==previous_key:
      if value!=previous_value:raise RuntimeError('independent conflicting canonical fact')
      continue
     previous_key=key;previous_value=value
    yield row
  audit.ckrows=canonical_rows
  audit.stage.verified=verified;audit.stage.sql=lambda q:mysql.sql('USE '+verified()['database']+';'+q);audit.stage.local.ADMIN=mysql.ADMIN
  import sys;sys.argv=['audit','--job',a.job,'--output',a.output];raise SystemExit(audit.main())
 elif a.action=='check':
  cmd=[str(ROOT/'bin/ticketpulse-rebuild'),'-mode=check','-job='+a.job,'-wait=5m','-page-trace='+a.output];raise SystemExit(subprocess.run(cmd,env=env()).returncode)
 else:print(json.dumps({k:v for k,v in verified().items() if k!='project'}))
