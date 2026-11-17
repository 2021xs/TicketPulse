#!/usr/bin/env python3
"""Owned independent M5-02 MySQL33307. Never stops/reconfigures demo33306."""
import argparse,json,os,resource,secrets,signal,socket,subprocess,time
from pathlib import Path
import local_mysql as demo
ROOT=demo.ROOT;R=ROOT/'.local/m5-02-mysql';STATE=R/'instance.json';ADMIN=R/'admin.cnf';PORT=33307;OUT=ROOT/'artifacts/m5-02/environment'
def sql(q,timeout=60):
 p=subprocess.run([str(demo.DIST/'bin/mysql'),'--defaults-file='+str(ADMIN),'--batch','--skip-column-names'],input=q,text=True,capture_output=True,timeout=timeout)
 if p.returncode:raise RuntimeError(p.stderr)
 return p.stdout

def state():
 v=json.loads(STATE.read_text())
 if v['project']!=str(ROOT) or v['datadir']!=str(R/'data') or v['port']!=PORT:raise RuntimeError('ownership mismatch')
 return v

def owned(pid):
 try:
  c=subprocess.check_output(['ps','-p',str(pid),'-o','command='],text=True)
  return str(demo.DIST/'bin/mysqld') in c and '--datadir='+str(R/'data') in c
 except subprocess.CalledProcessError:return False

def stop():
 v=state();pid=v.get('pid')
 if pid and owned(pid):
  os.kill(pid,signal.SIGTERM)
  for _ in range(300):
   if not owned(pid):break
   time.sleep(.1)
  else:raise RuntimeError('owned MySQL shutdown timeout')
 v['pid']=None;demo.private_file(STATE,json.dumps(v,indent=2))

def start(profile):
 OUT.mkdir(parents=True,exist_ok=True);R.mkdir(parents=True,exist_ok=True)
 if STATE.exists():
  v=state()
  if v.get('pid') and owned(v['pid']):raise RuntimeError('stop owned M502 instance before changing profile')
 else:
  if (R/'data').exists():raise RuntimeError('unowned existing data')
  subprocess.run([str(demo.DIST/'bin/mysqld'),'--no-defaults','--initialize-insecure','--basedir='+str(demo.DIST),'--datadir='+str(R/'data'),'--log-error='+str(R/'init.log')],check=True)
  v={'project':str(ROOT),'datadir':str(R/'data'),'port':PORT,'database':'ticketpulse_test_m502_'+secrets.token_hex(5),'pid':None}
  demo.private_file(STATE,json.dumps(v,indent=2))
  demo.private_file(ADMIN,chr(10).join(['[client]','user=root','protocol=socket','socket='+str(R/'mysql.sock'),'']))
 with socket.socket() as c:
  if c.connect_ex(('127.0.0.1',PORT))==0:raise RuntimeError('unowned occupied port')
 resources={'baseline':['--innodb-buffer-pool-size=64M','--max-connections=64','--table-open-cache=4000','--table-definition-cache=2000','--innodb-open-files=4000','--open-files-limit=8074'], 'reasonable':['--innodb-buffer-pool-size=2G','--max-connections=128','--table-open-cache=512','--table-definition-cache=512','--innodb-open-files=512','--open-files-limit=32768']}[profile]
 args=[str(demo.DIST/'bin/mysqld'),'--no-defaults','--basedir='+str(demo.DIST),'--datadir='+str(R/'data'),'--socket='+str(R/'mysql.sock'),'--pid-file='+str(R/'mysql.pid'),'--log-error='+str(R/'mysql.log'),'--bind-address=127.0.0.1','--port='+str(PORT),'--mysqlx=0','--log-bin='+str(R/'ticketpulse-bin'),'--server-id=502','--binlog-format=ROW','--binlog-row-image=FULL','--binlog-row-metadata=FULL','--binlog-expire-logs-seconds=604800','--sync-binlog=1','--innodb-flush-log-at-trx-commit=1','--performance-schema-digests-size=20000']+resources
 def budget():
  soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE);resource.setrlimit(resource.RLIMIT_NOFILE,(min(65536,hard),hard))
 with (R/'launch.log').open('a') as log:p=subprocess.Popen(args,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,preexec_fn=budget)
 v['pid']=p.pid;v['profile']=profile;demo.private_file(STATE,json.dumps(v,indent=2))
 for _ in range(200):
  try:
   uuid=sql('SELECT @@server_uuid;',3).strip();break
  except Exception:time.sleep(.1)
 else:raise RuntimeError('independent startup failed')
 if v.get('server_uuid') and uuid!=v['server_uuid']:raise RuntimeError('new instance UUID changed')
 v['server_uuid']=uuid;demo.private_file(STATE,json.dumps(v,indent=2))
 settings=sql("SHOW VARIABLES WHERE Variable_name IN ('innodb_buffer_pool_size','max_connections','table_open_cache','table_definition_cache','innodb_open_files','open_files_limit','server_uuid','server_id','binlog_format','binlog_row_image','performance_schema','port');")
 evidence={'profile':profile,'command':args,'effective_settings':settings,'pid':p.pid,'port':PORT,'database':v['database'],'uuid':uuid,'demo_untouched':True};(OUT/(profile+'-mysql.json')).write_text(json.dumps(evidence,indent=2));print(json.dumps(evidence))

def clone():
 v=state();old=json.loads((ROOT/'.local/m5-fixture.json').read_text());demo.verify(demo.marker());src=old['database'];db=v['database']
 exists=sql("SELECT count(*) FROM information_schema.schemata WHERE schema_name='"+db+"';").strip()
 if exists!='0':raise RuntimeError('refuse overwrite source')
 tables=demo.mysql("SELECT table_name FROM information_schema.tables WHERE table_schema='"+src+"' AND table_type='BASE TABLE' AND table_name NOT LIKE 'sync_%' ORDER BY table_name;").splitlines()
 dump=R/'source.sql';t=time.time()
 command=[str(demo.DIST/'bin/mysqldump'),'--defaults-file='+str(demo.ADMIN),'--single-transaction','--skip-lock-tables','--no-tablespaces','--set-gtid-purged=OFF','--hex-blob',src]+tables
 with dump.open('w') as f:subprocess.run(command,stdout=f,stderr=subprocess.PIPE,check=True)
 os.chmod(dump,0o600);sql('CREATE DATABASE '+db+' CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;')
 with dump.open() as f:subprocess.run([str(demo.DIST/'bin/mysql'),'--defaults-file='+str(ADMIN),db],stdin=f,check=True)
 migrations=['006_search_sync.sql','007_analytics_sync.sql','008_kafka_sync.sql','009_rebuild.sql']
 sql('USE '+db+';'+chr(10)+chr(10).join((ROOT/'db/migrations'/p).read_text() for p in migrations))
 sql("CREATE USER IF NOT EXISTS 'm502_cdc'@'127.0.0.1' IDENTIFIED BY 'm502-local-replication'; GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'm502_cdc'@'127.0.0.1';")
 counts=sql('USE '+db+';SELECT count(*) FROM tickets;SELECT count(*) FROM ticket_histories;SELECT count(*) FROM ticket_comments;')
 (OUT/'source-clone.json').write_text(json.dumps({'source_database':src,'target_database':db,'source_uuid':old['source_uuid'],'target_uuid':v['server_uuid'],'tables':tables,'dump_bytes':dump.stat().st_size,'seconds':time.time()-t,'counts':counts,'business_source_only':True},indent=2));print(counts)
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('action',choices=['start','stop','clone','status']);p.add_argument('--profile',choices=['baseline','reasonable'],default='reasonable');a=p.parse_args()
 if a.action=='start':start(a.profile)
 elif a.action=='stop':stop()
 elif a.action=='clone':clone()
 else:print(json.dumps(state()))
