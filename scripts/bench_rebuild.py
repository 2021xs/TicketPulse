#!/usr/bin/env python3
"""Finite M4 timing harness, real CLI/status/MySQL transaction evidence."""
import argparse,json,os,signal,subprocess,time
from pathlib import Path
import bench_stage as stage
ROOT=stage.ROOT
def execute(binary,job,output,window,kill_at=0):
 output=Path(output);output.mkdir(parents=True,exist_ok=False);env=stage.environment();v=stage.verified();binary=str((ROOT/binary).resolve())
 base=[binary,"-job="+job,"-batch-size=500","-wait=5m"]
 def call(mode):
  p=subprocess.run(base+["-mode="+mode],env=env,capture_output=True,text=True,timeout=30);return p
 created=call("create");(output/"create.json").write_text(created.stdout);(output/"create.stderr.log").write_text(created.stderr)
 if created.returncode:raise RuntimeError("job create failed")
 cmd=base+["-mode=run"];start=time.time();started=time.monotonic();frozen=None;killed=None;last={};timeline=[]
 with (output/"run.json").open("w") as log,(output/"run.stderr.log").open("w") as err,(output/"progress.jsonl").open("w") as progress:
  child=subprocess.Popen(cmd,env=env,stdout=log,stderr=err)
  while child.poll() is None:
   p=call("status")
   if p.returncode:row={"status_error":p.stderr}
   else:row=json.loads(p.stdout);last=row
   elapsed=time.monotonic()-started
   if row.get("total",0)>0 and frozen is None:frozen=elapsed
   try:
    trx=stage.local.mysql("SELECT JSON_OBJECT('active',count(*),'longest_seconds',COALESCE(max(timestampdiff(second,trx_started,now())),0),'rows_modified',COALESCE(sum(trx_rows_modified),0)) FROM information_schema.innodb_trx; SELECT JSON_OBJECT('history_list_length',COUNT) FROM information_schema.innodb_metrics WHERE NAME='trx_rseg_history_len';")
   except subprocess.CalledProcessError:
    trx='UNKNOWN: metric connection failed; workload retained'
   entry={"elapsed_seconds":elapsed,"observed_at":time.time(),"job":row,"mysql_trx":trx};progress.write(json.dumps(entry)+chr(10));progress.flush()
   if (kill_at and row.get("applied",0)>=kill_at and killed is None):
    killed=elapsed;os.kill(child.pid,signal.SIGKILL);break
   if elapsed>window:
    child.send_signal(signal.SIGTERM);break
   time.sleep(2)
  try:code=child.wait(timeout=15)
  except subprocess.TimeoutExpired:child.kill();code=child.wait()
 final=call("status");(output/"final-status.json").write_text(final.stdout);logs=call("logs");(output/"job-logs.jsonl").write_text(logs.stdout)
 storage=stage.sql("SELECT JSON_OBJECT('table',table_name,'data_bytes',data_length,'index_bytes',index_length) FROM information_schema.tables WHERE table_schema='"+v["database"]+"' AND table_name LIKE 'sync_rebuild%'; SELECT JSON_OBJECT('job',job_id,'rows',count(*),'payload_bytes',sum(JSON_STORAGE_SIZE(payload))) FROM sync_rebuild_items GROUP BY job_id;")
 (output/"control-storage.jsonl").write_text(storage);summary={"command":cmd,"source_database":v["database"],"ck_url":v.get("ck_url"),"started_at":start,"elapsed_seconds":time.monotonic()-started,"freeze_observed_upper_seconds":frozen,"exit_code":code,"window_seconds":window,"sigkill_elapsed_seconds":killed,"last_observed_job":last};(output/"summary.json").write_text(json.dumps(summary,indent=2));print(json.dumps(summary));return summary
if __name__=="__main__":
 p=argparse.ArgumentParser();p.add_argument("--binary",default="artifacts/m5-01/bulk-rebuild-bin");p.add_argument("--job",required=True);p.add_argument("--output",required=True);p.add_argument("--window",type=int,default=900);p.add_argument("--kill-at",type=int,default=0);a=p.parse_args();execute(a.binary,a.job,a.output,a.window,a.kill_at)
