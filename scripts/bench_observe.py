#!/usr/bin/env python3
"""Direct independent source and target observations, separate from Reconcile."""
import json,urllib.request
from pathlib import Path
import bench_stage as s
def observe(output,job=None):
 v=s.verified();index=v["database"]+("_rebuild_"+job+"_tickets" if job else "_tickets_v1");ck=v["database"]+("_rebuild_"+job if job else "_analytics_v1")
 queries={"counts":"SELECT JSON_OBJECT('tickets',(SELECT count(*) FROM tickets),'histories',(SELECT count(*) FROM ticket_histories),'comments',(SELECT count(*) FROM ticket_comments),'first_resolved',(SELECT count(*) FROM tickets WHERE first_resolved_at IS NOT NULL),'reopens',(SELECT sum(reopen_count) FROM tickets));", "source_team":"SELECT current_team_id,count(*) FROM tickets GROUP BY current_team_id; SELECT responsible_team_id,count(*) FROM ticket_histories WHERE action='resolved' GROUP BY responsible_team_id;", "source_versions":"SELECT id,version,status,current_team_id,assignee_id,title FROM tickets WHERE id IN (1,2,62,869,10000,100000) ORDER BY id;"}
 result={"database":v["database"],"mysql":{k:s.sql(q) for k,q in queries.items()},"ck":{},"es":{}}
 for table in ["ticket_events","ticket_states"]:
  q="SELECT count() AS physical_rows,uniqExact(tuple(organization_id,ticket_id,version)) AS business_version_keys,uniqExact(tuple(organization_id,ticket_id)) AS tickets FROM "+table+" FORMAT JSONEachRow"
  with urllib.request.urlopen(urllib.request.Request(v["ck_url"]+"/?database="+ck+"&log_queries=0&max_threads=1",data=q.encode()),timeout=15) as r:result["ck"][table]=r.read().decode()
 q="WITH first AS (SELECT ticket_id,argMin(event_team_id,version) AS team FROM ticket_events WHERE action='resolved' AND after_status='awaiting_confirmation' GROUP BY ticket_id) SELECT team,count() FROM first GROUP BY team FORMAT JSONEachRow"
 with urllib.request.urlopen(urllib.request.Request(v["ck_url"]+"/?database="+ck+"&log_queries=0&max_threads=1",data=q.encode()),timeout=15) as r:result["ck"]["first_resolution_team"]=r.read().decode()
 for name,path in [("count","_count"),("samples","_mget")]:
  data=None if name=="count" else json.dumps({"ids":["1","2","62","869","10000","100000"]}).encode()
  headers={"Content-Type":"application/json"}
  with urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:19200/"+index+"/"+path,data=data,headers=headers),timeout=10) as r:result["es"][name]=json.load(r)
 Path(output).write_text(json.dumps(result,indent=2));return result
if __name__=="__main__":
 import argparse
 p=argparse.ArgumentParser();p.add_argument("--output",required=True);p.add_argument("--job");a=p.parse_args();observe(a.output,a.job)
