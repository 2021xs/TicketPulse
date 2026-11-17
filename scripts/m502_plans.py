#!/usr/bin/env python3
"""Real first/middle/tail source queries with cost and handler counters."""
import argparse,json,time,subprocess
from pathlib import Path
import m502_mysql as m
ROOT=m.ROOT;OUT=ROOT/'artifacts/m5-02/sql'
def main():
 p=argparse.ArgumentParser();p.add_argument('--profile',required=True);p.add_argument('--variant',choices=['original','optimized','ordered-experiment'],default='original');p.add_argument('--output',help='new evidence directory');a=p.parse_args();db=m.state()['database'];out=Path(a.output) if a.output else OUT/(a.profile+'-'+a.variant);out.mkdir(parents=True,exist_ok=False)
 columns='h.id,h.organization_id,h.ticket_id,h.action,h.operator_id,h.version,h.before_state,h.after_state,h.responsible_team_id,h.occurred_at,h.reason,h.comment_id,t.ticket_no,t.created_at'
 data=[]
 for label,org,tid,ver in [('first',0,0,0),('middle',1,50000,7),('last',1,99990,7),('end',1,100000,999)]:
  q='SELECT '+columns+' FROM ticket_histories h JOIN tickets t ON t.id=h.ticket_id AND t.organization_id=h.organization_id WHERE (h.organization_id,h.ticket_id,h.version)>('+str(org)+','+str(tid)+','+str(ver)+') ORDER BY h.organization_id,h.ticket_id,h.version LIMIT 500'
  if a.variant!='original':
   join='STRAIGHT_JOIN' if a.variant=='ordered-experiment' else 'JOIN'
   q='SELECT '+columns+' FROM ticket_histories h FORCE INDEX (idx_history_reconcile) '+join+' tickets t ON t.id=h.ticket_id AND t.organization_id=h.organization_id WHERE h.organization_id>'+str(org)+' OR (h.organization_id='+str(org)+' AND (h.ticket_id>'+str(tid)+' OR (h.ticket_id='+str(tid)+' AND h.version>'+str(ver)+'))) ORDER BY h.organization_id,h.ticket_id,h.version LIMIT 500'
  (out/(label+'.sql')).write_text(q);start=time.monotonic()
  try:plan=m.sql('USE '+db+';EXPLAIN ANALYZE '+q+';',timeout=90);error=None
  except Exception as e:plan='';error=str(e)
  (out/(label+'.explain.txt')).write_text(plan or error);row={'label':label,'cursor':[org,tid,ver],'explain_seconds':time.monotonic()-start,'error':error}
  before="SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_next','Handler_read_rnd_next','Sort_rows','Sort_scan','Created_tmp_tables','Created_tmp_disk_tables');"
  start=time.monotonic()
  try:
   sample=m.sql('USE '+db+';'+before+q+';'+before,timeout=90);row['elapsed_seconds']=time.monotonic()-start;(out/(label+'.sample.txt')).write_text(sample)
   lines=sample.splitlines();row['returned_rows']=len(lines)-12;row['counters_before']=lines[:6];row['counters_after']=lines[-6:]
  except Exception as e:row['query_error']=str(e)
  data.append(row);(out/'summary.json').write_text(json.dumps(data,indent=2));print(json.dumps(row),flush=True)
if __name__=='__main__':main()
