#!/usr/bin/env python3
"""Independent read-only content hash audit of a stationary owned M5 fixture."""
import argparse, datetime, hashlib, json, re, subprocess, time, urllib.request
from pathlib import Path
import bench_stage as stage

def norm(v):
    if isinstance(v,dict): return {k:norm(x) for k,x in v.items()}
    if isinstance(v,list): return [norm(x) for x in v]
    if isinstance(v,str) and len(v)>=19 and v[4:5]=='-' and v[7:8]=='-' and v[10:11] in ('T',' '):
        try:
            v=re.sub(r"[.](\d{1,6})(?=Z|[+]00:00|$)",lambda m: "."+m.group(1).ljust(6,"0"),v)
            return datetime.datetime.fromisoformat(v.replace('Z','+00:00')).replace(tzinfo=None).isoformat(timespec='microseconds')
        except ValueError: pass
    return v

def digest(rows, fields):
    h=hashlib.sha256();n=0
    for r in rows:
        h.update(json.dumps(norm({k:r[k] for k in fields}),sort_keys=True,separators=(',',':'),ensure_ascii=False).encode());h.update(bytes([10]));n+=1
    return {'rows':n,'sha256':h.hexdigest()}

def sqlrows(query,db):
    p=subprocess.Popen([str(stage.local.DIST/'bin/mysql'),'--defaults-file='+str(stage.local.ADMIN),'--batch','--skip-column-names'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    p.stdin.write('USE '+db+'; SET time_zone="+00:00"; '+query);p.stdin.close()
    for row in p.stdout: yield json.loads(bytes.fromhex(row.strip()).decode())
    if p.wait(): raise RuntimeError('MySQL stream failed')

def ckrows(query,url,db):
    req=urllib.request.Request(url+'/?database='+db+'&log_queries=0&max_threads=1&output_format_json_quote_64bit_integers=0',data=(query+' FORMAT JSONEachRow').encode())
    with urllib.request.urlopen(req,timeout=45) as r:
        for row in r: yield json.loads(row)

def main():
    p=argparse.ArgumentParser();p.add_argument('--job',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    v=stage.verified();db=v['database'];target=db+'_rebuild_'+a.job;out={'database':db,'job':a.job,'scope':'Static full contents, independent SQL; no live synchronization or job completion claim','queries':{}};started=time.time()
    if stage.sql('SELECT count(DISTINCT organization_id) FROM tickets;').strip()!='1': raise RuntimeError('audit requires single-organization fixture')
    fields=['id','organization_id','ticket_no','title','description','status','priority','priority_rank','category','creator_id','assignee_id','initial_team_id','current_team_id','created_at','updated_at','version']
    values={k:k for k in fields};values['priority_rank']="CASE priority WHEN 'low' THEN 1 WHEN 'medium' THEN 2 WHEN 'high' THEN 3 WHEN 'urgent' THEN 4 END"
    for k in ['created_at','updated_at']:values[k]="DATE_FORMAT("+k+",'%Y-%m-%d %H:%i:%s.%f')"
    q='SELECT HEX(JSON_OBJECT('+','.join("'"+k+"',"+values[k] for k in fields)+')) FROM tickets ORDER BY id;';out['queries']['mysql_current']=q;out['mysql_current']=digest(sqlrows(q,db),fields)
    def esrows():
        last=0
        while True:
            body={'size':1000,'version':True,'sort':[{'id':'asc'}],'query':{'range':{'id':{'gt':last}}}}
            req=urllib.request.Request('http://127.0.0.1:19200/'+target+'_tickets/_search',data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(req,timeout=15) as r:d=json.load(r)
            if d['timed_out'] or d['_shards']['failed']:raise RuntimeError('ES partial result')
            hits=d['hits']['hits']
            if not hits:break
            for hit in hits:
                row=hit['_source']
                if row['version']!=hit['_version']:raise RuntimeError('ES external version differs')
                yield row;last=row['id']
    out['es_current']=digest(esrows(),fields)
    fields=['organization_id','ticket_id','version','business_event_id','action','occurred_at','event_team_id','initial_team_id','before_team_id','before_status','after_status','assignee_id','created_at','first_resolved_at']
    expr=['h.organization_id','h.ticket_id','h.version',"CONCAT(h.organization_id,':',h.ticket_id,':',h.version)",'h.action',"DATE_FORMAT(h.occurred_at,'%Y-%m-%d %H:%i:%s.%f')",'h.responsible_team_id',"JSON_EXTRACT(h.after_state,'$.initial_team_id')","JSON_EXTRACT(h.before_state,'$.current_team_id')","COALESCE(JSON_UNQUOTE(JSON_EXTRACT(h.before_state,'$.status')),'')","JSON_UNQUOTE(JSON_EXTRACT(h.after_state,'$.status'))","JSON_EXTRACT(h.after_state,'$.assignee_id')","DATE_FORMAT(t.created_at,'%Y-%m-%d %H:%i:%s.%f')","JSON_EXTRACT(h.after_state,'$.first_resolved_at')"]
    q='SELECT HEX(JSON_OBJECT('+','.join("'"+k+"',"+x for k,x in zip(fields,expr))+')) FROM ticket_histories h FORCE INDEX (uk_history_ticket_version) STRAIGHT_JOIN tickets t ON t.id=h.ticket_id ORDER BY h.ticket_id,h.version;';out['queries']['mysql_facts']=q;out['mysql_facts']=digest(sqlrows(q,db),fields)
    q='SELECT '+','.join(fields)+' FROM ticket_events ORDER BY organization_id,ticket_id,version';out['queries']['ck_facts']=q;out['ck_facts']=digest(ckrows(q,v['ck_url'],target),fields)
    fields=['organization_id','ticket_id','version','complete','current_team_id','assignee_id','status','initial_team_id','created_at','updated_at','first_resolved_at'];expr={k:k for k in fields};expr['ticket_id']='id';expr['complete']='1'
    for k in ['created_at','updated_at','first_resolved_at']:expr[k]="DATE_FORMAT("+k+",'%Y-%m-%d %H:%i:%s.%f')"
    q='SELECT HEX(JSON_OBJECT('+','.join("'"+k+"',"+expr[k] for k in fields)+')) FROM tickets ORDER BY id;';out['queries']['mysql_states']=q;out['mysql_states']=digest(sqlrows(q,db),fields)
    content=fields[2:];q='SELECT organization_id,ticket_id,'+','.join('c.'+str(i+1)+' AS '+k for i,k in enumerate(content))+' FROM (SELECT organization_id,ticket_id,argMax(tuple('+','.join(content)+'),tuple(version,complete)) AS c FROM ticket_states GROUP BY organization_id,ticket_id) ORDER BY organization_id,ticket_id';out['queries']['ck_states']=q;out['ck_states']=digest(ckrows(q,v['ck_url'],target),fields)
    out['consistent']=all(out['mysql_'+k]==out[t+'_'+k] for k,t in [('current','es'),('facts','ck'),('states','ck')]);out['elapsed_seconds']=time.time()-started;Path(a.output).write_text(json.dumps(out,indent=2));print(json.dumps(out));return 0 if out['consistent'] else 1
if __name__=='__main__':raise SystemExit(main())
