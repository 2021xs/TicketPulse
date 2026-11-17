#!/usr/bin/env python3
"""Independently compare exported SQL rows with raw ES/CK facts and PRD Gold."""
import datetime
import json
from pathlib import Path

import m402_environment as env
import m402_results as results

def read(path):return json.loads(path.read_text())
def timestamp(value):return results.moment(value.replace(' ','T')+('Z' if 'T' not in value else '')) if value is not None else None
def ckstamp(value):return results.moment(value.replace(' ','T')+'Z') if value is not None else None
def source_stamp(value):return results.moment(value) if value is not None else None

FACT=['organization_id','ticket_id','version','business_event_id','action','occurred_at','event_team_id','initial_team_id',
      'before_team_id','before_status','after_status','assignee_id','created_at','first_resolved_at']
STATE=['organization_id','ticket_id','version','complete','current_team_id','assignee_id','status','initial_team_id','created_at','updated_at','first_resolved_at']

def canonical(rows,keys,fields):
    unique={}
    for raw in rows:
        row={k:raw[k] for k in fields}
        for name in ['occurred_at','created_at','updated_at','first_resolved_at']:
            if name in row:row[name]=ckstamp(row[name])
        key=tuple(row[k] for k in keys)
        if key in unique and unique[key]!=row:raise RuntimeError('Same business-key content conflict: '+str(key))
        unique[key]=row
    return unique

def metrics(tickets,histories):
    start=results.moment('2026-10-31T16:00:00Z');end=results.moment('2026-11-03T16:00:00Z')
    first={}
    for h in sorted(histories,key=lambda h:(int(h['ticket_id']),int(h['version']))):
        if h['action']=='resolved':first.setdefault(int(h['ticket_id']),h)
    created=[t for t in tickets if start<=source_stamp(t['created_at'])<end]
    resolved=[h for h in first.values() if start<=source_stamp(h['occurred_at'])<end]
    byid={int(t['id']):t for t in tickets}
    durations=[(source_stamp(h['occurred_at'])-source_stamp(byid[int(h['ticket_id'])]['created_at'])).total_seconds() for h in resolved]
    return {'created':len(created),'first_resolved':len(resolved),
        'average_first_resolution_seconds':sum(durations)/len(durations) if durations else None,
        'current_backlog':sum(t['status'] in ['pending','in_progress'] for t in tickets)}

def audit(folder,label):
    tickets=read(folder/(label+'-source-tickets.json'));histories=read(folder/(label+'-source-histories.json'))
    raw=read(folder/(label+'-ck.json'));physical_facts=[json.loads(s) for s in raw['events'] if s];physical_states=[json.loads(s) for s in raw['states'] if s]
    facts=canonical(physical_facts,['organization_id','ticket_id','version'],FACT)
    states=canonical(physical_states,['organization_id','ticket_id','version','complete'],STATE)
    byid={int(t['id']):t for t in tickets};expected_facts={}
    for h in histories:
        after=json.loads(h['after_state']);before=json.loads(h['before_state']) if h['before_state'] else None
        t=byid[int(h['ticket_id'])];key=(int(h['organization_id']),int(h['ticket_id']),int(h['version']))
        expected_facts[key]={'organization_id':key[0],'ticket_id':key[1],'version':key[2],'business_event_id':':'.join(map(str,key)),
            'action':h['action'],'occurred_at':source_stamp(h['occurred_at']),'event_team_id':int(h['responsible_team_id']),
            'initial_team_id':int(after['initial_team_id']),'before_team_id':int(before['current_team_id']) if before else None,
            'before_status':before['status'] if before else '', 'after_status':after['status'],'assignee_id':after['assignee_id'],
            'created_at':source_stamp(t['created_at']),'first_resolved_at':source_stamp(after['first_resolved_at'])}
    if facts!=expected_facts:
        raise RuntimeError('Independent immutable-history mismatch: '+str([k for k in set(facts)|set(expected_facts) if facts.get(k)!=expected_facts.get(k)][:8]))
    current={}
    for row in states.values():
        key=(int(row['organization_id']),int(row['ticket_id']))
        if key not in current or (row['version'],row['complete'])>(current[key]['version'],current[key]['complete']):current[key]=row
    expected_current={}
    for t in tickets:
        key=(int(t['organization_id']),int(t['id']));expected_current[key]={'organization_id':key[0],'ticket_id':key[1],
            'version':int(t['version']),'complete':1,'current_team_id':int(t['current_team_id']),'assignee_id':t['assignee_id'],
            'status':t['status'],'initial_team_id':int(t['initial_team_id']),'created_at':source_stamp(t['created_at']),
            'updated_at':source_stamp(t['updated_at']),'first_resolved_at':source_stamp(t['first_resolved_at'])}
    if current!=expected_current:raise RuntimeError('Independent latest-state mismatch')
    documents=read(folder/(label+'-es.json'))['hits']['hits'];actual_docs={int(h['_id']):h['_source'] for h in documents}
    wanted={}
    ranks={'low':1,'medium':2,'high':3,'urgent':4}
    fields=['id','organization_id','ticket_no','title','description','status','priority','category','creator_id','assignee_id','initial_team_id','current_team_id','created_at','updated_at','version']
    for t in tickets:
        row={k:t[k] for k in fields};row['priority_rank']=ranks[t['priority']];wanted[int(t['id'])]=row
    if actual_docs!=wanted:raise RuntimeError('Independent ES full-field mismatch')
    # Derive the same four metric values separately from source histories and
    # CK canonical facts/current, using no analytics production SQL or code.
    source_metrics=metrics(tickets,histories)
    ck_histories=[{'ticket_id':r['ticket_id'],'version':r['version'],'action':r['action'],'occurred_at':r['occurred_at'].isoformat()} for r in facts.values()]
    ck_tickets=[{'id':r['ticket_id'],'status':r['status'],'created_at':r['created_at'].isoformat()} for r in current.values()]
    ck_metrics=metrics(ck_tickets,ck_histories)
    gold=read(env.ROOT/'tests/analytics/prd_gold.json')['overview']
    if source_metrics!=ck_metrics or source_metrics!=gold:raise RuntimeError('Independent source/CK/Gold mismatch')
    return {'phase':label,'source_tickets':len(tickets),'source_histories':len(histories),'CK_physical_histories':len(physical_facts),
        'CK_canonical_histories':len(facts),'same_key_conflicts':0,'CK_latest_all_fields_equal':True,'ES_all_fields_equal':True,
        'CK_immutable_all_fields_equal':True,'source_metrics':source_metrics,'CK_metrics':ck_metrics,'Gold':gold,
        'first_resolved_total':sum(t['first_resolved_at'] is not None for t in tickets),'reopened_total':sum(int(t['reopen_count']) for t in tickets)}

def main():
    matrix=env.OUT/'matrix/verified/faults';main=matrix/'TestM402ReplicaFaultMatrixAtoEAndH'
    checks=[audit(main,label) for label in ['A','B','C','E-final']]
    checks.append(audit(matrix/'TestM402DownstreamWriteBeforeOffsetAndVersionReplayFG','FG-final'))
    out=env.OUT/'correctness';out.mkdir(exist_ok=True);(out/'independent-export-audit.json').write_text(json.dumps({'all_equal':True,'production_SQL_used':False,'checks':checks},indent=2)+chr(10))
    print(json.dumps({'checks':len(checks),'all_equal':True}))

if __name__=='__main__':main()
