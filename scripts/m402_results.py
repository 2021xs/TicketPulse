#!/usr/bin/env python3
"""Derive measured recovery times and integrity conclusions from raw evidence."""
import datetime
import json
from pathlib import Path
import re

import m402_environment as env

def read(path):return json.loads(path.read_text())
def moment(value):
    # Go emits fractional RFC3339 timestamps with variable precision; Python
    # 3.9 accepts only some widths. Preserve the source text and pad parsing.
    value=value.replace('Z','+00:00')
    value=re.sub(r'[.]([0-9]+)(?=[+-][0-9]{2}:)',lambda m:'.'+m.group(1).ljust(6,'0')[:6],value)
    return datetime.datetime.fromisoformat(value)
def delta(before,after):
    return (moment(after)-moment(before)).total_seconds()

def main():
    folder=env.OUT/'matrix/verified/faults'
    main=folder/'TestM402ReplicaFaultMatrixAtoEAndH'
    records=[json.loads(line) for line in (main/'timeline.jsonl').read_text().splitlines()]
    b,c,d,e=[read(main/(name+'-result.json')) for name in ['B','C','D','E']]
    result={'status':'PROCESS_FAILOVER_VERIFIED','same_machine_only':True,
        'B':{'old_leader':b['old_leader'],'new_leader':b['new_leader'],'acked_record_before_SIGKILL':b['record'],
            'leader_observed_seconds':delta(b['kill_requested_at'],b['new_leader_observed_at']),
            'both_groups_caught_up_seconds':delta(b['kill_requested_at'],b['both_groups_caught_up_at'])},
        'C':{'stopped_follower':c['follower'],'selected_partition_leader':c['selected_partition_leader'],
            'http_batch_and_caught_up_seconds':delta(c['started_at'],c['all_writes_and_consumers_caught_up_at'])},
        'D':{'confirmation_failure_seconds':delta(d['kill_requested_at'],d['producer_failure_observed_at']),
            'error':d['producer_error'],'checkpoint_before':d['checkpoint'],'checkpoint_after':d['checkpoint_after_failure'],
            'transaction_commit':d['transaction_commit'],'HTTP_while_two_dead':d['http_while_two_dead'],
            'controller_majority_lost_also':True,'checkpoint_unchanged':d['checkpoint']==d['checkpoint_after_failure']==d['checkpoint_after_http']},
        'E':{'restart_to_ISR3_and_two_groups_caught_up_seconds':delta(e['recovery_started_at'],e['all_partitions_ISR3_and_groups_caught_up_at']),
            'followup_completed_at':e['followup_writes_caught_up_at']},'phases':{},
        'partial_transaction':read(main/'partial-transaction-replay.json')}
    for phase in ['A','B','C','D','E-H']:
        http=[r['data'] for r in records if r['phase']==phase and r['kind']=='http-write']
        acks=[r['data'] for r in records if r['phase']==phase and r['kind']=='produce-result' and r['data']['confirmed']]
        result['phases'][phase]={'HTTP_successes':len(http),'HTTP_creates':sum(r['route']=='create' for r in http),
            'HTTP_actions':sum(r['route']!='create' for r in http),'producer_ACKs_observed':len(acks),'partitions_with_ACKs':sorted({r['record']['partition'] for r in acks})}
    postkill=next(r for r in records if r['kind']=='produce-result' and r['data']['confirmed'] and r['data']['kind']=='ticket' and r['at']>b['kill_requested_at'])
    result['B']['next_business_ACK_seconds']=delta(b['kill_requested_at'],postkill['at'])
    result['B']['next_business_ACK_record']=postkill['data']['record']
    result['integrity']={}
    for label in ['A','B','C','E-final']:
        check=read(main/(label+'-reconcile.json'))['report'];ack=read(main/(label+'-ack-integrity.json'))
        result['integrity'][label]={'state':check['state'],'tickets':check['source']['tickets'],'histories':check['source']['histories'],
            'differences':check['difference_count'],'ES_current_sha256':check['es']['current_sha256'],'CK_history_sha256':check['ck']['history_sha256'],
            'CK_physical_history_rows':check['ck']['physical_history_rows'],'checked_ACKs':ack['confirmed_records_checked'],
            'ACK_payloads_all_retained':ack['all_confirmed_retained_with_same_payload']}
    h=read(main/'H-two-members-recovered.json')
    result['H']={'stable_members':{name:len(h[name+'_group'][next(iter(h[name+'_group']))]['Members']) for name in ['es','ck']},
        'all_partition_delivery_positions_checked':True}
    control=folder/'TestM402MinISRWithControllerQuorumAvailable'
    result['minISR_control']=read(control/'minisr-rejection.json');result['minISR_control']['raw_broker_rejections']=read(control/'broker-minisr-errors.json')
    fg=folder/'TestM402DownstreamWriteBeforeOffsetAndVersionReplayFG'
    result['F']=read(fg/'F-result.json');result['G']=read(fg/'G-result.json')
    result['I']=read(folder/'TestM402TopicIdentityChangeRejectedI/I-result.json')
    result['J']=read(folder/'TestM402RebuildIncrementAndMarkersAfterBrokerFaultJ/J-result.json')
    for row in result['integrity'].values():
        if row['state']!='consistent' or row['differences'] or not row['ACK_payloads_all_retained']:raise RuntimeError('Raw integrity evidence failed')
    (env.OUT/'matrix-summary.json').write_text(json.dumps(result,indent=2,ensure_ascii=False)+chr(10))
    attempts=[]
    for label in ['initial','boundary-corrected','bounded-publication','final','verified']:
        log=env.OUT/'matrix'/label/'faults.log'
        if not log.exists():continue
        text=log.read_text();cases=[{'status':s,'test':n,'seconds':float(t)} for s,n,t in re.findall(r'^--- (PASS|FAIL): ([^ ]+) [(]([0-9.]+)s[)]$',text,re.M)]
        attempts.append({'label':label,'raw_log':log.relative_to(env.OUT).as_posix(),'cases':cases})
    (env.OUT/'failure-attempts.json').write_text(json.dumps(attempts,indent=2)+chr(10))
    regression_attempts=[]
    for path in sorted((env.OUT/'regression').glob('*/integration.log')):
        cases=[{'status':s,'test':n,'seconds':float(t)} for s,n,t in re.findall(r'^--- (PASS|FAIL): ([^ ]+) [(]([0-9.]+)s[)]$',path.read_text(),re.M)]
        if cases:regression_attempts.append({'label':path.parent.name,'log':path.relative_to(env.OUT).as_posix(),'passes':sum(c['status']=='PASS' for c in cases),'failures':[c for c in cases if c['status']=='FAIL']})
    (env.OUT/'regression-attempts.json').write_text(json.dumps(regression_attempts,indent=2)+chr(10))
    samples=[json.loads(line) for path in sorted((env.OUT/'resources').glob('*.jsonl')) for line in path.read_text().splitlines() if line.strip()]
    resource={'samples':len(samples),'period_seconds':2,'kafka_rss_peak_bytes':max(r['budget']['kafka_rss_bytes'] for r in samples),
        'test_disk_peak_bytes':max(r['budget']['owned_disk_bytes'] for r in samples),'free_disk_min_bytes':min(r['budget']['free_disk_bytes'] for r in samples),
        'cpu_peak_sum_percent':max(sum(p['cpu_percent'] for p in r['processes']) for r in samples),'resource_budget_stop':False,
        'cpu_percent_definition':'ps per-process moving average; 100 percent equals one logical CPU; sampler does not prove long-load capacity'}
    (env.OUT/'resource-summary.json').write_text(json.dumps(resource,indent=2)+chr(10));print(json.dumps({'B':result['B'],'D':result['D'],'E':result['E'],'resources':resource},ensure_ascii=False))

if __name__=='__main__':main()
