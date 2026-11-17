#!/usr/bin/env python3
"""Summarize raw M5-04 measurements without merging warmup or budget failures."""
from collections import Counter
import csv
import gzip
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'artifacts/m5-04'

def records(folder,name):
    plain=folder/name
    path=plain if plain.exists() else plain.with_suffix(plain.suffix+'.gz')
    opener=gzip.open if path.suffix=='.gz' else open
    with opener(path,'rt') as stream:
        for line in stream:
            if line.strip():yield json.loads(line)

def main():
    rows=[]
    for file in sorted((OUT/'performance').glob('*/result.json')):
        value=json.loads(file.read_text());config=value['config'];m=value['measurement'];a=value['amplification'];samples=list(records(file.parent,'resources.jsonl'))
        sample=[r for r in samples if 'es_runtime' in r]
        heap=[];rejects=[];opened=[];gc=[];cpu=[];connections=[]
        for s in sample:
            node=next(iter(s['es_runtime']['nodes'].values()))
            heap.append(node['jvm']['mem']['heap_used_percent'])
            rejects.append(node['thread_pool']['search']['rejected'])
            opened.append(node['http']['total_opened'])
            gc.append(sum(c['collection_time_in_millis'] for c in node['jvm']['gc']['collectors'].values()))
            cpu.append(node['process']['cpu']['percent'])
            connections.append(s['pool']['pool']['InUse'])
        rows.append({'run':file.parent.name,'tickets':config['tickets'],'variant':Path(config['app_binary']).name,
            'profile':config['profile'],'authorized_percent':config['authorized_percentage'],'concurrency':config['concurrency'],'kind':config['kind'],
            'duration_seconds':config['duration_seconds'],'warmup_seconds':config['warmup_seconds'],
            'requests':m['requests'],'success':m['success'],'success_qps':round(m['success_qps'],3),
            'p50_ms':m['latency_ms']['p50'],'p95_ms':m['latency_ms']['p95'],'p99_ms':m['latency_ms']['p99'],
            'budget_errors':m['codes'].get('SEARCH_TOO_BROAD',0),'other_errors':sum(v for k,v in m['codes'].items() if k!='SEARCH_TOO_BROAD'),
            'returned_items':m['returned_items'],'candidates_per_returned':a['candidates_per_returned'],
            'candidate_batches_mean':a['stats']['candidate_batches']['mean'],'authorization_queries_mean':a['stats']['authorization_queries']['mean'],
            'mysql_queries_mean':a['stats']['mysql_queries']['mean'],'connection_held_mean_ms':a['stats']['connection_held_ms']['mean'],
            'connection_begin_p99_ms':a['stats']['connection_acquire_and_begin_ms']['p99'],
            'es_total_mean_ms':a['stats']['es_ms']['mean'],'authorization_mean_ms':a['stats']['authorization_ms']['mean'],
            'es_heap_max_percent':max(heap) if heap else None,'es_search_rejection_delta':max(rejects)-min(rejects) if rejects else None,
            'es_connections_opened_delta':max(opened)-min(opened) if opened else None,'es_gc_ms_delta':max(gc)-min(gc) if gc else None,
            'es_cpu_max_percent':max(cpu) if cpu else None,'pool_in_use_max':max(connections) if connections else None,
            'safety_stop':value['safety_stop'],'artifact':file.relative_to(OUT).as_posix()})
    (OUT/'performance-matrix.json').write_text(json.dumps(rows,indent=2))
    if rows:
        with (OUT/'performance-matrix.csv').open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    index={r['run']:r for r in rows}
    pairs=[]
    for r in rows:
        if r['variant'] not in ['adaptive','final']:continue
        before=r['run'].replace('adaptive-','transport-',1).replace('final-','transport-',1)
        if before not in index:continue
        a=index[before];b=r
        pairs.append({'before':before,'after':r['run'],'tickets':r['tickets'],'profile':r['profile'],'concurrency':r['concurrency'],'kind':r['kind'],
            'duration_seconds':r['duration_seconds'],'qps_before':a['success_qps'],'qps_after':b['success_qps'],
            'qps_change_percent':100*(b['success_qps']/a['success_qps']-1) if a['success_qps'] else None,
            'p99_before_ms':a['p99_ms'],'p99_after_ms':b['p99_ms'],'budget_before':a['budget_errors'],'budget_after':b['budget_errors'],
            'authorization_queries_before':a['authorization_queries_mean'],'authorization_queries_after':b['authorization_queries_mean'],
            'candidates_per_result_before':a['candidates_per_returned'],'candidates_per_result_after':b['candidates_per_returned'],
            'connection_held_before_ms':a['connection_held_mean_ms'],'connection_held_after_ms':b['connection_held_mean_ms'],
            'same_queries_and_resources':True})
    (OUT/'ab-summary.json').write_text(json.dumps(pairs,indent=2))
    print(json.dumps({'performance_runs':len(rows),'ab_pairs':len(pairs),'steady_runs':sum(r['duration_seconds']>=60 for r in rows)}))

if __name__=='__main__':main()
