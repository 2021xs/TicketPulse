#!/usr/bin/env python3
"""Independent full SQL key/version audit of the frozen ES indices."""
import hashlib
import http.client
import json
from pathlib import Path

import m504_environment as env
import m504_fixture as fixture

def run(n):
    dataset=fixture.fixtures()['datasets'][str(n)]
    database,index=dataset['database'],dataset['index']
    rows=env.sql('USE '+database+';SELECT id,version FROM tickets ORDER BY id;').splitlines()
    source=[tuple(map(int,row.split())) for row in rows]
    candidate=[];after=None;pages=0
    client=http.client.HTTPConnection('127.0.0.1',env.ES_PORT,timeout=5)
    try:
        while True:
            query={'query':{'term':{'organization_id':1}},'sort':[{'id':'asc'}],'size':1000,'track_total_hits':False,'_source':['id','version']}
            if after:query['search_after']=after
            client.request('POST','/'+index+'/_search?allow_partial_search_results=false',json.dumps(query),headers={'Content-Type':'application/json'})
            response=client.getresponse();result=json.loads(response.read())
            if response.status!=200 or result['timed_out'] or result['_shards']['failed']:raise RuntimeError('Independent ES audit failed')
            hits=result['hits']['hits'];pages+=1
            candidate.extend((h['_source']['id'],h['_source']['version']) for h in hits)
            if len(hits)<1000:break
            after=hits[-1]['sort']
    finally:client.close()
    def digest(rows):return hashlib.sha256(''.join(str(i)+':'+str(v)+chr(10) for i,v in rows).encode()).hexdigest()
    result={'tickets':n,'database':database,'index':index,'sql_rows':len(source),'es_rows':len(candidate),
        'same_all_business_keys_and_versions':source==candidate,'sql_sha256':digest(source),'es_sha256':digest(candidate),
        'duplicate_es_ids':len(candidate)-len({r[0] for r in candidate}),'es_pages':pages,'derivation':'independent SELECT id,version vs direct ES search_after ID order; production SearchService not used for this audit'}
    folder=env.OUT/'correctness/frozen-full-audit';folder.mkdir(exist_ok=True)
    (folder/(str(n)+'.json')).write_text(json.dumps(result,indent=2))
    if source!=candidate:raise RuntimeError('Frozen full source/ES key/version mismatch')
    print(json.dumps({'tickets':n,'independent_all_keys_versions_match':True}),flush=True)

if __name__=='__main__':
    for n in [10000,100000]:run(n)
