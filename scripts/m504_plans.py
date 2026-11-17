#!/usr/bin/env python3
"""Actual candidate profiles and SQL EXPLAIN for identical 100/400-ID batches."""
import json
import http.client
from pathlib import Path
import time
from urllib.parse import urlencode

import m504_environment as env
import m504_fixture as fixture

def run(n):
    item=fixture.fixtures()['datasets'][str(n)]
    database=item['selective_database'];index=item['selective_index']
    out=env.OUT/'plans'/str(n);out.mkdir(parents=True,exist_ok=False)
    source=(env.ROOT/'internal/mysqlstore/search.go').read_text()
    (out/'authorization-source.go.txt').write_text(source)
    # Exact existing authorize statement, expanded solely with fixture literals.
    select='SELECT t.id,t.ticket_no,t.title,t.priority,t.status,t.assignee_id,u.name,t.current_team_id,tm.name,t.version,t.updated_at FROM tickets t LEFT JOIN users u ON u.organization_id=t.organization_id AND u.id=t.assignee_id JOIN teams tm ON tm.organization_id=t.organization_id AND tm.id=t.current_team_id WHERE '
    member='EXISTS (SELECT 1 FROM team_members m JOIN teams tm ON tm.organization_id=m.organization_id AND tm.id=m.team_id WHERE m.organization_id=t.organization_id AND m.team_id=t.current_team_id AND m.user_id=503 AND m.active=TRUE AND tm.active=TRUE)'
    for count in [100,400]:
        sql=select+'t.organization_id=1 AND t.id IN ('+','.join(map(str,range(1,count+1)))+') AND (t.creator_id=503 OR '+member+')'
        (out/('authorize-'+str(count)+'.sql')).write_text(sql+';'+chr(10))
        result=env.sql('USE '+database+';EXPLAIN FORMAT=JSON '+sql+';')
        (out/('authorize-'+str(count)+'-explain.json')).write_text(result)
        analysis=env.sql('USE '+database+';EXPLAIN ANALYZE '+sql+';')
        (out/('authorize-'+str(count)+'-analyze.txt')).write_text(analysis)
    base={'query':{'bool':{'filter':[{'term':{'organization_id':1}},{'bool':{'should':[{'term':{'creator_id':503}},{'terms':{'current_team_id':[300]+list(range(1000,1030))}}],'minimum_should_match':1}}],
                           'must':{'multi_match':{'query':'benchmark','fields':['title^2','description'],'operator':'and'}}}},
          'sort':[{'updated_at':{'order':'desc'}},{'id':{'order':'desc'}}], 'size':100,'track_total_hits':False,'_source':['id','version'],'timeout':'3s'}
    for size in [100,400]:
        query={**base,'size':size,'profile':True}
        (out/('candidate-'+str(size)+'.json')).write_text(json.dumps(query,indent=2))
        result=fixture.es('POST','/'+index+'/_search?allow_partial_search_results=false',query)
        # Hit source is only id/version. No ticket text in profile artifacts.
        (out/('candidate-'+str(size)+'-profile.json')).write_text(json.dumps(result,indent=2))
    stats=env.sql("SELECT DIGEST,DIGEST_TEXT,COUNT_STAR,SUM_TIMER_WAIT,SUM_ROWS_EXAMINED,SUM_ROWS_SENT FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME='"+database+"' ORDER BY SUM_TIMER_WAIT DESC LIMIT 20;")
    (out/'sql-digests.tsv').write_text(stats)
    (out/'ddl.txt').write_text(env.sql('USE '+database+';SHOW CREATE TABLE tickets;SHOW CREATE TABLE team_members;SHOW CREATE TABLE teams;SHOW CREATE TABLE users;'))
    print(json.dumps({'plans':n,'unchanged_mysql_authorization_sql':True}))

if __name__=='__main__':
    for n in [10000,100000]:run(n)
