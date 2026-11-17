#!/usr/bin/env python3
"""Controlled search failures: classify actual HTTP errors without raw inputs."""
import argparse
from collections import Counter
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import threading
import time
from urllib.parse import urlencode

import m504_environment as env
import m504_fixture as fixture
import m504_bench as bench

SECRET='m504-sensitive-marker-never-in-server-logs'

class Handler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    mode='pass'
    def log_message(self,*args): pass
    def do_POST(self):
        data=self.rfile.read(int(self.headers.get('Content-Length','0')))
        mode=type(self).mode
        if mode=='refused':
            self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
        status=200
        if mode=='rejected':
            status=429;body={'error':{'type':'es_rejected_execution_exception','reason':SECRET}}
        elif mode=='breaker':
            status=429;body={'error':{'type':'circuit_breaking_exception','reason':SECRET}}
        elif mode=='bad_json':
            payload=SECRET.encode();self.respond(200,payload);return
        elif mode=='partial':
            body={'timed_out':False,'_shards':{'failed':1},'hits':{'hits':[]}}
        elif mode=='query_timeout':
            body={'timed_out':True,'took':3000,'_shards':{'failed':0},'hits':{'hits':[]}}
        elif mode=='transport_timeout':
            time.sleep(5.5);body={'timed_out':False,'_shards':{'failed':0},'hits':{'hits':[]}}
        else:
            client=http.client.HTTPConnection('127.0.0.1',env.ES_PORT,timeout=5)
            try:
                client.request('POST',self.path,data,headers={'Content-Type':'application/json','X-Opaque-ID':self.headers.get('X-Opaque-ID','')})
                response=client.getresponse();payload=response.read();self.respond(response.status,payload)
            finally:client.close()
            return
        self.respond(status,json.dumps(body).encode())
    def respond(self,status,payload):
        self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(payload)));self.end_headers()
        try:self.wfile.write(payload)
        except (BrokenPipeError,ConnectionResetError):pass

def run(binary='adaptive',label='classified-faults'):
    if env.occupied(19203):raise RuntimeError('Fault proxy port occupied')
    folder=env.OUT/'failures'/label;folder.mkdir(parents=True,exist_ok=False)
    server=ThreadingHTTPServer(('127.0.0.1',19203),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    fixture.stop_app();worker=fixture.start_app(10000,binary,False,'http://127.0.0.1:19203',label=label+'-app')
    results=[]
    client=http.client.HTTPConnection('127.0.0.1',env.APP_PORT,timeout=12)
    try:
        for mode,code in [('rejected','SEARCH_UNAVAILABLE'),('breaker','SEARCH_UNAVAILABLE'),('bad_json','SEARCH_UNAVAILABLE'),('partial','SEARCH_UNAVAILABLE'),('query_timeout','SEARCH_TIMEOUT'),('transport_timeout','SEARCH_TIMEOUT'),('refused','SEARCH_UNAVAILABLE'),('pass','')]:
            Handler.mode=mode
            sample,data=bench.request(client,urlencode({'q':SECRET,'page_size':20}),'local-supervisor')
            if sample['code']!=code or sample['status']!=(503 if code else 200):raise RuntimeError('External error contract changed: '+mode)
            results.append({**sample,'fault':mode,'public_contract_unchanged':True})
        bench.debug()
    finally:
        client.close();fixture.stop_app();server.shutdown();server.server_close();thread.join(timeout=2)
    records=[json.loads(line) for line in (env.ROOT/worker['diagnostics']).read_text().splitlines()]
    by_id={r['request_id']:r for r in records}
    for row in results:row['diagnostic']=by_id[row['request_id']]
    for path in (env.ROOT/worker['diagnostics']).parent.iterdir():
        if path.suffix=='.jsonl' and SECRET in path.read_text():raise RuntimeError('Sensitive text leaked to internal log')
    (folder/'results.json').write_text(json.dumps({'controlled_injections':results,'secret_marker_absent':True,
        'historical_cause_claimed':False,'server_logs':worker['diagnostics']},indent=2))
    print(json.dumps({'faults':len(results),'privacy_passed':True,'codes':dict(Counter(r['code'] for r in results))}),flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--binary',default='adaptive');parser.add_argument('--label',default='classified-faults')
    args=parser.parse_args();run(args.binary,args.label)
