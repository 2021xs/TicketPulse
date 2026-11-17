#!/usr/bin/env python3
"""Read-only ES forwarding evidence for owned M502 search diagnosis."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json,time,urllib.request,urllib.error,threading
from pathlib import Path
OUT=Path('artifacts/m5-02/search');OUT.mkdir(parents=True,exist_ok=True);LOCK=threading.Lock()
class Handler(BaseHTTPRequestHandler):
 protocol_version='HTTP/1.1'
 def log_message(self,*args):pass
 def do_POST(self):
  started=time.monotonic();body=self.rfile.read(int(self.headers.get('Content-Length','0')));status=502;payload=b'';record={'path':self.path}
  try:
   req=urllib.request.Request('http://127.0.0.1:19200'+self.path,data=body,headers={'Content-Type':'application/json'})
   try:
    with urllib.request.urlopen(req,timeout=6) as r:status=r.status;payload=r.read()
   except urllib.error.HTTPError as e:status=e.code;payload=e.read()
   try:
    data=json.loads(payload);hits=data.get('hits',{}).get('hits',[]);record.update({'http_status':status,'timed_out':data.get('timed_out'),'shards':data.get('_shards'),'hits':len(hits),'missing_source':sum('_source' not in h for h in hits),'invalid_candidate_ids':sum(h.get('_source',{}).get('id',0)<=0 for h in hits),'missing_sort':sum(not h.get('sort') for h in hits),'error':data.get('error')})
   except ValueError:record.update({'http_status':status,'invalid_json':True})
  except Exception as e:record['transport_exception']=type(e).__name__
  record['duration_ms']=(time.monotonic()-started)*1000
  if status!=200 or record.get('timed_out') or record.get('missing_source') or record.get('invalid_candidate_ids') or record.get('missing_sort'):
   with LOCK:
    with (OUT/'es-errors.jsonl').open('a') as f:f.write(json.dumps(record)+chr(10))
    if not (OUT/'first-error-response.json').exists():(OUT/'first-error-response.json').write_bytes(payload);(OUT/'first-error-query.json').write_bytes(body)
  else:
   with LOCK:
    with (OUT/'es-summary.jsonl').open('a') as f:f.write(json.dumps(record)+chr(10))
  self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
ThreadingHTTPServer(('127.0.0.1',19201),Handler).serve_forever()
