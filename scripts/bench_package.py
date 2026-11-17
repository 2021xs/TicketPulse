#!/usr/bin/env python3
"""Package source diff against immutable pre-M5 archive without staging."""
import csv,datetime,difflib,hashlib,json,subprocess,tarfile,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];OUT=ROOT/'artifacts/m5-01'
def sha(data):return hashlib.sha256(data).hexdigest()
def main():
    baseline=json.loads((OUT/'baseline-source-manifest.json').read_text());base={r['path']:r for r in baseline['files']}
    with tarfile.open(OUT/'pre-m5-source.tar.gz') as archive:old={p:archive.extractfile(p).read() for p in base}
    now={}
    for f in ROOT.rglob('*'):
        if not f.is_file():continue
        p=f.relative_to(ROOT).as_posix()
        if any(x in p.split('/') for x in ['.git','.local','node_modules','bin','artifacts','__pycache__']):continue
        now[p]=f.read_bytes()
    changed=[];patch=[]
    for p in sorted(set(old)|set(now)):
        a=old.get(p,b'');b=now.get(p,b'')
        if a==b:continue
        changed.append({'path':p,'status':'added' if p not in old else 'deleted' if p not in now else 'modified','before_bytes':len(a),'after_bytes':len(b),'before_sha256':sha(a),'after_sha256':sha(b)})
        patch.extend(difflib.unified_diff(a.decode().splitlines(True),b.decode().splitlines(True),fromfile='a/'+p if p in old else '/dev/null',tofile='b/'+p if p in now else '/dev/null'))
    text=''.join(patch);(OUT/'full.diff').write_text(text);(OUT/'file-manifest.json').write_text(json.dumps({'baseline':'pre-m5-source.tar.gz','source_files_before':len(old),'source_files_after':len(now),'changes':changed},indent=2))
    with tempfile.TemporaryDirectory(prefix='ticketpulse-m5-diff-') as temp:
        path=Path(temp)
        for p,data in old.items():f=path/p;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(data)
        applied=subprocess.run(['git','apply','--unsafe-paths','--verbose',str(OUT/'full.diff')],cwd=path,capture_output=True,text=True)
        restored={f.relative_to(path).as_posix():f.read_bytes() for f in path.rglob('*') if f.is_file()}
        exact=applied.returncode==0 and restored==now
        (OUT/'diff-verification.json').write_text(json.dumps({'command':['git','apply','--unsafe-paths','full.diff'],'exit_code':applied.returncode,'exact_source_bytes_restored':exact,'baseline_files':len(old),'current_files':len(now),'stdout':applied.stdout,'stderr':applied.stderr},indent=2))
        if not exact:raise RuntimeError('source diff does not reconstruct final sources')
    rows=json.loads((OUT/'http-summary.json').read_text())
    with (OUT/'http-summary.csv').open('w',newline='') as f:
        names=['run','tickets','concurrency','scenario','requests','success','errors','timeouts','version_conflicts','successful_qps','durable_write_tps','duration_seconds','p50_ms','p95_ms','p99_ms'];writer=csv.DictWriter(f,fieldnames=names);writer.writeheader()
        for row in rows:values={k:v for k,v in row.items() if k!='latency_ms'};values.update({k+'_ms':v for k,v in row['latency_ms'].items()});writer.writerow(values)
    artifacts=[]
    for f in sorted(OUT.rglob('*')):
        if f.is_file() and f.name!='artifact-manifest.json':data=f.read_bytes();artifacts.append({'path':f.relative_to(OUT).as_posix(),'bytes':len(data),'sha256':sha(data)})
    (OUT/'artifact-manifest.json').write_text(json.dumps({'captured_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':artifacts},indent=2))
    print(json.dumps({'source_changes':len(changed),'artifact_files':len(artifacts),'diff_roundtrip_exact':exact}))
if __name__=='__main__':main()
