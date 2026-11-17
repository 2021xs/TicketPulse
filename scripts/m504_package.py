#!/usr/bin/env python3
"""Full Diff, raw evidence hashes and independent milestone preservation."""
import argparse
import datetime
import difflib
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tarfile
import tempfile

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'artifacts/m5-04'
EXCLUDED={'.git','.local','node_modules','bin','artifacts','__pycache__','test-results','playwright-report'}

def sha(data):return hashlib.sha256(data).hexdigest()
def checksum(path):
    digest=hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda:source.read(1<<20),b''):digest.update(chunk)
    return digest.hexdigest()
def write(name,value):(OUT/name).write_text(json.dumps(value,indent=2,ensure_ascii=False)+chr(10))
def reachable(port):
    with socket.socket() as connection:
        connection.settimeout(1);return connection.connect_ex(('127.0.0.1',port))==0

def compress():
    # Only ended app logs and completed measurements are eligible. Never touch
    # previous milestones or evidence still being written by any test process.
    import m504_fixture as f
    worker=f.fixtures()['workers'].get('app',{})
    active=ROOT/worker['diagnostics'] if worker else None
    candidates=[]
    for folder in (OUT/'environment/apps').glob('*'):
        if active and active.parent==folder:continue
        candidates.extend(folder.glob('*.jsonl'))
    for folder in (OUT/'performance').glob('*'):
        if not (folder/'result.json').is_file():continue
        candidates.extend(folder.glob('*.jsonl'))
    manifest=OUT/'compression-manifest.json'
    old=json.loads(manifest.read_text()) if manifest.exists() else {'files':[]}
    for path in candidates:
        if path.stat().st_size<256*1024:continue
        target=path.with_suffix(path.suffix+'.gz')
        if target.exists():raise RuntimeError('Refuse existing compressed evidence')
        before=checksum(path);length=path.stat().st_size
        with path.open('rb') as source,target.open('xb') as output,gzip.GzipFile(filename='',mode='wb',compresslevel=6,fileobj=output,mtime=0) as zipped:
            for block in iter(lambda:source.read(1<<20),b''):zipped.write(block)
        digest=hashlib.sha256();actual=0
        with gzip.open(target,'rb') as source:
            for block in iter(lambda:source.read(1<<20),b''):digest.update(block);actual+=len(block)
        if digest.hexdigest()!=before or actual!=length:raise RuntimeError('Lossless evidence verification failed')
        old['files'].append({'original':path.relative_to(OUT).as_posix(),'compressed':target.relative_to(OUT).as_posix(),
            'raw_sha256':before,'raw_bytes':length,'gzip_sha256':checksum(target),'gzip_bytes':target.stat().st_size})
        path.unlink()
    old['updated_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();write('compression-manifest.json',old)
    print(json.dumps({'compressed_files':len(old['files']),'saved_bytes':sum(r['raw_bytes']-r['gzip_bytes'] for r in old['files'])}))

def main():
    baseline=json.loads((OUT/'baseline-source-manifest.json').read_text())
    with tarfile.open(OUT/'pre-m5-04-source.tar.gz') as archive:
        old={r['path']:archive.extractfile(r['path']).read() for r in baseline['files']}
    for row in baseline['files']:
        if sha(old[row['path']])!=row['sha256']:raise RuntimeError('Source baseline changed')
    now={}
    for parent,dirs,files in os.walk(ROOT):
        dirs[:]=[x for x in dirs if x not in EXCLUDED]
        for name in files:
            path=Path(parent)/name;now[path.relative_to(ROOT).as_posix()]=path.read_bytes()
    changes=[];patch=[]
    for name in sorted(set(old)|set(now)):
        before,after=old.get(name,b''),now.get(name,b'')
        if before==after:continue
        changes.append({'path':name,'status':'added' if name not in old else 'modified' if name in now else 'deleted',
            'before_bytes':len(before),'after_bytes':len(after),'before_sha256':sha(before),'after_sha256':sha(after)})
        patch.extend(difflib.unified_diff(before.decode().splitlines(True),after.decode().splitlines(True),
            fromfile='a/'+name if name in old else '/dev/null',tofile='b/'+name if name in now else '/dev/null'))
    (OUT/'full.diff').write_text(''.join(patch));write('file-manifest.json',{'source_files_before':len(old),'source_files_after':len(now),'changes':changes})
    with tempfile.TemporaryDirectory(prefix='ticketpulse-m504-diff-') as temp:
        folder=Path(temp)
        for name,data in old.items():
            path=folder/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
        result=subprocess.run(['git','apply','--unsafe-paths',str(OUT/'full.diff')],cwd=folder,capture_output=True,text=True)
        restored={p.relative_to(folder).as_posix():p.read_bytes() for p in folder.rglob('*') if p.is_file()}
        exact=result.returncode==0 and restored==now
        write('diff-verification.json',{'exact_source_bytes_restored':exact,'exit_code':result.returncode,'stderr':result.stderr,'source_files':len(now)})
        if not exact:raise RuntimeError('Full Diff roundtrip differs')
    previous=json.loads((OUT/'previous-evidence-manifest.json').read_text())['files']
    changed=[r['path'] for r in previous if not (ROOT/r['path']).is_file() or checksum(ROOT/r['path'])!=r['sha256']]
    protected=[name for name in old if name.startswith(('internal/analytics/','internal/syncpipe/rebuild','db/'))]
    protocol_changed=[name for name in protected if old[name]!=now.get(name)]
    write('previous-evidence-preservation.json',{'checked':len(previous),'changed':changed,'all_prior_evidence_unchanged':not changed,
        'protected_ck_m4_and_ddl_files':len(protected),'protected_files_changed':protocol_changed})
    if changed or protocol_changed:raise RuntimeError('Prior evidence / CK / M4 / schema changed')
    report=ROOT/'docs/m5-04-delivery.md'
    links=re.findall(r'[]][(]([^)]*)[)]',report.read_text())
    missing=[x for x in links if not x.startswith(('http:','https:','#')) and not (report.parent/x.split('#',1)[0]).exists()
             and x not in ['../artifacts/m5-04/artifact-manifest.json','../artifacts/m5-04/closeout.json']]
    regression=json.loads((OUT/'regression/final/test-summary.json').read_text())
    demo=json.loads((OUT/'environment/demo-preservation-baseline.json').read_text())
    ab=json.loads((OUT/'correctness/ab-result-check.json').read_text())
    stage_clean=subprocess.run(['git','diff','--cached','--quiet'],cwd=ROOT).returncode==0
    demo_ports={str(p):reachable(p) for p in [33306,18080,18123,19092,19200]}
    test_ports={str(p):reachable(p) for p in [33309,19202,19302,18087,18126,19004]}
    good=(exact and not changed and not protocol_changed and not missing and regression['failed']==0 and demo['preserved']
          and ab['same'] and stage_clean and all(demo_ports.values()) and not any(test_ports.values()))
    write('closeout.json',{'checks_passed':good,'diff_roundtrip_exact':exact,'no_staged_changes':stage_clean,
        'no_commit_or_push_performed':True,'previous_evidence_checked':len(previous),
        'regression_pass':regression['passed'],'regression_fail':regression['failed'],'demo_preserved':demo['preserved'],
        'ab_correctness_same':ab['same'],'demo_ports':demo_ports,'test_ports':test_ports,'missing_links':missing})
    if not good:raise RuntimeError('Closeout checks incomplete')
    rows=[{'path':p.relative_to(OUT).as_posix(),'bytes':p.stat().st_size,'sha256':checksum(p)} for p in sorted(OUT.rglob('*')) if p.is_file() and p.name!='artifact-manifest.json']
    write('artifact-manifest.json',{'captured_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':rows})
    print(json.dumps({'source_changes':len(changes),'artifact_files':len(rows),'checks_passed':good}))

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--compress',action='store_true');args=parser.parse_args()
    if args.compress:compress()
    else:main()
