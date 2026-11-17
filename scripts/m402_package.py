#!/usr/bin/env python3
"""Roundtrip the full source Diff and verify evidence/demo preservation."""
import datetime
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tarfile
import tempfile

import m402_environment as env

EXCLUDED={'.git','.local','node_modules','bin','artifacts','__pycache__','test-results','playwright-report'}
def sha(data):return hashlib.sha256(data).hexdigest()
def write(name,value):(env.OUT/name).write_text(json.dumps(value,indent=2,ensure_ascii=False)+chr(10))
def reachable(port):
    with socket.socket() as connection:connection.settimeout(1);return connection.connect_ex(('127.0.0.1',port))==0

def main():
    baseline=json.loads((env.OUT/'baseline-source-manifest.json').read_text())
    with tarfile.open(env.OUT/'pre-m4-02-source.tar.gz') as archive:
        old={r['path']:archive.extractfile(r['path']).read() for r in baseline['files']}
    for row in baseline['files']:
        if sha(old[row['path']])!=row['sha256']:raise RuntimeError('Immutable source baseline changed')
    now={}
    for parent,dirs,files in os.walk(env.ROOT):
        dirs[:]=[d for d in dirs if d not in EXCLUDED]
        for name in files:
            path=Path(parent)/name;now[path.relative_to(env.ROOT).as_posix()]=path.read_bytes()
    changes=[];patch=[]
    for name in sorted(set(old)|set(now)):
        before,after=old.get(name,b''),now.get(name,b'')
        if before==after:continue
        changes.append({'path':name,'status':'added' if name not in old else 'modified' if name in now else 'deleted',
            'before_bytes':len(before),'after_bytes':len(after),'before_sha256':sha(before),'after_sha256':sha(after)})
        patch.extend(difflib.unified_diff(before.decode().splitlines(True),after.decode().splitlines(True),
            fromfile='a/'+name if name in old else '/dev/null',tofile='b/'+name if name in now else '/dev/null'))
    (env.OUT/'full.diff').write_text(''.join(patch));write('file-manifest.json',{'source_files_before':len(old),'source_files_after':len(now),'changes':changes})
    with tempfile.TemporaryDirectory(prefix='ticketpulse-m402-diff-') as temp:
        folder=Path(temp)
        for name,data in old.items():
            path=folder/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
        result=subprocess.run(['git','apply','--unsafe-paths',str(env.OUT/'full.diff')],cwd=folder,capture_output=True,text=True)
        restored={p.relative_to(folder).as_posix():p.read_bytes() for p in folder.rglob('*') if p.is_file()}
        exact=result.returncode==0 and restored==now
        write('diff-verification.json',{'exact_source_bytes_restored':exact,'exit_code':result.returncode,'stderr':result.stderr,'source_files':len(now)})
        if not exact:raise RuntimeError('Full Diff roundtrip differs')
    previous=json.loads((env.OUT/'previous-evidence-manifest.json').read_text())['files']
    changed=[r['path'] for r in previous if not (env.ROOT/r['path']).is_file() or env.checksum(env.ROOT/r['path'])!=r['sha256']]
    protected=[name for name in old if name.startswith(('internal/analytics/','internal/search/','internal/syncpipe/rebuild','db/','web/'))]
    protocol_changed=[name for name in protected if old[name]!=now.get(name)]
    write('previous-evidence-preservation.json',{'checked':len(previous),'changed':changed,'all_prior_evidence_unchanged':not changed,
        'protected_ck_search_m4_ddl_web_files':len(protected),'protected_files_changed':protocol_changed})
    if changed or protocol_changed:raise RuntimeError('Prior evidence or protected business protocol changed')
    report=env.ROOT/'docs/m4-02-delivery.md';links=re.findall(r'[]][(]([^)]*)[)]',report.read_text())
    allowed={'../artifacts/m4-02/artifact-manifest.json','../artifacts/m4-02/closeout.json'}
    missing=[x for x in links if not x.startswith(('http:','https:','#')) and not (report.parent/x.split('#',1)[0]).exists() and x not in allowed]
    regression=json.loads((env.OUT/'regression/verified/test-summary.json').read_text())
    matrix=json.loads((env.OUT/'matrix/verified/test-summary.json').read_text())
    final_contract=json.loads((env.OUT/'matrix/final-readiness-contract/test-summary.json').read_text())
    audits=json.loads((env.OUT/'correctness/independent-export-audit.json').read_text())
    regression_commands=json.loads((env.OUT/'regression/verified/result.json').read_text())
    demo=json.loads((env.OUT/'environment/demo-preservation-baseline.json').read_text())
    stage_clean=subprocess.run(['git','diff','--cached','--quiet'],cwd=env.ROOT).returncode==0
    demo_ports={str(p):reachable(p) for p in [33306,18080,18123,19092,19200]}
    test_ports={str(p):reachable(p) for p in [33310,19204,19304,18127,19005,19094,19095,19096,19097,19098,19099,18088]}
    good=exact and not changed and not protocol_changed and not missing and regression['failed']==0 and all(r['exit_code']==0 for r in regression_commands) and matrix['failed']==0 and matrix['passed']==5 and final_contract['failed']==0 and final_contract['passed']==1 and audits['all_equal'] and demo['preserved'] and stage_clean and all(demo_ports.values()) and not any(test_ports.values())
    write('closeout.json',{'checks_passed':good,'status':'PROCESS_FAILOVER_VERIFIED' if good else 'INCOMPLETE',
        'diff_roundtrip_exact':exact,'no_staged_changes':stage_clean,'no_commit_or_push_performed':True,
        'previous_evidence_checked':len(previous),'regression_pass':regression['passed'],'regression_fail':regression['failed'],
        'matrix_test_pass':matrix['passed'],'matrix_test_fail':matrix['failed'],'final_contract_pass':final_contract['passed'],'independent_export_audit_equal':audits['all_equal'],'demo_preserved':demo['preserved'],'demo_ports':demo_ports,'test_ports':test_ports,'missing_links':missing})
    if not good:raise RuntimeError('Closeout checks incomplete')
    rows=[{'path':p.relative_to(env.OUT).as_posix(),'bytes':p.stat().st_size,'sha256':env.checksum(p)} for p in sorted(env.OUT.rglob('*')) if p.is_file() and p.name!='artifact-manifest.json']
    write('artifact-manifest.json',{'captured_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':rows})
    print(json.dumps({'source_changes':len(changes),'artifact_files':len(rows),'checks_passed':good}))

if __name__=='__main__':main()
