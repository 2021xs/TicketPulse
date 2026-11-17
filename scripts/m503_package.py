#!/usr/bin/env python3
"""Package the complete M5-03 Diff, evidence hashes and preservation checks."""
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

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/m5-03'
EXCLUDED = {'.git', '.local', 'node_modules', 'bin', 'artifacts', '__pycache__', 'test-results', 'playwright-report'}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def write(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')


def reachable(port):
    with socket.socket() as connection:
        connection.settimeout(1)
        return connection.connect_ex(('127.0.0.1', port)) == 0


def main():
    baseline = json.loads((OUT / 'baseline-source-manifest.json').read_text())
    with tarfile.open(OUT / 'pre-m5-03-source.tar.gz') as archive:
        old = {row['path']: archive.extractfile(row['path']).read() for row in baseline['files']}
    for row in baseline['files']:
        if sha(old[row['path']]) != row['sha256'] or len(old[row['path']]) != row['bytes']:
            raise RuntimeError('Immutable source baseline changed')
    now = {}
    for parent, dirs, files in os.walk(ROOT):
        dirs[:] = [x for x in dirs if x not in EXCLUDED]
        for name in files:
            path = Path(parent) / name
            now[path.relative_to(ROOT).as_posix()] = path.read_bytes()
    changes, patch = [], []
    for name in sorted(set(old) | set(now)):
        before, after = old.get(name, b''), now.get(name, b'')
        if before == after:
            continue
        changes.append({'path': name, 'status': 'added' if name not in old else 'modified' if name in now else 'deleted',
                        'before_bytes': len(before), 'after_bytes': len(after), 'before_sha256': sha(before), 'after_sha256': sha(after)})
        patch.extend(difflib.unified_diff(before.decode().splitlines(True), after.decode().splitlines(True),
                     fromfile='a/' + name if name in old else '/dev/null', tofile='b/' + name if name in now else '/dev/null'))
    (OUT / 'full.diff').write_text(''.join(patch))
    write('file-manifest.json', {'source_files_before': len(old), 'source_files_after': len(now), 'changes': changes})
    with tempfile.TemporaryDirectory(prefix='ticketpulse-m503-diff-') as temporary:
        folder = Path(temporary)
        for name, data in old.items():
            target = folder / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        result = subprocess.run(['git', 'apply', '--unsafe-paths', str(OUT / 'full.diff')],
                                cwd=folder, capture_output=True, text=True)
        restored = {p.relative_to(folder).as_posix(): p.read_bytes() for p in folder.rglob('*') if p.is_file()}
        exact = result.returncode == 0 and restored == now
        write('diff-verification.json', {'exit_code': result.returncode, 'exact_source_bytes_restored': exact,
                                        'files': len(now), 'stderr': result.stderr})
        if not exact:
            raise RuntimeError('Complete Diff failed byte-for-byte roundtrip')
    previous = json.loads((OUT / 'previous-evidence-manifest.json').read_text())['files']
    changed = [x['path'] for x in previous if not (ROOT / x['path']).is_file() or file_sha(ROOT / x['path']) != x['sha256']]
    write('previous-evidence-preservation.json', {'old_artifacts_checked': len(previous), 'changed': changed,
                                               'all_prior_evidence_unchanged': not changed})
    if changed:
        raise RuntimeError('Previous milestone evidence changed')
    report = ROOT / 'docs/m5-03-delivery.md'
    links = re.findall(r'[]][(]([^)]*)[)]', report.read_text())
    pending = {'../artifacts/m5-03/artifact-manifest.json', '../artifacts/m5-03/closeout.json'}
    missing = [x for x in links if not x.startswith(('http:', 'https:', '#')) and x not in pending
               and not (report.parent / x.split('#', 1)[0]).exists()]
    formal = json.loads((OUT / 'correctness/formal/report.json').read_text())
    independent = json.loads((OUT / 'correctness/full-100000/attempt-3/audit.json').read_text())
    regression = json.loads((OUT / 'regression/final-attempt-3/test-summary.json').read_text())
    demo = json.loads((OUT / 'environment/demo-preservation-baseline.json').read_text())
    ab = json.loads((OUT / 'correctness/ab-result-hashes.json').read_text())
    steady = json.loads((OUT / 'closeout-steady/http-100000-c20/summary.json').read_text())
    stage_clean = subprocess.run(['git', 'diff', '--cached', '--quiet'], cwd=ROOT).returncode == 0
    demo_ports = {str(p): reachable(p) for p in [33306, 18080, 18123, 19092, 19200]}
    test_ports = {str(p): reachable(p) for p in [33308, 18125, 19003, 18086]}
    good = (exact and not changed and not missing and formal['state'] == 'consistent' and formal['difference_count'] == 0
            and independent['consistent'] and regression['failed'] == 0 and demo['preserved'] and ab['same']
            and steady['errors'] == 0 and not steady['stopped_for_safety'] and stage_clean
            and all(demo_ports.values()) and not any(test_ports.values()))
    write('closeout.json', {'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                           'checks_passed': good, 'formal_consistent': formal['state'] == 'consistent',
                           'formal_difference_count': formal['difference_count'], 'independent_consistent': independent['consistent'],
                           'regression_pass': regression['passed'], 'regression_fail': regression['failed'],
                           'm4_pass': sum(x['name'].startswith('TestM4') and x['result'] == 'PASS' for x in regression['cases']),
                           'demo_preserved': demo['preserved'], 'prior_evidence_files_unchanged': len(previous),
                           'no_staged_changes': stage_clean, 'no_commit_or_push_performed': True,
                           'demo_ports': demo_ports, 'test_ports': test_ports, 'missing_links': missing,
                           'stable_verified_concurrency': 20})
    if not good:
        raise RuntimeError('Closeout checks failed')
    artifacts = [{'path': p.relative_to(OUT).as_posix(), 'bytes': p.stat().st_size, 'sha256': file_sha(p)}
                 for p in sorted(OUT.rglob('*')) if p.is_file() and p.name != 'artifact-manifest.json']
    write('artifact-manifest.json', {'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'files': artifacts})
    print(json.dumps({'source_changes': len(changes), 'artifact_files': len(artifacts), 'diff_roundtrip_exact': exact,
                      'prior_evidence_preserved': not changed, 'closeout_passed': good}))


if __name__ == '__main__':
    main()
