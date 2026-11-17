#!/usr/bin/env python3
"""Verify and package M5-02 against its immutable source/evidence baselines."""
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
OUT = ROOT / 'artifacts/m5-02'
EXCLUDED = {'.git', '.local', 'node_modules', 'bin', 'artifacts', '__pycache__'}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def write(name, value):
    (OUT / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + chr(10))


def reachable(port):
    with socket.socket() as stream:
        stream.settimeout(1)
        return stream.connect_ex(('127.0.0.1', port)) == 0


def main():
    baseline = json.loads((OUT / 'baseline-source-manifest.json').read_text())
    with tarfile.open(OUT / 'pre-m5-02-source.tar.gz') as archive:
        old = {row['path']: archive.extractfile(row['path']).read() for row in baseline['files']}
    for row in baseline['files']:
        if len(old[row['path']]) != row['bytes'] or sha(old[row['path']]) != row['sha256']:
            raise RuntimeError('Source baseline checksum mismatch: ' + row['path'])
    now = {}
    for parent, dirs, files in os.walk(ROOT):
        dirs[:] = [name for name in dirs if name not in EXCLUDED]
        for name in files:
            path = Path(parent) / name
            now[path.relative_to(ROOT).as_posix()] = path.read_bytes()
    changes, patch = [], []
    for name in sorted(set(old) | set(now)):
        before, after = old.get(name, b''), now.get(name, b'')
        if before == after:
            continue
        changes.append({'path': name, 'status': 'added' if name not in old else 'deleted' if name not in now else 'modified',
                        'before_bytes': len(before), 'after_bytes': len(after),
                        'before_sha256': sha(before), 'after_sha256': sha(after)})
        patch.extend(difflib.unified_diff(before.decode().splitlines(True), after.decode().splitlines(True),
                     fromfile='a/' + name if name in old else '/dev/null',
                     tofile='b/' + name if name in now else '/dev/null'))
    (OUT / 'full.diff').write_text(''.join(patch))
    write('file-manifest.json', {'baseline': 'pre-m5-02-source.tar.gz', 'source_files_before': len(old),
                               'source_files_after': len(now), 'changes': changes})
    with tempfile.TemporaryDirectory(prefix='ticketpulse-m502-diff-') as temp:
        path = Path(temp)
        for name, data in old.items():
            target = path / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        applied = subprocess.run(['git', 'apply', '--unsafe-paths', '--verbose', str(OUT / 'full.diff')], cwd=path, capture_output=True, text=True)
        restored = {file.relative_to(path).as_posix(): file.read_bytes() for file in path.rglob('*') if file.is_file()}
        exact = applied.returncode == 0 and restored == now
        write('diff-verification.json', {'command': ['git', 'apply', '--unsafe-paths', 'full.diff'], 'exit_code': applied.returncode,
                                       'exact_source_bytes_restored': exact, 'baseline_files': len(old), 'current_files': len(now),
                                       'stdout': applied.stdout, 'stderr': applied.stderr})
        if not exact:
            raise RuntimeError('Source Diff failed byte-for-byte reconstruction')
    previous = json.loads((OUT / 'previous-evidence-manifest.json').read_text())
    changed = [row['path'] for row in previous if not (ROOT / row['path']).is_file() or file_sha(ROOT / row['path']) != row['sha256']]
    write('previous-evidence-preservation.json', {'old_artifacts_checked': len(previous), 'changed': changed, 'all_prior_evidence_unchanged': not changed})
    if changed:
        raise RuntimeError('Prior evidence changed: ' + ', '.join(changed))
    report = ROOT / 'docs/m5-02-delivery.md'
    links = re.findall(r'[]][(]([^)]*)[)]', report.read_text())
    # artifact-manifest is generated after this check.
    missing = [target for target in links if not target.startswith(('http:', 'https:', '#'))
               and target != '../artifacts/m5-02/artifact-manifest.json'
               and not (report.parent / target.split('#', 1)[0]).exists()]
    job = json.loads((OUT / 'recovery/final-status.json').read_text())
    audit = json.loads((OUT / 'independent-final-audit.json').read_text())
    regression = json.loads((OUT / 'regression/test-summary.json').read_text())
    demo = json.loads((OUT / 'environment/demo-preservation-baseline.json').read_text())
    staged = subprocess.run(['git', 'diff', '--cached', '--quiet'], cwd=ROOT).returncode == 0
    demo_ports = {str(port): reachable(port) for port in [33306, 18080, 18123, 19092, 19200]}
    test_ports = {str(port): reachable(port) for port in [33307, 18085, 18124, 19201]}
    good = (not missing and job['status'] == 'completed' and job['report']['state'] == 'consistent'
            and job['report']['difference_count'] == 0 and audit['consistent'] and regression['failed'] == 0
            and demo['preserved'] and staged and all(demo_ports.values()) and not any(test_ports.values()))
    write('closeout.json', {'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                          'formal_status': job['status'], 'formal_state': job['report']['state'],
                          'difference_count': job['report']['difference_count'], 'independent_audit_consistent': audit['consistent'],
                          'regression_pass': regression['passed'], 'regression_fail': regression['failed'],
                          'original_m4_pass': regression['m4'], 'demo_preserved': demo['preserved'],
                          'no_staged_changes': staged, 'demo_ports': demo_ports, 'test_ports': test_ports,
                          'missing_report_links': missing, 'checks_passed': good})
    if not good:
        raise RuntimeError('Closeout evidence check failed')
    artifacts = []
    for path in sorted(OUT.rglob('*')):
        if path.is_file() and path.name != 'artifact-manifest.json':
            artifacts.append({'path': path.relative_to(OUT).as_posix(), 'bytes': path.stat().st_size, 'sha256': file_sha(path)})
    write('artifact-manifest.json', {'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'files': artifacts})
    print(json.dumps({'source_changes': len(changes), 'artifact_files': len(artifacts), 'diff_roundtrip_exact': exact,
                      'prior_evidence_preserved': not changed, 'closeout_passed': good}))


if __name__ == '__main__':
    main()
