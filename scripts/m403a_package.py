#!/usr/bin/env python3
"""Create verifiable M4-03A source/evidence package without using Git's index."""
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
OUT = ROOT / 'artifacts/m4-03a'
EXCLUDED = {'.git', '.local', 'node_modules', 'bin', 'artifacts', '__pycache__', 'test-results', 'playwright-report'}


def checksum(path):
    digest = hashlib.sha256()
    with path.open('rb') as src:
        for block in iter(lambda: src.read(1048576), b''):
            digest.update(block)
    return digest.hexdigest()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def write(name, value):
    (OUT / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + chr(10))


def latest(mode):
    folders = [path for path in (OUT / 'runs').glob('*-' + mode) if (path / 'summary.json').exists()]
    if not folders:
        raise RuntimeError('No completed ' + mode + ' run')
    path = sorted(folders)[-1]
    summary = json.loads((path / 'summary.json').read_text())
    if summary['state'] != 'PASS':
        raise RuntimeError('Latest run is not PASS')
    return path, summary


def parse_cases(path):
    log = path.read_text()
    return [{'name': name, 'result': state, 'seconds': float(seconds)} for state, name, seconds
            in re.findall(r'^--- (PASS|FAIL): ([^ ]+) \(([0-9.]+)s\)$', log, re.M)]


def timestamp(value):
    return datetime.datetime.strptime(value.replace('Z', '').split('+')[0], '%Y-%m-%dT%H:%M:%S.%f')


def summarize():
    focused, focus_summary = latest('focused')
    regression, regression_summary = latest('regression')
    static, static_summary = latest('static')
    focus_cases = parse_cases(focused / 'integration.log')
    regression_cases = parse_cases(regression / 'integration.log')
    sync_cases = parse_cases(regression / 'syncpipe-integration.log')
    if len(focus_cases) != 7 or any(row['result'] != 'PASS' for row in focus_cases + regression_cases + sync_cases):
        raise RuntimeError('Actual test case results are incomplete')
    evidence = focused / 'verification'
    coverage = json.loads((evidence / 'TestM403ALegacyGateAndFrozenBusinessCoverage/coverage.json').read_text())
    drain = json.loads((evidence / 'TestM403AFreezeDrainsActualTransactionAndConcurrentCreates/drain-race.json').read_text())
    crash = json.loads((evidence / 'TestM403AFreezeSurvivesCLIKillApplicationKillAndResume/process-crash.json').read_text())
    targets = json.loads((evidence / 'TestM403AActiveIdentityReadinessAndSameKeyCursor/target-and-cursor.json').read_text())
    prepared = json.loads((evidence / 'TestM403APreparedRequiresFreshFrozenContentAndPreservesOffsets/prepared.json').read_text())
    internal = json.loads((evidence / 'TestM403AFrozenCDCMarkersAndCheckpointsContinue/internal-progress.json').read_text())
    workers = json.loads((evidence / 'TestM403AManagedExistingWorkersResumeAndLateIdentityLoss/managed-workers.json').read_text())
    changed = timestamp(workers['identity_changed_at'])
    workers['detection_seconds'] = {key: (timestamp(at) - changed).total_seconds() for key, at in workers['identity_loss_discovered_at'].items()}
    matrix = {'A': 'legacy schema and full M1–M5 regression', 'B': 'shared/exclusive locks and real in-flight comment + twelve creates',
              'C': 'all twelve business entry/replay checks refused', 'D': 'details/list/health/readiness/login/logout remained available',
              'E': 'CDC source marker + six partition watermarks + checkpoint advanced under freeze',
              'F': 'operator and application SIGKILL preserved freeze; explicit resume',
              'G': 'real API/consumer wrong ES/CK config refused before startup', 'H': 'independent ES and CK UUID replacement refused',
              'I': 'same-key same-version second page', 'J': 'same-key separate real target index refused old cursor with HTTP409',
              'K': 'explicit resume retained tickets/history/comment and managed workers caught up', 'L': 'M1/M2/M3/M4/M5 regression passed'}
    commands = focus_summary['results'] + regression_summary['results'] + static_summary['results']
    samples = [sample for result in commands for sample in result.get('resource_samples', [])]
    summary = {'state': 'PASS', 'scope': 'M4-03A_FOUNDATIONS_ONLY', 'real_projection_switch_verified': False,
               'focused_run': focused.relative_to(ROOT).as_posix(), 'focused_cases': focus_cases,
               'regression_run': regression.relative_to(ROOT).as_posix(), 'regression_cases': regression_cases,
               'syncpipe_cases': sync_cases, 'regression_top_level_pass': len(regression_cases) + len(sync_cases),
               'static_run': static.relative_to(ROOT).as_posix(), 'acceptance_matrix': {k: {'state': 'PASS', 'observation': v} for k, v in matrix.items()},
               'commands': [{k: row[k] for k in ['command', 'exit_code', 'log', 'started_at', 'completed_at']} for row in commands],
               'drain': drain, 'crash': crash, 'managed_detection_seconds': workers['detection_seconds'],
               'frozen_checkpoint': {'before': internal['checkpoint_before'], 'after': internal['checkpoint_after']},
               'prepared_positions_unchanged': prepared['candidate_positions_before'] == prepared['candidate_positions_after'],
               'prepared_fresh_source': prepared['version']['validation_report']['source'],
               'old_report_rejected': prepared['old_report_rejected'], 'prepared_cli_sigkill_recovered': prepared['registration_survived_cli_sigkill']['phase'] == 'PREPARED',
               'uncommitted_registration_rolled_back': prepared['uncommitted_registration_rolled_back'],
               'http_old_cursor_error': targets['old_cursor_error'], 'business_refused': coverage['refused'],
               'resource_observed_max': {key: max(sample[key] for sample in samples) for key in ['test_disk_bytes', 'owned_rss_bytes']},
               'unverified': ['real ES/CK promotion', 'M4 candidate to standard Kafka group offset handoff', 'freeze under sustained high concurrency',
                              'ten/hundred-thousand target enrollment performance', 'machine/disk/network failure', 'Docker or Linux execution',
                              'automatic rollback', 'Binlog endpoint replacement after stream established', 'mixed legacy/new binaries during 011 installation'],
               'historical_m402': {'reexecuted': False, 'evidence': 'artifacts/m4-02/matrix-summary.json', 'scope': 'PROCESS_FAILOVER_VERIFIED historical only'}}
    write('test-summary.json', summary)
    return summary


def reachable(port):
    with socket.socket() as connection:
        connection.settimeout(1)
        return connection.connect_ex(('127.0.0.1', port)) == 0


def main():
    baseline = json.loads((OUT / 'baseline-source-manifest.json').read_text())
    with tarfile.open(OUT / 'pre-m4-03a-source.tar.gz') as archive:
        old = {row['path']: archive.extractfile(row['path']).read() for row in baseline['files']}
    for row in baseline['files']:
        if sha(old[row['path']]) != row['sha256']:
            raise RuntimeError('Immutable baseline checksum changed')
    current = {}
    for parent, directories, files in os.walk(ROOT):
        directories[:] = [name for name in directories if name not in EXCLUDED]
        for name in files:
            path = Path(parent) / name
            current[path.relative_to(ROOT).as_posix()] = path.read_bytes()
    changes, patch = [], []
    for name in sorted(set(old) | set(current)):
        before, after = old.get(name, b''), current.get(name, b'')
        if before == after:
            continue
        changes.append({'path': name, 'status': 'added' if name not in old else 'modified' if name in current else 'deleted',
                        'before_bytes': len(before), 'after_bytes': len(after), 'before_sha256': sha(before), 'after_sha256': sha(after)})
        patch.extend(difflib.unified_diff(before.decode().splitlines(True), after.decode().splitlines(True),
                     fromfile='a/' + name if name in old else '/dev/null', tofile='b/' + name if name in current else '/dev/null'))
    (OUT / 'full.diff').write_text(''.join(patch))
    write('file-manifest.json', {'source_files_before': len(old), 'source_files_after': len(current), 'changes': changes})
    write('final-source-manifest.json', {'at': __import__('m403a_environment').stamp(), 'branch': baseline['branch'], 'head': None,
              'files': [{'path': name, 'bytes': len(data), 'sha256': sha(data)} for name, data in sorted(current.items())]})
    with tarfile.open(OUT / 'final-source.tar.gz', 'w:gz') as archive:
        for name in sorted(current):
            archive.add(ROOT / name, arcname=name, recursive=False)
    for change in changes:
        for kind, data in [('original', old.get(change['path'])), ('final', current.get(change['path']))]:
            if data is not None:
                target = OUT / 'source' / kind / change['path']
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
    with tempfile.TemporaryDirectory(prefix='ticketpulse-m403a-diff-') as directory:
        root = Path(directory)
        for name, data in old.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        result = subprocess.run(['git', 'apply', str(OUT / 'full.diff')], cwd=root, capture_output=True, text=True)
        restored = {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob('*') if path.is_file()}
        exact = result.returncode == 0 and restored == current
        write('diff-verification.json', {'exact_source_bytes_restored': exact, 'exit_code': result.returncode, 'stderr': result.stderr, 'source_files': len(current)})
        if not exact:
            raise RuntimeError('Diff roundtrip did not restore every final source byte')
    prior = json.loads((OUT / 'previous-evidence-manifest.json').read_text())['files']
    modified = [row['path'] for row in prior if not (ROOT / row['path']).is_file() or checksum(ROOT / row['path']) != row['sha256']]
    write('previous-evidence-preservation.json', {'checked': len(prior), 'bytes_checked': sum(row['bytes'] for row in prior),
                                                  'changed': modified, 'all_prior_evidence_unchanged': not modified})
    if modified:
        raise RuntimeError('Historical evidence changed')
    tests = summarize()
    demo_files = sorted((OUT / 'preservation').glob('*/demo.json'))
    if not demo_files:
        raise RuntimeError('Demo preservation was not observed')
    demo = json.loads(demo_files[-1].read_text())
    staged = subprocess.run(['git', 'diff', '--cached', '--quiet'], cwd=ROOT).returncode == 0
    demo_ports = {str(port): reachable(port) for port in [33306, 18080, 19200, 18123, 19092]}
    private_ports = {str(port): reachable(port) for port in [33311, 19205, 19305, 18128, 19006, 19100, 19101]}
    report = ROOT / 'docs/m4-03a-delivery.md'
    links = re.findall(r'\]\(([^)]+)\)', report.read_text())
    generated = {'../artifacts/m4-03a/closeout.json', '../artifacts/m4-03a/artifact-manifest.json'}
    missing = [link for link in links if not link.startswith(('http:', 'https:', '#')) and link not in generated
               and not (report.parent / link.split('#', 1)[0]).exists()]
    passed = staged and demo['preserved'] and all(demo_ports.values()) and not any(private_ports.values()) and not missing
    write('closeout.json', {'checks_passed': passed, 'acceptance': 'M4-03A_PASS' if passed else 'INCOMPLETE',
             'diff_roundtrip_exact': exact, 'no_staged_changes': staged, 'head': None,
             'demo_preservation': demo_files[-1].relative_to(ROOT).as_posix(), 'demo_ports': demo_ports, 'private_ports': private_ports,
             'historical_evidence_unchanged': not modified, 'historical_files_checked': len(prior),
             'focused_pass': len(tests['focused_cases']), 'regression_top_level_pass': tests['regression_top_level_pass'],
             'missing_links': missing, 'real_switch_verified': False, 'offset_migration_performed': False,
             'no_commit_or_push_performed': True})
    if not passed:
        raise RuntimeError('Closeout checks incomplete')
    rows = [{'path': path.relative_to(OUT).as_posix(), 'bytes': path.stat().st_size, 'sha256': checksum(path)}
            for path in sorted(OUT.rglob('*')) if path.is_file() and path.name != 'artifact-manifest.json']
    write('artifact-manifest.json', {'files': rows})
    print(json.dumps({'source_changes': len(changes), 'artifacts': len(rows), 'prior_files_checked': len(prior), 'checks_passed': passed}))


if __name__ == '__main__':
    main()
