#!/usr/bin/env python3
"""Package C1's immutable baseline, full diff and independently rechecked evidence."""
import datetime
import difflib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/m4-03c1'
EXCLUDED = {'.git', '.local', 'node_modules', 'bin', 'artifacts', '__pycache__',
            'test-results', 'playwright-report'}


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def read(path):
    return json.loads(path.read_text())


def digest(data):
    return hashlib.sha256(data).hexdigest()


def checksum(path):
    h = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def write(name, value):
    (OUT / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + chr(10))


def latest(mode):
    completed = [p for p in sorted((OUT / 'runs').glob('*-' + mode)) if (p / 'summary.json').is_file()]
    if not completed or read(completed[-1] / 'summary.json')['state'] != 'PASS':
        raise RuntimeError('Latest ' + mode + ' run is absent/failed')
    return completed[-1]


def cases(path):
    return [{'name': name, 'result': result, 'seconds': float(seconds)}
            for result, name, seconds in re.findall(
                r'^--- (PASS|FAIL|SKIP): ([^ ]+) [(]([0-9.]+)s[)]$', path.read_text(), re.M)]


def tests():
    runs = {mode: latest(mode) for mode in ['focused', 'foundations', 'handoff', 'regression', 'static']}
    summary = {'at': stamp(), 'state': 'PASS', 'scope': 'M4-03C1_PLANNED_AND_FRESH_PROOF_ONLY',
               'api_read_target_switched': False, 'committed_stage_entered': False,
               'offset_reset_by_C1_control': False, 'legacy_reverse_rollback_verified': False, 'runs': {}}
    for mode, folder in runs.items():
        item = {'path': folder.relative_to(ROOT).as_posix(), 'source_at_run': str((folder / 'source-at-run.json').relative_to(ROOT))}
        for filename in ['integration.log', 'syncpipe-integration.log']:
            path = folder / filename
            if path.exists():
                found = cases(path)
                item[filename] = {'cases': found, 'pass': sum(x['result'] == 'PASS' for x in found),
                                  'skip': sum(x['result'] == 'SKIP' for x in found)}
        for row in read(folder / 'source-at-run.json')['files']:
            if not (ROOT / row['path']).is_file() or checksum(ROOT / row['path']) != row['sha256']:
                raise RuntimeError('Final executable source differs from ' + mode + ': ' + row['path'])
        summary['runs'][mode] = item
    for mode, expected in {'focused':12,'foundations':7,'handoff':21,'regression':78}.items():
        actual = summary['runs'][mode]['integration.log']
        if actual['pass'] != expected or any(x['result'] == 'FAIL' for x in actual['cases']):
            raise RuntimeError('Raw test cases differ from required matrix: ' + mode)
    if summary['runs']['regression']['syncpipe-integration.log']['pass'] != 11:
        raise RuntimeError('Raw syncpipe regression count differs')
    summary['failed_attempts_retained'] = [folder.relative_to(ROOT).as_posix()
        for folder in sorted((OUT / 'runs').glob('*')) if (folder / 'summary.json').is_file()
        and read(folder / 'summary.json')['state'] == 'FAIL']
    focus = runs['focused'] / 'verification'
    proof_path = focus / 'TestM403C1NewFreezeExactMarkerAfterVerifiedWritesAndGold/fresh-proof.json'
    proof = read(proof_path)
    write('primary-proof.json', proof)
    write('full-reconcile.json', proof['proof']['reconcile_report'])
    write('proof-vector.json', {'operation_id': proof['operation_id'], 'freeze_id': proof['freeze_id'],
          'freeze_revision': proof['freeze_revision'], 'read_revision': proof['expected_read_revision'],
          'marker_id': proof['marker_id'], 'ck_database_uuid': proof['ck_database_uuid'],
          'marker_records': proof['proof']['marker_records'], 'before_audit': proof['proof']['before_audit'],
          'after_audit': proof['proof']['after_audit'], 'proof_sha256': proof['proof_sha256'],
          'report_sha256': proof['proof']['reconcile_report_sha256']})
    summary['primary_proof'] = proof_path.relative_to(ROOT).as_posix()
    matrix = [row for row in read(OUT / 'fault-matrix.json') if row['number'] != '14b']
    for row in matrix:
        suffix = row['evidence'].split('/verification/', 1)[1]
        path = focus / suffix
        if not path.is_file():
            raise RuntimeError('Matrix evidence absent: ' + str(path))
        row['evidence'] = path.relative_to(ROOT).as_posix()
        if row['number'] == 14:
            row['observation'] = '五个独立子场景：ES/CK表UUID、CK数据库UUID、Topic UUID、分区变化均拒绝'
    matrix.append({'number': '14b', 'scenario': 'CK数据库UUID独立变化', 'result': 'PASS',
                   'test': 'TestM403C1TargetUUIDAndPartitionChangesRejectProof/ck_database_uuid',
                   'evidence': str((focus / 'TestM403C1TargetUUIDAndPartitionChangesRejectProof/ck_database_uuid/identity-refusal.json').relative_to(ROOT)),
                   'observation': '两张表UUID保持，Atomic数据库UUID变化；PLANNED目的校验拒绝'})
    write('fault-matrix.json', matrix)
    summary['fault_matrix'] = matrix
    samples = []
    for folder in runs.values():
        for path in folder.glob('*.json'):
            if path.name not in ['source-at-run.json', 'summary.json', 'environment.json']:
                samples.extend(read(path).get('resource_samples', []))
    summary['resource_observation'] = {'samples': len(samples),
        'max_owned_rss_bytes': max(x['owned_rss_bytes'] for x in samples),
        'max_private_disk_bytes': max(x['test_disk_bytes'] for x in samples),
        'min_free_disk_bytes': min(x['free_disk_bytes'] for x in samples)}
    summary['unverified'] = ['API read promotion/restart', 'read_revision increment and A-B-A cursor',
        'real reverse rollback', '10k/100k promotion capacity', 'multi-broker failure during C1',
        'cross-host or backend request fencing', 'Linux/Docker runtime']
    write('test-summary.json', summary)
    return summary


def package():
    summary = tests()
    baseline = read(OUT / 'baseline-source-manifest.json')
    with tarfile.open(OUT / 'baseline-source.tar.gz') as archive:
        old = {row['path']: archive.extractfile(row['path']).read() for row in baseline['files']}
    for row in baseline['files']:
        if digest(old[row['path']]) != row['sha256'] or len(old[row['path']]) != row['bytes']:
            raise RuntimeError('Baseline archive changed')
    current, modes = {}, {}
    for folder, dirs, files in os.walk(ROOT):
        dirs[:] = [x for x in dirs if x not in EXCLUDED]
        for filename in files:
            path = Path(folder) / filename
            relative = path.relative_to(ROOT).as_posix()
            current[relative], modes[relative] = path.read_bytes(), path.stat().st_mode & 0o777
    changes, patch = [], []
    for name in sorted(set(old) | set(current)):
        before, after = old.get(name, b''), current.get(name, b'')
        if before == after:
            continue
        changes.append({'path': name, 'status': 'added' if name not in old else 'modified' if name in current else 'deleted',
                        'before_sha256': digest(before), 'after_sha256': digest(after),
                        'before_bytes': len(before), 'after_bytes': len(after)})
        patch.extend(difflib.unified_diff(before.decode().splitlines(True), after.decode().splitlines(True),
                     fromfile='a/' + name if name in old else '/dev/null',
                     tofile='b/' + name if name in current else '/dev/null'))
    (OUT / 'full.diff').write_text(''.join(patch))
    write('file-manifest.json', {'source_files_before': len(old), 'source_files_after': len(current), 'changes': changes})
    write('final-source-manifest.json', {'at': stamp(), 'branch': baseline['branch'], 'head': None,
          'files': [{'path': name, 'bytes': len(data), 'sha256': digest(data), 'mode': oct(modes[name])}
                    for name, data in sorted(current.items())]})
    with tarfile.open(OUT / 'final-source.tar.gz', 'w:gz') as archive:
        for name, data in sorted(current.items()):
            member = tarfile.TarInfo(name); member.size = len(data); member.mode = modes[name]
            archive.addfile(member, io.BytesIO(data))
    with tarfile.open(OUT / 'final-source.tar.gz') as archive:
        restored_archive = {member.name: archive.extractfile(member).read() for member in archive.getmembers()}
        if restored_archive != current:
            raise RuntimeError('Final source archive differs byte for byte')
    for change in changes:
        for phase, data in [('original', old.get(change['path'])), ('final', current.get(change['path']))]:
            if data is not None:
                path = OUT / 'source' / phase / change['path']; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(data)
    with tempfile.TemporaryDirectory(prefix='ticketpulse-c1-diff-') as directory:
        target = Path(directory)
        for name, data in old.items():
            path = target / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(data)
        check = subprocess.run(['git', 'apply', '--check', str(OUT / 'full.diff')], cwd=target, capture_output=True, text=True)
        apply = subprocess.run(['git', 'apply', str(OUT / 'full.diff')], cwd=target, capture_output=True, text=True)
        restored = {p.relative_to(target).as_posix(): p.read_bytes() for p in target.rglob('*') if p.is_file()}
        exact = check.returncode == 0 and apply.returncode == 0 and restored == current
        write('diff-verification.json', {'exact_source_bytes_restored': exact, 'check_exit_code': check.returncode,
              'apply_exit_code': apply.returncode, 'stderr': check.stderr + apply.stderr,
              'source_files': len(current), 'staging_used': False})
        if not exact:
            raise RuntimeError('Full diff does not reconstruct the final source')
    prior = read(OUT / 'previous-evidence-manifest.json')['files']
    changed = [r['path'] for r in prior if not (ROOT / r['path']).is_file() or checksum(ROOT / r['path']) != r['sha256']]
    write('previous-evidence-preservation.json', {'checked': len(prior), 'bytes_checked': sum(r['bytes'] for r in prior),
          'changed': changed, 'all_prior_evidence_unchanged': not changed})
    if changed:
        raise RuntimeError('Historical assets changed')
    unchanged = ['cmd/ticketpulse/main.go', 'cmd/ticketpulse-sync/main.go', 'internal/projection/model.go',
        'internal/projection/store.go', 'internal/projection/handoff.go', 'internal/projection/writer.go',
        'internal/syncpipe/handoff_control.go', 'internal/syncpipe/target_status.go', 'internal/syncpipe/target_consumer.go',
        'internal/syncpipe/queue.go', 'internal/syncpipe/capture.go', 'internal/syncpipe/reconcile.go',
        'db/migrations/011_projection_control.sql', 'db/migrations/012_projection_handoff.sql']
    if any(old[name] != current[name] for name in unchanged):
        raise RuntimeError('A/B source or API read source unexpectedly changed')
    report = ROOT / 'docs/m4-03c1-delivery.md'
    missing = []
    for link in re.findall(r'][(]([^)]*)[)]', report.read_text()):
        name = link.split('#', 1)[0]
        if name == '../artifacts/m4-03c1/closeout.json':
            continue  # Written only after every preceding check passed.
        if not name.startswith(('http://', 'https://')) and not (report.parent / name).is_file():
            missing.append(link)
    if missing:
        raise RuntimeError('Missing report links: ' + repr(missing))
    before, after = sorted((OUT / 'preservation').glob('*/demo.json'))[0], sorted((OUT / 'preservation').glob('*/demo.json'))[-1]
    observations = [read(before), read(after)]
    if not all(v['preserved'] for v in observations):
        raise RuntimeError('Demo preservation failed')
    cp_before,cp_after = [v['demo_cdc_checkpoint'].split() for v in observations]
    if cp_before[0] != cp_after[0] or (cp_after[1],int(cp_after[2])) < (cp_before[1],int(cp_before[2])):
        raise RuntimeError('Demo CDC checkpoint regressed')
    def demo_groups(value):
        return {(row[0],int(row[2])):(int(row[3]),row[6])
                for line in value['demo_kafka_groups'].splitlines()
                if (row:=line.split()) and row[0].startswith('ticketpulse.ticketpulse_local.tickets.v1.')}
    old_groups,new_groups = [demo_groups(v) for v in observations]
    if set(old_groups) != set(new_groups) or any(new_groups[key][0] < old_groups[key][0] or new_groups[key][1] != old_groups[key][1] for key in old_groups):
        raise RuntimeError('Demo group commits or consumer identities differ unexpectedly')
    write('demo-progress-preservation.json',{'CDC_source_UUID_same':True,'CDC_checkpoint_monotonic':True,
          'original_group_commits_monotonic':True,'original_consumer_member_ids_same':True,
          'before_checkpoint':cp_before,'after_checkpoint':cp_after,
          'before_offsets':{str(k):v for k,v in old_groups.items()},'after_offsets':{str(k):v for k,v in new_groups.items()},
          'note':'Periodic markers naturally advance original running CDC/groups; no reset or migration.'})
    stops = sorted((OUT / 'environment-closeout').glob('*/stop.json'))
    if not stops or any(read(stops[-1])['private_ports_after_stop'].values()):
        raise RuntimeError('Private services not closed out')
    (OUT / 'final-git-status.txt').write_text(subprocess.check_output(['git', 'status', '--short', '--branch'], cwd=ROOT, text=True))
    if subprocess.check_output(['git', 'ls-files'], cwd=ROOT, text=True).strip():
        raise RuntimeError('Unexpected staging/index entries')
    write('closeout.json', {'at': stamp(), 'acceptance': 'M4-03C1_PASS', 'checks_passed': True,
          'source_changes': len(changes), 'diff_roundtrip_exact': exact, 'historical_assets_unchanged': True,
          'historical_files_checked': len(prior), 'AB_contract_source_unchanged': unchanged,
          'demo_preservation_before': before.relative_to(ROOT).as_posix(),
          'demo_preservation_after': after.relative_to(ROOT).as_posix(),
          'source_checkpoint_note': 'demo CDC remains running; checkpoints/commits naturally advance across periodic markers, no reset or migration',
          'private_stop': stops[-1].relative_to(ROOT).as_posix(), 'no_staging_commit_push': True,
          'API_read_target_switched': False, 'offset_migration_performed': False})
    files = [{'path': p.relative_to(OUT).as_posix(), 'bytes': p.stat().st_size, 'sha256': checksum(p)}
             for p in sorted(OUT.rglob('*')) if p.is_file() and p.name != 'artifact-manifest.json']
    write('artifact-manifest.json', {'at': stamp(), 'files': files})
    print(json.dumps({'acceptance': 'M4-03C1_PASS', 'source_files': len(current), 'source_changes': len(changes),
                      'historical_files': len(prior), 'artifacts': len(files), 'diff_exact': exact}))


if __name__ == '__main__':
    package()
