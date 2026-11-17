#!/usr/bin/env python3
"""Package C2's immutable baseline, full diff and independently rechecked evidence."""
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
OUT = ROOT / 'artifacts/m4-03c2'
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
    modes = ['focused', 'read-control', 'foundations', 'handoff', 'regression', 'static']
    runs = {mode: latest(mode) for mode in modes}
    summary = {'at': stamp(), 'state': 'PASS', 'scope': 'M4-03C2_SAME_HOST_CONTROLLED_FORWARD_READ_PROMOTION',
               'api_read_target_switched': True, 'committed_stage_entered': True,
               'offset_reset_by_C2_control': False, 'formal_reverse_rollback_verified': False, 'runs': {}}
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
    for mode, expected in {'focused':14, 'read-control':12, 'foundations':7, 'handoff':21, 'regression':78}.items():
        actual = summary['runs'][mode]['integration.log']
        if actual['pass'] != expected or any(x['result'] == 'FAIL' for x in actual['cases']):
            raise RuntimeError('Raw matrix differs from requirement: ' + mode + ' ' + repr(actual))
    if summary['runs']['regression']['syncpipe-integration.log']['pass'] != 11:
        raise RuntimeError('Syncpipe regression count differs')
    summary['failed_attempts_retained'] = [folder.relative_to(ROOT).as_posix()
        for folder in sorted((OUT / 'runs').glob('*')) if (folder / 'summary.json').is_file()
        and read(folder / 'summary.json')['state'] == 'FAIL']
    focus = runs['focused'] / 'verification'
    path = focus / 'TestM403C2RealForwardSwitchNewBusinessAndIndependentTargets/forward-complete.json'
    primary = read(path)
    write('primary-switch.json', primary)
    write('fresh-proof.json', primary['committed']['proof'])
    write('full-reconcile.json', primary['A_B_reconcile'])
    execution = primary['final']['execution']
    write('api-drain.json', execution['drained'])
    write('api-ready.json', execution['readiness'])
    write('api-resumed.json', execution['resumed'])
    proof = primary['committed']['proof']
    write('proof-vector.json', {'operation_id': primary['committed']['operation_id'],
        'freeze_id': proof['freeze_id'], 'freeze_revision': proof['freeze_revision'],
        'read_revision_from': proof['read_revision'], 'read_revision_to': primary['final']['selection']['read_revision'],
        'marker_id': proof['marker_id'], 'marker_records': proof['marker_records'],
        'before_audit': proof['before_audit'], 'after_audit': proof['after_audit'],
        'proof_sha256': primary['committed']['proof_sha256'], 'report_sha256': proof['reconcile_report_sha256']})
    before, after = primary['progress_before'], primary['progress_after']
    if before['pinned'] != after['pinned'] or before['propagation_processes'] != after['propagation_processes']:
        raise RuntimeError('Pinned offsets or shared propagation processes changed')
    for group, topics in before['groups'].items():
        for topic, partitions in topics.items():
            for part, old in partitions.items():
                new = after['groups'][group][topic][part]
                if old['Err'] is not None or new['Err'] is not None or new['At'] < old['At']:
                    raise RuntimeError('Primary Kafka group position regressed')
    def position(cp):
        value=cp
        return value['server_uuid'], value['binlog_file'], value['binlog_pos']
    cp_before,cp_after = position(before['cdc_checkpoint']),position(after['cdc_checkpoint'])
    if cp_before[0] != cp_after[0] or cp_after[1:] < cp_before[1:]:
        raise RuntimeError('Primary CDC checkpoint regressed')
    write('source-offset-preservation.json', {'shared_CDC_and_A_workers_same':True, 'pinned_offsets_same':True,
        'groups_monotonic':True, 'CDC_checkpoint_monotonic':True, 'before':before, 'after':after,
        'C2_offset_initialization_or_reset':False})
    matrix = read(OUT / 'fault-matrix.json')
    for row in matrix:
        if '/verification/' in row['evidence']:
            suffix = row['evidence'].split('/verification/', 1)[1]
            evidence = focus / suffix
        else:
            evidence = ROOT / row['evidence']
        if not evidence.is_file(): raise RuntimeError('Matrix evidence missing: ' + str(evidence))
        row['evidence'] = evidence.relative_to(ROOT).as_posix()
    write('fault-matrix.json',matrix)
    summary['fault_matrix'] = matrix
    samples=[]
    for folder in runs.values():
        for path in folder.glob('*.json'):
            if path.name not in ['source-at-run.json','summary.json','environment.json']:
                samples.extend(read(path).get('resource_samples',[]))
    summary['resource_observation']={'samples':len(samples),'max_owned_rss_bytes':max(x['owned_rss_bytes'] for x in samples),
        'max_private_disk_bytes':max(x['test_disk_bytes'] for x in samples),'min_free_disk_bytes':min(x['free_disk_bytes'] for x in samples)}
    summary['unverified']=['Formal B-to-A rollback and old-projection lifecycle','Cross-host or arbitrary launchers',
        'ES/CK request fencing','10k/100k promotion capacity','New multi-broker fault matrix','Linux/Docker runtime']
    summary['primary_switch']=str((focus / 'TestM403C2RealForwardSwitchNewBusinessAndIndependentTargets/forward-complete.json').relative_to(ROOT))
    write('test-summary.json',summary)
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
    with tempfile.TemporaryDirectory(prefix='ticketpulse-c2-diff-') as directory:
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
    unchanged = ['cmd/ticketpulse-sync/main.go', 'internal/projection/model.go',
        'internal/projection/store.go', 'internal/projection/handoff.go', 'internal/projection/writer.go',
        'internal/syncpipe/handoff_control.go', 'internal/syncpipe/target_status.go', 'internal/syncpipe/target_consumer.go',
        'internal/syncpipe/queue.go', 'internal/syncpipe/capture.go', 'internal/syncpipe/reconcile.go',
        'db/migrations/011_projection_control.sql', 'db/migrations/012_projection_handoff.sql', 'db/migrations/013_projection_read_control.sql']
    if any(old[name] != current[name] for name in unchanged):
        raise RuntimeError('A/B source or API read source unexpectedly changed')
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
    if not stops or any(read(stops[-1])['private_ports_after_stop'].values()) or not all(v['actual_exit'] for v in read(stops[-1])['stopped']):
        raise RuntimeError('Private services not closed out')
    applications = read(OUT / 'private-application-closeout.json')
    if applications['private_API_or_worker_processes'] or applications['private_restart_entries_loaded'] or not applications['all_checked_private_API_workers_exited']:
        raise RuntimeError('Private application process/restart entry remains')
    processes = read(OUT / 'demo-processes-after-private-stop.json')
    if not processes['all_same_after_private_stop'] or not all(v['same'] for v in processes['processes']):
        raise RuntimeError('Original process identity changed across private closeout')
    # Verify links after generating the preservation summaries they reference.
    report = ROOT / 'docs/m4-03c2-delivery.md'
    missing = []
    for link in re.findall(r'][(]([^)]*)[)]', report.read_text()):
        name = link.split('#', 1)[0]
        if name == '../artifacts/m4-03c2/closeout.json':
            continue  # Written only after every preceding check passed.
        if not name.startswith(('http://', 'https://')) and not (report.parent / name).is_file():
            missing.append(link)
    if missing:
        raise RuntimeError('Missing report links: ' + repr(missing))
    (OUT / 'final-git-status.txt').write_text(subprocess.check_output(['git', 'status', '--short', '--branch'], cwd=ROOT, text=True))
    if subprocess.check_output(['git', 'ls-files'], cwd=ROOT, text=True).strip():
        raise RuntimeError('Unexpected staging/index entries')
    write('closeout.json', {'at': stamp(), 'acceptance': 'M4-03C2_PASS', 'checks_passed': True,
          'source_changes': len(changes), 'diff_roundtrip_exact': exact, 'historical_assets_unchanged': True,
          'historical_files_checked': len(prior), 'AB_contract_source_unchanged': unchanged,
          'demo_preservation_before': before.relative_to(ROOT).as_posix(),
          'demo_preservation_after': after.relative_to(ROOT).as_posix(),
          'demo_process_identity_preserved': True, 'private_application_processes_exited': True,
          'source_checkpoint_note': 'demo CDC remains running; checkpoints/commits naturally advance across periodic markers, no reset or migration',
          'private_stop': stops[-1].relative_to(ROOT).as_posix(), 'no_staging_commit_push': True,
          'API_read_target_switched': True, 'offset_migration_performed': False})
    files = [{'path': p.relative_to(OUT).as_posix(), 'bytes': p.stat().st_size, 'sha256': checksum(p)}
             for p in sorted(OUT.rglob('*')) if p.is_file() and p.name != 'artifact-manifest.json']
    write('artifact-manifest.json', {'at': stamp(), 'files': files})
    print(json.dumps({'acceptance': 'M4-03C2_PASS', 'source_files': len(current), 'source_changes': len(changes),
                      'historical_files': len(prior), 'artifacts': len(files), 'diff_exact': exact}))


if __name__ == '__main__':
    package()
