#!/usr/bin/env python3
"""Package C3 only after raw real-test, live-retention and byte checks pass."""
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
OUT = ROOT / 'artifacts/m4-03c3'
EXCLUDED = {'.git', '.local', 'node_modules', 'bin', 'artifacts', '__pycache__', 'test-results', 'playwright-report'}


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def read(path):
    return json.loads(path.read_text())


def digest(data):
    return hashlib.sha256(data).hexdigest()


def checksum(path):
    h = hashlib.sha256()
    with path.open('rb') as file:
        for block in iter(lambda: file.read(1048576), b''):
            h.update(block)
    return h.hexdigest()


def write(name, value):
    (OUT / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + chr(10))


def latest(mode):
    paths = [path for path in sorted((OUT / 'runs').glob('*-' + mode)) if (path / 'summary.json').is_file()]
    if not paths or read(paths[-1] / 'summary.json')['state'] != 'PASS':
        raise RuntimeError('Latest ' + mode + ' run is absent or failed')
    return paths[-1]


def cases(path):
    return [{'name': name, 'result': state, 'seconds': float(seconds)}
            for state, name, seconds in re.findall(r'^--- (PASS|FAIL|SKIP): ([^ ]+) [(]([0-9.]+)s[)]$', path.read_text(), re.M)]


def exact_snapshot(path):
    for row in read(path)['files']:
        if not (ROOT / row['path']).is_file() or checksum(ROOT / row['path']) != row['sha256']:
            raise RuntimeError('Final tested source differs: ' + row['path'] + ' in ' + str(path))


def progress(before, after):
    if before['pinned'] != after['pinned'] or before['propagation_processes'] != after['propagation_processes']:
        raise RuntimeError('Pinned positions or original CDC/writer processes changed')
    for label, topics in before['groups'].items():
        for topic, partitions in topics.items():
            for partition, old in partitions.items():
                new = after['groups'][label][topic][partition]
                if old['Err'] is not None or new['Err'] is not None or old['At'] < 0 or new['At'] < old['At']:
                    raise RuntimeError('Roundtrip group position regressed or became missing')
    old, new = before['cdc_checkpoint'], after['cdc_checkpoint']
    if old['server_uuid'] != new['server_uuid'] or (new['binlog_file'], new['binlog_pos']) < (old['binlog_file'], old['binlog_pos']):
        raise RuntimeError('Shared CDC checkpoint regressed')


def validate_primary(primary):
    if primary['revisions'] != [1, 2, 3] or primary['selection']['version_id'] != 'active_original' or primary['selection']['read_revision'] != 3 or primary['selection']['handoff_id'] is not None:
        raise RuntimeError('Not a real A revision1, B revision2, A revision3 roundtrip')
    op = primary['committed']
    if op['direction'] != 'ROLLBACK' or op['rollback_of'] != 'forward' or op['from_version_id'] != 'candidate_v1' or op['to_version_id'] != 'active_original' or op['stage'] != 'COMMITTED':
        raise RuntimeError('Rollback linkage differs')
    proof = op['proof']
    if proof['marker_id'] != op['marker_id'] or proof['operation_id'] != op['operation_id'] or proof['freeze_id'] != op['freeze_id'] or proof['freeze_revision'] != op['freeze_revision'] or proof['read_revision'] != 2:
        raise RuntimeError('Rollback fresh proof is not this exact operation/freeze')
    rows = []
    for component in ['es', 'ck']:
        evidence = proof['after_audit'][component]
        if evidence.get('owner') is not None or not evidence.get('legacy_process') or evidence['state_namespace'] != 'legacy' or evidence['group_id'] != proof['topic']['topic'] + '.' + component + '.v1':
            raise RuntimeError('Legacy writer was not proved independently')
        if evidence['assigned_partitions'] != proof['topic']['partitions'] or not evidence['writer_live']:
            raise RuntimeError('Current writer assignment not proved')
        positions = {p['partition']: p for p in evidence['positions']}
        for marker in proof['marker_records'][component]:
            if marker['event']['event_id'] != proof['marker_id'] or marker['event']['source'] != 'binlog':
                raise RuntimeError('A different or non-CDC marker was used')
            p = positions[marker['partition']]
            raw = evidence['raw_fetch_offsets'][proof['topic']['topic']][str(marker['partition'])]
            if raw['Err'] is not None or raw['At'] != p['committed_offset'] or not p['log_start_offset'] <= raw['At'] <= p['log_end_offset'] or raw['At'] <= marker['record_offset']:
                raise RuntimeError('Exact marker/raw offset/retained range differs')
            rows.append({'component': component, 'partition': marker['partition'], 'marker_offset': marker['record_offset'],
                         'committed_offset': raw['At'], 'log_start': p['log_start_offset'], 'log_end': p['log_end_offset']})
    if len(rows) != 6:
        raise RuntimeError('Rollback not confirmed in both groups/all three partitions')
    reports = [proof['reconcile_report']] + list(primary['A_B_before_rollback'].values()) + list(primary['A_B_after_resume'].values())
    if any(r['state'] != 'consistent' or r['difference_count'] != 0 or r['source_changed'] for r in reports):
        raise RuntimeError('Full facts do not reconcile')
    if not all(primary[key] for key in ['old_A_cursor_rejected', 'B_cursor_rejected', 'new_A_paging_pass', 'handoff_unchanged']):
        raise RuntimeError('Cursor or Handoff acceptance is missing')
    progress(primary['progress_before'], primary['progress_after'])
    return rows


def tests():
    modes = ['focused', 'forward', 'read-control', 'foundations', 'handoff', 'regression', 'static']
    runs = {mode: latest(mode) for mode in modes}
    summary = {'at': stamp(), 'state': 'PASS', 'scope': 'M4-03C3_MACOS_SAME_HOST_CONTROLLED_READ_ROLLBACK', 'runs': {}}
    for mode, folder in runs.items():
        exact_snapshot(folder / 'source-at-run.json')
        row = {'path': str(folder.relative_to(ROOT)), 'source_at_run': str((folder / 'source-at-run.json').relative_to(ROOT))}
        for filename in ['integration.log', 'syncpipe-integration.log']:
            if (folder / filename).is_file():
                found = cases(folder / filename)
                row[filename] = {'cases': found, 'pass': sum(x['result'] == 'PASS' for x in found), 'skip': sum(x['result'] == 'SKIP' for x in found)}
                if any(x['result'] == 'FAIL' for x in found):
                    raise RuntimeError('A failed case was hidden by the suite: ' + mode)
        summary['runs'][mode] = row
    for mode, count in {'focused': 10, 'forward': 14, 'read-control': 12, 'foundations': 7, 'handoff': 21, 'regression': 78}.items():
        if summary['runs'][mode]['integration.log']['pass'] != count:
            raise RuntimeError('Acceptance count differs: ' + mode)
    if summary['runs']['regression']['syncpipe-integration.log']['pass'] != 11:
        raise RuntimeError('Syncpipe regression count differs')
    static = read(runs['static'] / 'summary.json')['results']
    for result in static:
        if result['exit_code'] or result['safety_error']:
            raise RuntimeError('Static regression failed')
    python = (runs['static'] / 'python-tests.log').read_text()
    if not re.search(r'Ran 3 tests', python) or 'OK' not in python:
        raise RuntimeError('Python unittest evidence missing')
    summary['static'] = {'go_race': True, 'go_vet': True, 'python_unittest': 3}
    summary['failed_attempts_retained'] = [str(folder.relative_to(ROOT)) for folder in sorted((OUT / 'runs').glob('*'))
        if (folder / 'summary.json').is_file() and read(folder / 'summary.json')['state'] == 'FAIL']
    focus = runs['focused'] / 'verification'
    primary_path = focus / 'TestM403C3RealForwardRollbackRevisionCursorAndBothTargets/roundtrip-complete.json'
    primary = read(primary_path)
    vector = validate_primary(primary)
    write('primary-roundtrip.json', primary)
    write('fresh-rollback-proof.json', primary['committed']['proof'])
    write('legacy-raw-fetch-offsets.json', {c: primary['committed']['proof']['after_audit'][c]['raw_fetch_offsets'] for c in ['es', 'ck']})
    write('full-reconcile.json', primary['A_B_after_resume'])
    write('proof-vector.json', {'operation_id': primary['committed']['operation_id'], 'rollback_of': 'forward',
        'marker_id': primary['committed']['marker_id'], 'freeze_id': primary['committed']['freeze_id'],
        'freeze_revision': primary['committed']['freeze_revision'], 'revisions': [1, 2, 3], 'partitions': vector,
        'proof_sha256': primary['committed']['proof_sha256'], 'report_sha256': primary['committed']['proof']['reconcile_report_sha256']})
    write('source-offset-preservation.json', {'shared_CDC_and_A_workers_same': True, 'pinned_offsets_same': True,
        'groups_monotonic': True, 'CDC_checkpoint_monotonic': True, 'before': primary['progress_before'],
        'after': primary['progress_after'], 'C3_offset_initialization_or_reset': False,
        'note': 'Only named private defect fixtures deliberately alter Offset; control protocol never does.'})
    forward = runs['focused'] / 'forward' / 'TestM403C3RealForwardRollbackRevisionCursorAndBothTargets'
    write('api-drain.json', read(forward / 'drain-rollback.json'))
    # API_READY returns the operation. Full readiness/resume evidence is in the
    # actual crash recovery status and the retained live roundtrip execution.
    crash_dir = focus / 'TestM403C3RollbackSIGKILLDurableStagesAndBootRecovery'
    killed = {}
    for boundary in ['plan', 'marker', 'proof', 'read', 'api_ready', 'read_resume']:
        for timing in ['before', 'after']:
            name = timing + '_' + boundary + '_commit'
            path = crash_dir / (name + '-persistent.json')
            result = read(path)
            if not result.get('killed_process'):
                raise RuntimeError('Actual controller SIGKILL identity missing: ' + name)
            killed[name] = result
    write('sigkill-state-timeline.json', killed)
    recovered = read(crash_dir / 'crash-recovered.json')
    write('api-ready.json', recovered['final']['execution']['readiness'])
    write('api-resumed.json', recovered['final']['execution']['resumed'])
    if recovered['controller_SIGKILL_boundaries'] != 12 or recovered['final']['operation']['stage'] != 'RESUMED':
        raise RuntimeError('Controller failure recovery incomplete')
    faults = ['stopped_writer', 'missing_offset', 'offset_outside_range', 'retention_start_passed', 'topic_uuid', 'partition_set',
              'es_uuid', 'ck_uuid', 'isolation', 'missing_content', 'business_conflict', 'unknown_process', 'freeze_revision', 'read_revision']
    fault_rows = []
    for fault in faults:
        path = focus / 'TestM403C3LegacyFaultMatrixRefusesRollbackKeepsB' / fault / 'rollback-refused.json'
        value = read(path)
        if not value['error'] or value['selection']['version_id'] != 'candidate_v1' or value['selection']['read_revision'] != 2 or value['gate']['state'] != 'FROZEN':
            raise RuntimeError('Fault did not preserve B/freeze: ' + fault)
        fault_rows.append({'scenario': fault, 'result': 'REFUSED_KEEP_B_FROZEN', 'error': value['error'], 'evidence': str(path.relative_to(ROOT))})
    write('refusal-matrix.json', fault_rows)
    recovery = read(focus / 'TestM403C3CommittedRollbackExplicitReverseRecovery/explicit-committed-recovery.json')
    if recovery['revisions'] != [1, 2, 3, 4] or recovery['automatic_reversion'] or not recovery['cancelled_recovery_retry_kept_predecessor_frozen']:
        raise RuntimeError('Explicit committed recovery not proved')
    replay = read(focus / 'TestM403C3ActualKafkaReplayPreservesGoldAndHistory/actual-replay-idempotence.json')
    if replay['replayed_ticket_id'] != 4 or len(replay['resolution_versions']) != 2 or len(replay['published_replay_records']) != 4:
        raise RuntimeError('Actual same-ticket first/repeated resolution replay missing')
    write('replay-idempotence.json', replay)
    mapping = [
        ('1', 'PLANNED before/after SIGKILL', crash_dir / 'after_plan_commit-persistent.json'),
        ('2', 'Exact marker before/after SIGKILL', crash_dir / 'after_marker_commit-persistent.json'),
        ('3', 'Fresh proof before/after SIGKILL', crash_dir / 'after_proof_commit-persistent.json'),
        ('4', 'B API in-flight SIGKILL with exit observation', focus / 'TestM403C3BInFlightReadSIGKILLDrainThenRollback/inflight-SIGKILL-drained.json'),
        ('5', 'Live B API refuses commit', primary_path),
        ('6', 'Atomic B-to-A before/after SIGKILL', crash_dir / 'after_read_commit-persistent.json'),
        ('7', 'A startup fails; explicit reverse recovery', focus / 'TestM403C3CommittedRollbackExplicitReverseRecovery/explicit-committed-recovery.json'),
        ('8', 'A dies after API_READY; Resume stays frozen', crash_dir / 'crash-recovered.json'),
        ('9', 'New A boot re-observed', crash_dir / 'crash-recovered.json'),
        ('10', 'Resume before/after SIGKILL', crash_dir / 'after_read_resume_commit-persistent.json'),
        ('11', 'Repeated rollback intent/commit keeps one revision', primary_path),
        ('12', 'Concurrent promote/rollback intents and corrupt audit', focus / 'TestM403C3ConcurrentReverseIntentsAndAuditTampering/concurrent-and-tamper-refused.json'),
        ('13', 'Legacy raw committed Offset actually removed', focus / 'TestM403C3LegacyFaultMatrixRefusesRollbackKeepsB/missing_offset/raw-missing-offset.json'),
        ('14', 'A missing content/canonical conflicting state', focus / 'TestM403C3LegacyFaultMatrixRefusesRollbackKeepsB/business_conflict/rollback-refused.json'),
        ('15', 'Failed A proof, cancel plus real healthy B Resume', focus / 'TestM403C3CancelledRollbackRequiresHealthyCurrentB/cancelled-safe-resume.json'),
        ('16', 'Post-rollback HTTP writes and both groups catch up', primary_path)]
    matrix = []
    for requirement, scenario, evidence in mapping:
        if not evidence.is_file():
            raise RuntimeError('Fault matrix evidence absent: ' + scenario)
        matrix.append({'requirement': requirement, 'scenario': scenario, 'result': 'PASS', 'evidence': str(evidence.relative_to(ROOT))})
    write('fault-matrix.json', matrix)
    samples = []
    for folder in runs.values():
        for path in folder.glob('*.json'):
            if path.name not in ['summary.json', 'source-at-run.json', 'environment.json']:
                samples.extend(read(path).get('resource_samples', []))
    summary.update(controller_sigkill_boundaries=12, legacy_refusal_scenarios=len(faults), fault_matrix=matrix,
        actual_roundtrip=True, revisions=[1, 2, 3], primary_roundtrip=str(primary_path.relative_to(ROOT)),
        explicit_committed_recovery=True, offset_reset_by_control=False, automatic_downgrade=False,
        resource_observation={'samples': len(samples), 'max_owned_rss_bytes': max(x['owned_rss_bytes'] for x in samples),
                             'max_private_disk_bytes': max(x['test_disk_bytes'] for x in samples),
                             'min_free_disk_bytes': min(x['free_disk_bytes'] for x in samples)},
        unverified=['Cross-host or arbitrary launchers/writers', 'ES/CK request fencing', '10k/100k rollback capacity',
                    'Linux runtime', 'A/B retirement', 'New multi-broker fault matrix'])
    write('test-summary.json', summary)
    return summary


def package():
    summary = tests()
    marker = read(ROOT / '.local/m4-03c3/retained-live.json')
    live = ROOT / marker['evidence']
    observations = sorted(live.glob('observation-*.json'))
    if not observations or read(observations[-1])['state'] != 'RETAINED_LIVE_VERIFIED':
        raise RuntimeError('Retained live environment was not independently verified')
    exact_snapshot(live / 'source-at-run.json')
    retained = read(live / 'verification/TestM403C3RetainedPrivateRoundtrip/retained-live.json')
    validate_primary(read(live / 'verification/TestM403C3RetainedPrivateRoundtrip/roundtrip-complete.json'))
    if not read(observations[-1])['all_four_groups_retained'] or not read(observations[-1])['all_services_running']:
        raise RuntimeError('Live projections/groups were not retained')
    write('retained-live-summary.json', {'observation': str(observations[-1].relative_to(ROOT)), 'identity': retained,
        'not_counted_as_completed_test': True, 'evidence_is_independent_live_observation': True})
    write('live-api-ready.json', retained['status']['execution']['readiness'])
    write('live-api-resumed.json', retained['status']['execution']['resumed'])
    baseline = read(OUT / 'baseline-source-manifest.json')
    with tarfile.open(OUT / 'baseline-source.tar.gz') as archive:
        old = {row['path']: archive.extractfile(row['path']).read() for row in baseline['files']}
    for row in baseline['files']:
        if digest(old[row['path']]) != row['sha256'] or len(old[row['path']]) != row['bytes']:
            raise RuntimeError('Baseline archive differs')
    current, modes = {}, {}
    for folder, dirs, files in os.walk(ROOT):
        dirs[:] = [name for name in dirs if name not in EXCLUDED]
        for name in files:
            path = Path(folder) / name
            relative = path.relative_to(ROOT).as_posix()
            current[relative], modes[relative] = path.read_bytes(), path.stat().st_mode & 0o777
    changes, patch = [], []
    for name in sorted(set(old) | set(current)):
        before, after = old.get(name, b''), current.get(name, b'')
        if before == after:
            continue
        changes.append({'path': name, 'status': 'added' if name not in old else 'modified' if name in current else 'deleted',
            'before_sha256': digest(before), 'after_sha256': digest(after), 'before_bytes': len(before), 'after_bytes': len(after)})
        patch.extend(difflib.unified_diff(before.decode().splitlines(True), after.decode().splitlines(True),
            fromfile='a/' + name if name in old else '/dev/null', tofile='b/' + name if name in current else '/dev/null'))
    (OUT / 'full.diff').write_text(''.join(patch))
    write('file-manifest.json', {'source_files_before': len(old), 'source_files_after': len(current), 'changes': changes})
    write('final-source-manifest.json', {'at': stamp(), 'branch': baseline['branch'], 'head': None,
        'files': [{'path': name, 'bytes': len(data), 'sha256': digest(data), 'mode': oct(modes[name])} for name, data in sorted(current.items())]})
    facts = []
    for name in sorted(current):
        if name.startswith(('internal/projection/read', 'internal/syncpipe/read', 'internal/readruntime/', 'cmd/ticketpulse', 'internal/search/', 'internal/analytics/')) and name.endswith('.go'):
            for line, text in enumerate(current[name].decode().splitlines(), 1):
                if text.startswith(('func ', 'type ', 'const (')):
                    facts.append({'file': name, 'line': line, 'symbol': text, 'source_sha256': digest(current[name])})
    write('final-source-facts.json', facts)
    with tarfile.open(OUT / 'final-source.tar.gz', 'w:gz') as archive:
        for name, data in sorted(current.items()):
            info = tarfile.TarInfo(name); info.size, info.mode = len(data), modes[name]
            archive.addfile(info, io.BytesIO(data))
    with tarfile.open(OUT / 'final-source.tar.gz') as archive:
        if {item.name: archive.extractfile(item).read() for item in archive.getmembers()} != current:
            raise RuntimeError('Final archive byte mismatch')
    for change in changes:
        for phase, data in [('original', old.get(change['path'])), ('final', current.get(change['path']))]:
            if data is not None:
                path = OUT / 'source' / phase / change['path']; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(data)
    with tempfile.TemporaryDirectory(prefix='ticketpulse-c3-diff-') as directory:
        target = Path(directory)
        for name, data in old.items():
            path = target / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(data)
        check = subprocess.run(['git', 'apply', '--check', str(OUT / 'full.diff')], cwd=target, capture_output=True, text=True)
        apply = subprocess.run(['git', 'apply', str(OUT / 'full.diff')], cwd=target, capture_output=True, text=True)
        restored = {path.relative_to(target).as_posix(): path.read_bytes() for path in target.rglob('*') if path.is_file()}
        exact = check.returncode == 0 and apply.returncode == 0 and restored == current
        write('diff-verification.json', {'exact_source_bytes_restored': exact, 'check_exit_code': check.returncode,
            'apply_exit_code': apply.returncode, 'stderr': check.stderr + apply.stderr, 'source_files': len(current), 'staging_used': False})
        if not exact:
            raise RuntimeError('Full diff cannot reconstruct exact final bytes')
    prior = read(OUT / 'previous-evidence-manifest.json')['files']
    changed = [row['path'] for row in prior if not (ROOT / row['path']).is_file() or checksum(ROOT / row['path']) != row['sha256']]
    write('previous-evidence-preservation.json', {'checked': len(prior), 'bytes_checked': sum(row['bytes'] for row in prior),
        'changed': changed, 'all_prior_evidence_unchanged': not changed})
    if changed:
        raise RuntimeError('Historical evidence changed')
    unchanged = ['cmd/ticketpulse-sync/main.go', 'cmd/ticketpulse/main.go',
        'internal/projection/model.go', 'internal/projection/store.go', 'internal/projection/handoff.go', 'internal/projection/writer.go',
        'internal/syncpipe/handoff_control.go', 'internal/syncpipe/target_consumer.go', 'internal/syncpipe/target_status.go',
        'internal/syncpipe/capture.go', 'internal/syncpipe/reconcile.go', 'internal/syncpipe/rebuild.go',
        'internal/syncpipe/rebuild_tail.go', 'internal/syncpipe/rebuild_check.go', 'internal/syncpipe/read_api_selection.go',
        'internal/mysqlstore/actions.go', 'internal/mysqlstore/search.go', 'internal/search/service.go',
        'internal/analytics/service.go', 'internal/analytics/clickhouse.go',
        'db/migrations/011_projection_control.sql', 'db/migrations/012_projection_handoff.sql',
        'db/migrations/013_projection_read_control.sql', 'db/migrations/014_projection_read_runtime.sql']
    for name in unchanged:
        if name not in old or name not in current or old[name] != current[name]:
            raise RuntimeError('Frozen business/ETL/Contract source changed: ' + name)
    write('frozen-contract-preservation.json', {'all_source_bytes_unchanged': True, 'files': unchanged})
    before, after = sorted((OUT / 'preservation').glob('*/demo.json'))[0], sorted((OUT / 'preservation').glob('*/demo.json'))[-1]
    demo = [read(before), read(after)]
    if not all(row['preserved'] for row in demo) or demo[-1]['demo_migration_015_tables'] or demo[-1]['demo_migration_015_columns']:
        raise RuntimeError('Original demo differs or C3 migration installed there')
    old_cp, new_cp = [row['demo_cdc_checkpoint'].split() for row in demo]
    if old_cp[0] != new_cp[0] or (new_cp[1], int(new_cp[2])) < (old_cp[1], int(old_cp[2])):
        raise RuntimeError('Original demo checkpoint regressed')
    def demo_groups(value):
        return {(row[0], int(row[2])): (int(row[3]), row[6]) for line in value['demo_kafka_groups'].splitlines()
            if (row := line.split()) and row[0].startswith('ticketpulse.ticketpulse_local.tickets.v1.')}
    old_groups, new_groups = [demo_groups(row) for row in demo]
    if set(old_groups) != set(new_groups) or any(new_groups[key][0] < old_groups[key][0] or new_groups[key][1] != old_groups[key][1] for key in old_groups):
        raise RuntimeError('Original demo positions or members changed incorrectly')
    write('demo-progress-preservation.json', {'source_UUID_same': True, 'checkpoint_monotonic': True, 'group_commits_monotonic': True,
        'original_consumer_members_same': True, 'before_checkpoint': old_cp, 'after_checkpoint': new_cp,
        'before_offsets': {str(k): v for k, v in old_groups.items()}, 'after_offsets': {str(k): v for k, v in new_groups.items()},
        'note': 'Original periodic markers naturally advance progress; no reset/migration.'})
    processes = read(OUT / 'demo-process-preservation.json')
    if not processes['all_same']:
        raise RuntimeError('Original demo process identities changed')
    report = ROOT / 'docs/m4-03c3-delivery.md'
    missing = []
    for link in re.findall(r'][(]([^)]*)[)]', report.read_text()):
        name = link.split('#', 1)[0]
        if name in ['../artifacts/m4-03c3/closeout.json', '../artifacts/m4-03c3/artifact-manifest.json']:
            continue
        if name and not name.startswith(('http://', 'https://')) and not (report.parent / name).is_file():
            missing.append(link)
    if missing:
        raise RuntimeError('Delivery evidence links absent: ' + repr(missing))
    (OUT / 'final-git-status.txt').write_text(subprocess.check_output(['git', 'status', '--short', '--branch'], cwd=ROOT, text=True))
    if subprocess.check_output(['git', 'ls-files'], cwd=ROOT, text=True).strip():
        raise RuntimeError('Unexpected index/staging')
    write('closeout.json', {'at': stamp(), 'acceptance': 'M4-03C3_PASS', 'checks_passed': True, 'source_changes': len(changes),
        'diff_roundtrip_exact': exact, 'historical_assets_unchanged': True, 'historical_files_checked': len(prior),
        'AB_contract_source_unchanged': unchanged, 'original_demo_preserved': True, 'original_processes_preserved': True,
        'actual_B_to_A': True, 'fresh_legacy_proof': True, 'revisions': [1, 2, 3], 'automatic_downgrade': False,
        'offset_reset_by_control': False, 'retained_live_observation': str(observations[-1].relative_to(ROOT)),
        'private_A_B_projections_groups_and_CDC_retained': True, 'no_staging_commit_push': True,
        'scope': 'macOS same-host cooperative controlled processes, not cross-host HA or request fencing'})
    files = [{'path': str(path.relative_to(OUT)), 'bytes': path.stat().st_size, 'sha256': checksum(path)}
        for path in sorted(OUT.rglob('*')) if path.is_file() and path.name != 'artifact-manifest.json']
    write('artifact-manifest.json', {'at': stamp(), 'files': files})
    print(json.dumps({'acceptance': 'M4-03C3_PASS', 'source_files': len(current), 'source_changes': len(changes),
        'historical_files': len(prior), 'artifacts': len(files), 'diff_exact': exact, 'retained_environment': marker['evidence']}))


if __name__ == '__main__':
    package()
