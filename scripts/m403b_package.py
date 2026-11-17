#!/usr/bin/env python3
"""Package M4-03B source and actual evidence without staging or changing history."""
import argparse
import datetime
import difflib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/m4-03b'
EXCLUDED = {'.git', '.local', 'node_modules', 'bin', 'artifacts', '__pycache__',
            'test-results', 'playwright-report'}


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


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


def read(path):
    return json.loads(path.read_text())


def latest(mode, full=False):
    choices = []
    for folder in sorted((OUT / 'runs').glob('*-' + mode)):
        if not (folder / 'summary.json').exists():
            continue
        summary = read(folder / 'summary.json')
        if full:
            command = read(folder / 'integration.json').get('command', [])
            if '-run=^TestM403B' not in command:
                continue
        choices.append((folder, summary))
    if not choices or choices[-1][1]['state'] != 'PASS':
        raise RuntimeError('Latest completed ' + mode + ' check is absent or failed')
    return choices[-1]


def cases(path):
    return [{'name': name, 'result': result, 'seconds': float(seconds)}
            for result, name, seconds in re.findall(
                r'^--- (PASS|FAIL|SKIP): ([^ ]+) \(([0-9.]+)s\)$', path.read_text(), re.M)]


# The numbers follow the user's eighteen required scenarios; all observations
# point to real private-service tests, including deliberate failed operations.
MATRIX = [
    (1, '三分区不同起点', 'OfflineNewGroupsExactPositionsAndGold',
     '两组逐分区原始读回等于 pinned 34/28/22；不加一'),
    (2, '空分区、起点等于 log_end', 'EmptyPartitionsOfflineCommitAtLogEnd',
     '真实空组三分区提交 0，AtCommitted 等待后读取第一条 offset=0'),
    (3, 'M4 写入成功、位置保存失败', 'M4ProjectionBeforeNextOffsetFailure',
     'MySQL trigger 拒绝 next_offset 保存；CK 已增加物理行，重放后 Gold 和全量对账一致'),
    (4, '封存前后 SIGKILL', 'SealAndInitialCommitSIGKILL',
     '真实子进程分别在 PINNED 事务前后被 SIGKILL；读取 DRAINING/PINNED 持久状态后恢复'),
    (5, 'Kafka 初始化部分成功', 'PartialInitializationUnknownResponseAndResume',
     'Broker 实际只保存第一个分区；重试读回该分区并补齐相同 pinned 向量'),
    (6, 'CommitOffsets 超时或响应未知', 'RealKafkaCommitResponseTimeout',
     '私有 socket 将实际 Broker 响应读出后截留至 i/o timeout；独立正常客户端确认 offset 已保存'),
    (7, '初始化后、消费前 SIGKILL', 'SealAndInitialCommitSIGKILL',
     'OFFSETS_READY 持久提交后 operator 子进程被 SIGKILL，位置保留；随后按原任务开放消费'),
    (8, '消费开始后 SIGKILL', 'StandardProjectionProgressFailureAndSIGKILLRestart',
     '真正 ticketpulse-sync 进程被 SIGKILL；新进程从当前 Kafka 提交位置继续'),
    (9, 'ES/CK 成功、进度保存失败', 'StandardProjectionProgressFailureAndSIGKILLRestart',
     '两个组件分别注入 terminal delivery SQL 失败；Offset 不越过失败记录，恢复仍完整一致'),
    (10, 'Kafka Offset Commit 失败', 'OffsetCommitFailureAndGroupRebalance',
     '真实 coordinator 移除 member 后 CommitRecords 得到 UNKNOWN_MEMBER_ID；进度不推进并重放'),
    (11, 'Consumer Group 重平衡', 'NormalConsumerRebalancePreservesPositions',
     '只读 Broker peer 加入/离开，记录实际 assigned/revoked；位置连续，无第二个投影 writer'),
    (12, '重复消费者进程启动', 'DuplicateCandidateProcessRejectedAndLockLoss',
     '真实重复进程准入失败；SIGSTOP 旧 PID 后断开锁连接，仍拒绝接管，旧进程死亡后 epoch 才增长'),
    (13, 'Topic UUID/分区集合变化', 'TopicIdentityAndPartitionRefusal',
     '分别增加第四分区、同名删除重建 Topic；目标检查拒绝两种独立故障'),
    (14, '已提交位置缺失或越界', 'CommittedOffsetMissingRangeAndActualTargetMismatch',
     'DeleteOffsets 和大于 log_end 的真实提交被拒绝；franz-go AtCommitted 明确报无历史提交'),
    (15, '旧 ACTIVE/候选状态隔离', 'VersionRecoveryNamespaceAndOldState',
     '相同 Topic/Partition/Offset 的候选恢复不改旧投递行；ES lease 不能写 CK 或另一版本状态'),
    (16, 'VERIFIED 前错误 Resume', 'VersionStateIsolationAndFailureRefusesVerify',
     '候选持久隔离阻止 VERIFY 和 Resume；旧 ACTIVE freshness 仍读取自己的表'),
    (17, 'VERIFIED 后显式恢复业务', 'OfflineNewGroupsExactPositionsAndGold',
     '真实 HTTP 评论提交新版本，旧 ACTIVE 与新候选 ES/CK 同步到 v11，候选再次全量一致'),
    (18, 'PRD Gold 与完整对账', 'OfflineNewGroupsExactPositionsAndGold',
     '十工单/46 历史，新增10、首次解决6、平均60600秒、积压5；两次新完整报告 consistent'),
]


def summarize():
    focused, focus = latest('focused', full=True)
    foundations, foundation = latest('foundations')
    regression, regress = latest('regression')
    static, stat = latest('static')
    focus_cases = cases(focused / 'integration.log')
    foundation_cases = cases(foundations / 'integration.log')
    regression_cases = cases(regression / 'integration.log')
    sync_cases = cases(regression / 'syncpipe-integration.log')
    passed = {row['name'] for row in focus_cases if row['result'] == 'PASS'}
    all_cases = focus_cases + foundation_cases + regression_cases + sync_cases
    if len(passed) != 21 or len(foundation_cases) != 7 or len(regression_cases) != 78 or len(sync_cases) != 11:
        raise RuntimeError('Final full test population is incomplete')
    if any(row['result'] == 'FAIL' for row in all_cases):
        raise RuntimeError('Actual test contains a failing case')
    if [row['name'] for row in focus_cases if row['result'] == 'SKIP'] != ['TestM403BHandoffCrashChild']:
        raise RuntimeError('Unexpectedly skipped acceptance case')
    evidence = focused / 'verification'
    matrix = []
    for number, scenario, short, observation in MATRIX:
        name = 'TestM403B' + short
        if name not in passed:
            raise RuntimeError('Missing fault scenario: ' + scenario)
        directory = evidence / name
        assets = [path.relative_to(ROOT).as_posix() for path in sorted(directory.rglob('*')) if path.is_file()]
        if not assets:
            raise RuntimeError('Missing runtime proof for ' + name)
        matrix.append({'scenario': number, 'required': scenario, 'state': 'PASS',
                       'evidence_level': 'RUNTIME', 'test': name, 'observation': observation, 'assets': assets})
    for name in ['TestM403BCKIdentityAndRetainedOffsetsRefuseRecovery',
                 'TestM403BOperatorCLIFullHandoffAndCandidateStatus']:
        if name not in passed:
            raise RuntimeError('Additional closeout proof absent: ' + name)
    commands = focus['results'] + foundation['results'] + regress['results'] + stat['results']
    final_run_source = {}
    for folder in [focused, foundations, regression, static]:
        snapshot = read(folder / 'source-at-run.json')
        changed = [row['path'] for row in snapshot['files']
                   if not (ROOT / row['path']).is_file() or checksum(ROOT / row['path']) != row['sha256']]
        if changed:
            raise RuntimeError('Test run differs from final executable source: ' + str(changed))
        final_run_source[folder.relative_to(ROOT).as_posix()] = len(snapshot['files'])
    for command in commands:
        if command['exit_code'] != 0 or command.get('safety_error'):
            raise RuntimeError('A required command failed')
    samples = [sample for command in commands for sample in command.get('resource_samples', [])]
    main = evidence / 'TestM403BOfflineNewGroupsExactPositionsAndGold'
    handoff, gold, status = read(main / 'handoff.json'), read(main / 'gold.json'), read(main / 'target-status.json')
    reports = {'final': handoff['final_report'], 'verified': handoff['verification_report'],
               'after_resume': read(main / 'after-resume.json')}
    for report in reports.values():
        if report['state'] != 'consistent' or report['difference_count'] != 0 or report['source_changed']:
            raise RuntimeError('Complete content reconciliation is not consistent')
    timeout = read(evidence / 'TestM403BRealKafkaCommitResponseTimeout/real-timeout-before-retry.json')
    partition_errors = read(evidence / 'TestM403BKafkaInitialPartitionErrorsAndGroupNotEmpty/real-partition-errors.json')
    failed_attempts = []
    for path in sorted((OUT / 'runs').glob('*/summary.json')):
        value = read(path)
        if value['state'] == 'FAIL':
            failed_attempts.append({'run': path.parent.relative_to(ROOT).as_posix(),
                                    'failure': value.get('failure')})
    result = {
        'state': 'PASS', 'scope': 'M4-03B_CANDIDATE_HANDOFF_ONLY', 'at': stamp(),
        'api_active_promoted': False, 'cross_host_backend_fencing_verified': False,
        'focused_run': focused.relative_to(ROOT).as_posix(), 'focused_cases': focus_cases,
        'focused_pass': len(passed), 'child_helper_skipped_in_parent': True,
        'foundations_run': foundations.relative_to(ROOT).as_posix(), 'foundation_cases': foundation_cases,
        'regression_run': regression.relative_to(ROOT).as_posix(), 'regression_cases': regression_cases,
        'syncpipe_cases': sync_cases, 'regression_top_level_pass': len(regression_cases) + len(sync_cases),
        'static_run': static.relative_to(ROOT).as_posix(), 'fault_matrix': matrix,
        'commands': [{k: row[k] for k in ['command', 'exit_code', 'log', 'started_at', 'completed_at']} for row in commands],
        'runs_match_final_executable_source': final_run_source,
        'resource_observed_max': {key: max(sample[key] for sample in samples)
                                  for key in ['test_disk_bytes', 'owned_rss_bytes']},
        'resource_observed_min_free_disk': min(sample['free_disk_bytes'] for sample in samples),
        'identity': read(focused / 'environment.json'),
        'pinned_next_offsets': handoff['pinned_next_offsets'],
        'final_source_marker': handoff['final_source_marker'],
        'verification_marker': handoff['verification_marker'],
        'final_report_sha256': handoff['final_report_sha256'],
        'verification_report_sha256': handoff['verification_report_sha256'],
        'gold_overview': gold['overview'],
        'reports_source_counts': {key: report['source'] for key, report in reports.items()},
        'candidate_status': {key: status[key] for key in ['es', 'ck']},
        'real_commit_timeout': timeout, 'real_commit_partition_errors': partition_errors,
        'failed_attempts_preserved': failed_attempts,
        'actual_integration': ['offline empty-group commits on Kafka 4.4.0',
            'exact partition vector and empty log-end positions', 'persistent follower drain and writer seal',
            'operator and worker SIGKILL', 'real response timeout and per-partition errors',
            'ES/CK success before progress/commit failures and replay', 'single cooperating writer rejection',
            'topic, partition, ES/CK UUID and retention refusal', 'state, recovery, heartbeat and watermark isolation',
            'new marker plus complete MySQL/ES/CK reconciliation', 'unchanged ACTIVE plus post-resume HTTP write',
            'actual operator CLI and candidate status CLI'],
        'unit_or_source_only': ['remote/unknown PID conservatively refused (unit)',
            'arbitrary backend writers cannot be fenced (source boundary)',
            'dynamic handoff state stays outside target cursor fingerprint (unit, foundation regression)'],
        'unverified': ['ten/hundred-thousand handoff capacity', 'multi-broker failure during this new handoff',
            'cross-host or disk/network fault-domain HA', 'request-level ES/CK fencing', 'Linux or Docker execution',
            'ACTIVE read/status promotion', 'alias or CK query-space promotion', 'automatic abort/rollback',
            'multi-member candidate projection writing'],
        'historical_m402': {'reexecuted': False, 'evidence': 'artifacts/m4-02/matrix-summary.json',
                           'scope': 'PROCESS_FAILOVER_VERIFIED historical only'},
    }
    write('fault-matrix.json', {'state': 'PASS', 'required_cases': matrix})
    write('test-summary.json', result)
    return result


def reachable(port):
    with socket.socket() as connection:
        connection.settimeout(1)
        return connection.connect_ex(('127.0.0.1', port)) == 0


def package(tests):
    baseline = read(OUT / 'baseline-source-manifest.json')
    with tarfile.open(OUT / 'pre-m4-03b-source.tar.gz') as archive:
        old = {row['path']: archive.extractfile(row['path']).read() for row in baseline['files']}
    for row in baseline['files']:
        if sha(old[row['path']]) != row['sha256'] or len(old[row['path']]) != row['bytes']:
            raise RuntimeError('Immutable baseline checksum changed')
    current, modes = {}, {}
    for parent, directories, files in os.walk(ROOT):
        directories[:] = [name for name in directories if name not in EXCLUDED]
        for name in files:
            path = Path(parent) / name
            relative = path.relative_to(ROOT).as_posix()
            current[relative] = path.read_bytes()
            modes[relative] = path.stat().st_mode & 0o777
    changes, patch = [], []
    for name in sorted(set(old) | set(current)):
        before, after = old.get(name, b''), current.get(name, b'')
        if before == after:
            continue
        changes.append({'path': name, 'status': 'added' if name not in old else 'modified' if name in current else 'deleted',
                        'before_bytes': len(before), 'after_bytes': len(after),
                        'before_sha256': sha(before), 'after_sha256': sha(after)})
        patch.extend(difflib.unified_diff(before.decode('utf-8').splitlines(True), after.decode('utf-8').splitlines(True),
                     fromfile='a/' + name if name in old else '/dev/null', tofile='b/' + name if name in current else '/dev/null'))
    (OUT / 'full.diff').write_text(''.join(patch))
    write('file-manifest.json', {'source_files_before': len(old), 'source_files_after': len(current), 'changes': changes})
    write('final-source-manifest.json', {'at': stamp(), 'branch': baseline['branch'], 'head': None,
          'files': [{'path': name, 'bytes': len(data), 'sha256': sha(data), 'mode': oct(modes[name])}
                    for name, data in sorted(current.items())]})
    with tarfile.open(OUT / 'final-source.tar.gz', 'w:gz') as archive:
        for name, data in sorted(current.items()):
            member = tarfile.TarInfo(name)
            member.size, member.mode = len(data), modes[name]
            archive.addfile(member, io.BytesIO(data))
    with tarfile.open(OUT / 'final-source.tar.gz') as archive:
        final_archived = {member.name: archive.extractfile(member).read() for member in archive.getmembers()}
        if final_archived != current:
            raise RuntimeError('Final archive failed byte verification')
    for change in changes:
        for kind, data in [('original', old.get(change['path'])), ('final', current.get(change['path']))]:
            if data is not None:
                target = OUT / 'source' / kind / change['path']
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
    with tempfile.TemporaryDirectory(prefix='ticketpulse-m403b-diff-') as directory:
        target = Path(directory)
        for name, data in old.items():
            path = target / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        check = subprocess.run(['git', 'apply', '--check', str(OUT / 'full.diff')], cwd=target, capture_output=True, text=True)
        result = subprocess.run(['git', 'apply', str(OUT / 'full.diff')], cwd=target, capture_output=True, text=True)
        restored = {path.relative_to(target).as_posix(): path.read_bytes() for path in target.rglob('*') if path.is_file()}
        exact = check.returncode == 0 and result.returncode == 0 and restored == current
        write('diff-verification.json', {'exact_source_bytes_restored': exact, 'check_exit_code': check.returncode,
              'apply_exit_code': result.returncode, 'stderr': check.stderr + result.stderr,
              'source_files': len(current), 'final_archive_exact': final_archived == current,
              'staging_used': False})
        if not exact:
            raise RuntimeError('Complete diff did not restore every final source byte')
    prior = read(OUT / 'previous-evidence-manifest.json')['files']
    modified = [row['path'] for row in prior if not (ROOT / row['path']).is_file()
                or checksum(ROOT / row['path']) != row['sha256']]
    write('previous-evidence-preservation.json', {'checked': len(prior), 'bytes_checked': sum(row['bytes'] for row in prior),
          'changed': modified, 'all_prior_evidence_unchanged': not modified})
    if modified:
        raise RuntimeError('Historical evidence changed')
    # Historical delivery prose is also part of the immutable source baseline.
    historical_reports = [name for name in old if name.startswith('docs/') and name.endswith('-delivery.md')]
    changed_reports = [name for name in historical_reports if current.get(name) != old[name]]
    if changed_reports:
        raise RuntimeError('Historical delivery report changed')
    branch = subprocess.check_output(['git', 'symbolic-ref', '--short', 'HEAD'], cwd=ROOT, text=True).strip()
    head = subprocess.run(['git', 'rev-parse', '--verify', 'HEAD'], cwd=ROOT, capture_output=True, text=True)
    staged = subprocess.run(['git', 'diff', '--cached', '--quiet'], cwd=ROOT).returncode == 0
    status = subprocess.check_output(['git', 'status', '--short', '--branch'], cwd=ROOT, text=True)
    (OUT / 'final-git-status.txt').write_text(status)
    demos = sorted((OUT / 'preservation').glob('*/demo.json'))
    if not demos:
        raise RuntimeError('No demo preservation observation')
    demo = read(demos[-1])
    demo_ports = {str(port): reachable(port) for port in [33306, 19092, 19200, 18123, 18080]}
    private_ports = {str(port): reachable(port) for port in [33312, 19102, 19103, 19206, 19306, 18129, 19007, 18095]}
    report = ROOT / 'docs/m4-03b-delivery.md'
    links = re.findall(r'\]\(([^)]+)\)', report.read_text())
    generated = {'../artifacts/m4-03b/closeout.json', '../artifacts/m4-03b/artifact-manifest.json'}
    missing = [link for link in links if not link.startswith(('http:', 'https:', '#')) and link not in generated
               and not (report.parent / link.split('#', 1)[0]).exists()]
    passed = (staged and head.returncode != 0 and branch == baseline['branch'] and demo['preserved']
              and demo.get('demo_migration_011_tables') == 0 and demo.get('demo_migration_012_tables') == 0
              and all(demo_ports.values()) and not any(private_ports.values()) and not missing)
    write('closeout.json', {'checks_passed': passed, 'acceptance': 'M4-03B_PASS' if passed else 'INCOMPLETE',
          'at': stamp(), 'diff_roundtrip_exact': exact, 'source_changes': len(changes),
          'no_staged_changes': staged, 'head': None, 'branch': branch,
          'demo_preservation': demos[-1].relative_to(ROOT).as_posix(),
          'demo_ports': demo_ports, 'private_ports_after_owned_shutdown': private_ports,
          'historical_evidence_unchanged': True, 'historical_files_checked': len(prior),
          'historical_delivery_reports_checked': len(historical_reports), 'historical_delivery_reports_unchanged': True,
          'focused_pass': tests['focused_pass'], 'foundation_pass': len(tests['foundation_cases']),
          'regression_top_level_pass': tests['regression_top_level_pass'], 'missing_report_links': missing,
          'api_active_promoted': False, 'legacy_group_offsets_migrated': False, 'cdc_checkpoint_migrated': False,
          'only_new_private_groups_initialized': True, 'no_commit_or_push_performed': True})
    if not passed:
        raise RuntimeError('Closeout is incomplete; see closeout.json')
    rows = [{'path': path.relative_to(OUT).as_posix(), 'bytes': path.stat().st_size, 'sha256': checksum(path)}
            for path in sorted(OUT.rglob('*')) if path.is_file() and path.name != 'artifact-manifest.json']
    write('artifact-manifest.json', {'at': stamp(), 'files': rows})
    print(json.dumps({'source_changes': len(changes), 'source_files': len(current), 'artifacts': len(rows),
                      'historical_files_checked': len(prior), 'acceptance': 'M4-03B_PASS'}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--summary-only', action='store_true')
    args = parser.parse_args()
    tests = summarize()
    if args.summary_only:
        print(json.dumps({'focused_pass': tests['focused_pass'], 'regression_pass': tests['regression_top_level_pass']}))
    else:
        package(tests)


if __name__ == '__main__':
    main()
