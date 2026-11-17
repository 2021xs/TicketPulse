#!/usr/bin/env python3
"""Build and run M4-03C2 checks only against verified private resources."""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import time

import m403c2_environment as runtime

ROOT, OUT, RUNTIME = runtime.ROOT, runtime.OUT, runtime.RUNTIME


def source_snapshot():
    # Pin the exact executable source for each run. Historical artifacts and
    # private runtime directories are deliberately outside package discovery.
    paths = [ROOT / 'go.mod', ROOT / 'go.sum']
    for directory in ['cmd', 'internal', 'db', 'web']:
        paths.extend(path for path in (ROOT / directory).rglob('*') if path.is_file())
    paths.extend(ROOT / 'scripts' / name for name in ['m403c2_environment.py', 'm403c2_suite.py', 'm403c2_preservation.py'])
    return {'at': runtime.stamp(), 'files': [
        {'path': path.relative_to(ROOT).as_posix(), 'bytes': path.stat().st_size,
         'sha256': hashlib.sha256(path.read_bytes()).hexdigest()} for path in sorted(paths)]}


def run(command, folder, name, env, cwd=None):
    started = runtime.stamp()
    samples = []
    safety_error = None
    log_path = folder / (name + '.log')
    with log_path.open('x') as log:
        child = subprocess.Popen(command, cwd=cwd or ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        while child.poll() is None:
            try:
                samples.append(runtime.budget())
            except RuntimeError as error:
                safety_error = str(error)
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                break
            time.sleep(2)
    code = child.wait()
    value = {'command': command, 'started_at': started, 'completed_at': runtime.stamp(),
             'exit_code': code, 'log': str(log_path.relative_to(ROOT)), 'safety_error': safety_error,
             'resource_samples': samples}
    (folder / (name + '.json')).write_text(json.dumps(value, indent=2) + chr(10))
    print(json.dumps({key: value[key] for key in ['command', 'exit_code', 'log', 'safety_error']}), flush=True)
    if code or safety_error:
        (folder / 'summary.json').write_text(json.dumps({'state': 'FAIL', 'failure': value}, indent=2) + chr(10))
        raise RuntimeError('Check failed: ' + str(log_path.relative_to(ROOT)))
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['build', 'focused', 'regression', 'static', 'foundations', 'handoff', 'read-control'])
    parser.add_argument('--run', default='^TestM403C2')
    args = parser.parse_args()
    # The immutable executable identity is part of process admission. A new
    # run must never replace binaries while another run still uses them.
    lock_path = RUNTIME / 'suite.lock'
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open('a') as suite_lock:
        try:
            fcntl.flock(suite_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError('Another C2 build/test owns the private binaries; retry after it exits') from error
        execute(args)

def execute(args):
    env = runtime.environment()
    folder = OUT / 'runs' / (datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ') + '-' + args.mode)
    folder.mkdir(parents=True)
    (folder / 'source-at-run.json').write_text(json.dumps(source_snapshot(), indent=2) + chr(10))
    (folder / 'environment.json').write_text(json.dumps(runtime.verify(), indent=2) + chr(10))
    # All regression evidence stays under this new attempt, never old M4/M5 paths.
    env.update(TICKETPULSE_M403A_EVIDENCE_DIR=str(folder / 'foundations'), TICKETPULSE_M403B_EVIDENCE_DIR=str(folder / 'handoff'), TICKETPULSE_M403C1_EVIDENCE_DIR=str(folder / 'c1'), TICKETPULSE_M403C2_EVIDENCE_DIR=str(folder / 'verification'),
               TICKETPULSE_M4_EVIDENCE_DIR=str(folder / 'm4'),
               TICKETPULSE_M503_EVIDENCE_DIR=str(folder / 'm503'),
               TICKETPULSE_M504_EVIDENCE_DIR=str(folder / 'm504'))
    (RUNTIME / 'bin').mkdir(exist_ok=True)
    results = []
    if args.mode in ['build', 'focused', 'regression', 'foundations', 'handoff', 'read-control']:
        for target in ['ticketpulse', 'ticketpulse-sync', 'ticketpulse-control', 'ticketpulse-rebuild']:
            results.append(run(['go', 'build', '-race', '-o', str(RUNTIME / 'bin' / target), './cmd/' + target], folder, 'build-' + target, env))
        if args.mode=='focused':
            legacy=RUNTIME/'legacy-source'
            if not legacy.exists():
                legacy.mkdir()
                with tarfile.open(OUT/'baseline-source.tar.gz') as archive:
                    if any(p.name.startswith('/') or '..' in Path(p.name).parts for p in archive.getmembers()):
                        raise RuntimeError('Unsafe baseline archive')
                    archive.extractall(legacy)
            results.append(run(['go','build','-race','-o',str(RUNTIME/'bin/ticketpulse-legacy-api'),'./cmd/ticketpulse'],folder,'build-legacy-api',env,cwd=legacy))
            env['TICKETPULSE_LEGACY_API_BINARY']=str(RUNTIME/'bin/ticketpulse-legacy-api')
    if args.mode in ['focused', 'regression', 'foundations', 'handoff', 'read-control']:
        regex = '^TestM403A' if args.mode == 'foundations' else ('^TestM403B' if args.mode == 'handoff' else (args.run if args.mode == 'focused' else ('^TestM403C1' if args.mode == 'read-control' else '.')))
        tags = 'integration,m2,m3,m4,m403b,m403c1,m403c2' if args.mode == 'focused' else ('integration,m2,m3,m4,m403b,m403c1' if args.mode == 'read-control' else ('integration,m2,m3,m4,m403b' if args.mode == 'handoff' else ('integration,m2,m3,m4,m403a' if args.mode == 'foundations' else 'integration,m2,m3,m4')))
        test_binary = RUNTIME / 'bin/ticketpulse-c2.test'
        results.append(run(['go', 'test', '-race', '-c', '-tags=' + tags, '-o', str(test_binary), './internal/mysqlstore'], folder, 'build-integration', env))
        results.append(run([str(test_binary), '-test.count=1', '-test.timeout=20m', '-test.v', '-test.run=' + regex], folder, 'integration', env,cwd=ROOT/'internal/mysqlstore'))
        if args.mode == 'regression':
            results.append(run(['go', 'test', '-race', '-count=1', '-timeout=5m', '-v', '-tags=' + tags,
                                './internal/syncpipe'], folder, 'syncpipe-integration', env))
    if args.mode == 'static':
        results.append(run(['go', 'test', '-race', '-count=1', './cmd/...', './internal/...', './db/...', './web/...'], folder, 'unit-race', env))
        results.append(run(['go', 'vet', './cmd/...', './internal/...', './db/...', './web/...'], folder, 'vet', env))
        results.append(run(['python3', '-m', 'unittest', 'discover', '-s', 'scripts', '-p', '*_test.py'], folder, 'python-tests', env))
    (folder / 'summary.json').write_text(json.dumps({'state': 'PASS', 'results': results}, indent=2) + chr(10))
    print('PASS ' + str(folder.relative_to(ROOT)))


if __name__ == '__main__':
    main()
