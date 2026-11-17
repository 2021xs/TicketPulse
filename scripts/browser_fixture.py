#!/usr/bin/env python3
"""Own one disposable MySQL database and HTTP server for browser acceptance."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time
import urllib.request

import local_mysql as local

ROOT = Path(__file__).resolve().parents[1]
MARKER = ROOT / '.local' / 'm104-browser-fixture.json'
ARTIFACTS = ROOT / 'artifacts' / 'm1-04'
PORT = 18081


def verified():
    owner = local.marker()
    local.verify(owner)
    state = json.loads(MARKER.read_text())
    if state.get('project') != str(ROOT) or state.get('server_uuid') != owner['server_uuid']:
        raise RuntimeError('Browser fixture ownership mismatch')
    if not state['database'].startswith('ticketpulse_test_web_'):
        raise RuntimeError('Refusing non-isolated database')
    exists = local.mysql("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='" + state['database'] + "';").strip()
    if exists != '1':
        raise RuntimeError('Owned browser test database does not exist')
    return state


def sql(statement):
    state = verified()
    return local.mysql('USE ' + state['database'] + ';\n' + statement)


def start():
    owner = local.marker()
    local.verify(owner)
    if MARKER.exists():
        state = verified()
        print(json.dumps({'url': state['url'], 'database': state['database'], 'existing': True}))
        return
    with socket.socket() as probe:
        if probe.connect_ex(('127.0.0.1', PORT)) == 0:
            raise RuntimeError('Browser test port is occupied')
    database = 'ticketpulse_test_web_' + secrets.token_hex(8)
    local.mysql('CREATE DATABASE ' + database + ' CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;')
    try:
        scripts = [ROOT / 'db/schema.sql', ROOT / 'db/seed.sql', ROOT / 'db/login_seed.sql']
        local.mysql('USE ' + database + ';\n' + '\n'.join(path.read_text() for path in scripts))
        env = os.environ.copy()
        env['TICKETPULSE_DSN'] = owner['admin_dsn'] + database
        ARTIFACTS.mkdir(exist_ok=True)
        with (ARTIFACTS / 'browser-server.log').open('a') as log:
            child = subprocess.Popen([str(ROOT / 'bin/ticketpulse'), '-listen', '127.0.0.1:' + str(PORT)], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        state = {'project': str(ROOT), 'database': database, 'server_uuid': owner['server_uuid'],
                 'pid': child.pid, 'url': 'http://127.0.0.1:' + str(PORT),
                 'started_at': datetime.now(timezone.utc).isoformat()}
        local.private_file(MARKER, json.dumps(state, indent=2))
        for _ in range(80):
            if child.poll() is not None:
                raise RuntimeError('Browser HTTP server exited')
            try:
                with urllib.request.urlopen(state['url'] + '/login', timeout=1) as response:
                    if response.status == 200:
                        print(json.dumps({'url': state['url'], 'database': database, 'created': True}))
                        return
            except OSError:
                time.sleep(.1)
        raise RuntimeError('Browser server failed to start')
    except BaseException:
        if MARKER.exists():
            stop()
        else:
            local.mysql('DROP DATABASE ' + database + ';')
        raise


def stop():
    if not MARKER.exists():
        print('No browser fixture; nothing deleted')
        return
    state = verified()
    pid = state['pid']
    try:
        cwd = subprocess.check_output(['lsof', '-a', '-p', str(pid), '-d', 'cwd', '-Fn'], text=True)
        if 'n' + str(ROOT) not in cwd.splitlines():
            raise RuntimeError('Server PID belongs to another checkout; refusing to stop')
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except subprocess.CalledProcessError:
        pass
    local.mysql('DROP DATABASE ' + state['database'] + ';')
    MARKER.unlink()
    (ARTIFACTS / 'browser-cleanup.json').write_text(json.dumps({'database': state['database'], 'removed_only_owned_database': True, 'finished_at': datetime.now(timezone.utc).isoformat(), 'passed': True}, indent=2) + chr(10))
    print('Owned browser database removed; demonstration database preserved')


def mutate(action, value):
    if action in ['disable-user', 'enable-user']:
        sql('UPDATE users SET active=' + ('FALSE' if action == 'disable-user' else 'TRUE') + ' WHERE id=' + str(value) + ';')
    elif action in ['disable-team', 'enable-team']:
        sql('UPDATE teams SET active=' + ('FALSE' if action == 'disable-team' else 'TRUE') + ' WHERE id=' + str(value) + ';')
    elif action == 'expire':
        sql('UPDATE user_sessions SET created_at=UTC_TIMESTAMP(6)-INTERVAL 2 HOUR,expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE user_id=' + str(value) + ' AND revoked_at IS NULL;')
    elif action == 'fail-comments':
        sql("CREATE TRIGGER fail_web_comment BEFORE INSERT ON ticket_comments FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='browser test comment failure';")
    elif action == 'restore-comments':
        sql('DROP TRIGGER IF EXISTS fail_web_comment;')
    print(json.dumps({'action': action, 'fixture_only': True}))


def inspect(value):
    expression = "JSON_OBJECT('id',id,'ticket_no',ticket_no,'status',status,'version',version,'creator_id',creator_id,'assignee_id',assignee_id,'initial_team_id',initial_team_id,'current_team_id',current_team_id,'first_resolved_at',DATE_FORMAT(first_resolved_at,'%Y-%m-%dT%H:%i:%s.%fZ'),'closed_at',DATE_FORMAT(closed_at,'%Y-%m-%dT%H:%i:%s.%fZ'),'reopen_count',reopen_count,'resolution_note',resolution_note)"
    result = sql('SELECT HEX(CAST(' + expression + ' AS CHAR CHARACTER SET utf8mb4)) FROM tickets WHERE id=' + str(value) + ';').strip()
    ticket = json.loads(bytes.fromhex(result).decode())
    histories = sql('SELECT HEX(CAST(JSON_OBJECT(\'action\',action,\'version\',version,\'operator_id\',operator_id,\'reason\',reason,\'comment_id\',comment_id,\'responsible_team_id\',responsible_team_id,\'before\',before_state,\'after\',after_state,\'occurred_at\',DATE_FORMAT(occurred_at,\'%Y-%m-%dT%H:%i:%s.%fZ\')) AS CHAR CHARACTER SET utf8mb4)) FROM ticket_histories WHERE ticket_id=' + str(value) + ' ORDER BY version,id;').splitlines()
    comments = sql('SELECT HEX(CAST(JSON_OBJECT(\'id\',id,\'author_id\',author_id,\'content\',content,\'version\',version) AS CHAR CHARACTER SET utf8mb4)) FROM ticket_comments WHERE ticket_id=' + str(value) + ' ORDER BY created_at,id;').splitlines()
    data = {'ticket': ticket, 'history': [json.loads(bytes.fromhex(x).decode()) for x in histories], 'comments': [json.loads(bytes.fromhex(x).decode()) for x in comments]}
    print(json.dumps(data, ensure_ascii=False))


def main():
    actions = ['start', 'stop', 'inspect', 'disable-user', 'enable-user', 'disable-team', 'enable-team', 'expire', 'fail-comments', 'restore-comments']
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=actions)
    parser.add_argument('value', nargs='?', type=int, default=0)
    args = parser.parse_args()
    if args.action == 'start':
        start()
    elif args.action == 'stop':
        stop()
    elif args.action == 'inspect':
        inspect(args.value)
    else:
        mutate(args.action, args.value)


if __name__ == '__main__':
    main()
