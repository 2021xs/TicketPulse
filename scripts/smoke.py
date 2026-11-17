#!/usr/bin/env python3
"""Exercise the running HTTP service with a persistent local request identity."""
import json
from pathlib import Path
import sys
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
BASE = 'http://127.0.0.1:18080'
ARTIFACTS = ROOT / 'artifacts'


def call(method, path, token, payload=None):
    data = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={'Authorization': 'Bearer ' + token,
                                          'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return {'status': response.status, 'body': json.load(response)}
    except urllib.error.HTTPError as response:
        return {'status': response.code, 'body': json.load(response)}


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    ARTIFACTS.mkdir(exist_ok=True)
    request_file = ARTIFACTS / 'smoke-request.json'
    if not request_file.exists():
        payload = {'request_id': 'SMOKE-' + uuid.uuid4().hex, 'title': '办公网络无法连接',
                   'description': '三楼无线网络持续断开，请协助排查。',
                   'category': 'network', 'priority': 'urgent'}
        request_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + chr(10))
    payload = json.loads(request_file.read_text())
    create = call('POST', '/api/v1/tickets', 'local-alice', payload)
    check(create['status'] in [200, 201], f'Create failed: {create}')
    ticket = create['body']['ticket']
    check(ticket['creator_id'] == 1 and ticket['initial_team_id'] == 10
          and ticket['current_team_id'] == 10 and ticket['status'] == 'pending'
          and ticket['assignee_id'] is None and ticket['version'] == 1, 'Wrong initial ticket state')
    path = '/api/v1/tickets/' + str(ticket['id'])
    detail = call('GET', path, 'local-alice')
    check(detail['status'] == 200, f'Detail failed: {detail}')
    body = detail['body']
    check(body['ticket'] == ticket and body['current_team']['id'] == 10
          and body['assignee'] is None and len(body['history']) == 1, 'Wrong committed detail')
    history = body['history'][0]
    check(history['action'] == 'created' and history['version'] == 1
          and history['before'] is None and history['after']['current_team_id'] == 10
          and history['responsible_team_id'] == 10 and history['operator_id'] == 1, 'Wrong creation history')
    retry = call('POST', '/api/v1/tickets', 'local-alice', payload)
    check(retry['status'] == 200 and retry['body']['replayed']
          and retry['body']['ticket']['id'] == ticket['id'], 'Retry created a different ticket')
    conflict = call('POST', '/api/v1/tickets', 'local-alice',
                    dict(payload, title='同标识但不同的工单标题'))
    check(conflict['status'] == 409
          and conflict['body']['error']['code'] == 'IDEMPOTENCY_CONFLICT', 'Content conflict accepted')
    denied = call('GET', path, 'local-bob')
    check(denied['status'] == 403 and set(denied['body']) == {'error'}, 'Unauthorized detail disclosed')
    evidence = {'base_url': BASE, 'request': payload, 'create': create, 'detail': detail,
                'retry': retry, 'content_conflict': conflict, 'unauthorized': denied, 'passed': True}
    (ARTIFACTS / 'smoke-result.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + chr(10))
    print(f"PASS {ticket['ticket_no']}: creator=1, team=10, version=1, history=1; retry=200, conflict=409, denied=403")
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, OSError, KeyError, ValueError) as error:
        print(f'Smoke failed: {error}; preserve artifacts/smoke-request.json to retry safely', file=sys.stderr)
        sys.exit(1)
