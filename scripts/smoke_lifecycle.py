#!/usr/bin/env python3
"""Verify both lifecycle paths through the running local HTTP service, preserving request identities."""
from datetime import datetime, timezone
import json
import uuid

from smoke import call, check, ROOT

ARTIFACTS = ROOT / 'artifacts' / 'm1-02b'
NOTE_FIRST = '  首次解决：主线路恢复\n验证中文、空格与 😀。  '
NOTE_SECOND = '再次解决：备用交换机已更换\n全部连通性检查通过。'


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + chr(10))


def steps(with_reopen):
    actions = [('claim', 'local-it-agent'), ('start', 'local-it-agent'), ('resolve', 'local-it-agent')]
    if with_reopen:
        actions += [('reopen', 'local-alice'), ('resolve', 'local-it-agent')]
    return actions + [('close', 'local-alice')]


def verify_flow(detail, with_reopen):
    current, history = detail['ticket'], detail['history']
    expected_actions = ['created', 'claimed', 'processing_started', 'resolved']
    if with_reopen:
        expected_actions += ['reopened', 'resolved']
    expected_actions += ['closed']
    check([h['version'] for h in history] == list(range(1, len(expected_actions) + 1)), 'Missing or duplicate versions')
    check([h['action'] for h in history] == expected_actions, 'Incorrect lifecycle history actions')
    check(current['status'] == 'closed' and current['version'] == len(expected_actions)
          and current['assignee_id'] == 3 and current['current_team_id'] == 10
          and current['reopen_count'] == int(with_reopen), 'Wrong final lifecycle state')
    first_at = history[3]['occurred_at']
    check(current['first_resolved_at'] == first_at and current['closed_at'] == history[-1]['occurred_at']
          and current['closed_at'] == current['updated_at'], 'Lifecycle timestamps are wrong')
    check(history[3]['after']['resolution_note'] == NOTE_FIRST, 'First resolution text lost')
    check(current['resolution_note'] == (NOTE_SECOND if with_reopen else NOTE_FIRST), 'Latest resolution text lost')
    if with_reopen:
        check(history[4]['after']['status'] == 'in_progress' and history[4]['after']['assignee_id'] == 3
              and history[4]['after']['resolution_note'] == NOTE_FIRST, 'Reopen lost prior owner or resolution')
        check(history[5]['after']['resolution_note'] == NOTE_SECOND, 'Second resolution text missing in history')
    for i, h in enumerate(history):
        check(h['responsible_team_id'] == 10, 'Wrong operation team')
        check(h['operator_id'] == (1 if h['action'] in ['created', 'reopened', 'closed'] else 3), 'Wrong operator')
        if i:
            check(h['before'] == history[i-1]['after'], 'Broken before/after chain')
        if i >= 3:
            check(h['after']['first_resolved_at'] == first_at, 'First resolution time overwritten')


def run_flow(with_reopen):
    label = 'reopen' if with_reopen else 'direct-close'
    request_file = ARTIFACTS / (label + '-request.json')
    result_file = ARTIFACTS / (label + '-result.json')
    if not request_file.exists():
        save(request_file, {'request_id': 'M102B-' + label + '-' + uuid.uuid4().hex,
                            'title': '网络工单完整生命周期验证 ' + label,
                            'description': '本地合成工单：解决、重开与确认关闭，核对时间、版本和历史。',
                            'category': 'network', 'priority': 'high'})
    payload = json.loads(request_file.read_text())
    evidence = json.loads(result_file.read_text()) if result_file.exists() else {'request': payload, 'attempts': []}
    created = call('POST', '/api/v1/tickets', 'local-alice', payload)
    check(created['status'] in [200, 201], f'Create failed: {created}')
    evidence.setdefault('create', created)
    ticket_id = created['body']['ticket']['id']
    path = '/api/v1/tickets/' + str(ticket_id)
    for expected_version, (action, token) in enumerate(steps(with_reopen), start=1):
        latest = call('GET', path, 'local-alice')
        check(latest['status'] == 200, f'Detail failed: {latest}')
        current = latest['body']['ticket']
        if current['version'] > expected_version:
            # A previous run may have committed before losing its response. Verify history before continuing.
            recorded = latest['body']['history'][expected_version]
            expected_action = {'claim': 'claimed', 'start': 'processing_started', 'resolve': 'resolved', 'reopen': 'reopened', 'close': 'closed'}[action]
            check(recorded['action'] == expected_action, 'Unexpected existing version; stop without writing')
            continue
        check(current['version'] == expected_version, 'Unexpected source version')
        request = {'ticket_id': ticket_id, 'expected_version': expected_version}
        if action == 'resolve':
            request['resolution_note'] = NOTE_FIRST if expected_version == 3 else NOTE_SECOND
        attempt = {'action': action, 'request': request, 'before_detail': latest}
        evidence['attempts'].append(attempt)
        save(result_file, evidence)
        response = call('POST', '/api/v1/tickets/' + action, token, request)
        attempt['response'] = response
        save(result_file, evidence)
        check(response['status'] == 200, f'{action} failed: {response}')
        check(response['body']['ticket']['version'] == expected_version + 1, 'Action did not advance one version')
    final = call('GET', path, 'local-alice')
    check(final['status'] == 200, f'Final detail failed: {final}')
    verify_flow(final['body'], with_reopen)
    denied = []
    for action, token in [('resolve', 'local-it-agent'), ('reopen', 'local-alice'), ('close', 'local-alice')]:
        request = {'ticket_id': ticket_id, 'expected_version': final['body']['ticket']['version']}
        if action == 'resolve':
            request['resolution_note'] = '关闭后不允许再次解决'
        response = call('POST', '/api/v1/tickets/' + action, token, request)
        check(response['status'] == 409 and response['body']['error']['code'] == 'INVALID_STATE', 'Closed state accepted an action')
        denied.append({'action': action, 'response': response})
    check(call('GET', path, 'local-alice') == final, 'Rejected actions changed closed ticket')
    evidence.update({'final_detail': final, 'closed_action_rejections': denied,
                     'checked_at': datetime.now(timezone.utc).isoformat(), 'passed': True})
    save(result_file, evidence)
    return {'label': label, 'ticket_id': ticket_id, 'ticket_no': final['body']['ticket']['ticket_no'],
            'version': final['body']['ticket']['version'], 'first_resolved_at': final['body']['ticket']['first_resolved_at'],
            'closed_at': final['body']['ticket']['closed_at'], 'reopen_count': int(with_reopen), 'passed': True}


def main():
    ARTIFACTS.mkdir(exist_ok=True)
    flows = [run_flow(False), run_flow(True)]
    save(ARTIFACTS / 'smoke-result.json', {'checked_at': datetime.now(timezone.utc).isoformat(), 'flows': flows, 'passed': True})
    for flow in flows:
        print(f"PASS {flow['ticket_no']}: {flow['label']}; closed version={flow['version']}; reopen_count={flow['reopen_count']}; history={flow['version']}")


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        raise SystemExit(f'Lifecycle smoke failed: {error}; preserve request identities and read detail before retrying')
