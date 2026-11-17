#!/usr/bin/env python3
"""One persistent cross-team collaboration flow against the running owned HTTP service."""
from datetime import datetime, timezone
import json
import uuid

from smoke import call, check, ROOT

ARTIFACTS = ROOT / 'artifacts' / 'm1-03'
COMMENTS = ['网络异常影响会议，请协助定位。', '已更换交换机并验证连通性。']
NOTES = ['首次更换设备，等待验证。', '再次检查设备固件，故障已解决。']


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + chr(10))


def actions():
    return [
        ('claim', 'local-it-agent', 'claimed', {}),
        ('start', 'local-it-agent', 'processing_started', {}),
        ('comments', 'local-alice', 'commented', {'content': COMMENTS[0]}),
        ('transfer', 'local-it-agent', 'assignee_transferred', {'assignee_id': 11, 'reason': '同团队交接排查'}),
        ('transfer-team', 'local-supervisor', 'team_transferred', {'team_id': 20, 'reason': '设备团队更换交换机'}),
        ('claim', 'local-device-agent', 'claimed', {}),
        ('start', 'local-device-agent', 'processing_started', {}),
        ('comments', 'local-device-agent', 'commented', {'content': COMMENTS[1]}),
        ('resolve', 'local-device-agent', 'resolved', {'resolution_note': NOTES[0]}),
        ('reopen', 'local-alice', 'reopened', {}),
        ('resolve', 'local-device-agent', 'resolved', {'resolution_note': NOTES[1]}),
        ('manual-close', 'local-supervisor', 'manually_closed', {'reason': '主管与员工核验后人工确认关闭'}),
    ]


def list_has(evidence, view, token, ticket_id, expected):
    response = call('GET', '/api/v1/tickets?view=' + view + '&page=1&page_size=100', token)
    check(response['status'] == 200, f'List failed: {response}')
    body = response['body']
    check(not body['has_more'], 'Demo exceeds first page; stop without assuming omitted rows are absent')
    check(any(item['id'] == ticket_id for item in body['items']) == expected, 'List current state mismatch')
    evidence['list_checks'].append({'view': view, 'account': token, 'expected_member': expected, 'response': response})


def verify(detail):
    current, history, comments = detail['ticket'], detail['history'], detail['comments']
    expected = ['created'] + [action[2] for action in actions()]
    check([h['version'] for h in history] == list(range(1, 14)), 'Missing or duplicate history versions')
    check([h['action'] for h in history] == expected, 'Unexpected collaboration actions')
    check(current['version'] == 13 and current['status'] == 'closed' and current['assignee_id'] == 4
          and current['initial_team_id'] == 10 and current['current_team_id'] == 20
          and current['reopen_count'] == 1, 'Wrong final collaboration state')
    check(current['first_resolved_at'] == history[9]['occurred_at']
          and history[11]['after']['first_resolved_at'] == current['first_resolved_at']
          and current['closed_at'] == history[12]['occurred_at'] == current['updated_at'], 'Lifecycle times lost')
    check(history[9]['after']['resolution_note'] == NOTES[0] and history[11]['after']['resolution_note'] == NOTES[1]
          and current['resolution_note'] == NOTES[1], 'Resolution history lost')
    check([c['content'] for c in comments] == COMMENTS and [c['author_id'] for c in comments] == [1, 4], 'Comments lost')
    for c in comments:
        h = history[c['version'] - 1]
        check(h['action'] == 'commented' and h['comment_id'] == c['id']
              and h['occurred_at'] == c['created_at'] and h['operator_id'] == c['author_id'], 'Comment audit mismatch')
    operators = [1, 3, 3, 1, 3, 5, 4, 4, 4, 4, 1, 4, 5]
    for i, h in enumerate(history):
        check(h['operator_id'] == operators[i] and h['responsible_team_id'] == (10 if i < 5 else 20), 'Wrong actor/team history')
        if i:
            check(h['before'] == history[i-1]['after'], 'Broken before/after history chain')
    transfer = history[5]
    check(transfer['before']['current_team_id'] == 10 and transfer['before']['assignee_id'] == 11
          and transfer['before']['status'] == 'in_progress' and transfer['after']['status'] == 'pending'
          and transfer['after']['assignee_id'] is None and transfer['reason'] == '设备团队更换交换机', 'Cross-team audit missing')
    check(history[12]['reason'] == '主管与员工核验后人工确认关闭', 'Manual close reason missing')


def main():
    ARTIFACTS.mkdir(exist_ok=True)
    request_file, result_file = ARTIFACTS / 'smoke-request.json', ARTIFACTS / 'smoke-result.json'
    if not request_file.exists():
        save(request_file, {'request_id': 'M103-' + uuid.uuid4().hex, 'title': '跨团队网络与设备协作工单',
                            'description': '本地合成工单：IT 排查、设备团队修复、评论、重开与主管人工关闭。',
                            'category': 'network', 'priority': 'high'})
    request = json.loads(request_file.read_text())
    evidence = json.loads(result_file.read_text()) if result_file.exists() else {'request': request, 'attempts': []}
    evidence.setdefault('list_checks', [])
    created = call('POST', '/api/v1/tickets', 'local-alice', request)
    check(created['status'] in [200, 201], f'Create failed: {created}')
    evidence.setdefault('create', created)
    ticket_id = created['body']['ticket']['id']
    path = '/api/v1/tickets/' + str(ticket_id)
    for version, (action, token, history_action, fields) in enumerate(actions(), start=1):
        latest = call('GET', path, 'local-alice')
        check(latest['status'] == 200, f'Detail failed: {latest}')
        current = latest['body']['ticket']
        if current['version'] > version:
            check(latest['body']['history'][version]['action'] == history_action, 'Unexpected existing action; stop without writing')
            continue
        check(current['version'] == version, 'Unexpected source version')
        if version == 1:
            list_has(evidence, 'created_by_me', 'local-alice', ticket_id, True)
            list_has(evidence, 'team_pending', 'local-it-agent', ticket_id, True)
            list_has(evidence, 'assigned_to_me', 'local-it-agent', ticket_id, False)
        if version == 3:
            list_has(evidence, 'team_pending', 'local-it-agent', ticket_id, False)
            list_has(evidence, 'assigned_to_me', 'local-it-agent', ticket_id, True)
        if version == 5:
            list_has(evidence, 'assigned_to_me', 'local-it-agent', ticket_id, False)
            list_has(evidence, 'assigned_to_me', 'local-it-agent-2', ticket_id, True)
            denied = call('POST', '/api/v1/tickets/resolve', 'local-it-agent',
                          {'ticket_id': ticket_id, 'expected_version': version, 'resolution_note': '旧负责人不能解决'})
            check(denied['status'] == 403 and denied['body']['error']['code'] == 'ACTION_FORBIDDEN', 'Old owner still resolves')
            evidence['old_owner_denied'] = denied
        if version == 6:
            list_has(evidence, 'team_pending', 'local-it-agent', ticket_id, False)
            list_has(evidence, 'team_pending', 'local-device-agent', ticket_id, True)
            denied = call('GET', path, 'local-it-agent')
            check(denied['status'] == 403 and set(denied['body']) == {'error'}, 'Old team can read data')
            evidence['old_team_denied'] = denied
        payload = dict(fields, ticket_id=ticket_id, expected_version=version)
        attempt = {'action': action, 'account': token, 'request': payload}
        evidence['attempts'].append(attempt)
        save(result_file, evidence)
        response = call('POST', '/api/v1/tickets/' + action, token, payload)
        attempt['response'] = response
        save(result_file, evidence)
        check(response['status'] == 200 and response['body']['ticket']['version'] == version + 1, f'{action} failed: {response}')
    final = call('GET', path, 'local-alice')
    check(final['status'] == 200, f'Final detail failed: {final}')
    verify(final['body'])
    list_has(evidence, 'created_by_me', 'local-alice', ticket_id, True)
    list_has(evidence, 'assigned_to_me', 'local-device-agent', ticket_id, True)
    list_has(evidence, 'team_pending', 'local-device-agent', ticket_id, False)
    evidence.update({'final_detail': final, 'checked_at': datetime.now(timezone.utc).isoformat(), 'passed': True})
    save(result_file, evidence)
    print(f"PASS {final['body']['ticket']['ticket_no']}: closed version=13; initial_team=10, current_team=20; comments=2, history=13, reopen_count=1")


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        raise SystemExit(f'Collaboration smoke failed: {error}; preserve the request identity and read detail before retrying')
