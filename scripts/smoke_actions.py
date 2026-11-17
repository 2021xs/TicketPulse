#!/usr/bin/env python3
"""Create, compete for claim with two engineers, start, and verify the committed timeline."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import threading
import uuid

from smoke import call, check, ROOT

ARTIFACTS = ROOT / 'artifacts' / 'm1-02a'
ENGINEERS = {'local-it-agent': 3, 'local-it-agent-2': 11}


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + chr(10))


def verify_history(detail, owner):
    current = detail['ticket']
    check(current['version'] == 3 and current['status'] == 'in_progress'
          and current['assignee_id'] == owner, 'Wrong current status/owner/version')
    check(detail['assignee']['id'] == owner and detail['current_team']['id'] == 10,
          'Detail did not return the current team and assignee')
    history = detail['history']
    check([h['version'] for h in history] == [1, 2, 3], 'Timeline has extra or missing versions')
    check([h['action'] for h in history] == ['created', 'claimed', 'processing_started'],
          'Wrong timeline actions')
    check([h['operator_id'] for h in history] == [1, owner, owner], 'Wrong timeline actors')
    check(history[0]['before'] is None and history[0]['after']['assignee_id'] is None,
          'Creation snapshot changed')
    check([h['after']['status'] for h in history] == ['pending', 'pending', 'in_progress'],
          'History state transitions incorrect')
    check([h['after']['assignee_id'] for h in history] == [None, owner, owner],
          'History assignment transitions incorrect')
    for previous, next_item in zip(history, history[1:]):
        check(next_item['before'] == previous['after'], 'History before/after chain is inconsistent')
    check(all(h['responsible_team_id'] == 10 for h in history), 'History team is wrong')
    check(history[-1]['occurred_at'] == current['updated_at'], 'Operation timestamp differs from ticket')


def main():
    ARTIFACTS.mkdir(exist_ok=True)
    request_file = ARTIFACTS / 'smoke-request.json'
    evidence_file = ARTIFACTS / 'smoke-result.json'
    if not request_file.exists():
        save(request_file, {'request_id': 'M102A-' + uuid.uuid4().hex,
                            'title': '两位工程师竞争认领网络工单',
                            'description': '本地合成工单：并发认领后由成功者开始处理，验证版本和历史。',
                            'category': 'network', 'priority': 'high'})
    payload = json.loads(request_file.read_text())
    create = call('POST', '/api/v1/tickets', 'local-alice', payload)
    check(create['status'] in [200, 201], f'Create failed: {create}')
    ticket = create['body']['ticket']
    path = '/api/v1/tickets/' + str(ticket['id'])
    evidence = json.loads(evidence_file.read_text()) if evidence_file.exists() else {}
    evidence['request'] = payload
    evidence.setdefault('create', create)
    evidence['checked_at'] = datetime.now(timezone.utc).isoformat()
    if ticket['version'] == 1:
        check(ticket['assignee_id'] is None and ticket['status'] == 'pending', 'Wrong initial ticket')
        barrier = threading.Barrier(2)

        def claim(token):
            barrier.wait(timeout=10)
            result = call('POST', '/api/v1/tickets/claim', token,
                          {'ticket_id': ticket['id'], 'expected_version': 1})
            return {'user_id': ENGINEERS[token], 'response': result}

        with ThreadPoolExecutor(max_workers=2) as pool:
            attempts = list(pool.map(claim, ENGINEERS))
        evidence['competition'] = attempts
        save(evidence_file, evidence)
        check(sorted(a['response']['status'] for a in attempts) == [200, 409],
              f'Competition must have one success and one conflict: {attempts}')
        conflict = next(a['response'] for a in attempts if a['response']['status'] == 409)
        check(conflict['body']['error']['code'] == 'VERSION_CONFLICT', 'Wrong conflict code')
    detail = call('GET', path, 'local-alice')
    check(detail['status'] == 200, f'Detail failed: {detail}')
    current = detail['body']['ticket']
    owner = current['assignee_id']
    check(owner in ENGINEERS.values(), 'Ticket was not claimed by either engineer')
    winner = next(token for token, user_id in ENGINEERS.items() if user_id == owner)
    loser = next(token for token, user_id in ENGINEERS.items() if user_id != owner)
    if current['version'] == 2:
        check(current['status'] == 'pending', 'Claim changed processing status')
        evidence['claimed_detail'] = detail
        started = call('POST', '/api/v1/tickets/start', winner,
                       {'ticket_id': ticket['id'], 'expected_version': 2})
        evidence['start'] = started
        save(evidence_file, evidence)
        check(started['status'] == 200, f'Start failed: {started}')
    final = call('GET', path, 'local-alice')
    check(final['status'] == 200, f'Final detail failed: {final}')
    verify_history(final['body'], owner)
    stale_claim = call('POST', '/api/v1/tickets/claim', winner,
                       {'ticket_id': ticket['id'], 'expected_version': 1})
    stale_start = call('POST', '/api/v1/tickets/start', winner,
                       {'ticket_id': ticket['id'], 'expected_version': 2})
    non_owner = call('POST', '/api/v1/tickets/start', loser,
                     {'ticket_id': ticket['id'], 'expected_version': 3})
    for result in [stale_claim, stale_start]:
        check(result['status'] == 409 and result['body']['error']['code'] == 'VERSION_CONFLICT',
              'Old action version was not rejected')
    check(non_owner['status'] == 403 and non_owner['body']['error']['code'] == 'ACTION_FORBIDDEN',
          'Non-owner started processing')
    unchanged = call('GET', path, 'local-alice')
    check(unchanged == final, 'Failed retries changed ticket/history')
    check(evidence.get('competition') and sorted(a['response']['status'] for a in evidence['competition']) == [200, 409],
          'No recorded competition for this request; do not claim a completed HTTP competition')
    evidence.update({'winner_id': owner, 'final_detail': final, 'stale_claim': stale_claim,
                     'stale_start': stale_start, 'non_owner_start': non_owner, 'passed': True})
    save(evidence_file, evidence)
    print(f"PASS {ticket['ticket_no']}: two engineers competed (200/409); owner={owner}; version=1→2→3; pending→pending→in_progress; history=3")
    return 0


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        raise SystemExit(f'Action smoke failed: {error}; preserve the request id and read detail before retrying')
