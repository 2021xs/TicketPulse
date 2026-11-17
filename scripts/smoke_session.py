#!/usr/bin/env python3
"""Check live local Web, password login, cookie identity, CSRF and logout without creating data."""
from datetime import datetime, timezone
from http.cookiejar import CookieJar
import json
from pathlib import Path
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BASE = 'http://127.0.0.1:18080'


def main():
    jar = CookieJar()
    client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    def call(method, path, body=None, csrf=None, extra=None):
        headers = {'Content-Type': 'application/json', 'Origin': BASE}
        if csrf:
            headers['X-CSRF-Token'] = csrf
        if extra:
            headers.update(extra)
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
        try:
            response = client.open(request, timeout=15)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}, response.headers

    with client.open(BASE + '/login', timeout=15) as response:
        shell = response.read()
        assert response.status == 200 and b'id="app"' in shell and b'/app.js' in shell
        assert "script-src 'self'" in response.headers['Content-Security-Policy']
    status, _, _ = call('GET', '/api/v1/me')
    assert status == 401
    status, _, _ = call('POST', '/api/v1/auth/login', {'username': 'alice', 'password': 'wrong'})
    assert status == 401
    status, login, headers = call('POST', '/api/v1/auth/login', {'username': 'alice', 'password': 'TicketPulse!2026'})
    assert status == 200 and login['user']['id'] == 1
    cookie = next(c for c in jar if c.name == 'ticketpulse_session')
    secret = cookie.value
    attributes = headers.get('Set-Cookie', '')
    assert 'HttpOnly' in attributes and 'SameSite=Strict' in attributes and 'Max-Age=' in attributes
    status, me, _ = call('GET', '/api/v1/me')
    assert status == 200 and me['csrf_token'] == login['csrf_token'] and me['expires_at']
    assert 'password' not in me and secret not in json.dumps(me)
    status, listing, _ = call('GET', '/api/v1/tickets?view=created_by_me&page=1&page_size=10')
    assert status == 200 and isinstance(listing['items'], list)
    if listing['items']:
        status, options, _ = call('GET', '/api/v1/tickets/' + str(listing['items'][0]['id']) + '/options')
        assert status == 200 and options['actions'] and not options['assignees'] and not options['teams']
    status, rejected, _ = call('POST', '/api/v1/auth/logout', {})
    assert status == 403 and rejected['error']['code'] == 'CSRF_REJECTED'
    status, _, _ = call('POST', '/api/v1/auth/logout', {}, login['csrf_token'])
    assert status == 200
    status, _, _ = call('GET', '/api/v1/me')
    assert status == 401
    status, _, _ = call('GET', '/api/v1/me', extra={'Cookie': 'ticketpulse_session=' + secret})
    assert status == 401
    result = {'checked_at': datetime.now(timezone.utc).isoformat(), 'web_shell': 'passed',
              'invalid_login': 'passed', 'cookie_login_and_me': 'passed', 'csrf': 'passed',
              'authorized_list_and_options': 'passed', 'logout_and_revoked_cookie': 'passed',
              'expires_at': login['expires_at'], 'credentials_or_cookies_saved': False, 'passed': True}
    output = ROOT / 'artifacts' / 'm1-04'
    output.mkdir(exist_ok=True)
    (output / 'session-smoke-result.json').write_text(json.dumps(result, indent=2) + chr(10))
    print('PASS local Web: password login, HttpOnly session, current user, authorized list, CSRF, logout and revoked cookie')


if __name__ == '__main__':
    main()
