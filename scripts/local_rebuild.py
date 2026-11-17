#!/usr/bin/env python3
"""Owned local rebuild wrapper; its migrate mode installs only 009.

For 009 plus the 010 ordered reconciliation index, use the official
ticketpulse-rebuild -mode=migrate with an explicitly owned operator DSN.
See docs/demo-runbook.md for a separate, fresh demonstration environment.
"""
import argparse
import json
import re
from pathlib import Path
import subprocess
import sys

import local_mysql as local
import local_analytics as runtime

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['migrate', 'create', 'run', 'status', 'follow', 'check', 'logs'])
    parser.add_argument('--database', default='ticketpulse_local')
    args, extra = parser.parse_known_args()
    owner = local.marker()
    local.verify(owner)
    if not re.fullmatch(r'[a-z0-9_]{1,64}', args.database) or (args.database != 'ticketpulse_local' and not args.database.startswith('ticketpulse_test_')):
        raise RuntimeError('local helper only accepts the owned demo or isolated test schemas')
    if args.mode == 'migrate':
        statement = (ROOT / 'db/migrations/009_rebuild.sql').read_text()
        local.mysql('USE ' + args.database + ';\n' + statement)
        if args.database == 'ticketpulse_local':
            local.mysql(''.join("GRANT SELECT,INSERT,UPDATE ON ticketpulse_local." + table + " TO 'ticketpulse_sync'@'localhost';" for table in ['sync_rebuild_jobs', 'sync_rebuild_items', 'sync_rebuild_offsets', 'sync_rebuild_log']))
        print(json.dumps({'migration': '009_rebuild', 'database': args.database, 'business_data_changed': False}))
        return 0
    return subprocess.run([str(ROOT / 'bin/ticketpulse-rebuild'), '-mode=' + args.mode] + extra, cwd=ROOT, env=runtime.environment(args.database)).returncode

if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print('TicketPulse rebuild: ' + str(error), file=sys.stderr)
        sys.exit(1)
