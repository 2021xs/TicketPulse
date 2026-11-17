#!/usr/bin/env python3
"""Reuse the owned ClickHouse runtime without overwriting earlier evidence."""
import datetime
import sys
import bench_ck_runtime as runtime

if __name__ == '__main__':
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    runtime.OUTPUT = runtime.ROOT / 'artifacts/m5-02/environment/ck-runtime' / stamp
    try:
        runtime.main()
    except (OSError, RuntimeError, runtime.subprocess.CalledProcessError) as error:
        print('TicketPulse M5-02 ClickHouse: ' + str(error), file=sys.stderr)
        sys.exit(1)
