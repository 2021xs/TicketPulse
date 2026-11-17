#!/usr/bin/env python3
"""Owned copy with real duplicate replay and enabled background merges."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading
import time

import m503_bench as bench
import m503_correctness as correctness
import m503_fixture as fixture
import m503_queries as queries
import m503_suite as suite

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/m5-03/merge'


def main():
    OUT.mkdir(parents=True, exist_ok=False)
    suite.recover()
    values = fixture.fixtures()
    original = dict(values['datasets']['100000'])
    source = original['ck_database']
    target = 'ticketpulse_m503_100000_merges'
    if fixture.ckquery("SELECT count() FROM system.databases WHERE name='" + target + "'").strip() != '0':
        raise RuntimeError('Merge experiment target already exists; preserve it')
    fixture.ckquery('CREATE DATABASE ' + target)
    for table in ['ticket_events', 'ticket_states']:
        fixture.ckquery('CREATE TABLE ' + target + '.' + table + ' AS ' + source + '.' + table)
        fixture.ckquery('SYSTEM STOP MERGES ' + target + '.' + table)
        # Multiple real Parts with physically repeated business versions.
        for low in range(1, 100001, 5000):
            fixture.ckquery('INSERT INTO ' + target + '.' + table + ' SELECT * FROM ' + source + '.' + table
                            + ' WHERE ticket_id>=' + str(low) + ' AND ticket_id<' + str(low + 5000))
    before = correctness.metrics(queries.query('selected'), target, bench.DISTRIBUTION[0])
    values['datasets']['100000']['ck_database'] = target
    fixture.demo.private_file(fixture.MARKER, json.dumps(values, indent=2))
    samples, inserts = [], []
    done = threading.Event()
    def replay():
        serial = 0
        while not done.is_set() and serial < 90:
            low = 1 + (serial % 20) * 5000
            for table in ['ticket_events', 'ticket_states']:
                started = time.perf_counter()
                try:
                    fixture.ckquery('INSERT INTO ' + target + '.' + table + ' SELECT * FROM ' + source + '.' + table
                                    + ' WHERE ticket_id>=' + str(low) + ' AND ticket_id<' + str(low + 500))
                    inserts.append({'serial': serial, 'table': table, 'ticket_range': [low, low + 500],
                                    'seconds': time.perf_counter() - started, 'ok': True})
                except Exception as error:
                    inserts.append({'serial': serial, 'table': table, 'ok': False, 'error': str(error)})
                    done.set()
                    return
            serial += 1
            done.wait(1)
    def observe():
        while not done.is_set():
            sample = {'utc': bench.stamp()}
            try:
                sample['parts'] = [json.loads(x) for x in bench.control("SELECT table,count() AS active_parts,sum(rows) AS physical_rows FROM system.parts WHERE active AND database='" + target + "' GROUP BY table FORMAT JSONEachRow").splitlines()]
                sample['merges'] = [json.loads(x) for x in bench.control("SELECT table,elapsed,progress,num_parts,memory_usage,bytes_read_uncompressed,bytes_written_uncompressed FROM system.merges WHERE database='" + target + "' FORMAT JSONEachRow").splitlines()]
            except Exception as error:
                sample['error'] = str(error)
            samples.append(sample)
            done.wait(.5)
    try:
        for table in ['ticket_events', 'ticket_states']:
            fixture.ckquery('SYSTEM START MERGES ' + target + '.' + table)
        a = threading.Thread(target=replay)
        b = threading.Thread(target=observe)
        a.start()
        b.start()
        fixture.start_app(100000, '.local/m5-03-bench-app-final')
        summary = suite.call_bench('http', 'selected', 100000, 20, 120, 3, 'merge-http-final-c20', 'merge/http-c20')
        fixture.stop_app()
    finally:
        done.set()
        a.join(timeout=40)
        b.join(timeout=5)
        values = fixture.fixtures()
        values['datasets']['100000'] = original
        fixture.demo.private_file(fixture.MARKER, json.dumps(values, indent=2))
    after = correctness.metrics(queries.query('selected'), target, bench.DISTRIBUTION[0])
    (OUT / 'parts-merges.jsonl').write_text(''.join(json.dumps(x) + '\n' for x in samples))
    (OUT / 'replay-inserts.jsonl').write_text(''.join(json.dumps(x) + '\n' for x in inserts))
    evidence = {'source': source, 'target': target, 'before_metrics': before, 'after_metrics': after,
                'metrics_unchanged': before == after, 'merge_observed_samples': sum(bool(x.get('merges')) for x in samples),
                'successful_inserts': sum(x['ok'] for x in inserts), 'failed_inserts': sum(not x['ok'] for x in inserts),
                'load_summary': summary, 'fixture_restored': True}
    (OUT / 'result.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({k: evidence[k] for k in ['metrics_unchanged', 'merge_observed_samples', 'successful_inserts', 'failed_inserts']}))
    if not evidence['metrics_unchanged'] or evidence['failed_inserts']:
        raise RuntimeError('Merge/replay experiment did not preserve canonical facts')


if __name__ == '__main__':
    main()
