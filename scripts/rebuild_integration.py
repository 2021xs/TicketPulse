#!/usr/bin/env python3
"""Owned M4 recovery tests; never overwrite earlier milestone artifacts."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.request

import local_mysql as local
import local_analytics as runtime

ROOT = Path(__file__).resolve().parents[1]


def audit_summary(output):
    """Summarize actual saved rows and OS-kill timestamps for independent review."""
    log = (output / "integration.log").read_text()
    cases = [{"name": name, "result": outcome, "elapsed_seconds": float(seconds)}
             for outcome, name, seconds in re.findall(r"^--- (PASS|FAIL): (TestM4\w+) \(([\d.]+)s\)$", log, re.MULTILINE)]
    evidence = []
    for path in sorted(output.glob("TestM4*.json")):
        data = json.loads(path.read_text())
        row = {"file": path.name, "label": data["label"], "job_id": data["job"]["job_id"],
               "status": data["job"]["status"], "applied": data["job"]["applied"], "total": data["job"]["total"],
               "mysql_current_tickets": len(data.get("mysql_tickets", []))}
        if "mysql_histories" in data:
            row["mysql_history_rows"] = len(data["mysql_histories"])
        for source, destination, key in [("ck_states_json_each_row", "ck_current", ("organization_id", "ticket_id")),
                                         ("ck_events_json_each_row", "ck_history", ("organization_id", "ticket_id", "version"))]:
            if source in data:
                values = [json.loads(line) for line in data[source].splitlines() if line.strip()]
                row[destination + "_physical_rows"] = len(values)
                row[destination + "_canonical_keys"] = len({tuple(value[field] for field in key) for value in values})
                if destination == "ck_history":
                    first = {}
                    for value in values:
                        if value["action"] == "resolved" and value["after_status"] == "awaiting_confirmation":
                            identity = (value["organization_id"], value["ticket_id"])
                            if identity not in first or value["version"] < first[identity]["version"]:
                                first[identity] = value
                    row["first_resolved_tickets"] = len(first)
                    row["first_resolution_by_event_team"] = {str(team): sum(v["event_team_id"] == team for v in first.values())
                                                            for team in sorted({v["event_team_id"] for v in first.values()})}
        extra = data.get("extra")
        if isinstance(extra, dict) and "sigkill_at" in extra:
            killed = datetime.fromisoformat(extra["sigkill_at"].replace("Z", "+00:00"))
            recovered = datetime.fromisoformat(extra["recovered_at"].replace("Z", "+00:00"))
            row["sigkill_to_reconciled_seconds"] = (recovered - killed).total_seconds()
            row["applied_before_sigkill"] = extra["durable_progress_before_sigkill"]["applied"]
        evidence.append(row)
    summary = {"test_cases": cases, "passed": sum(c["result"] == "PASS" for c in cases),
               "failed": sum(c["result"] == "FAIL" for c in cases), "evidence_counts": evidence,
               "source_of_counts": "saved raw MySQL rows and ClickHouse JSONEachRow; first resolution from historical action version",
               "freeze_transaction_process_kill_tested": False}
    (output / "audit-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def main():
    owner = local.marker()
    local.verify(owner)
    runtime.kafka.start()
    runtime.search.start_component("es", runtime.search.state())
    runtime.start()
    env = runtime.environment()
    env["TICKETPULSE_TEST_DSN"] = owner["admin_dsn"]
    env["TICKETPULSE_TEST_SERVER_UUID"] = owner["server_uuid"]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output = ROOT / "artifacts" / "m4-01" / "fault-tests" / stamp
    output.mkdir(parents=True)
    binary = output / "ticketpulse-rebuild"
    build = ["go", "build", "-race", "-o", str(binary), "./cmd/ticketpulse-rebuild"]
    started = datetime.now(timezone.utc)
    built = subprocess.run(build, cwd=ROOT, env=env, text=True, capture_output=True)
    (output / "build.log").write_text(built.stdout + built.stderr)
    if built.returncode:
        print(built.stdout + built.stderr, file=sys.stderr)
        (output / "result.json").write_text(json.dumps({"build_command": build, "exit_code": built.returncode}, indent=2) + "\n")
        return built.returncode
    env["TICKETPULSE_REBUILD_BINARY"] = str(binary)
    env["TICKETPULSE_M4_EVIDENCE_DIR"] = str(output)
    command = ["go", "test", "-race", "-tags=integration,m2,m3,m4", "-count=1", "-v", "./internal/mysqlstore", "-run", "^TestM4"] + sys.argv[1:]
    with (output / "integration.log").open("w") as log:
        child = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in child.stdout:
            print(line, end="", flush=True)
            log.write(line)
            log.flush()
        code = child.wait()
    versions = {}
    for service, endpoint, query in [
        ("clickhouse", env["TICKETPULSE_CK_URL"], b"SELECT version()"),
        ("elasticsearch", env["TICKETPULSE_ES_URL"], None),
    ]:
        try:
            request = urllib.request.Request(endpoint, data=query)
            with urllib.request.urlopen(request, timeout=5) as response:
                value = response.read().decode()
            versions[service] = value.strip() if query else json.loads(value)["version"]["number"]
        except (OSError, ValueError, KeyError) as error:
            versions[service] = "unavailable: " + str(error)
    result = {"command": command, "build_command": build, "exit_code": code,
              "started_at": started.isoformat(), "finished_at": datetime.now(timezone.utc).isoformat(),
              "mysql_version": local.mysql("SELECT VERSION();").strip(), "versions": versions,
              "kafka_version": runtime.kafka.KAFKA.name, "gold_source": "tests/analytics/prd_gold.json",
              "isolated_random_resources": True, "evidence_directory": str(output.relative_to(ROOT)),
              "checks": ["PRD rebuild without demo consumers", "isolated ES deletion and CK truncation",
                         "HTTP writes during frozen snapshot replay", "durable batch interruption and same-job resume",
                         "duplicate and late old versions", "Kafka DeleteRecords then MySQL history reconstruction",
                         "actual CLI SIGKILL and persisted checkpoint resume",
                         "projection commit before MySQL cursor confirmation fault",
                         "four reconciliation states and corrupted projection recovery",
                         "active Follow HTTP writes, cancel/resume and same-name Topic replacement",
                         "Check deadline and missing target invalidate completed status",
                         "frozen overlap expiration fails then new MySQL snapshot restores",
                         "metrics organization does not omit other organizations from reconciliation"],
              "earlier_artifacts_modified": False}
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    audit_summary(output)
    # Tiny pointer to the newest run; all failed/successful run evidence remains.
    (ROOT / "artifacts" / "m4-01" / "fault-tests" / "latest.json").write_text(json.dumps(result, indent=2) + "\n")
    print("M4 recovery evidence: " + str(output.relative_to(ROOT)), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
