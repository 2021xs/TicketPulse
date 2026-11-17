#!/usr/bin/env python3
"""Bounded real HTTP closed-loop load; each worker reuses one TCP connection.

Metadata interface (all fields optional except sparse-auth prerequisites):
  database, ticket_ids or id_start/id_end, hot_ticket_id, hot_team_id,
  search_keyword, filtered_keyword, analytics_start, analytics_end,
  sparse_query, sparse_authorized_ids, sparse_page_size, sparse_identity.
Credentials come only from process environment TICKETPULSE_BENCH_<ALIAS>_TOKEN.
Defaults are the repository's synthetic local fixture identities. Never point
write scenarios at ticketpulse_local or the demonstration HTTP port 18080.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import http.client
import json
import math
import os
from pathlib import Path
import queue
import random
import re
import socket
import threading
import time
from urllib.parse import urlencode, urlsplit


SCENARIOS = ("mixed-read", "core-read", "analytics-only", "team-filter", "mixed-rw",
             "burst-write", "hot-ticket", "hot-team", "lifecycle-writes", "sparse-auth")
READ_ONLY = ("mixed-read", "core-read", "analytics-only", "team-filter", "sparse-auth")
WRITE_ONLY = ("burst-write", "hot-ticket", "hot-team", "lifecycle-writes")
IDENTITIES = {"requester": "local-alice", "agent": "local-it-agent", "manager": "local-supervisor",
              "device_agent": "local-device-agent"}


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    low, high = math.floor(index), math.ceil(index)
    return round(ordered[low] + (ordered[high] - ordered[low]) * (index - low), 3)


def metadata_for(args):
    source = json.loads(Path(args.metadata).read_text()) if args.metadata else {}
    if not isinstance(source, dict):
        raise ValueError("metadata must be one JSON object")
    if source.get("database", args.database) != args.database:
        raise ValueError("metadata database differs from --database")
    # Save a concrete workload contract rather than copying incidental fields
    # from a dataset manifest, which might include authentication material.
    result = {"database": args.database, "id_start": int(source.get("id_start", 1)),
              "id_end": int(source.get("id_end", args.tickets)),
              "hot_ticket_id": int(source.get("hot_ticket_id", source.get("id_start", 1))),
              "hot_team_id": int(source.get("hot_team_id", 10)),
              "search_keyword": str(source.get("search_keyword", "benchmark")),
              "filtered_keyword": str(source.get("filtered_keyword", source.get("search_keyword", "benchmark"))),
              "analytics_start": str(source.get("analytics_start", "2026-11-01")),
              "analytics_end": str(source.get("analytics_end", "2026-12-01")),
              "hot_identity": str(source.get("hot_identity", "agent")),
              "hot_mode": str(source.get("hot_mode", "comments"))}
    if "ticket_ids" in source:
        result["ticket_ids"] = [int(value) for value in source["ticket_ids"]]
    if "hot_team_ticket_ids" in source:
        result["hot_team_ticket_ids"] = [int(value) for value in source["hot_team_ticket_ids"]]
    if args.scenario == "lifecycle-writes":
        result["lifecycle_ticket_ids"] = [int(value) for value in source.get("lifecycle_ticket_ids", [])]
        if len(result["lifecycle_ticket_ids"]) < args.concurrency or len(set(result["lifecycle_ticket_ids"])) != len(result["lifecycle_ticket_ids"]) or any(value < 1 for value in result["lifecycle_ticket_ids"]):
            raise ValueError("lifecycle-writes requires at least concurrency unique lifecycle_ticket_ids")
    if result["id_start"] < 1 or result["id_end"] < result["id_start"]:
        raise ValueError("metadata ticket ID range is invalid")
    if "ticket_ids" in result and (not result["ticket_ids"] or any(v < 1 for v in result["ticket_ids"])):
        raise ValueError("metadata ticket_ids must be nonempty positive IDs")
    if "hot_team_ticket_ids" in result and (not result["hot_team_ticket_ids"] or any(v < 1 for v in result["hot_team_ticket_ids"])):
        raise ValueError("metadata hot_team_ticket_ids must be nonempty positive IDs")
    if result["hot_identity"] not in IDENTITIES or result["hot_mode"] not in ("comments", "lifecycle"):
        raise ValueError("metadata hot_identity/hot_mode is unsupported")
    if args.scenario == "sparse-auth":
        if not source.get("sparse_query") or "sparse_authorized_ids" not in source:
            raise ValueError("sparse-auth requires metadata sparse_query and sparse_authorized_ids")
        result.update({"sparse_query": str(source["sparse_query"]),
                       "sparse_authorized_ids": sorted({int(v) for v in source["sparse_authorized_ids"]}),
                       "sparse_page_size": int(source.get("sparse_page_size", 20)),
                       "sparse_identity": str(source.get("sparse_identity", "agent"))})
        if result["sparse_identity"] not in IDENTITIES or not 1 <= result["sparse_page_size"] <= 100:
            raise ValueError("metadata sparse identity/page size is invalid")
    return result


def validate_args(args):
    parsed = urlsplit(args.url)
    if parsed.scheme not in ("http", "https") or parsed.hostname not in ("127.0.0.1", "localhost", "::1") or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("--url must be a credential-free loopback HTTP endpoint")
    if parsed.port == 18080:
        raise ValueError("benchmark refuses the existing demonstration port 18080")
    if not re.fullmatch(r"ticketpulse_(?:test|bench|perf|m5)[a-z0-9_]*", args.database):
        raise ValueError("benchmark requires an isolated ticketpulse_test/bench/perf/m5 database")
    if args.tickets < 1 or args.concurrency not in (10, 50, 200):
        raise ValueError("--tickets must be positive; concurrency is 10, 50 or 200")
    if not 0 < args.duration <= 30 or not 0 <= args.warmup <= 10 or not 0 < args.timeout <= 15:
        raise ValueError("duration <=30s, warmup <=10s, timeout <=15s are required")
    if args.run_id and not re.fullmatch(r"[a-zA-Z0-9_-]{1,32}", args.run_id):
        raise ValueError("--run-id must contain 1–32 letters, digits, underscores or hyphens")
    parts = args.read_write_ratio.split(":")
    if len(parts) != 2:
        raise ValueError("--read-write-ratio must be READ:WRITE, such as 90:10")
    read, write = map(float, parts)
    if not math.isfinite(read + write) or read < 0 or write < 0 or read + write <= 0:
        raise ValueError("read/write weights must be finite nonnegative values with positive sum")
    args.write_fraction = write / (read + write)


@dataclass
class Response:
    status: int
    code: str
    data: object
    latency_ms: float
    response_bytes: int
    timeout: bool = False
    transport_error: bool = False
    request_id: str = ""


class HTTPWorker:
    def __init__(self, base_url, timeout, credentials):
        parsed = urlsplit(base_url)
        self.host, self.port, self.scheme = parsed.hostname, parsed.port, parsed.scheme
        self.prefix = parsed.path.rstrip("/")
        if not self.prefix.endswith("/api/v1"):
            self.prefix += "/api/v1"
        self.timeout, self.credentials, self.connection = timeout, credentials, None

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def request(self, method, path, identity, body=None):
        started = time.perf_counter()
        try:
            if self.connection is None:
                kind = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
                self.connection = kind(self.host, self.port, timeout=self.timeout)
            headers = {"Authorization": "Bearer " + self.credentials[identity], "Accept": "application/json", "Connection": "keep-alive"}
            encoded = None
            if body is not None:
                encoded = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode()
                headers["Content-Type"] = "application/json"
            self.connection.request(method, self.prefix + path, body=encoded, headers=headers)
            response = self.connection.getresponse()
            raw = response.read()
            status, close = response.status, response.will_close
            if close:
                self.close()
            try:
                data = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                return Response(status, "INVALID_JSON_RESPONSE", None, (time.perf_counter() - started) * 1000, len(raw))
            code = data.get("error", {}).get("code", "") if isinstance(data, dict) else ""
            return Response(status, code, data, (time.perf_counter() - started) * 1000, len(raw), request_id=response.getheader("X-Request-ID", ""))
        except (socket.timeout, TimeoutError):
            self.close()
            return Response(0, "CLIENT_TIMEOUT", None, (time.perf_counter() - started) * 1000, 0, timeout=True)
        except (OSError, http.client.HTTPException):
            # Request outcome may be unknown. Never automatically replay a write
            # with a fresh intent or expected_version after a transport failure.
            self.close()
            return Response(0, "CLIENT_TRANSPORT_ERROR", None, (time.perf_counter() - started) * 1000, 0, transport_error=True)


class LogWriter:
    def __init__(self, path):
        self.path = path
        self.queue = queue.Queue()
        self.error = None
        self.thread = threading.Thread(target=self._run, name="benchmark-jsonl-writer", daemon=True)
        self.thread.start()

    def _run(self):
        try:
            with self.path.open("x") as output:
                while True:
                    item = self.queue.get()
                    if item is None:
                        break
                    output.write(json.dumps(item, separators=(",", ":"), ensure_ascii=False) + "\n")
        except BaseException as error:
            self.error = error

    def put(self, sample):
        if self.error:
            raise RuntimeError("request evidence logging failed") from self.error
        self.queue.put(sample)

    def close(self):
        self.queue.put(None)
        self.thread.join()
        if self.error:
            raise RuntimeError("request evidence logging failed") from self.error


class Aggregate:
    def __init__(self):
        self.latency = []
        self.success_latency = []
        self.counts = Counter()
        self.statuses = Counter()
        self.codes = Counter()
        self.response_bytes = 0
        self.item_count = 0

    def add(self, sample):
        self.latency.append(sample["latency_ms"])
        self.counts["requests"] += 1
        self.counts["write_requests" if sample["method"] == "POST" else "read_requests"] += 1
        self.statuses[str(sample["status"])] += 1
        if sample["code"]:
            self.codes[sample["code"]] += 1
        success = 200 <= sample["status"] < 300 and not sample["code"]
        if success:
            self.counts["success"] += 1
            self.success_latency.append(sample["latency_ms"])
        elif sample["status"] == 409 and sample["code"] == "VERSION_CONFLICT":
            self.counts["version_conflicts"] += 1
        elif sample["timeout"]:
            self.counts["timeouts"] += 1
        else:
            self.counts["errors"] += 1
        if sample["transport_error"]:
            self.counts["transport_errors"] += 1
        if sample["status"] == 409:
            self.counts["all_http_409"] += 1
        if sample["durable_write"]:
            self.counts["durable_writes"] += 1
        if sample["method"] == "POST" and sample["route"] == "create" and sample["status"] == 200:
            self.counts["idempotent_create_replays"] += 1
        if sample.get("unauthorized_items", 0):
            self.counts["unauthorized_items"] += sample["unauthorized_items"]
        self.response_bytes += sample["response_bytes"]
        self.item_count += sample["item_count"] or 0

    def merge(self, other):
        self.latency.extend(other.latency)
        self.success_latency.extend(other.success_latency)
        self.counts.update(other.counts)
        self.statuses.update(other.statuses)
        self.codes.update(other.codes)
        self.response_bytes += other.response_bytes
        self.item_count += other.item_count

    def report(self, elapsed):
        return {**{name: self.counts[name] for name in ("requests", "read_requests", "write_requests", "success", "errors", "version_conflicts", "all_http_409", "timeouts", "transport_errors", "durable_writes", "idempotent_create_replays", "unauthorized_items")},
                "actual_write_request_fraction": round(self.counts["write_requests"] / max(1,self.counts["requests"]), 6),
                "total_qps": round(self.counts["requests"] / elapsed, 3), "successful_qps": round(self.counts["success"] / elapsed, 3),
                "actual_success_qps": round(self.counts["success"] / elapsed, 3),
                "durable_write_tps": round(self.counts["durable_writes"] / elapsed, 3),
                "latency_ms": {"p50": percentile(self.latency, .5), "p95": percentile(self.latency, .95), "p99": percentile(self.latency, .99)},
                "successful_latency_ms": {"p50": percentile(self.success_latency, .5), "p95": percentile(self.success_latency, .95), "p99": percentile(self.success_latency, .99)},
                "http_status_counts": dict(self.statuses), "business_code_counts": dict(self.codes),
                "response_bytes": self.response_bytes, "returned_items": self.item_count}


def query(path, **values):
    return path + "?" + urlencode(values, doseq=True)


def run_stage(args, metadata, credentials, run_id, phase, seconds, directory):
    if seconds == 0:
        return {"phase": phase, "admission_window_seconds": 0, "completion_window_seconds": 0, "requests": 0, "skipped": True}
    logger = LogWriter(directory / (phase + "-requests.jsonl"))
    barrier = threading.Barrier(args.concurrency + 1)
    started = [0.0]
    stage_start_at = timestamp()

    def worker(number):
        rng = random.Random(args.seed + number * 1000003 + (0 if phase == "measurement" else 999983))
        client = HTTPWorker(args.url, args.timeout, credentials)
        aggregate, routes = Aggregate(), {}
        sequence, tasks, task_successes = 0, 0, 0
        task_latencies = []
        # Initial stagger spreads admission; no worker can issue two writes less
        # than two seconds apart, including warmup and each mutation attempt.
        next_write = 0.0

        def request(method, path, identity, body=None, route="", task_id=0):
            nonlocal sequence
            sequence += 1
            response = client.request(method, path, identity, body)
            items = response.data.get("items") if isinstance(response.data, dict) else None
            item_count = len(items) if isinstance(items, list) else None
            unauthorized = 0
            if args.scenario == "sparse-auth" and isinstance(items, list):
                allowed = set(metadata["sparse_authorized_ids"])
                unauthorized = sum(item.get("id") not in allowed for item in items)
            durable = method == "POST" and ((route == "create" and response.status == 201) or (route != "create" and response.status == 200)) and not response.code
            sample = {"phase": phase, "worker": number, "sequence": sequence, "task": task_id,
                      "started_offset_seconds": round(time.perf_counter() - started[0] - response.latency_ms / 1000, 6),
                      "method": method, "path": path, "route": route, "identity": identity,
                      "latency_ms": round(response.latency_ms, 6), "status": response.status, "code": response.code,
                      "item_count": item_count, "response_bytes": response.response_bytes, "timeout": response.timeout,
                      "transport_error": response.transport_error, "durable_write": durable, "unauthorized_items": unauthorized}
            if response.request_id:
                sample["request_id"] = response.request_id
            if body is not None:
                sample["request_json"] = body
            aggregate.add(sample)
            routes.setdefault(route, Aggregate()).add(sample)
            logger.put(sample)
            return response

        try:
            barrier.wait()
            deadline = started[0] + seconds
            next_write = started[0] + number / args.concurrency * 2
            while time.perf_counter() < deadline:
                if args.scenario in WRITE_ONLY:
                    wait = next_write - time.perf_counter()
                    if wait > 0:
                        time.sleep(min(wait, max(0, deadline - time.perf_counter())))
                    if time.perf_counter() >= deadline:
                        break
                tasks += 1
                task_started = time.perf_counter()
                response = None
                mixed_write = args.scenario == "mixed-rw" and rng.random() < args.write_fraction and time.perf_counter() >= next_write
                if args.scenario in ("mixed-read", "core-read", "analytics-only", "team-filter", "mixed-rw") and not mixed_write:
                    ticket_id = rng.choice(metadata["ticket_ids"]) if "ticket_ids" in metadata else rng.randint(metadata["id_start"], metadata["id_end"])
                    reads = [(query("/tickets", view="created_by_me", page=1, page_size=20), "requester", "list_created"),
                             (query("/tickets", view="team_pending", page=1, page_size=20), "agent", "list_team"),
                             ("/tickets/" + str(ticket_id), "manager", "detail"),
                             (query("/search/tickets", q=metadata["search_keyword"], page_size=20), "manager", "search_keyword"),
                             (query("/search/tickets", q=metadata["filtered_keyword"], status=["pending", "in_progress"], current_team_id=metadata["hot_team_id"], priority=["high", "medium"], page_size=20), "manager", "search_filtered"),
                             (query("/analytics", start=metadata["analytics_start"], end=metadata["analytics_end"]), "manager", "analytics")]
                    if args.scenario == "analytics-only":
                        reads = reads[-1:]
                    elif args.scenario == "team-filter":
                        reads = reads[4:5]
                    elif args.scenario in ("core-read", "mixed-rw"):
                        reads = reads[:-1]
                    path, identity, route = reads[rng.randrange(len(reads))]
                    response = request("GET", path, identity, route=route, task_id=tasks)
                elif args.scenario == "sparse-auth":
                    path = query("/search/tickets", q=metadata["sparse_query"], page_size=metadata["sparse_page_size"])
                    response = request("GET", path, metadata["sparse_identity"], route="search_sparse_auth", task_id=tasks)
                elif args.scenario == "burst-write" or mixed_write:
                    intent = "m5-" + run_id + "-" + phase[0] + "-" + str(number) + "-" + str(tasks)
                    body = {"request_id": intent, "title": "Benchmark create " + str(args.seed) + " " + str(number),
                            "description": "Isolated reproducible benchmark create request", "category": "network", "priority": "high"}
                    response = request("POST", "/tickets", "requester", body, "create", tasks)
                    next_write = time.perf_counter() + 2
                else:
                    identity = metadata["hot_identity"]
                    # hot-team selects many tickets belonging to one team;
                    # hot-ticket makes every worker contend on one version.
                    team_ids = metadata.get("hot_team_ticket_ids", metadata.get("ticket_ids"))
                    ticket_id = metadata["lifecycle_ticket_ids"][number] if args.scenario == "lifecycle-writes" else (metadata["hot_ticket_id"] if args.scenario == "hot-ticket" else (rng.choice(team_ids) if team_ids else rng.randint(metadata["id_start"], metadata["id_end"])))
                    detail = request("GET", "/tickets/" + str(ticket_id), identity, route="hot_detail", task_id=tasks)
                    response = detail
                    if detail.status == 200 and not detail.code:
                        ticket = detail.data.get("ticket", {})
                        action = "comments"
                        body = {"ticket_id": ticket_id, "expected_version": ticket.get("version"), "content": "M5 benchmark hot write " + run_id + " " + str(number) + " " + str(tasks)}
                        if args.scenario == "lifecycle-writes" or metadata["hot_mode"] == "lifecycle" and args.scenario == "hot-ticket":
                            if ticket.get("status") == "pending":
                                action = "claim" if ticket.get("assignee_id") is None else "start"
                            elif ticket.get("status") == "in_progress":
                                action = "resolve"
                            elif ticket.get("status") == "awaiting_confirmation":
                                action = "reopen"
                                identity = "requester"
                            else:
                                action = None
                            body.pop("content")
                            if action == "resolve":
                                body["resolution_note"] = "Isolated benchmark resolution"
                        if action is not None and time.perf_counter() < deadline:
                            response = request("POST", "/tickets/" + action, identity, body, "hot_" + action, tasks)
                    next_write = time.perf_counter() + 2
                task_latencies.append((time.perf_counter() - task_started) * 1000)
                if response is not None and 200 <= response.status < 300 and not response.code:
                    task_successes += 1
        finally:
            client.close()
        return aggregate, routes, tasks, task_successes, task_latencies

    try:
        with ThreadPoolExecutor(max_workers=args.concurrency, thread_name_prefix="benchmark-http") as pool:
            futures = [pool.submit(worker, number) for number in range(args.concurrency)]
            started[0] = time.perf_counter()
            barrier.wait()
            results = [future.result() for future in futures]
        elapsed = time.perf_counter() - started[0]
    finally:
        logger.close()
    aggregate, routes, tasks, successes, task_latency = Aggregate(), {}, 0, 0, []
    for counts, endpoints, total_tasks, good_tasks, latency in results:
        aggregate.merge(counts)
        for route, values in endpoints.items():
            routes.setdefault(route, Aggregate()).merge(values)
        tasks += total_tasks
        successes += good_tasks
        task_latency.extend(latency)
    report = aggregate.report(elapsed)
    report.update({"phase": phase, "started_at": stage_start_at, "finished_at": timestamp(),
                   "admission_window_seconds": seconds, "completion_window_seconds": round(elapsed, 6),
                   "tasks": tasks, "successful_tasks": successes, "task_tps": round(tasks / elapsed, 3),
                   "successful_task_tps": round(successes / elapsed, 3),
                   "task_latency_ms": {"p50": percentile(task_latency, .5), "p95": percentile(task_latency, .95), "p99": percentile(task_latency, .99)},
                   "endpoints": {route: values.report(elapsed) for route, values in sorted(routes.items())}})
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--tickets", type=int, required=True)
    parser.add_argument("--seed", type=int, default=20261109)
    parser.add_argument("--concurrency", type=int, choices=(10, 50, 200), required=True)
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument("--warmup", type=float, default=3)
    parser.add_argument("--scenario", choices=SCENARIOS, default="mixed-read")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metadata", help="isolated dataset metadata JSON path")
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--run-id", help="optional reproducible intent namespace; do not reuse for independent write runs")
    parser.add_argument("--read-write-ratio", default="90:10", help="mixed-rw target task weights READ:WRITE; write rate remains capped per worker")
    args = parser.parse_args(argv)
    try:
        validate_args(args)
        metadata = metadata_for(args)
        credentials = {alias: os.environ.get("TICKETPULSE_BENCH_" + alias.upper() + "_TOKEN", fixture) for alias, fixture in IDENTITIES.items()}
        run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        args.output.mkdir(parents=True, exist_ok=False)
        config = {"schema": "ticketpulse-http-benchmark/v1", "url": args.url, "database": args.database, "tickets": args.tickets, "seed": args.seed,
                  "concurrency": args.concurrency, "duration_seconds": args.duration, "warmup_seconds": args.warmup,
                  "scenario": args.scenario, "request_timeout_seconds": args.timeout, "run_id": run_id,
                  "read_write_ratio": args.read_write_ratio if args.scenario == "mixed-rw" else None,
                  "mixed_rw_target_write_fraction": args.write_fraction if args.scenario == "mixed-rw" else None,
                  "mixed_rw_read_routes": ["list_created","list_team","detail","search_keyword","search_filtered"] if args.scenario == "mixed-rw" else None,
                  "mixed_rw_ratio_semantics": "target task selection weights; excess write selections become reads until per-worker write-rate cap permits" if args.scenario == "mixed-rw" else None,
                  "metadata": metadata, "identity_aliases": sorted(IDENTITIES), "writer_max_per_worker_per_second": .5,
                  "load_model": "fixed-duration closed-loop workers with per-worker HTTP keep-alive; concurrency is not target QPS",
                  "throughput_denominator": "elapsed phase start through final response completion, including drain",
                  "measurement_result": "real HTTP response timings; source durability requires independent database verification"}
        (args.output / "input.json").write_text(json.dumps(config, indent=2) + "\n")
        warmup = run_stage(args, metadata, credentials, run_id, "warmup", args.warmup, args.output)
        (args.output / "warmup-summary.json").write_text(json.dumps(warmup, indent=2) + "\n")
        # Enforce the same write-rate boundary between phases; warmup and
        # measurement have new connections and independent evidence files.
        if args.warmup and args.scenario not in READ_ONLY:
            time.sleep(2)
        measured = run_stage(args, metadata, credentials, run_id, "measurement", args.duration, args.output)
        result = {"config": config, "measurement": measured, "warmup": warmup,
                  "durability_definition": "HTTP 201 creates and HTTP 200 mutation acknowledgements, independently verify committed rows before concluding source durability",
                  "unverified_write_outcomes": sum(values["timeouts"]+values["transport_errors"] for route,values in measured["endpoints"].items() if route=="create" or route.startswith("hot_") and route!="hot_detail")}
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"scenario": args.scenario, "concurrency": args.concurrency,
                          "requests": measured["requests"], "successful_qps": measured["successful_qps"],
                          "durable_write_tps": measured["durable_write_tps"], "errors": measured["errors"],
                          "version_conflicts": measured["version_conflicts"], "timeouts": measured["timeouts"],
                          "latency_ms": measured["latency_ms"], "output": str(args.output)}))
        return 1 if measured["unauthorized_items"] else 0
    except (ValueError, OSError, RuntimeError) as error:
        parser.exit(2, "benchmark: " + str(error) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
