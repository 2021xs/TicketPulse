"""Local fake-server checks verify the driver, never produce performance evidence."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

import bench_http as bench


class FakeAPI(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def emit(self, code, body):
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        if self.path.endswith("/slow"):
            time.sleep(.03)
        path=urlsplit(self.path).path
        if path.startswith("/api/v1/tickets/"):
            self.emit(200, {"ticket": {"id": int(path.rsplit("/",1)[-1]), "version": 1, "status": "in_progress", "assignee_id": 3}})
        else:
            self.emit(200, {"items": [{"id": 1}]})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path.endswith("/comments"):
            self.emit(409, {"error": {"code": "VERSION_CONFLICT"}})
        elif self.path.endswith("/resolve"):
            if set(body)!={"ticket_id","expected_version","resolution_note"}:
                self.emit(400,{"error":{"code":"INVALID_ARGUMENT"}})
            else:
                self.emit(200,{"ticket":{"id":body["ticket_id"],"version":2,"status":"awaiting_confirmation"}})
        else:
            self.emit(201, {"ticket": {"id": 99, "request": body["request_id"]}})


class DriverCorrectnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), FakeAPI)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = "http://127.0.0.1:" + str(cls.server.server_port)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def test_keep_alive_timeout_and_conflict_are_distinct(self):
        client = bench.HTTPWorker(self.url, 1, {"agent": "fake-secret"})
        try:
            first = client.request("GET", "/tickets/1", "agent")
            connection = client.connection
            second = client.request("GET", "/tickets/1", "agent")
            self.assertIs(client.connection, connection)
            self.assertEqual((first.status, second.status), (200, 200))
            client.close()
            client.timeout = .01
            timed = client.request("GET", "/slow", "agent")
            self.assertTrue(timed.timeout)
            self.assertEqual(timed.code, "CLIENT_TIMEOUT")
            self.assertIsNone(client.connection)
            client.timeout = 1
            conflict = client.request("POST", "/tickets/comments", "agent", {"ticket_id": 1, "expected_version": 1, "content": "fake"})
            self.assertEqual((conflict.status, conflict.code), (409, "VERSION_CONFLICT"))
            self.assertFalse(conflict.timeout)
        finally:
            client.close()

    def test_fixed_window_logs_and_write_intents(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = root / "metadata.json"
            metadata.write_text(json.dumps({"database": "ticketpulse_bench_fake", "id_start": 1, "id_end": 1,
                                            "hot_ticket_id": 1, "hot_team_ticket_ids": [1], "sparse_query": "sparse",
                                            "sparse_authorized_ids": [1],"lifecycle_ticket_ids":list(range(1,11))}))
            secrets = {"TICKETPULSE_BENCH_" + alias.upper() + "_TOKEN": "fake-test-secret-" + alias for alias in bench.IDENTITIES}
            for scenario in bench.SCENARIOS:
                output = root / scenario
                with patch.dict(os.environ, secrets), redirect_stdout(io.StringIO()):
                    code = bench.main(["--url", self.url, "--database", "ticketpulse_bench_fake", "--tickets", "1",
                                       "--concurrency", "10", "--duration", ".15", "--warmup", "0",
                                       "--scenario", scenario, "--metadata", str(metadata), "--run-id", "fake-check", "--output", str(output),
                                       "--read-write-ratio","0:100" if scenario=="mixed-rw" else "90:10"])
                self.assertEqual(code, 0)
                result = json.loads((output / "result.json").read_text())
                samples = [json.loads(line) for line in (output / "measurement-requests.jsonl").read_text().splitlines()]
                self.assertEqual(result["measurement"]["requests"], len(samples))
                self.assertGreater(len(samples), 0)
                self.assertEqual(result["warmup"]["requests"], 0)
                for file in output.iterdir():
                    self.assertNotIn("fake-test-secret", file.read_text())
                if scenario == "burst-write":
                    intents = [s["request_json"]["request_id"] for s in samples]
                    self.assertEqual(len(intents), len(set(intents)))
                    self.assertEqual(result["measurement"]["durable_writes"], len(samples))
                if scenario in ("hot-ticket", "hot-team"):
                    self.assertGreater(result["measurement"]["version_conflicts"], 0)
                    self.assertEqual(result["measurement"]["durable_writes"], 0)
                if scenario in bench.READ_ONLY:
                    self.assertTrue(all(s["method"]=="GET" for s in samples))
                if scenario in ("core-read","mixed-rw"):
                    self.assertNotIn("analytics",result["measurement"]["endpoints"])
                if scenario=="analytics-only":
                    self.assertEqual(set(result["measurement"]["endpoints"]),{"analytics"})
                if scenario=="team-filter":
                    self.assertEqual(set(result["measurement"]["endpoints"]),{"search_filtered"})
                    self.assertTrue(all("current_team_id=10" in s["path"] for s in samples))
                if scenario=="mixed-rw":
                    self.assertGreater(result["measurement"]["write_requests"],0)
                    self.assertGreater(result["measurement"]["read_requests"],0)
                if scenario=="lifecycle-writes":
                    writes=[s for s in samples if s["method"]=="POST"]
                    self.assertTrue(all(s["route"]=="hot_resolve" and s["request_json"]["ticket_id"]==s["worker"]+1 for s in writes))
                    self.assertGreater(result["measurement"]["durable_writes"],0)

    def test_budget_and_demo_target_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            for changed in [("--url", "http://127.0.0.1:18080"), ("--duration", "31"), ("--database", "ticketpulse_local")]:
                args = ["--url", self.url, "--database", "ticketpulse_bench_fake", "--tickets", "1", "--concurrency", "10", "--output", str(Path(directory) / "rejected")]
                args.extend(changed)
                with self.assertRaises(SystemExit) as error:
                    bench.main(args)
                self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
