from __future__ import annotations

import json
import threading
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from converter.daemon import ConverterDaemon, ConverterDaemonHTTPServer, ConverterDaemonRequestHandler, QueueJob
from converter.sync import SyncJob, SyncOutcome


def _wait_until(predicate, *, timeout_sec: float = 2.0, sleep_sec: float = 0.02) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(sleep_sec)
    return predicate()


class _FakeSyncService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.jobs: list[SyncJob] = []

    def run(self, job: SyncJob) -> SyncOutcome:
        with self._lock:
            self.jobs.append(job)
        return SyncOutcome(
            batches=1,
            total_processed=0,
            cursor_ingested_at=None,
            cursor_product_id=None,
        )

    def size(self) -> int:
        with self._lock:
            return len(self.jobs)


class ConverterDaemonTests(unittest.TestCase):
    def test_queue_deduplicates_same_job_key(self) -> None:
        fake = _FakeSyncService()
        daemon = ConverterDaemon(sync_service=fake, max_queue_size=10)
        daemon.start()
        try:
            first = daemon.enqueue(
                QueueJob(
                    receiver_db="/tmp/receiver.db",
                    catalog_db="/tmp/catalog.db",
                    parser_name="fixprice",
                )
            )
            second = daemon.enqueue(
                QueueJob(
                    receiver_db="/tmp/receiver.db",
                    catalog_db="/tmp/catalog.db",
                    parser_name="fixprice",
                )
            )

            self.assertTrue(first.accepted)
            self.assertEqual(first.reason, "accepted")

            self.assertFalse(second.accepted)
            self.assertTrue(second.duplicate)
            self.assertEqual(second.reason, "duplicate")

            self.assertTrue(_wait_until(lambda: fake.size() == 1))
            snapshot = daemon.snapshot()
            self.assertEqual(snapshot["total_processed"], 1)
        finally:
            daemon.stop()

    def test_http_trigger_requires_token_and_enqueues_job(self) -> None:
        fake = _FakeSyncService()
        daemon = ConverterDaemon(sync_service=fake, max_queue_size=10)
        daemon.start()
        server = ConverterDaemonHTTPServer(
            ("127.0.0.1", 0),
            ConverterDaemonRequestHandler,
            daemon=daemon,
            default_receiver_db="/tmp/receiver.db",
            default_catalog_db="/tmp/catalog.db",
            auth_token="test-token",
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        try:
            host, port = server.server_address
            base_url = f"http://{host}:{port}"

            with self.assertRaises(HTTPError) as auth_error:
                _post_json(
                    f"{base_url}/trigger",
                    payload={"parser_name": "fixprice", "run_id": "run-1"},
                    headers={},
                )
            self.assertEqual(auth_error.exception.code, 401)

            status, body = _post_json(
                f"{base_url}/trigger",
                payload={"parser_name": "fixprice", "run_id": "run-1"},
                headers={"Authorization": "Bearer test-token"},
            )
            self.assertEqual(status, 202)
            self.assertTrue(body.get("accepted"))
            self.assertEqual(body.get("reason"), "accepted")

            self.assertTrue(_wait_until(lambda: fake.size() == 1))
            self.assertEqual(fake.jobs[0].parser_name, "fixprice")
            self.assertEqual(fake.jobs[0].receiver_db, "/tmp/receiver.db")
            self.assertEqual(fake.jobs[0].catalog_db, "/tmp/catalog.db")
        finally:
            server.shutdown()
            thread.join(timeout=2.0)
            server.server_close()
            daemon.stop()


def _post_json(url: str, *, payload: dict[str, object], headers: dict[str, str]) -> tuple[int, dict[str, object]]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(url=url, data=data, method="POST")
    request.add_header("Content-Type", "application/json")
    for key, value in headers.items():
        request.add_header(key, value)

    with urlopen(request, timeout=2.0) as response:
        status = int(response.status)
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise AssertionError("response is not JSON object")
    return status, parsed


if __name__ == "__main__":
    unittest.main()
