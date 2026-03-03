from __future__ import annotations

import threading
import time
import unittest

from converter.daemon import ConverterDaemon, PollingJob
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
        self.outbox_calls: list[tuple[str, int]] = []

    def run(self, job: SyncJob) -> SyncOutcome:
        with self._lock:
            self.jobs.append(job)
        return SyncOutcome(
            batches=1,
            total_processed=7,
            cursor_ingested_at=None,
            cursor_product_id=None,
        )

    def process_storage_delete_outbox(self, catalog_db: str, *, limit: int = 200) -> dict[str, int]:
        with self._lock:
            self.outbox_calls.append((catalog_db, int(limit)))
        return {"processed": 0, "deleted": 0, "failed": 0}

    def size(self) -> int:
        with self._lock:
            return len(self.jobs)


class _FlakySyncService(_FakeSyncService):
    def __init__(self) -> None:
        super().__init__()
        self._attempt = 0

    def run(self, job: SyncJob) -> SyncOutcome:
        self._attempt += 1
        if self._attempt == 1:
            raise RuntimeError("boom")
        return super().run(job)


class ConverterDaemonTests(unittest.TestCase):
    def test_poller_runs_cycles_and_drains_outbox(self) -> None:
        fake = _FakeSyncService()
        daemon = ConverterDaemon(
            sync_service=fake,
            job=PollingJob(
                receiver_db="/tmp/receiver.db",
                catalog_db="/tmp/catalog.db",
                parser_name="fixprice",
                write_chunk_size=31,
            ),
            poll_interval_sec=0.05,
        )

        daemon.start()
        try:
            self.assertTrue(_wait_until(lambda: fake.size() >= 2))
            snapshot = daemon.snapshot()
            self.assertGreaterEqual(snapshot["cycles_success"], 2)
            self.assertEqual(snapshot["cycles_failed"], 0)
            self.assertGreaterEqual(snapshot["total_processed"], 14)
            self.assertEqual(fake.jobs[0].write_chunk_size, 31)
            self.assertGreaterEqual(len(fake.outbox_calls), 1)
        finally:
            daemon.stop()

    def test_poller_recovers_after_cycle_error(self) -> None:
        fake = _FlakySyncService()
        daemon = ConverterDaemon(
            sync_service=fake,
            job=PollingJob(
                receiver_db="/tmp/receiver.db",
                catalog_db="/tmp/catalog.db",
                parser_name="fixprice",
            ),
            poll_interval_sec=0.05,
        )

        daemon.start()
        try:
            self.assertTrue(_wait_until(lambda: fake.size() >= 1))
            snapshot = daemon.snapshot()
            self.assertGreaterEqual(snapshot["cycles_failed"], 1)
            self.assertGreaterEqual(snapshot["cycles_success"], 1)
            self.assertIsNotNone(snapshot["last_success_at"])
        finally:
            daemon.stop()


if __name__ == "__main__":
    unittest.main()
