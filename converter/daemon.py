from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from time import monotonic
from typing import Any

from .sync import ConverterSyncService, SyncJob


LOGGER = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class PollingJob:
    receiver_db: str
    catalog_db: str
    parser_name: str = "fixprice"
    receiver_fetch_size: int = 2000
    write_chunk_size: int = 1000
    sync_version: str = "v2"
    writer_mode: str = "mysql_v2"
    max_batches: int = 0

    def to_sync_job(self) -> SyncJob:
        return SyncJob(
            receiver_db=self.receiver_db,
            catalog_db=self.catalog_db,
            parser_name=self.parser_name,
            receiver_fetch_size=self.receiver_fetch_size,
            write_chunk_size=self.write_chunk_size,
            sync_version=self.sync_version,
            writer_mode=self.writer_mode,
            max_batches=self.max_batches,
        )


class ConverterDaemon:
    def __init__(
        self,
        *,
        job: PollingJob,
        sync_service: ConverterSyncService | None = None,
        poll_interval_sec: float = 5.0,
        outbox_drain_limit: int = 200,
    ) -> None:
        self._job = job
        self._sync_service = sync_service or ConverterSyncService()
        self._poll_interval_sec = max(0.1, float(poll_interval_sec))
        self._outbox_drain_limit = max(1, int(outbox_drain_limit))

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None

        self._cycles_total = 0
        self._cycles_success = 0
        self._cycles_failed = 0
        self._total_processed = 0
        self._total_batches = 0

        self._last_started_at: datetime | None = None
        self._last_finished_at: datetime | None = None
        self._last_success_at: datetime | None = None
        self._last_error: str | None = None

    def start(self) -> None:
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                LOGGER.debug("Converter daemon start skipped: worker already running")
                return
            self._stop_event.clear()
            self._worker = threading.Thread(target=self._worker_loop, name="converter-poller", daemon=True)
            self._worker.start()
            LOGGER.info(
                "Converter daemon poller started: parser=%s poll_interval_sec=%.2f receiver_fetch_size=%s write_chunk_size=%s max_batches=%s",
                self._job.parser_name,
                self._poll_interval_sec,
                self._job.receiver_fetch_size,
                self._job.write_chunk_size,
                self._job.max_batches,
            )

    def stop(self, *, timeout: float = 10.0) -> None:
        LOGGER.info("Converter daemon stop requested: timeout_sec=%.2f", max(0.1, float(timeout)))
        self._stop_event.set()

        worker = self._worker
        if worker is not None:
            worker.join(timeout=max(0.1, float(timeout)))
            if worker.is_alive():
                LOGGER.warning("Converter daemon poller did not stop before timeout")
            else:
                LOGGER.info("Converter daemon poller stopped")

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "running": bool(self._worker is not None and self._worker.is_alive()),
                "poll_interval_sec": self._poll_interval_sec,
                "parser_name": self._job.parser_name,
                "receiver_fetch_size": self._job.receiver_fetch_size,
                "write_chunk_size": self._job.write_chunk_size,
                "sync_version": self._job.sync_version,
                "writer_mode": self._job.writer_mode,
                "max_batches": self._job.max_batches,
                "cycles_total": self._cycles_total,
                "cycles_success": self._cycles_success,
                "cycles_failed": self._cycles_failed,
                "total_processed": self._total_processed,
                "total_batches": self._total_batches,
                "last_started_at": None if self._last_started_at is None else self._last_started_at.isoformat(),
                "last_finished_at": None if self._last_finished_at is None else self._last_finished_at.isoformat(),
                "last_success_at": None if self._last_success_at is None else self._last_success_at.isoformat(),
                "last_error": self._last_error,
            }

    def _worker_loop(self) -> None:
        LOGGER.info("Converter daemon poller loop started")
        while not self._stop_event.is_set():
            cycle_started = monotonic()
            self._run_cycle()
            elapsed = monotonic() - cycle_started
            sleep_for = max(0.0, self._poll_interval_sec - elapsed)
            if sleep_for > 0:
                self._stop_event.wait(timeout=sleep_for)
        LOGGER.info("Converter daemon poller loop finished")

    def _run_cycle(self) -> None:
        started_at = _utc_now()
        with self._lock:
            self._cycles_total += 1
            cycle_no = self._cycles_total
            self._last_started_at = started_at

        LOGGER.debug("Polling cycle started: cycle=%s parser=%s", cycle_no, self._job.parser_name)
        started = monotonic()

        try:
            outcome = self._sync_service.run(self._job.to_sync_job())
            self._drain_storage_outbox()

            finished_at = _utc_now()
            with self._lock:
                self._cycles_success += 1
                self._total_processed += int(outcome.total_processed)
                self._total_batches += int(outcome.batches)
                self._last_success_at = finished_at
                self._last_finished_at = finished_at
                self._last_error = None

            LOGGER.info(
                "Polling cycle finished: cycle=%s success=true batches=%s processed=%s elapsed_sec=%.3f",
                cycle_no,
                outcome.batches,
                outcome.total_processed,
                monotonic() - started,
            )
        except Exception as exc:
            finished_at = _utc_now()
            with self._lock:
                self._cycles_failed += 1
                self._last_finished_at = finished_at
                self._last_error = str(exc)

            LOGGER.exception(
                "Polling cycle failed: cycle=%s elapsed_sec=%.3f",
                cycle_no,
                monotonic() - started,
            )

    def _drain_storage_outbox(self) -> None:
        drain_outbox = getattr(self._sync_service, "process_storage_delete_outbox", None)
        if not callable(drain_outbox):
            return

        try:
            outbox_result = drain_outbox(self._job.catalog_db, limit=self._outbox_drain_limit)
        except Exception:
            LOGGER.exception("Storage delete outbox drain failed")
            return

        LOGGER.debug(
            "Storage delete outbox drained: processed=%s deleted=%s failed=%s",
            None if outbox_result is None else outbox_result.get("processed"),
            None if outbox_result is None else outbox_result.get("deleted"),
            None if outbox_result is None else outbox_result.get("failed"),
        )
