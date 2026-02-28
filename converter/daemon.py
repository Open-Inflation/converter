from __future__ import annotations

import json
import logging
import queue
import threading
from dataclasses import asdict, dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .sync import ConverterSyncService, SyncJob, SyncOutcome


LOGGER = logging.getLogger(__name__)
_STOP_SENTINEL = object()


@dataclass(frozen=True, slots=True)
class QueueJob:
    receiver_db: str
    catalog_db: str
    parser_name: str = "fixprice"
    batch_size: int = 250
    max_batches: int = 0
    run_id: str | None = None
    source: str = "receiver"

    def dedupe_key(self) -> tuple[str, str, str]:
        return (
            self.receiver_db.strip(),
            self.catalog_db.strip(),
            self.parser_name.strip().lower() or "fixprice",
        )

    def to_sync_job(self) -> SyncJob:
        return SyncJob(
            receiver_db=self.receiver_db,
            catalog_db=self.catalog_db,
            parser_name=self.parser_name,
            batch_size=self.batch_size,
            max_batches=self.max_batches,
        )


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    accepted: bool
    duplicate: bool
    reason: str
    queue_size: int
    key: tuple[str, str, str]


@dataclass(frozen=True, slots=True)
class JobExecution:
    job: QueueJob
    success: bool
    error: str | None
    outcome: SyncOutcome | None


class ConverterDaemon:
    def __init__(
        self,
        *,
        sync_service: ConverterSyncService | None = None,
        max_queue_size: int = 100,
    ) -> None:
        self._sync_service = sync_service or ConverterSyncService()
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max(1, int(max_queue_size)))
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None

        self._pending_keys: set[tuple[str, str, str]] = set()
        self._active_keys: set[tuple[str, str, str]] = set()

        self._total_enqueued = 0
        self._total_duplicates = 0
        self._total_processed = 0
        self._total_failed = 0

    def start(self) -> None:
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                return
            self._stop_event.clear()
            self._worker = threading.Thread(target=self._worker_loop, name="converter-worker", daemon=True)
            self._worker.start()

    def stop(self, *, timeout: float = 10.0) -> None:
        self._stop_event.set()
        try:
            self._queue.put_nowait(_STOP_SENTINEL)
        except queue.Full:
            pass

        worker = self._worker
        if worker is not None:
            worker.join(timeout=max(0.1, float(timeout)))

    def enqueue(self, job: QueueJob) -> EnqueueResult:
        key = job.dedupe_key()

        with self._lock:
            if key in self._pending_keys or key in self._active_keys:
                self._total_duplicates += 1
                return EnqueueResult(
                    accepted=False,
                    duplicate=True,
                    reason="duplicate",
                    queue_size=self._queue.qsize(),
                    key=key,
                )
            self._pending_keys.add(key)

        try:
            self._queue.put_nowait(job)
        except queue.Full:
            with self._lock:
                self._pending_keys.discard(key)
                return EnqueueResult(
                    accepted=False,
                    duplicate=False,
                    reason="queue_full",
                    queue_size=self._queue.qsize(),
                    key=key,
                )

        with self._lock:
            self._total_enqueued += 1
            queue_size = self._queue.qsize()
        return EnqueueResult(
            accepted=True,
            duplicate=False,
            reason="accepted",
            queue_size=queue_size,
            key=key,
        )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "running": bool(self._worker is not None and self._worker.is_alive()),
                "queue_size": self._queue.qsize(),
                "active_jobs": len(self._active_keys),
                "pending_jobs": len(self._pending_keys),
                "total_enqueued": self._total_enqueued,
                "total_duplicates": self._total_duplicates,
                "total_processed": self._total_processed,
                "total_failed": self._total_failed,
            }

    def _worker_loop(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=0.25)
            except queue.Empty:
                if self._stop_event.is_set():
                    return
                continue

            if item is _STOP_SENTINEL:
                self._queue.task_done()
                return

            job = item
            if not isinstance(job, QueueJob):
                self._queue.task_done()
                continue

            key = job.dedupe_key()
            with self._lock:
                self._pending_keys.discard(key)
                self._active_keys.add(key)

            execution = self._run_job(job)
            if execution.success:
                LOGGER.info(
                    "Queue job done: parser=%s run_id=%s batches=%s processed=%s",
                    job.parser_name,
                    job.run_id,
                    execution.outcome.batches if execution.outcome else 0,
                    execution.outcome.total_processed if execution.outcome else 0,
                )
            else:
                LOGGER.error(
                    "Queue job failed: parser=%s run_id=%s error=%s",
                    job.parser_name,
                    job.run_id,
                    execution.error,
                )

            with self._lock:
                self._active_keys.discard(key)
                if execution.success:
                    self._total_processed += 1
                else:
                    self._total_failed += 1

            self._queue.task_done()

    def _run_job(self, job: QueueJob) -> JobExecution:
        try:
            outcome = self._sync_service.run(job.to_sync_job())
            return JobExecution(job=job, success=True, error=None, outcome=outcome)
        except Exception as exc:
            return JobExecution(job=job, success=False, error=str(exc), outcome=None)


class ConverterDaemonHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        request_handler_cls: type[BaseHTTPRequestHandler],
        *,
        daemon: ConverterDaemon,
        default_receiver_db: str | None = None,
        default_catalog_db: str | None = None,
        default_parser_name: str = "fixprice",
        default_batch_size: int = 250,
        default_max_batches: int = 0,
        auth_token: str | None = None,
    ) -> None:
        super().__init__(server_address, request_handler_cls)
        self.converter_daemon = daemon
        self.default_receiver_db = (default_receiver_db or "").strip() or None
        self.default_catalog_db = (default_catalog_db or "").strip() or None
        self.default_parser_name = default_parser_name.strip() or "fixprice"
        self.default_batch_size = max(1, int(default_batch_size))
        self.default_max_batches = max(0, int(default_max_batches))
        self.auth_token = (auth_token or "").strip() or None


class ConverterDaemonRequestHandler(BaseHTTPRequestHandler):
    server: ConverterDaemonHTTPServer

    def do_GET(self) -> None:  # noqa: N802
        if self.path.rstrip("/") in {"/health", "/queue"}:
            self._send_json(HTTPStatus.OK, self.server.converter_daemon.snapshot())
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path.rstrip("/") not in {"/trigger", "/enqueue"}:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return

        auth_error = self._check_auth()
        if auth_error is not None:
            self._send_json(HTTPStatus.UNAUTHORIZED, {"error": auth_error})
            return

        try:
            payload = self._read_json()
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        receiver_db = _first_non_empty(payload.get("receiver_db"), self.server.default_receiver_db)
        catalog_db = _first_non_empty(payload.get("catalog_db"), self.server.default_catalog_db)
        parser_name = _first_non_empty(payload.get("parser_name"), self.server.default_parser_name) or "fixprice"
        run_id = _first_non_empty(payload.get("run_id"), None)
        source = _first_non_empty(payload.get("source"), "receiver") or "receiver"

        if receiver_db is None:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "receiver_db is required"})
            return
        if catalog_db is None:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "catalog_db is required"})
            return

        batch_size = _to_int(payload.get("batch_size"), default=self.server.default_batch_size, minimum=1)
        max_batches = _to_int(payload.get("max_batches"), default=self.server.default_max_batches, minimum=0)

        job = QueueJob(
            receiver_db=receiver_db,
            catalog_db=catalog_db,
            parser_name=parser_name,
            batch_size=batch_size,
            max_batches=max_batches,
            run_id=run_id,
            source=source,
        )
        result = self.server.converter_daemon.enqueue(job)

        status = HTTPStatus.ACCEPTED
        if result.reason == "queue_full":
            status = HTTPStatus.TOO_MANY_REQUESTS

        self._send_json(
            status,
            {
                "accepted": result.accepted,
                "duplicate": result.duplicate,
                "reason": result.reason,
                "queue_size": result.queue_size,
                "key": list(result.key),
                "job": asdict(job),
            },
        )

    def log_message(self, fmt: str, *args: object) -> None:
        LOGGER.debug("HTTP %s - %s", self.address_string(), fmt % args)

    def _check_auth(self) -> str | None:
        token = self.server.auth_token
        if token is None:
            return None

        auth_header = (self.headers.get("Authorization") or "").strip()
        if auth_header.lower().startswith("bearer "):
            supplied = auth_header[7:].strip()
            if supplied == token:
                return None

        supplied = (self.headers.get("X-Converter-Token") or "").strip()
        if supplied == token:
            return None

        return "invalid_token"

    def _read_json(self) -> dict[str, Any]:
        raw_length = (self.headers.get("Content-Length") or "").strip()
        if not raw_length:
            return {}

        try:
            length = max(0, int(raw_length))
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc

        body = self.rfile.read(length)
        if not body:
            return {}
        try:
            parsed = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("request body must be valid JSON") from exc
        if not isinstance(parsed, dict):
            raise ValueError("request body must be a JSON object")
        return parsed

    def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def _first_non_empty(value: Any, fallback: str | None) -> str | None:
    token = _safe_str(value)
    if token is not None:
        return token
    return fallback


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _to_int(value: Any, *, default: int, minimum: int) -> int:
    if value is None:
        return max(minimum, int(default))
    if isinstance(value, bool):
        return max(minimum, int(default))
    if isinstance(value, int):
        return max(minimum, value)
    if isinstance(value, float):
        return max(minimum, int(value))
    token = _safe_str(value)
    if token is None:
        return max(minimum, int(default))
    try:
        return max(minimum, int(token))
    except ValueError:
        return max(minimum, int(default))
