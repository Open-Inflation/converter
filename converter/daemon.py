from __future__ import annotations

import json
import logging
import queue
import threading
from dataclasses import asdict, dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import monotonic
from typing import Any

from .sync import ConverterSyncService, SyncJob, SyncOutcome


LOGGER = logging.getLogger(__name__)
_STOP_SENTINEL = object()


@dataclass(frozen=True, slots=True)
class QueueJob:
    receiver_db: str
    catalog_db: str
    parser_name: str = "fixprice"
    receiver_fetch_size: int = 2000
    write_chunk_size: int = 1000
    sync_version: str = "v2"
    writer_mode: str = "mysql_v2"
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
            receiver_fetch_size=self.receiver_fetch_size,
            write_chunk_size=self.write_chunk_size,
            sync_version=self.sync_version,
            writer_mode=self.writer_mode,
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
        self._max_queue_size = self._queue.maxsize
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
                LOGGER.debug("Converter daemon start skipped: worker already running")
                return
            self._stop_event.clear()
            self._worker = threading.Thread(target=self._worker_loop, name="converter-worker", daemon=True)
            self._worker.start()
            LOGGER.info("Converter daemon worker started: max_queue_size=%s", self._max_queue_size)

    def stop(self, *, timeout: float = 10.0) -> None:
        LOGGER.info("Converter daemon stop requested: timeout_sec=%.2f", max(0.1, float(timeout)))
        self._stop_event.set()
        try:
            self._queue.put_nowait(_STOP_SENTINEL)
        except queue.Full:
            LOGGER.debug("Converter daemon stop sentinel was not queued: queue is full")

        worker = self._worker
        if worker is not None:
            worker.join(timeout=max(0.1, float(timeout)))
            if worker.is_alive():
                LOGGER.warning("Converter daemon worker did not stop before timeout")
            else:
                LOGGER.info("Converter daemon worker stopped")

    def enqueue(self, job: QueueJob) -> EnqueueResult:
        key = job.dedupe_key()
        LOGGER.debug(
            "Queue enqueue requested: key=%s run_id=%s source=%s sync_version=%s writer_mode=%s receiver_fetch_size=%s write_chunk_size=%s max_batches=%s",
            key,
            job.run_id,
            job.source,
            job.sync_version,
            job.writer_mode,
            job.receiver_fetch_size,
            job.write_chunk_size,
            job.max_batches,
        )

        with self._lock:
            if key in self._pending_keys or key in self._active_keys:
                self._total_duplicates += 1
                LOGGER.info("Queue duplicate rejected: key=%s queue_size=%s", key, self._queue.qsize())
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
                LOGGER.warning("Queue full, job rejected: key=%s queue_size=%s", key, self._queue.qsize())
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
        LOGGER.info("Queue job accepted: key=%s queue_size=%s", key, queue_size)
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
        LOGGER.info("Converter worker loop started")
        while True:
            try:
                item = self._queue.get(timeout=0.25)
            except queue.Empty:
                if self._stop_event.is_set():
                    LOGGER.info("Converter worker loop stopping after stop_event")
                    return
                continue

            if item is _STOP_SENTINEL:
                self._queue.task_done()
                LOGGER.info("Converter worker received stop sentinel")
                return

            job = item
            if not isinstance(job, QueueJob):
                self._queue.task_done()
                LOGGER.warning("Converter worker skipped unexpected queue item type: %s", type(job).__name__)
                continue

            key = job.dedupe_key()
            with self._lock:
                self._pending_keys.discard(key)
                self._active_keys.add(key)
                queue_size = self._queue.qsize()
            LOGGER.info(
                "Queue job started: key=%s run_id=%s parser=%s queue_size=%s",
                key,
                job.run_id,
                job.parser_name,
                queue_size,
            )

            execution = self._run_job(job)
            if execution.success:
                LOGGER.info(
                    "Queue job done: key=%s parser=%s run_id=%s batches=%s processed=%s",
                    key,
                    job.parser_name,
                    job.run_id,
                    execution.outcome.batches if execution.outcome else 0,
                    execution.outcome.total_processed if execution.outcome else 0,
                )
            else:
                LOGGER.error(
                    "Queue job failed: key=%s parser=%s run_id=%s error=%s",
                    key,
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
                pending = len(self._pending_keys)
                active = len(self._active_keys)
                queue_size = self._queue.qsize()
            LOGGER.debug(
                "Queue job finalized: key=%s queue_size=%s pending=%s active=%s processed=%s failed=%s",
                key,
                queue_size,
                pending,
                active,
                self._total_processed,
                self._total_failed,
            )

            self._queue.task_done()

    def _run_job(self, job: QueueJob) -> JobExecution:
        started_at = monotonic()
        LOGGER.info(
            "Sync job execution started: parser=%s run_id=%s sync_version=%s writer_mode=%s receiver_fetch_size=%s write_chunk_size=%s max_batches=%s",
            job.parser_name,
            job.run_id,
            job.sync_version,
            job.writer_mode,
            job.receiver_fetch_size,
            job.write_chunk_size,
            job.max_batches,
        )
        try:
            outcome = self._sync_service.run(job.to_sync_job())
            outbox_result: dict[str, int] | None = None
            drain_outbox = getattr(self._sync_service, "process_storage_delete_outbox", None)
            if callable(drain_outbox):
                try:
                    outbox_result = drain_outbox(job.catalog_db, limit=200)
                except Exception:
                    LOGGER.exception(
                        "Storage delete outbox drain failed: parser=%s run_id=%s",
                        job.parser_name,
                        job.run_id,
                    )
            LOGGER.info(
                "Sync job execution finished: parser=%s run_id=%s success=true batches=%s processed=%s elapsed_sec=%.3f outbox_processed=%s outbox_deleted=%s outbox_failed=%s",
                job.parser_name,
                job.run_id,
                outcome.batches,
                outcome.total_processed,
                monotonic() - started_at,
                None if outbox_result is None else outbox_result.get("processed"),
                None if outbox_result is None else outbox_result.get("deleted"),
                None if outbox_result is None else outbox_result.get("failed"),
            )
            return JobExecution(job=job, success=True, error=None, outcome=outcome)
        except Exception as exc:
            LOGGER.exception(
                "Sync job execution failed: parser=%s run_id=%s elapsed_sec=%.3f",
                job.parser_name,
                job.run_id,
                monotonic() - started_at,
            )
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
        default_receiver_fetch_size: int = 2000,
        default_write_chunk_size: int = 1000,
        default_sync_version: str = "v2",
        default_writer_mode: str = "mysql_v2",
        default_max_batches: int = 0,
        auth_token: str | None = None,
    ) -> None:
        super().__init__(server_address, request_handler_cls)
        self.converter_daemon = daemon
        self.default_receiver_db = (default_receiver_db or "").strip() or None
        self.default_catalog_db = (default_catalog_db or "").strip() or None
        self.default_parser_name = default_parser_name.strip() or "fixprice"
        self.default_receiver_fetch_size = max(1, int(default_receiver_fetch_size))
        self.default_write_chunk_size = max(1, int(default_write_chunk_size))
        self.default_sync_version = (default_sync_version or "").strip() or "v2"
        self.default_writer_mode = (default_writer_mode or "").strip() or "mysql_v2"
        self.default_max_batches = max(0, int(default_max_batches))
        self.auth_token = (auth_token or "").strip() or None


class ConverterDaemonRequestHandler(BaseHTTPRequestHandler):
    server: ConverterDaemonHTTPServer

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.rstrip("/")
        if path in {"/health", "/queue"}:
            LOGGER.debug("HTTP GET %s from %s", path, self.address_string())
            self._send_json(HTTPStatus.OK, self.server.converter_daemon.snapshot())
            return
        LOGGER.warning("HTTP GET unknown path: path=%s client=%s", path, self.address_string())
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.rstrip("/")
        if path not in {"/trigger", "/enqueue"}:
            LOGGER.warning("HTTP POST unknown path: path=%s client=%s", path, self.address_string())
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        LOGGER.debug("HTTP POST %s from %s", path, self.address_string())

        auth_error = self._check_auth()
        if auth_error is not None:
            LOGGER.warning("HTTP auth failed: path=%s client=%s reason=%s", path, self.address_string(), auth_error)
            self._send_json(HTTPStatus.UNAUTHORIZED, {"error": auth_error})
            return

        try:
            payload = self._read_json()
        except ValueError as exc:
            LOGGER.warning("HTTP invalid payload: path=%s client=%s error=%s", path, self.address_string(), exc)
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        receiver_db = _first_non_empty(payload.get("receiver_db"), self.server.default_receiver_db)
        catalog_db = _first_non_empty(payload.get("catalog_db"), self.server.default_catalog_db)
        parser_name = _first_non_empty(payload.get("parser_name"), self.server.default_parser_name) or "fixprice"
        run_id = _first_non_empty(payload.get("run_id"), None)
        source = _first_non_empty(payload.get("source"), "receiver") or "receiver"
        sync_version = _first_non_empty(payload.get("sync_version"), self.server.default_sync_version) or "v2"
        writer_mode = _first_non_empty(payload.get("writer_mode"), self.server.default_writer_mode) or "mysql_v2"
        if sync_version.strip().lower() != "v2":
            LOGGER.warning("HTTP rejected request: unsupported sync_version=%s", sync_version)
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "unsupported sync_version, expected 'v2'"})
            return
        if writer_mode.strip().lower() != "mysql_v2":
            LOGGER.warning("HTTP rejected request: unsupported writer_mode=%s", writer_mode)
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "unsupported writer_mode, expected 'mysql_v2'"})
            return

        if receiver_db is None:
            LOGGER.warning("HTTP rejected request: receiver_db is required (path=%s client=%s)", path, self.address_string())
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "receiver_db is required"})
            return
        if catalog_db is None:
            LOGGER.warning("HTTP rejected request: catalog_db is required (path=%s client=%s)", path, self.address_string())
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "catalog_db is required"})
            return
        if "batch_size" in payload or "txn_chunk_size" in payload:
            LOGGER.warning("HTTP rejected request: legacy payload fields are not supported")
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "legacy payload fields are not supported; use receiver_fetch_size/write_chunk_size"},
            )
            return

        receiver_fetch_size = _to_int(
            payload.get("receiver_fetch_size"),
            default=self.server.default_receiver_fetch_size,
            minimum=1,
        )
        write_chunk_size = _to_int(
            payload.get("write_chunk_size"),
            default=self.server.default_write_chunk_size,
            minimum=1,
        )
        max_batches = _to_int(payload.get("max_batches"), default=self.server.default_max_batches, minimum=0)

        job = QueueJob(
            receiver_db=receiver_db,
            catalog_db=catalog_db,
            parser_name=parser_name,
            receiver_fetch_size=receiver_fetch_size,
            write_chunk_size=write_chunk_size,
            sync_version=sync_version,
            writer_mode=writer_mode,
            max_batches=max_batches,
            run_id=run_id,
            source=source,
        )
        result = self.server.converter_daemon.enqueue(job)

        status = HTTPStatus.ACCEPTED
        if result.reason == "queue_full":
            status = HTTPStatus.TOO_MANY_REQUESTS
        LOGGER.info(
            "HTTP enqueue result: path=%s client=%s status=%s accepted=%s duplicate=%s reason=%s key=%s queue_size=%s",
            path,
            self.address_string(),
            int(status),
            result.accepted,
            result.duplicate,
            result.reason,
            result.key,
            result.queue_size,
        )

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
            LOGGER.debug("HTTP request without body: path=%s", self.path.rstrip("/"))
            return {}

        try:
            length = max(0, int(raw_length))
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc

        LOGGER.debug("HTTP reading JSON body: path=%s content_length=%s", self.path.rstrip("/"), length)
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
        LOGGER.debug(
            "HTTP response sent: path=%s status=%s bytes=%s",
            self.path.rstrip("/"),
            int(status),
            len(encoded),
        )


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
