from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import logging
from pathlib import Path
from time import monotonic
from typing import Any, Callable
from urllib.parse import urlparse
from uuid import uuid4

from .adapters import (
    CatalogMySQLRepository,
    CatalogSQLiteRepository,
    ReceiverMySQLRepository,
    ReceiverSQLiteRepository,
    is_mysql_dsn,
)
from .core.models import RawProductRecord, SyncChunkV2
from .core.registry import HandlerRegistry
from .parsers import register_builtin_handlers


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SyncJob:
    receiver_db: str
    catalog_db: str
    parser_name: str = "fixprice"
    receiver_fetch_size: int = 2000
    write_chunk_size: int = 1000
    sync_version: str = "v2"
    writer_mode: str = "mysql_v2"
    max_batches: int = 0


@dataclass(frozen=True, slots=True)
class SyncBatchEvent:
    batch_number: int
    batch_size: int
    total_processed: int
    cursor_ingested_at: str
    cursor_product_id: int


@dataclass(frozen=True, slots=True)
class SyncOutcome:
    batches: int
    total_processed: int
    cursor_ingested_at: str | None
    cursor_product_id: int | None


def _cursor_from_records(records: list[RawProductRecord]) -> tuple[str, int]:
    max_ingested_at = ""
    max_product_id = -1

    for record in records:
        observed_at = record.observed_at
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        ingested_at = observed_at.isoformat()

        raw_product_id = record.payload.get("receiver_product_id") if isinstance(record.payload, dict) else None
        product_id = _to_int(raw_product_id) or 0

        if ingested_at > max_ingested_at or (ingested_at == max_ingested_at and product_id > max_product_id):
            max_ingested_at = ingested_at
            max_product_id = product_id

    if not max_ingested_at:
        now = datetime.now(tz=timezone.utc).isoformat()
        return now, 0

    return max_ingested_at, max_product_id


def _to_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    token = str(value).strip()
    if not token:
        return None
    try:
        return int(token)
    except ValueError:
        return None


def _chunk_id(
    parser_name: str,
    cursor_ingested_at: str,
    cursor_product_id: int,
    records: list[RawProductRecord],
) -> str:
    if not records:
        return f"{parser_name}:{cursor_ingested_at}:{cursor_product_id}:{uuid4().hex}"

    source_tokens: list[str] = []
    for item in records:
        if item.source_id:
            source_tokens.append(item.source_id.strip())
    source_tokens = [token for token in source_tokens if token]

    first_source = source_tokens[0] if source_tokens else ""
    last_source = source_tokens[-1] if source_tokens else ""
    seed = "|".join(
        (
            parser_name.strip().lower(),
            cursor_ingested_at,
            str(int(cursor_product_id)),
            str(len(records)),
            first_source,
            last_source,
        )
    )
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]
    return f"{parser_name.strip().lower()}:{cursor_ingested_at}:{cursor_product_id}:{digest}"


def build_receiver_repository(dsn_or_path: str) -> Any:
    token = dsn_or_path.strip()
    if is_mysql_dsn(token):
        return ReceiverMySQLRepository.from_dsn(token)
    return ReceiverSQLiteRepository(Path(token).resolve())


def build_catalog_repository(dsn_or_path: str) -> Any:
    token = dsn_or_path.strip()
    if is_mysql_dsn(token):
        return CatalogMySQLRepository.from_dsn(token)
    return CatalogSQLiteRepository(Path(token).resolve())


def _safe_db_ref(dsn_or_path: str) -> str:
    token = (dsn_or_path or "").strip()
    if not token:
        return "<empty>"
    if is_mysql_dsn(token):
        parsed = urlparse(token)
        host = parsed.hostname or "<host>"
        port = parsed.port
        db = parsed.path.lstrip("/") if parsed.path else ""
        port_part = f":{port}" if port else ""
        db_part = f"/{db}" if db else ""
        return f"mysql://{host}{port_part}{db_part}"
    return str(Path(token).resolve())


class ConverterSyncService:
    def __init__(self, registry: HandlerRegistry | None = None) -> None:
        if registry is None:
            registry = HandlerRegistry()
            register_builtin_handlers(registry)
        self._registry = registry

    def run(
        self,
        job: SyncJob,
        *,
        on_batch: Callable[[SyncBatchEvent], None] | None = None,
    ) -> SyncOutcome:
        started_at = monotonic()
        parser_name = job.parser_name.strip() or "fixprice"
        receiver_fetch_size = max(1, int(job.receiver_fetch_size))
        write_chunk_size = max(1, int(job.write_chunk_size))
        max_batches = max(0, int(job.max_batches))
        sync_version = (job.sync_version or "").strip().lower() or "v2"
        writer_mode = (job.writer_mode or "").strip().lower() or "mysql_v2"
        if sync_version != "v2":
            raise ValueError(f"Unsupported sync_version: {sync_version!r}. Only 'v2' is supported.")
        if writer_mode != "mysql_v2":
            raise ValueError(f"Unsupported writer_mode: {writer_mode!r}. Only 'mysql_v2' is supported.")

        LOGGER.info(
            "Sync started: parser=%s receiver=%s catalog=%s sync_version=%s writer_mode=%s receiver_fetch_size=%s write_chunk_size=%s max_batches=%s",
            parser_name,
            _safe_db_ref(job.receiver_db),
            _safe_db_ref(job.catalog_db),
            sync_version,
            writer_mode,
            receiver_fetch_size,
            write_chunk_size,
            max_batches,
        )

        receiver_repo = build_receiver_repository(job.receiver_db)
        catalog_repo = build_catalog_repository(job.catalog_db)
        apply_chunk = getattr(catalog_repo, "apply_chunk", None)
        if not callable(apply_chunk):
            raise RuntimeError("Catalog repository does not support v2 apply_chunk API")

        watermark_ingested_at, watermark_product_id = catalog_repo.get_receiver_cursor(parser_name)
        LOGGER.info(
            "Sync cursor loaded: parser=%s ingested_at=%s product_id=%s",
            parser_name,
            watermark_ingested_at,
            watermark_product_id,
        )
        total_processed = 0
        batches = 0

        while True:
            if max_batches > 0 and batches >= max_batches:
                LOGGER.info(
                    "Sync reached max_batches limit: parser=%s batches=%s max_batches=%s",
                    parser_name,
                    batches,
                    max_batches,
                )
                break

            next_batch_number = batches + 1
            LOGGER.debug(
                "Sync fetching batch: parser=%s batch_number=%s cursor_ingested_at=%s cursor_product_id=%s",
                parser_name,
                next_batch_number,
                watermark_ingested_at,
                watermark_product_id,
            )
            raw_records = receiver_repo.fetch_batch(
                limit=receiver_fetch_size,
                parser_name=parser_name,
                after_ingested_at=watermark_ingested_at,
                after_product_id=watermark_product_id,
            )
            if not raw_records:
                LOGGER.info(
                    "Sync finished input stream: parser=%s batches=%s total_processed=%s",
                    parser_name,
                    batches,
                    total_processed,
                )
                break

            LOGGER.info(
                "Sync fetched batch: parser=%s batch_number=%s fetched=%s",
                parser_name,
                next_batch_number,
                len(raw_records),
            )
            for chunk_start in range(0, len(raw_records), write_chunk_size):
                raw_chunk = raw_records[chunk_start : chunk_start + write_chunk_size]
                LOGGER.debug(
                    "Sync processing chunk: parser=%s batch_number=%s chunk_index=%s chunk_size=%s",
                    parser_name,
                    next_batch_number,
                    (chunk_start // write_chunk_size) + 1,
                    len(raw_chunk),
                )
                try:
                    normalized = [self._registry.get(item.parser_name).handle(item) for item in raw_chunk]
                except Exception:
                    LOGGER.exception(
                        "Sync normalization failed: parser=%s batch_number=%s chunk_index=%s",
                        parser_name,
                        next_batch_number,
                        (chunk_start // write_chunk_size) + 1,
                    )
                    raise
                chunk_ingested_at, chunk_product_id = _cursor_from_records(raw_chunk)

                chunk = SyncChunkV2(
                    parser_name=parser_name,
                    chunk_id=_chunk_id(parser_name, chunk_ingested_at, chunk_product_id, raw_chunk),
                    records=normalized,
                    cursor_ingested_at=chunk_ingested_at,
                    cursor_product_id=chunk_product_id,
                )
                outcome = apply_chunk(chunk)
                LOGGER.debug(
                    "Sync apply_chunk done: parser=%s batch_number=%s chunk_index=%s inserted_snapshots=%s reused_snapshots=%s upserted_products=%s elapsed_ms=%s",
                    parser_name,
                    next_batch_number,
                    (chunk_start // write_chunk_size) + 1,
                    getattr(outcome, "inserted_snapshots", None),
                    getattr(outcome, "reused_snapshots", None),
                    getattr(outcome, "upserted_products", None),
                    getattr(outcome, "elapsed_ms", None),
                )
                LOGGER.debug(
                    "Sync chunk committed: parser=%s batch_number=%s chunk_index=%s cursor_ingested_at=%s cursor_product_id=%s",
                    parser_name,
                    next_batch_number,
                    (chunk_start // write_chunk_size) + 1,
                    chunk_ingested_at,
                    chunk_product_id,
                )
                watermark_ingested_at, watermark_product_id = chunk_ingested_at, chunk_product_id

            batches += 1
            total_processed += len(raw_records)

            LOGGER.info(
                "Sync batch complete: parser=%s batch_number=%s batch_size=%s total_processed=%s cursor_ingested_at=%s cursor_product_id=%s",
                parser_name,
                batches,
                len(raw_records),
                total_processed,
                watermark_ingested_at,
                watermark_product_id,
            )
            if on_batch is not None:
                on_batch(
                    SyncBatchEvent(
                        batch_number=batches,
                        batch_size=len(raw_records),
                        total_processed=total_processed,
                        cursor_ingested_at=watermark_ingested_at,
                        cursor_product_id=watermark_product_id,
                    )
                )

        outcome = SyncOutcome(
            batches=batches,
            total_processed=total_processed,
            cursor_ingested_at=watermark_ingested_at,
            cursor_product_id=watermark_product_id,
        )
        LOGGER.info(
            "Sync finished: parser=%s batches=%s total_processed=%s elapsed_sec=%.3f final_cursor_ingested_at=%s final_cursor_product_id=%s",
            parser_name,
            outcome.batches,
            outcome.total_processed,
            monotonic() - started_at,
            outcome.cursor_ingested_at,
            outcome.cursor_product_id,
        )
        return outcome

    @staticmethod
    def process_storage_delete_outbox(
        catalog_db: str,
        *,
        limit: int = 100,
    ) -> dict[str, int]:
        catalog_repo = build_catalog_repository(catalog_db)
        handler = getattr(catalog_repo, "process_storage_delete_outbox", None)
        if not callable(handler):
            return {"processed": 0, "deleted": 0, "failed": 0}
        result = handler(limit=max(1, int(limit)))
        if not isinstance(result, dict):
            return {"processed": 0, "deleted": 0, "failed": 0}
        return {
            "processed": int(result.get("processed", 0)),
            "deleted": int(result.get("deleted", 0)),
            "failed": int(result.get("failed", 0)),
        }
