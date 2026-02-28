from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .adapters import (
    CatalogMySQLRepository,
    CatalogSQLiteRepository,
    ReceiverMySQLRepository,
    ReceiverSQLiteRepository,
    is_mysql_dsn,
)
from .core.models import RawProductRecord
from .core.registry import HandlerRegistry
from .parsers import register_builtin_handlers


@dataclass(frozen=True, slots=True)
class SyncJob:
    receiver_db: str
    catalog_db: str
    parser_name: str = "fixprice"
    batch_size: int = 250
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
        receiver_repo = build_receiver_repository(job.receiver_db)
        catalog_repo = build_catalog_repository(job.catalog_db)

        parser_name = job.parser_name.strip() or "fixprice"
        batch_size = max(1, int(job.batch_size))
        max_batches = max(0, int(job.max_batches))

        watermark_ingested_at, watermark_product_id = catalog_repo.get_receiver_cursor(parser_name)
        total_processed = 0
        batches = 0

        while True:
            if max_batches > 0 and batches >= max_batches:
                break

            raw_records = receiver_repo.fetch_batch(
                limit=batch_size,
                parser_name=parser_name,
                after_ingested_at=watermark_ingested_at,
                after_product_id=watermark_product_id,
            )
            if not raw_records:
                break

            normalized = [self._registry.get(item.parser_name).handle(item) for item in raw_records]
            catalog_repo.upsert_many(normalized)

            watermark_ingested_at, watermark_product_id = _cursor_from_records(raw_records)
            catalog_repo.set_receiver_cursor(
                parser_name,
                ingested_at=watermark_ingested_at,
                product_id=watermark_product_id,
            )

            batches += 1
            total_processed += len(raw_records)

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

        return SyncOutcome(
            batches=batches,
            total_processed=total_processed,
            cursor_ingested_at=watermark_ingested_at,
            cursor_product_id=watermark_product_id,
        )
