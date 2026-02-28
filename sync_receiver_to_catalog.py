from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from converter import (
    CatalogMySQLRepository,
    CatalogSQLiteRepository,
    ReceiverMySQLRepository,
    ReceiverSQLiteRepository,
    is_mysql_dsn,
)
from converter.core.registry import HandlerRegistry
from converter.parsers import register_builtin_handlers
from converter.core.models import RawProductRecord


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync products from receiver DB into catalog DB",
    )
    parser.add_argument(
        "--receiver-db",
        required=True,
        help="Receiver DB path (SQLite) or MySQL DSN",
    )
    parser.add_argument(
        "--catalog-db",
        required=True,
        help="Catalog DB path (SQLite) or MySQL DSN",
    )
    parser.add_argument(
        "--parser-name",
        default="fixprice",
        help="Filter by parser_name from receiver run_artifacts",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250,
        help="Max records per batch",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="Stop after N batches (0 means no limit)",
    )
    return parser


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


def main() -> None:
    args = _build_parser().parse_args()

    receiver_repo = _build_receiver_repository(args.receiver_db)
    catalog_repo = _build_catalog_repository(args.catalog_db)
    registry = HandlerRegistry()
    register_builtin_handlers(registry)

    watermark_ingested_at, watermark_product_id = catalog_repo.get_receiver_cursor(args.parser_name)

    print(
        "Starting sync:",
        f"receiver={args.receiver_db}",
        f"catalog={args.catalog_db}",
        f"parser={args.parser_name}",
        f"batch_size={args.batch_size}",
        f"cursor=({watermark_ingested_at!r}, {watermark_product_id!r})",
    )

    total_processed = 0
    batches = 0

    while True:
        if args.max_batches > 0 and batches >= args.max_batches:
            break

        raw_records = receiver_repo.fetch_batch(
            limit=args.batch_size,
            parser_name=args.parser_name,
            after_ingested_at=watermark_ingested_at,
            after_product_id=watermark_product_id,
        )
        if not raw_records:
            break

        normalized = [registry.get(item.parser_name).handle(item) for item in raw_records]
        catalog_repo.upsert_many(normalized)

        watermark_ingested_at, watermark_product_id = _cursor_from_records(raw_records)
        catalog_repo.set_receiver_cursor(
            args.parser_name,
            ingested_at=watermark_ingested_at,
            product_id=watermark_product_id,
        )

        batches += 1
        total_processed += len(raw_records)
        print(
            f"Batch {batches}: processed={len(raw_records)} total={total_processed} "
            f"cursor=({watermark_ingested_at}, {watermark_product_id})"
        )

    print(f"Sync finished: batches={batches} total_processed={total_processed}")

def _build_receiver_repository(dsn_or_path: str) -> Any:
    token = dsn_or_path.strip()
    if is_mysql_dsn(token):
        return ReceiverMySQLRepository.from_dsn(token)
    return ReceiverSQLiteRepository(Path(token).resolve())


def _build_catalog_repository(dsn_or_path: str) -> Any:
    token = dsn_or_path.strip()
    if is_mysql_dsn(token):
        return CatalogMySQLRepository.from_dsn(token)
    return CatalogSQLiteRepository(Path(token).resolve())


if __name__ == "__main__":
    main()
