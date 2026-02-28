from __future__ import annotations

import argparse

from converter.sync import ConverterSyncService, SyncBatchEvent, SyncJob


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


def main() -> None:
    args = _build_parser().parse_args()

    print(
        "Starting sync:",
        f"receiver={args.receiver_db}",
        f"catalog={args.catalog_db}",
        f"parser={args.parser_name}",
        f"batch_size={args.batch_size}",
    )

    service = ConverterSyncService()

    def _on_batch(event: SyncBatchEvent) -> None:
        print(
            f"Batch {event.batch_number}: processed={event.batch_size} total={event.total_processed} "
            f"cursor=({event.cursor_ingested_at}, {event.cursor_product_id})"
        )

    result = service.run(
        SyncJob(
            receiver_db=args.receiver_db,
            catalog_db=args.catalog_db,
            parser_name=args.parser_name,
            batch_size=args.batch_size,
            max_batches=args.max_batches,
        ),
        on_batch=_on_batch,
    )
    print(f"Sync finished: batches={result.batches} total_processed={result.total_processed}")


if __name__ == "__main__":
    main()
