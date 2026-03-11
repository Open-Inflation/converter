from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from converter.core.models import AckResult, ChunkApplyResultV2, NormalizedProductRecord, RawProductRecord, SyncChunkV2
from converter.sync import ConverterSyncService, SyncJob, build_catalog_repository, build_receiver_repository


class _FakeHandler:
    def handle(self, raw: RawProductRecord) -> NormalizedProductRecord:
        return NormalizedProductRecord(
            parser_name=raw.parser_name,
            title_original=raw.title,
            title_normalized=raw.title.lower(),
            title_original_no_stopwords=raw.title.lower(),
            title_normalized_no_stopwords=raw.title.lower(),
            brand=None,
            unit="PCE",
            available_count=None,
            package_quantity=None,
            package_unit=None,
            source_id=raw.source_id,
            observed_at=raw.observed_at,
            source_payload=raw.payload,
        )


class _FakeRegistry:
    def __init__(self) -> None:
        self._handler = _FakeHandler()

    def get(self, _parser_name: str) -> _FakeHandler:
        return self._handler


class _FakeReceiverRepository:
    def __init__(self, records: list[RawProductRecord], *, fail_delete_attempts: int = 0) -> None:
        self._records = list(records)
        self._fail_delete_attempts = max(0, int(fail_delete_attempts))
        self.calls = 0
        self.delete_calls = 0
        self.deleted_batches: list[list[int]] = []
        self.delete_chunk_started_at: list[float] = []
        self._consumed = False

    def fetch_batch(self, **_kwargs):  # type: ignore[override]
        self.calls += 1
        if not self._consumed:
            return list(self._records)
        return []

    def delete_processed_products(self, product_ids, *, chunk_started_at):  # type: ignore[override]
        self.delete_calls += 1
        ids = [int(item) for item in product_ids]
        self.deleted_batches.append(ids)
        self.delete_chunk_started_at.append(float(chunk_started_at))
        if self.delete_calls <= self._fail_delete_attempts:
            raise RuntimeError("forced ack failure")
        self._consumed = True
        return AckResult(
            requested_products=len(ids),
            deleted_products=len(ids),
            deleted_artifacts=1,
        )


class _FakeCatalogRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str, str]] = []

    def apply_chunk(
        self,
        chunk: SyncChunkV2,
    ) -> ChunkApplyResultV2:
        self.calls.append(
            (
                len(chunk.records),
                chunk.parser_name,
                chunk.chunk_id,
            )
        )
        return ChunkApplyResultV2(
            inserted_snapshots=len(chunk.records),
            reused_snapshots=0,
            upserted_products=len(chunk.records),
            elapsed_ms=1,
        )


class ConverterSyncServiceTests(unittest.TestCase):
    def test_repository_builders_reject_non_postgresql_dsn(self) -> None:
        with self.assertRaises(ValueError):
            build_receiver_repository("mysql+pymysql://u:p@127.0.0.1:3306/receiver")
        with self.assertRaises(ValueError):
            build_catalog_repository("mysql+pymysql://u:p@127.0.0.1:3306/catalog")

    def test_run_splits_batch_into_write_chunks(self) -> None:
        records: list[RawProductRecord] = []
        for idx in range(5):
            observed_at = datetime(2026, 2, 28, 10, idx, tzinfo=timezone.utc)
            records.append(
                RawProductRecord(
                    parser_name="fixprice",
                    title=f"Product {idx}",
                    source_id=f"receiver:run:{idx}",
                    observed_at=observed_at,
                    payload={"receiver_product_id": idx + 1},
                )
            )

        fake_receiver = _FakeReceiverRepository(records)
        fake_catalog = _FakeCatalogRepository()
        service = ConverterSyncService(registry=_FakeRegistry())

        with (
            patch("converter.sync.build_receiver_repository", return_value=fake_receiver),
            patch("converter.sync.build_catalog_repository", return_value=fake_catalog),
        ):
            outcome = service.run(
                SyncJob(
                    receiver_db="/tmp/receiver.db",
                    catalog_db="/tmp/catalog.db",
                    parser_name="fixprice",
                    receiver_fetch_size=120,
                    write_chunk_size=2,
                )
            )

        self.assertEqual(outcome.batches, 1)
        self.assertEqual(outcome.total_processed, 5)
        self.assertEqual([item[0] for item in fake_catalog.calls], [2, 2, 1])
        self.assertEqual(fake_receiver.delete_calls, 3)
        self.assertEqual(fake_receiver.deleted_batches, [[1, 2], [3, 4], [5]])

    def test_run_fails_when_receiver_ack_fails_and_retries_same_chunk(self) -> None:
        observed_at = datetime(2026, 2, 28, 10, 0, tzinfo=timezone.utc)
        records = [
            RawProductRecord(
                parser_name="fixprice",
                title="Product A",
                source_id="receiver:run:a",
                observed_at=observed_at,
                payload={"receiver_product_id": 11},
            ),
            RawProductRecord(
                parser_name="fixprice",
                title="Product B",
                source_id="receiver:run:b",
                observed_at=observed_at,
                payload={"receiver_product_id": 12},
            ),
        ]

        fake_receiver = _FakeReceiverRepository(records, fail_delete_attempts=1)
        fake_catalog = _FakeCatalogRepository()
        service = ConverterSyncService(registry=_FakeRegistry())

        with (
            patch("converter.sync.build_receiver_repository", return_value=fake_receiver),
            patch("converter.sync.build_catalog_repository", return_value=fake_catalog),
        ):
            with self.assertRaises(RuntimeError):
                service.run(
                    SyncJob(
                        receiver_db="/tmp/receiver.db",
                        catalog_db="/tmp/catalog.db",
                        parser_name="fixprice",
                        receiver_fetch_size=100,
                        write_chunk_size=100,
                    )
                )

            outcome = service.run(
                SyncJob(
                    receiver_db="/tmp/receiver.db",
                    catalog_db="/tmp/catalog.db",
                    parser_name="fixprice",
                    receiver_fetch_size=100,
                    write_chunk_size=100,
                )
            )
            empty_outcome = service.run(
                SyncJob(
                    receiver_db="/tmp/receiver.db",
                    catalog_db="/tmp/catalog.db",
                    parser_name="fixprice",
                    receiver_fetch_size=100,
                    write_chunk_size=100,
                )
            )

        self.assertEqual(outcome.batches, 1)
        self.assertEqual(outcome.total_processed, 2)
        self.assertEqual(empty_outcome.batches, 0)
        self.assertEqual(empty_outcome.total_processed, 0)
        self.assertEqual(len(fake_catalog.calls), 2)
        self.assertEqual(fake_receiver.delete_calls, 2)


if __name__ == "__main__":
    unittest.main()
