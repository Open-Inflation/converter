from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from converter.core.models import ChunkApplyResultV2, NormalizedProductRecord, RawProductRecord, SyncChunkV2
from converter.sync import ConverterSyncService, SyncJob


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
    def __init__(self, records: list[RawProductRecord]) -> None:
        self._records = list(records)
        self.calls = 0

    def fetch_batch(self, **_kwargs):  # type: ignore[override]
        self.calls += 1
        if self.calls == 1:
            return list(self._records)
        return []


class _FakeCatalogRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str, str, int]] = []

    def get_receiver_cursor(self, _parser_name: str):  # type: ignore[override]
        return None, None

    def apply_chunk(
        self,
        chunk: SyncChunkV2,
    ) -> ChunkApplyResultV2:
        self.calls.append(
            (
                len(chunk.records),
                chunk.parser_name,
                chunk.cursor_ingested_at,
                chunk.cursor_product_id,
            )
        )
        return ChunkApplyResultV2(
            inserted_snapshots=len(chunk.records),
            reused_snapshots=0,
            upserted_products=len(chunk.records),
            elapsed_ms=1,
        )


class ConverterSyncServiceTests(unittest.TestCase):
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
        self.assertEqual(outcome.cursor_product_id, 5)
        self.assertEqual([item[0] for item in fake_catalog.calls], [2, 2, 1])
        self.assertEqual([item[3] for item in fake_catalog.calls], [2, 4, 5])


if __name__ == "__main__":
    unittest.main()
