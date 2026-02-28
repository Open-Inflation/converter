from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from converter import CatalogSQLiteRepository, build_default_pipeline
from converter.core.models import NormalizedProductRecord, RawProductRecord
from converter.core.ports import StorageRepository


class _FakeStorageRepository(StorageRepository):
    def __init__(self) -> None:
        self.deleted_batches: list[list[str]] = []

    def delete_images(self, urls) -> None:  # type: ignore[override]
        self.deleted_batches.append(list(urls))


class CatalogSQLiteRepositoryTests(unittest.TestCase):
    def _make_db(self) -> Path:
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        return Path(tmp.name)

    def test_upsert_persists_identity_backfill_and_images(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            pipeline = build_default_pipeline()

            first_raw = RawProductRecord(
                parser_name="fixprice",
                source_id="receiver:run-1:1",
                plu="10002",
                title="Шоколад молочный, 200 г, 15 шт",
                category="Продукты",
                geo="Санкт-Петербург",
                composition="Сахар, какао, молоко",
                image_urls=["https://cdn.example/choco-main.jpg"],
                observed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
            )
            second_raw = RawProductRecord(
                parser_name="fixprice",
                source_id="receiver:run-2:2",
                plu="10002",
                title="Шоколад молочный, 200 г, 15 шт",
                category=None,
                geo=None,
                composition=None,
                image_urls=["https://cdn.example/choco-main.jpg"],
                observed_at=datetime(2026, 2, 2, tzinfo=timezone.utc),
            )

            first_norm = pipeline.process_one(first_raw)
            second_norm = pipeline.process_one(second_raw)

            repo.upsert_many([first_norm])
            repo.upsert_many([second_norm])

            self.assertIsNotNone(first_norm.canonical_product_id)
            self.assertEqual(first_norm.canonical_product_id, second_norm.canonical_product_id)
            self.assertEqual(second_norm.category_normalized, "продукт")
            self.assertEqual(second_norm.geo_normalized, "санкт-петербург")
            self.assertEqual(second_norm.composition_normalized, "сахар, какао, молоко")

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    """
                    SELECT
                        canonical_product_id,
                        parser_name,
                        source_id,
                        primary_category_id,
                        settlement_id,
                        composition_normalized
                    FROM catalog_products
                    ORDER BY id ASC
                    """
                ).fetchall()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0]["canonical_product_id"], rows[1]["canonical_product_id"])
                self.assertEqual(rows[1]["composition_normalized"], "сахар, какао, молоко")
                self.assertIsNotNone(rows[0]["primary_category_id"])
                self.assertIsNotNone(rows[0]["settlement_id"])

                identity = conn.execute(
                    "SELECT canonical_product_id FROM catalog_identity_map WHERE parser_name = ? AND identity_type = ? AND identity_value = ?",
                    ["fixprice", "plu", "10002"],
                ).fetchone()
                self.assertIsNotNone(identity)

                image_rows = conn.execute("SELECT fingerprint, canonical_url FROM catalog_image_fingerprints").fetchall()
                self.assertEqual(len(image_rows), 1)
                self.assertEqual(image_rows[0]["canonical_url"], "https://cdn.example/choco-main.jpg")

                snapshots = conn.execute("SELECT COUNT(*) AS cnt FROM catalog_product_snapshots").fetchone()
                self.assertEqual(int(snapshots["cnt"]), 2)

                source_rows = conn.execute("SELECT COUNT(*) AS cnt FROM catalog_product_sources").fetchone()
                self.assertEqual(int(source_rows["cnt"]), 2)

                categories = conn.execute("SELECT COUNT(*) AS cnt FROM catalog_categories").fetchone()
                self.assertGreaterEqual(int(categories["cnt"]), 1)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_receiver_cursor_roundtrip(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)

            self.assertEqual(repo.get_receiver_cursor("fixprice"), (None, None))

            repo.set_receiver_cursor(
                "fixprice",
                ingested_at="2026-02-28T10:00:00+00:00",
                product_id=77,
            )
            self.assertEqual(
                repo.get_receiver_cursor("fixprice"),
                ("2026-02-28T10:00:00+00:00", 77),
            )
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_many_handles_duplicate_normalized_identity_in_one_batch(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            observed_at = datetime(2026, 2, 28, tzinfo=timezone.utc)

            first = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Тарелка десертная O`Kit",
                title_normalized="тарелка десертный o kit",
                title_original_no_stopwords="тарелка десертная o kit",
                title_normalized_no_stopwords="тарелка десертный o kit",
                brand=None,
                unit="PCE",
                available_count=None,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-1:1",
                sku="5093200",
                observed_at=observed_at,
            )
            second = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Тарелка десертная O`Kit",
                title_normalized="тарелка десертный o kit",
                title_original_no_stopwords="тарелка десертная o kit",
                title_normalized_no_stopwords="тарелка десертный o kit",
                brand=None,
                unit="PCE",
                available_count=None,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-1:2",
                sku="5093201",
                observed_at=observed_at,
            )

            repo.upsert_many([first, second])

            self.assertIsNotNone(first.canonical_product_id)
            self.assertEqual(first.canonical_product_id, second.canonical_product_id)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                normalized_rows = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM catalog_identity_map
                    WHERE parser_name = ? AND identity_type = ? AND identity_value = ?
                    """,
                    ("fixprice", "normalized_name", "тарелка десертный o kit"),
                ).fetchone()
                self.assertEqual(int(normalized_rows["cnt"]), 1)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_persists_settlements_categories_and_geodata(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            pipeline = build_default_pipeline()

            raw = RawProductRecord(
                parser_name="fixprice",
                source_id="receiver:run-geo:1",
                sku="geo-1",
                title="Тарелка десертная O'Kitchen",
                category="Посуда / Тарелки",
                geo="RUS, Ленинградская область, Санкт-Петербург",
                observed_at=datetime(2026, 2, 10, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-geo",
                    "receiver_artifact_id": 101,
                    "receiver_product_id": 1,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": "Ленинградская область",
                    "receiver_geo_name": "Санкт-Петербург",
                    "receiver_geo_settlement_type": "city",
                    "receiver_geo_latitude": 59.93863,
                    "receiver_geo_longitude": 30.31413,
                    "receiver_categories": [
                        {
                            "uid": "cat-root",
                            "title": "Посуда",
                            "depth": 0,
                            "sort_order": 0,
                        },
                        {
                            "uid": "cat-plates",
                            "parent_uid": "cat-root",
                            "title": "Тарелки",
                            "depth": 1,
                            "sort_order": 1,
                        },
                    ],
                },
            )

            normalized = pipeline.process_one(raw)
            repo.upsert_many([normalized])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                settlement = conn.execute(
                    "SELECT name, region, country, latitude, longitude FROM catalog_settlements"
                ).fetchone()
                self.assertIsNotNone(settlement)
                self.assertEqual(settlement["name"], "Санкт-Петербург")
                self.assertEqual(settlement["region"], "Ленинградская область")
                self.assertEqual(settlement["country"], "RUS")
                self.assertAlmostEqual(float(settlement["latitude"]), 59.93863, places=5)
                self.assertAlmostEqual(float(settlement["longitude"]), 30.31413, places=5)

                geo_rows = conn.execute("SELECT COUNT(*) AS cnt FROM catalog_settlement_geodata").fetchone()
                self.assertEqual(int(geo_rows["cnt"]), 1)

                category_rows = conn.execute(
                    "SELECT source_uid, title, depth FROM catalog_categories ORDER BY depth ASC"
                ).fetchall()
                self.assertEqual(len(category_rows), 2)
                self.assertEqual(category_rows[0]["source_uid"], "cat-root")
                self.assertEqual(category_rows[1]["source_uid"], "cat-plates")

                link_rows = conn.execute("SELECT COUNT(*) AS cnt FROM catalog_product_category_links").fetchone()
                self.assertEqual(int(link_rows["cnt"]), 2)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_requests_storage_delete_for_duplicate_images(self) -> None:
        db_path = self._make_db()
        try:
            storage = _FakeStorageRepository()
            repo = CatalogSQLiteRepository(db_path, storage_repository=storage)

            record = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Тест",
                title_normalized="тест",
                title_original_no_stopwords="тест",
                title_normalized_no_stopwords="тест",
                brand=None,
                unit="PCE",
                available_count=None,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-dup:1",
                sku="dup-1",
                image_urls=[
                    "http://storage.local/images/dup.webp",
                    "http://storage.local/images/dup.webp",
                ],
                observed_at=datetime(2026, 2, 20, tzinfo=timezone.utc),
            )

            repo.upsert_many([record])

            self.assertEqual(storage.deleted_batches, [["http://storage.local/images/dup.webp"]])
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_does_not_erase_existing_values_with_nulls(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            observed = datetime(2026, 2, 11, tzinfo=timezone.utc)

            first = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Набор ложек",
                title_normalized="набор ложка",
                title_original_no_stopwords="набор ложек",
                title_normalized_no_stopwords="набор ложка",
                brand="O'Kitchen",
                unit="PCE",
                available_count=10.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-null:1",
                sku="null-1",
                category_normalized="посуда",
                geo_normalized="rus, москва",
                composition_normalized="сталь",
                image_urls=["https://cdn.example/spoons.jpg"],
                observed_at=observed,
            )
            second = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Набор ложек",
                title_normalized="набор ложка",
                title_original_no_stopwords="набор ложек",
                title_normalized_no_stopwords="набор ложка",
                brand=None,
                unit="PCE",
                available_count=None,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-null:1",
                sku="null-1",
                category_normalized=None,
                geo_normalized=None,
                composition_normalized=None,
                image_urls=[],
                observed_at=observed,
            )

            repo.upsert_many([first])
            repo.upsert_many([second])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    """
                    SELECT brand, primary_category_id, settlement_id, composition_normalized, image_urls_json
                    FROM catalog_products
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-null:1"),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["brand"], "O'Kitchen")
                self.assertIsNotNone(row["primary_category_id"])
                self.assertIsNotNone(row["settlement_id"])
                self.assertEqual(row["composition_normalized"], "сталь")
                self.assertEqual(json.loads(row["image_urls_json"]), ["https://cdn.example/spoons.jpg"])
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
