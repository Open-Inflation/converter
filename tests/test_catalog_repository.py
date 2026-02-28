from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from converter import CatalogSQLiteRepository, build_default_pipeline
from converter.core.models import NormalizedProductRecord, RawProductRecord


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
            self.assertEqual(second_norm.category_normalized, "продукты")
            self.assertEqual(second_norm.geo_normalized, "санкт-петербург")
            self.assertEqual(second_norm.composition_normalized, "сахар, какао, молоко")

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    "SELECT canonical_product_id, parser_name, source_id, category_normalized, geo_normalized, composition_normalized FROM catalog_products ORDER BY id ASC"
                ).fetchall()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0]["canonical_product_id"], rows[1]["canonical_product_id"])
                self.assertEqual(rows[1]["category_normalized"], "продукты")
                self.assertEqual(rows[1]["geo_normalized"], "санкт-петербург")
                self.assertEqual(rows[1]["composition_normalized"], "сахар, какао, молоко")

                identity = conn.execute(
                    "SELECT canonical_product_id FROM catalog_identity_map WHERE parser_name = ? AND identity_type = ? AND identity_value = ?",
                    ["fixprice", "plu", "10002"],
                ).fetchone()
                self.assertIsNotNone(identity)

                image_rows = conn.execute("SELECT fingerprint, canonical_url FROM catalog_image_fingerprints").fetchall()
                self.assertEqual(len(image_rows), 1)
                self.assertEqual(image_rows[0]["canonical_url"], "https://cdn.example/choco-main.jpg")
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
                raw_title="Тарелка десертная O`Kit",
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
                raw_title="Тарелка десертная O`Kit, 2",
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


if __name__ == "__main__":
    unittest.main()
