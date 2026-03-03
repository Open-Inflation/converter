from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy.exc import IntegrityError, OperationalError

from converter import CatalogSQLiteRepository, build_default_pipeline
from converter.core.models import NormalizedProductRecord, RawProductRecord
from converter.core.ports import StorageRepository


class _FakeStorageRepository(StorageRepository):
    def __init__(self) -> None:
        self.deleted_batches: list[list[str]] = []

    def delete_images(self, urls) -> None:  # type: ignore[override]
        self.deleted_batches.append(list(urls))


class _FailingCursorCatalogRepository(CatalogSQLiteRepository):
    def _set_receiver_cursor_in_session(  # type: ignore[override]
        self,
        session,
        parser_name: str,
        *,
        ingested_at: str,
        product_id: int,
    ) -> None:
        super()._set_receiver_cursor_in_session(
            session,
            parser_name,
            ingested_at=ingested_at,
            product_id=product_id,
        )
        raise RuntimeError("forced cursor failure")


class _DeadlockOnceCatalogRepository(CatalogSQLiteRepository):
    def __init__(self, db_path: str | Path) -> None:
        super().__init__(db_path)
        self.injected_deadlocks = 0

    def _upsert_many_in_session(self, session, records):  # type: ignore[override]
        if self.injected_deadlocks == 0:
            self.injected_deadlocks += 1
            raise OperationalError(
                "UPDATE catalog_products SET updated_at = ...",
                {"product_id": 1},
                RuntimeError(1213, "Deadlock found when trying to get lock; try restarting transaction"),
            )
        return super()._upsert_many_in_session(session, records)


class _DuplicateKeyOnceCatalogRepository(CatalogSQLiteRepository):
    def __init__(self, db_path: str | Path) -> None:
        super().__init__(db_path)
        self.injected_duplicate_keys = 0

    def _upsert_many_in_session(self, session, records):  # type: ignore[override]
        if self.injected_duplicate_keys == 0:
            self.injected_duplicate_keys += 1
            raise IntegrityError(
                "INSERT INTO catalog_identity_map (...) VALUES (...)",
                {"parser_name": "fixprice"},
                RuntimeError(1062, "Duplicate entry for key 'catalog_identity_map.PRIMARY'"),
            )
        return super()._upsert_many_in_session(session, records)


class CatalogSQLiteRepositoryTests(unittest.TestCase):
    def _make_db(self) -> Path:
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        return Path(tmp.name)

    @staticmethod
    def _asset_values(
        conn: sqlite3.Connection,
        *,
        table: str,
        id_column: str,
        row_id: int,
        kind: str,
    ) -> list[str]:
        rows = conn.execute(
            f"""
            SELECT value
            FROM {table}
            WHERE {id_column} = ? AND asset_kind = ?
            ORDER BY sort_order ASC
            """,
            (row_id, kind),
        ).fetchall()
        return [str(row["value"]) for row in rows]

    def test_schema_excludes_removed_columns(self) -> None:
        db_path = self._make_db()
        try:
            CatalogSQLiteRepository(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                product_columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(catalog_products)").fetchall()
                }
                product_types = {
                    str(row["name"]): str(row["type"]).upper()
                    for row in conn.execute("PRAGMA table_info(catalog_products)").fetchall()
                }
                snapshot_columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(catalog_product_snapshots)").fetchall()
                }
                snapshot_types = {
                    str(row["name"]): str(row["type"]).upper()
                    for row in conn.execute("PRAGMA table_info(catalog_product_snapshots)").fetchall()
                }

                self.assertIn("title_original", product_columns)
                self.assertIn("title_normalized_no_stopwords", product_columns)
                self.assertIn("price", product_columns)
                self.assertIn("composition_original", product_columns)
                self.assertNotIn("title_normalized", product_columns)
                self.assertNotIn("title_original_no_stopwords", product_columns)
                self.assertNotIn("source_payload_json", product_columns)
                self.assertNotIn("image_urls_json", product_columns)
                self.assertNotIn("duplicate_image_urls_json", product_columns)
                self.assertNotIn("image_fingerprints_json", product_columns)
                self.assertTrue(
                    "DECIMAL" in product_types["price"] or "NUMERIC" in product_types["price"]
                )
                self.assertTrue(
                    "DECIMAL" in product_types["discount_price"]
                    or "NUMERIC" in product_types["discount_price"]
                )
                self.assertTrue(
                    "DECIMAL" in product_types["loyal_price"]
                    or "NUMERIC" in product_types["loyal_price"]
                )

                self.assertIn("price", snapshot_columns)
                self.assertIn("discount_price", snapshot_columns)
                self.assertIn("loyal_price", snapshot_columns)
                self.assertIn("price_unit", snapshot_columns)
                self.assertIn("available_count", snapshot_columns)
                self.assertIn("canonical_product_id", snapshot_columns)
                self.assertIn("parser_name", snapshot_columns)
                self.assertIn("source_id", snapshot_columns)
                self.assertIn("source_event_uid", snapshot_columns)
                self.assertIn("content_fingerprint", snapshot_columns)
                self.assertIn("valid_from_at", snapshot_columns)
                self.assertIn("valid_to_at", snapshot_columns)
                self.assertIn("observed_at", snapshot_columns)
                self.assertIn("created_at", snapshot_columns)
                self.assertNotIn("title_original", snapshot_columns)
                self.assertNotIn("title_normalized_no_stopwords", snapshot_columns)
                self.assertNotIn("description", snapshot_columns)
                self.assertNotIn("composition_original", snapshot_columns)
                self.assertNotIn("package_quantity", snapshot_columns)
                self.assertNotIn("package_unit", snapshot_columns)
                self.assertNotIn("title_normalized", snapshot_columns)
                self.assertNotIn("title_original_no_stopwords", snapshot_columns)
                self.assertNotIn("source_payload_json", snapshot_columns)
                self.assertNotIn("image_urls_json", snapshot_columns)
                self.assertNotIn("duplicate_image_urls_json", snapshot_columns)
                self.assertNotIn("image_fingerprints_json", snapshot_columns)
                self.assertTrue(
                    "DECIMAL" in snapshot_types["price"] or "NUMERIC" in snapshot_types["price"]
                )
                self.assertTrue(
                    "DECIMAL" in snapshot_types["discount_price"]
                    or "NUMERIC" in snapshot_types["discount_price"]
                )
                self.assertTrue(
                    "DECIMAL" in snapshot_types["loyal_price"]
                    or "NUMERIC" in snapshot_types["loyal_price"]
                )

                tables = {
                    str(row["name"])
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                }
                self.assertIn("catalog_product_assets", tables)
                self.assertIn("catalog_product_snapshots", tables)
                self.assertNotIn("catalog_snapshot_events", tables)
                self.assertNotIn("catalog_snapshot_available_counts", tables)
                self.assertNotIn("catalog_snapshot_assets", tables)
                self.assertNotIn("catalog_product_payload_nodes", tables)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_schema_validation_rejects_legacy_snapshot_schema(self) -> None:
        db_path = self._make_db()
        try:
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE catalog_product_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        canonical_product_id VARCHAR(36) NOT NULL,
                        parser_name VARCHAR(64) NOT NULL,
                        source_id VARCHAR(255) NOT NULL,
                        source_run_id VARCHAR(64),
                        receiver_product_id INTEGER,
                        receiver_artifact_id INTEGER,
                        receiver_sort_order INTEGER,
                        price REAL,
                        discount_price REAL,
                        loyal_price REAL,
                        price_unit VARCHAR(32),
                        source_event_uid VARCHAR(191),
                        content_fingerprint VARCHAR(64),
                        valid_from_at DATETIME,
                        valid_to_at DATETIME,
                        observed_at DATETIME NOT NULL,
                        created_at DATETIME NOT NULL,
                        available_count REAL,
                        title_original TEXT,
                        composition_original TEXT,
                        settlement_id INTEGER
                    );
                    CREATE INDEX idx_legacy_settlement ON catalog_product_snapshots (settlement_id);
                    CREATE TABLE catalog_snapshot_assets (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        snapshot_id INTEGER NOT NULL,
                        asset_kind VARCHAR(32) NOT NULL,
                        sort_order INTEGER NOT NULL,
                        value TEXT NOT NULL,
                        created_at DATETIME NOT NULL
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT INTO catalog_product_snapshots (
                        id,
                        canonical_product_id,
                        parser_name,
                        source_id,
                        price,
                        discount_price,
                        loyal_price,
                        price_unit,
                        valid_from_at,
                        valid_to_at,
                        observed_at,
                        created_at,
                        available_count,
                        title_original,
                        composition_original,
                        settlement_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        1,
                        "canon-1",
                        "fixprice",
                        "receiver:legacy:1",
                        99.9,
                        79.9,
                        69.9,
                        "RUB",
                        "2026-02-28T10:00:00+00:00",
                        "2026-02-28T10:00:00+00:00",
                        "2026-02-28T10:00:00+00:00",
                        "2026-02-28T10:00:00+00:00",
                        7.0,
                        "Legacy title",
                        "Legacy composition",
                        42,
                    ),
                )
                conn.commit()
            finally:
                conn.close()

            with self.assertRaises(RuntimeError):
                CatalogSQLiteRepository(db_path)
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_persists_price_and_source_payload(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            observed_at = datetime(2026, 2, 28, tzinfo=timezone.utc)
            payload = {
                "receiver_product_id": 501,
                "receiver_product": {
                    "price": 199.9,
                    "discount_price": 149.9,
                    "loyal_price": 129.9,
                    "price_unit": "RUB",
                    "description": "Тестовое описание",
                },
                "receiver_product_meta": [{"name": "Жирность", "value_text": "2.5%"}],
            }

            record = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Тест",
                title_normalized="тест",
                title_original_no_stopwords="тест",
                title_normalized_no_stopwords="тест",
                brand="Brand",
                unit="PCE",
                available_count=1.0,
                package_quantity=None,
                package_unit=None,
                price=199.9,
                discount_price=149.9,
                loyal_price=129.9,
                price_unit="RUB",
                description="Тестовое описание",
                source_id="receiver:run-price:1",
                observed_at=observed_at,
                source_payload=payload,
            )

            repo.upsert_many([record])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                product = conn.execute(
                    """
                    SELECT id, price, discount_price, loyal_price, price_unit, description
                    FROM catalog_products
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-price:1"),
                ).fetchone()
                self.assertIsNotNone(product)
                assert product is not None
                self.assertAlmostEqual(float(product["price"]), 199.9, places=3)
                self.assertAlmostEqual(float(product["discount_price"]), 149.9, places=3)
                self.assertAlmostEqual(float(product["loyal_price"]), 129.9, places=3)
                self.assertEqual(product["price_unit"], "RUB")
                self.assertEqual(product["description"], "Тестовое описание")
                self.assertEqual(
                    self._asset_values(
                        conn,
                        table="catalog_product_assets",
                        id_column="product_id",
                        row_id=int(product["id"]),
                        kind="image_url",
                    ),
                    [],
                )

                snapshot = conn.execute(
                    """
                    SELECT
                        id,
                        price,
                        discount_price,
                        loyal_price,
                        price_unit,
                        available_count,
                        valid_from_at,
                        valid_to_at,
                        created_at
                    FROM catalog_product_snapshots
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-price:1"),
                ).fetchone()
                self.assertIsNotNone(snapshot)
                assert snapshot is not None
                self.assertAlmostEqual(float(snapshot["price"]), 199.9, places=3)
                self.assertAlmostEqual(float(snapshot["discount_price"]), 149.9, places=3)
                self.assertAlmostEqual(float(snapshot["loyal_price"]), 129.9, places=3)
                self.assertEqual(snapshot["price_unit"], "RUB")
                self.assertAlmostEqual(float(snapshot["available_count"]), 1.0, places=3)
                self.assertIn("2026-02-28", str(snapshot["created_at"]))
                self.assertIn("2026-02-28", str(snapshot["valid_from_at"]))
                self.assertIn("2026-02-28", str(snapshot["valid_to_at"]))
                snapshot_payload_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'catalog_snapshot_payload_nodes'"
                ).fetchone()
                self.assertIsNone(snapshot_payload_table)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_serializes_source_payload_datetimes(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            observed_at = datetime(2026, 2, 28, tzinfo=timezone.utc)
            payload = {
                "receiver_run_id": "run-dt",
                "receiver_artifact": {
                    "ingested_at": datetime(2026, 2, 28, 12, 30, tzinfo=timezone.utc),
                },
            }

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
                source_id="receiver:run-dt:1",
                observed_at=observed_at,
                source_payload=payload,
            )

            repo.upsert_many([record])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                product = conn.execute(
                    """
                    SELECT id
                    FROM catalog_products
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-dt:1"),
                ).fetchone()
                self.assertIsNotNone(product)
                assert product is not None
                product_payload_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'catalog_product_payload_nodes'"
                ).fetchone()
                self.assertIsNone(product_payload_table)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

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
            self.assertEqual(second_norm.composition_original, "Сахар, какао, молоко")
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
                        composition_original,
                        composition_normalized
                    FROM catalog_products
                    ORDER BY id ASC
                    """
                ).fetchall()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0]["canonical_product_id"], rows[1]["canonical_product_id"])
                self.assertEqual(rows[1]["composition_original"], "Сахар, какао, молоко")
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

    def test_upsert_many_with_cursor_writes_products_and_cursor_atomically(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            observed_at = datetime(2026, 2, 28, tzinfo=timezone.utc)
            record = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Тестовый товар",
                title_normalized="тестовый товар",
                title_original_no_stopwords="тестовый товар",
                title_normalized_no_stopwords="тестовый товар",
                brand=None,
                unit="PCE",
                available_count=1.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-atomic:1",
                observed_at=observed_at,
                source_payload={"receiver_product_id": 101},
            )

            repo.upsert_many_with_cursor(
                [record],
                parser_name="fixprice",
                cursor_ingested_at="2026-02-28T12:00:00+00:00",
                cursor_product_id=101,
            )

            self.assertEqual(
                repo.get_receiver_cursor("fixprice"),
                ("2026-02-28T12:00:00+00:00", 101),
            )

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                products = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM catalog_products WHERE source_id = ?",
                    ("receiver:run-atomic:1",),
                ).fetchone()
                assert products is not None
                self.assertEqual(int(products["cnt"]), 1)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_many_with_cursor_rolls_back_if_cursor_write_fails(self) -> None:
        db_path = self._make_db()
        try:
            repo = _FailingCursorCatalogRepository(db_path)
            observed_at = datetime(2026, 2, 28, tzinfo=timezone.utc)
            record = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Rollback товар",
                title_normalized="rollback товар",
                title_original_no_stopwords="rollback товар",
                title_normalized_no_stopwords="rollback товар",
                brand=None,
                unit="PCE",
                available_count=1.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-rollback:1",
                observed_at=observed_at,
                source_payload={"receiver_product_id": 202},
            )

            with self.assertRaises(RuntimeError):
                repo.upsert_many_with_cursor(
                    [record],
                    parser_name="fixprice",
                    cursor_ingested_at="2026-02-28T12:10:00+00:00",
                    cursor_product_id=202,
                )

            self.assertEqual(repo.get_receiver_cursor("fixprice"), (None, None))

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                products = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM catalog_products WHERE source_id = ?",
                    ("receiver:run-rollback:1",),
                ).fetchone()
                assert products is not None
                self.assertEqual(int(products["cnt"]), 0)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_many_retries_on_mysql_deadlock(self) -> None:
        db_path = self._make_db()
        try:
            repo = _DeadlockOnceCatalogRepository(db_path)
            record = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Retry товар",
                title_normalized="retry товар",
                title_original_no_stopwords="retry товар",
                title_normalized_no_stopwords="retry товар",
                brand=None,
                unit="PCE",
                available_count=1.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-retry:1",
                observed_at=datetime(2026, 2, 28, tzinfo=timezone.utc),
                source_payload={"receiver_product_id": 303},
            )

            with patch("converter.adapters.catalog.sleep", return_value=None):
                repo.upsert_many([record])

            self.assertEqual(repo.injected_deadlocks, 1)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                products = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM catalog_products WHERE source_id = ?",
                    ("receiver:run-retry:1",),
                ).fetchone()
                assert products is not None
                self.assertEqual(int(products["cnt"]), 1)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_many_does_not_retry_on_mysql_duplicate_key(self) -> None:
        db_path = self._make_db()
        try:
            repo = _DuplicateKeyOnceCatalogRepository(db_path)
            record = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Duplicate key retry",
                title_normalized="duplicate key retry",
                title_original_no_stopwords="duplicate key retry",
                title_normalized_no_stopwords="duplicate key retry",
                brand=None,
                unit="PCE",
                available_count=1.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-dupkey:1",
                observed_at=datetime(2026, 2, 28, tzinfo=timezone.utc),
                source_payload={"receiver_product_id": 304},
            )

            with patch("converter.adapters.catalog.sleep", return_value=None):
                with self.assertRaises(IntegrityError):
                    repo.upsert_many([record])

            self.assertEqual(repo.injected_duplicate_keys, 1)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                products = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM catalog_products WHERE source_id = ?",
                    ("receiver:run-dupkey:1",),
                ).fetchone()
                assert products is not None
                self.assertEqual(int(products["cnt"]), 0)
            finally:
                conn.close()
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

    def test_upsert_reuses_snapshot_when_payload_is_unchanged(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            first_observed = datetime(2026, 2, 28, 10, 0, tzinfo=timezone.utc)
            second_observed = datetime(2026, 2, 28, 11, 0, tzinfo=timezone.utc)

            first = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Сыр плавленый",
                title_normalized="сыр плавленый",
                title_original_no_stopwords="сыр плавленый",
                title_normalized_no_stopwords="сыр плавленый",
                brand="TestBrand",
                unit="PCE",
                available_count=10.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-stable:1",
                sku="stable-1",
                price=99.9,
                discount_price=79.9,
                loyal_price=69.9,
                price_unit="RUB",
                image_urls=["https://cdn.example/stable-1.jpg"],
                observed_at=first_observed,
                source_payload={
                    "receiver_run_id": "run-1",
                    "receiver_product_id": 1001,
                    "receiver_artifact_id": 2001,
                },
            )
            second = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Сыр плавленый",
                title_normalized="сыр плавленый",
                title_original_no_stopwords="сыр плавленый",
                title_normalized_no_stopwords="сыр плавленый",
                brand="TestBrand",
                unit="PCE",
                available_count=10.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-stable:1",
                sku="stable-1",
                price=99.9,
                discount_price=79.9,
                loyal_price=69.9,
                price_unit="RUB",
                image_urls=["https://cdn.example/stable-1.jpg"],
                observed_at=second_observed,
                source_payload={
                    "receiver_run_id": "run-2",
                    "receiver_product_id": 1002,
                    "receiver_artifact_id": 2002,
                },
            )

            repo.upsert_many([first])
            repo.upsert_many([second])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                snapshots = conn.execute(
                    """
                    SELECT id, content_fingerprint, valid_from_at, valid_to_at, available_count, created_at
                    FROM catalog_product_snapshots
                    WHERE parser_name = ? AND source_id = ?
                    ORDER BY id ASC
                    """,
                    ("fixprice", "receiver:run-stable:1"),
                ).fetchall()
                self.assertEqual(len(snapshots), 1)
                self.assertIsNotNone(snapshots[0]["content_fingerprint"])
                self.assertIn("2026-02-28 10:00:00", str(snapshots[0]["valid_from_at"]))
                self.assertIn("2026-02-28 11:00:00", str(snapshots[0]["valid_to_at"]))
                self.assertAlmostEqual(float(snapshots[0]["available_count"]), 10.0, places=3)
                self.assertIn("2026-02-28", str(snapshots[0]["created_at"]))
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_updates_projection_when_only_nonvolatile_fields_change(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            first = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Сыр плавленый",
                title_normalized="сыр плавленый",
                title_original_no_stopwords="сыр плавленый",
                title_normalized_no_stopwords="сыр плавленый",
                brand="Brand-1",
                description="Описание 1",
                unit="PCE",
                available_count=5.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-nonvolatile:1",
                sku="nonvolatile-1",
                price=129.9,
                discount_price=119.9,
                loyal_price=109.9,
                price_unit="RUB",
                observed_at=datetime(2026, 2, 28, 12, 0, tzinfo=timezone.utc),
            )
            second = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Сыр плавленый",
                title_normalized="сыр плавленый",
                title_original_no_stopwords="сыр плавленый",
                title_normalized_no_stopwords="сыр плавленый",
                brand="Brand-2",
                description="Описание 2",
                unit="PCE",
                available_count=5.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-nonvolatile:1",
                sku="nonvolatile-1",
                price=129.9,
                discount_price=119.9,
                loyal_price=109.9,
                price_unit="RUB",
                observed_at=datetime(2026, 2, 28, 13, 0, tzinfo=timezone.utc),
            )

            repo.upsert_many([first])
            repo.upsert_many([second])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                snapshots = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM catalog_product_snapshots
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-nonvolatile:1"),
                ).fetchone()
                assert snapshots is not None
                self.assertEqual(int(snapshots["cnt"]), 1)

                product = conn.execute(
                    """
                    SELECT brand, description
                    FROM catalog_products
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-nonvolatile:1"),
                ).fetchone()
                self.assertIsNotNone(product)
                assert product is not None
                self.assertEqual(product["brand"], "Brand-2")
                self.assertEqual(product["description"], "Описание 2")
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

    def test_upsert_normalizes_payload_categories_with_lemmatization_and_stopwords(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            pipeline = build_default_pipeline()

            raw = RawProductRecord(
                parser_name="fixprice",
                source_id="receiver:run-cat-norm:1",
                sku="cat-norm-1",
                title="Сок апельсиновый",
                observed_at=datetime(2026, 2, 10, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-cat-norm",
                    "receiver_artifact_id": 808,
                    "receiver_product_id": 55,
                    "receiver_categories": [
                        {
                            "uid": "cat-drinks",
                            "title": "Напитки и соки",
                            "depth": 0,
                            "sort_order": 0,
                        },
                    ],
                },
            )

            normalized = pipeline.process_one(raw)
            repo.upsert_many([normalized])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    """
                    SELECT source_uid, title, title_normalized
                    FROM catalog_categories
                    WHERE source_uid = ?
                    """,
                    ("cat-drinks",),
                ).fetchone()
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["title"], "Напитки и соки")
                self.assertEqual(row["title_normalized"], "напиток сок")
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_persists_geodata_from_artifact_coordinates_fallback(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            pipeline = build_default_pipeline()

            raw = RawProductRecord(
                parser_name="chizhik",
                source_id="receiver:run-geo-artifact:1",
                sku="geo-artifact-1",
                title="Тарелка",
                geo="RUS, Ленинградская область, Санкт-Петербург",
                observed_at=datetime(2026, 2, 10, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-geo-artifact",
                    "receiver_artifact_id": 777,
                    "receiver_product_id": 11,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": "Ленинградская область",
                    "receiver_geo_name": "Санкт-Петербург",
                    "receiver_geo_latitude": None,
                    "receiver_geo_longitude": None,
                    "receiver_artifact": {
                        "latitude": 59.93863,
                        "longitude": 30.31413,
                    },
                },
            )

            normalized = pipeline.process_one(raw)
            repo.upsert_many([normalized])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                geo = conn.execute(
                    "SELECT latitude, longitude FROM catalog_settlement_geodata ORDER BY id DESC LIMIT 1"
                ).fetchone()
                self.assertIsNotNone(geo)
                assert geo is not None
                self.assertAlmostEqual(float(geo["latitude"]), 59.93863, places=5)
                self.assertAlmostEqual(float(geo["longitude"]), 30.31413, places=5)
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

            self.assertEqual(storage.deleted_batches, [])
            outbox_result = repo.process_storage_delete_outbox(limit=10)
            self.assertEqual(outbox_result["processed"], 1)
            self.assertEqual(outbox_result["deleted"], 1)
            self.assertEqual(outbox_result["failed"], 0)
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
                composition_original="Сталь",
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
                    SELECT id, brand, primary_category_id, settlement_id, composition_original, composition_normalized
                    FROM catalog_products
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-null:1"),
                ).fetchone()
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["brand"], "O'Kitchen")
                self.assertIsNotNone(row["primary_category_id"])
                self.assertIsNotNone(row["settlement_id"])
                self.assertEqual(row["composition_original"], "Сталь")
                self.assertEqual(row["composition_normalized"], "сталь")
                self.assertEqual(
                    self._asset_values(
                        conn,
                        table="catalog_product_assets",
                        id_column="product_id",
                        row_id=int(row["id"]),
                        kind="image_url",
                    ),
                    ["https://cdn.example/spoons.jpg"],
                )
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
