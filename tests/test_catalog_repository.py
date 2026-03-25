from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import event
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.schema import CreateTable

from converter import CatalogSQLiteRepository, build_default_pipeline
from converter.adapters.catalog_schema import _CatalogProductGroup, _CatalogStore
from converter.core.models import NormalizedProductRecord, RawProductRecord, SyncChunkV2
from converter.core.ports import StorageRepository


class _FakeStorageRepository(StorageRepository):
    def __init__(self) -> None:
        self.deleted_batches: list[list[str]] = []
        self.persisted_batches: list[list[str]] = []
        self.size_batches: list[list[str]] = []
        self.size_map: dict[str, int | None] = {}

    def persist_images(self, urls) -> list[str]:  # type: ignore[override]
        batch = list(urls)
        self.persisted_batches.append(batch)
        out: list[str] = []
        for url in batch:
            token = str(url)
            if "/images/" in token:
                out.append(token.replace("/images/", "/images-permanent/"))
            else:
                out.append(token)
        return out

    def get_image_sizes(self, urls) -> list[int | None]:  # type: ignore[override]
        batch = [str(url) for url in urls]
        self.size_batches.append(batch)
        return [self.size_map.get(url) for url in batch]

    def delete_images(self, urls) -> None:  # type: ignore[override]
        self.deleted_batches.append(list(urls))


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
                RuntimeError("40P01", "deadlock detected"),
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
                RuntimeError("23505", "duplicate key value violates unique constraint"),
            )
        return super()._upsert_many_in_session(session, records)


class CatalogSQLiteRepositoryTests(unittest.TestCase):
    def _make_db(self) -> Path:
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        return Path(tmp.name)

    @staticmethod
    def _asset_urls(
        conn: sqlite3.Connection,
        *,
        table: str,
        id_column: str,
        row_id: int,
    ) -> list[str]:
        rows = conn.execute(
            f"""
            SELECT url
            FROM {table}
            WHERE {id_column} = ?
            ORDER BY sort_order ASC
            """,
            (row_id,),
        ).fetchall()
        return [str(row["url"]) for row in rows]

    @staticmethod
    def _asset_sizes(
        conn: sqlite3.Connection,
        *,
        table: str,
        id_column: str,
        row_id: int,
    ) -> list[int | None]:
        rows = conn.execute(
            f"""
            SELECT size
            FROM {table}
            WHERE {id_column} = ?
            ORDER BY sort_order ASC
            """,
            (row_id,),
        ).fetchall()
        out: list[int | None] = []
        for row in rows:
            raw = row["size"]
            out.append(None if raw is None else int(raw))
        return out

    @staticmethod
    def _make_chunk_record(
        idx: int,
        *,
        observed_at: datetime,
        image_url: str | None = None,
    ) -> NormalizedProductRecord:
        images = [image_url] if image_url is not None else []
        return NormalizedProductRecord(
            parser_name="fixprice",
            title_original=f"Chunk product {idx}",
            title_normalized=f"chunk product {idx}",
            title_original_no_stopwords=f"chunk product {idx}",
            title_normalized_no_stopwords=f"chunk product {idx}",
            brand="Brand",
            unit="PCE",
            available_count=1.0,
            package_quantity=None,
            package_unit=None,
            source_id=f"receiver:chunk:{idx}",
            sku=f"chunk-sku-{idx}",
            price=100.0 + idx,
            discount_price=90.0 + idx,
            loyal_price=80.0 + idx,
            price_unit="RUB",
            image_urls=images,
            observed_at=observed_at,
            source_payload={
                "receiver_run_id": "chunk-run",
                "receiver_product_id": idx + 1,
                "receiver_artifact_id": 1000 + idx,
            },
        )

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
                snapshot_columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(catalog_product_snapshots)").fetchall()
                }
                snapshot_types = {
                    str(row["name"]): str(row["type"]).upper()
                    for row in conn.execute("PRAGMA table_info(catalog_product_snapshots)").fetchall()
                }
                settlement_columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(catalog_settlements)").fetchall()
                }
                store_columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(catalog_stores)").fetchall()
                }
                store_types = {
                    str(row["name"]): str(row["type"]).upper()
                    for row in conn.execute("PRAGMA table_info(catalog_stores)").fetchall()
                }
                asset_columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(catalog_product_assets)").fetchall()
                }
                product_group_columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(catalog_product_groups)").fetchall()
                }
                category_columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(catalog_categories)").fetchall()
                }

                self.assertIn("title_original", product_columns)
                self.assertIn("title_normalized_no_stopwords", product_columns)
                self.assertIn("brand_normalized", product_columns)
                self.assertIn("composition_original", product_columns)
                self.assertIn("dimension_height_m", product_columns)
                self.assertIn("dimension_width_m", product_columns)
                self.assertIn("dimension_depth_m", product_columns)
                self.assertIn("package_weight_gross", product_columns)
                self.assertIn("package_count", product_columns)
                self.assertNotIn("title_normalized", product_columns)
                self.assertNotIn("title_original_no_stopwords", product_columns)
                self.assertNotIn("price", product_columns)
                self.assertNotIn("discount_price", product_columns)
                self.assertNotIn("loyal_price", product_columns)
                self.assertNotIn("price_unit", product_columns)
                self.assertNotIn("available_count", product_columns)
                self.assertNotIn("source_payload_json", product_columns)
                self.assertNotIn("image_urls_json", product_columns)
                self.assertNotIn("duplicate_image_urls_json", product_columns)
                self.assertNotIn("image_fingerprints_json", product_columns)

                self.assertIn("price", snapshot_columns)
                self.assertIn("discount_price", snapshot_columns)
                self.assertIn("loyal_price", snapshot_columns)
                self.assertIn("price_unit", snapshot_columns)
                self.assertIn("available_count", snapshot_columns)
                self.assertIn("store_id", snapshot_columns)
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
                self.assertNotIn("package_weight_gross", snapshot_columns)
                self.assertNotIn("package_count", snapshot_columns)
                self.assertNotIn("dimension_height_m", snapshot_columns)
                self.assertNotIn("dimension_width_m", snapshot_columns)
                self.assertNotIn("dimension_depth_m", snapshot_columns)
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
                self.assertIn("store_key", store_columns)
                self.assertIn("parser_name", store_columns)
                self.assertIn("source", store_columns)
                self.assertIn("code", store_columns)
                self.assertIn("address", store_columns)
                self.assertIn("longitude", store_columns)
                self.assertIn("latitude", store_columns)
                self.assertIn("rating", store_columns)
                self.assertIn("reviews_count", store_columns)
                self.assertIn("open_date", store_columns)
                self.assertIn("settlement_id", store_columns)
                self.assertEqual(store_types["open_date"], "DATE")
                self.assertIn("latitude", settlement_columns)
                self.assertIn("longitude", settlement_columns)
                self.assertNotIn("geo_point", settlement_columns)
                self.assertIn("url", asset_columns)
                self.assertIn("size", asset_columns)
                self.assertIn("fingerprint", asset_columns)
                self.assertNotIn("asset_kind", asset_columns)
                self.assertNotIn("value", asset_columns)
                self.assertIn("group_uid", product_group_columns)
                self.assertIn("product_id", product_group_columns)
                self.assertIn("source", product_group_columns)
                self.assertIn("created_at", product_group_columns)
                self.assertIn("adult", category_columns)
                self.assertIn("icon", category_columns)
                self.assertIn("banner", category_columns)

                tables = {
                    str(row["name"])
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                }
                self.assertIn("catalog_product_assets", tables)
                self.assertIn("catalog_product_groups", tables)
                self.assertIn("catalog_product_snapshots", tables)
                self.assertIn("catalog_stores", tables)
                self.assertNotIn("catalog_image_fingerprints", tables)
                self.assertNotIn("catalog_snapshot_events", tables)
                self.assertNotIn("catalog_snapshot_available_counts", tables)
                self.assertNotIn("catalog_snapshot_assets", tables)
                self.assertNotIn("catalog_product_payload_nodes", tables)
                self.assertNotIn("catalog_settlement_geodata", tables)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_catalog_store_open_date_uses_postgresql_date_type(self) -> None:
        ddl = str(CreateTable(_CatalogStore.__table__).compile(dialect=postgresql.dialect()))
        self.assertIn("open_date DATE", ddl)

    def test_catalog_product_groups_use_postgresql_uuid_type(self) -> None:
        ddl = str(CreateTable(_CatalogProductGroup.__table__).compile(dialect=postgresql.dialect()))
        self.assertIn("group_uid UUID", ddl)

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

    def test_schema_validation_rejects_legacy_settlement_geodata_table(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path, validate_schema=False)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    CREATE TABLE catalog_settlement_geodata (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        settlement_id INTEGER NOT NULL,
                        latitude REAL,
                        longitude REAL
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()
            repo._engine.dispose()

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
                "receiver_artifact_id": 701,
                "receiver_artifact": {
                    "code": "HD87",
                    "address": "Москва, Саянская ул, Дом 11Б",
                    "longitude": 37.83372708,
                    "latitude": 55.76833314,
                    "rating": 3.4,
                    "open_date": "2022-03-28",
                },
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
                dimension_height_m=0.21,
                dimension_width_m=0.31,
                dimension_depth_m=0.41,
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
                    SELECT id, description, dimension_height_m, dimension_width_m, dimension_depth_m
                    FROM catalog_products
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-price:1"),
                ).fetchone()
                self.assertIsNotNone(product)
                assert product is not None
                self.assertEqual(product["description"], "Тестовое описание")
                self.assertAlmostEqual(float(product["dimension_height_m"]), 0.21, places=6)
                self.assertAlmostEqual(float(product["dimension_width_m"]), 0.31, places=6)
                self.assertAlmostEqual(float(product["dimension_depth_m"]), 0.41, places=6)
                self.assertEqual(
                    self._asset_urls(
                        conn,
                        table="catalog_product_assets",
                        id_column="product_id",
                        row_id=int(product["id"]),
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
                        store_id,
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
                self.assertIsNotNone(snapshot["store_id"])
                self.assertIn("2026-02-28", str(snapshot["created_at"]))
                self.assertIn("2026-02-28", str(snapshot["valid_from_at"]))
                self.assertIn("2026-02-28", str(snapshot["valid_to_at"]))
                store = conn.execute(
                    """
                    SELECT source, retail_type, code, address, rating, reviews_count, open_date, settlement_id
                    FROM catalog_stores
                    WHERE id = ?
                    """,
                    (int(snapshot["store_id"]),),
                ).fetchone()
                self.assertIsNotNone(store)
                assert store is not None
                self.assertIsNone(store["source"])
                self.assertIsNone(store["retail_type"])
                self.assertEqual(store["code"], "HD87")
                self.assertEqual(store["address"], "Москва, Саянская ул, Дом 11Б")
                self.assertAlmostEqual(float(store["rating"]), 3.4, places=3)
                self.assertIsNone(store["reviews_count"])
                self.assertEqual(store["open_date"], "2022-03-28")
                self.assertIsNone(store["settlement_id"])
                snapshot_payload_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'catalog_snapshot_payload_nodes'"
                ).fetchone()
                self.assertIsNone(snapshot_payload_table)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_ignores_non_date_store_open_date_text(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            observed_at = datetime(2026, 3, 25, tzinfo=timezone.utc)
            record = NormalizedProductRecord(
                parser_name="chizhik",
                title_original="Тест",
                title_normalized="тест",
                title_original_no_stopwords="тест",
                title_normalized_no_stopwords="тест",
                brand=None,
                unit="PCE",
                available_count=None,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:run-open-date:1",
                observed_at=observed_at,
                source_payload={
                    "receiver_artifact_id": 26504,
                    "receiver_artifact": {
                        "code": "HD87",
                        "address": "Москва, Саянская ул, Дом 11Б",
                        "longitude": 37.83372708,
                        "latitude": 55.76833314,
                        "rating": 0.0,
                        "open_date": "Скоро открытие!",
                    },
                },
            )

            repo.upsert_many([record])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                store = conn.execute(
                    """
                    SELECT code, rating, open_date
                    FROM catalog_stores
                    WHERE parser_name = ? AND code = ?
                    """,
                    ("chizhik", "HD87"),
                ).fetchone()
                self.assertIsNotNone(store)
                assert store is not None
                self.assertAlmostEqual(float(store["rating"]), 0.0, places=3)
                self.assertIsNone(store["open_date"])
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
            self.assertEqual(second_norm.composition_normalized, "сахар какао молоко")

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
                self.assertEqual(rows[1]["composition_normalized"], "сахар какао молоко")
                self.assertIsNotNone(rows[0]["primary_category_id"])
                self.assertIsNotNone(rows[0]["settlement_id"])

                identity = conn.execute(
                    "SELECT canonical_product_id FROM catalog_identity_map WHERE parser_name = ? AND identity_type = ? AND identity_value = ?",
                    ["fixprice", "plu", "10002"],
                ).fetchone()
                self.assertIsNotNone(identity)

                image_rows = conn.execute(
                    """
                    SELECT fingerprint, url
                    FROM catalog_product_assets
                    ORDER BY product_id ASC, sort_order ASC
                    """
                ).fetchall()
                self.assertEqual(len(image_rows), 2)
                self.assertEqual(
                    {str(row["url"]) for row in image_rows},
                    {"https://cdn.example/choco-main.jpg"},
                )
                self.assertEqual(len({str(row["fingerprint"]) for row in image_rows}), 1)

                snapshots = conn.execute("SELECT COUNT(*) AS cnt FROM catalog_product_snapshots").fetchone()
                self.assertEqual(int(snapshots["cnt"]), 2)

                source_rows = conn.execute("SELECT COUNT(*) AS cnt FROM catalog_product_sources").fetchone()
                self.assertEqual(int(source_rows["cnt"]), 2)

                group_rows = conn.execute(
                    """
                    SELECT group_uid, product_id, source
                    FROM catalog_product_groups
                    ORDER BY product_id ASC
                    """
                ).fetchall()
                self.assertEqual(len(group_rows), 2)
                self.assertEqual({str(row["source"]) for row in group_rows}, {"converter"})
                self.assertEqual(len({str(row["group_uid"]) for row in group_rows}), 1)
                self.assertEqual(str(group_rows[0]["group_uid"]), str(rows[0]["canonical_product_id"]))

                categories = conn.execute("SELECT COUNT(*) AS cnt FROM catalog_categories").fetchone()
                self.assertGreaterEqual(int(categories["cnt"]), 1)
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

    def test_upsert_many_does_not_merge_different_sku_by_normalized_name(self) -> None:
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
            self.assertIsNotNone(second.canonical_product_id)
            self.assertNotEqual(first.canonical_product_id, second.canonical_product_id)

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
                self.assertEqual(int(normalized_rows["cnt"]), 0)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_many_merges_by_normalized_name_when_plu_and_sku_are_missing(self) -> None:
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
                source_id="receiver:run-2:2",
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
                    "receiver_artifact_id": 2001,
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

                product_groups = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM catalog_product_groups
                    """
                ).fetchone()
                assert product_groups is not None
                self.assertEqual(int(product_groups["cnt"]), 1)
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
                    SELECT brand, brand_normalized, description
                    FROM catalog_products
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-nonvolatile:1"),
                ).fetchone()
                self.assertIsNotNone(product)
                assert product is not None
                self.assertEqual(product["brand"], "Brand-2")
                self.assertEqual(product["brand_normalized"], "brand-2")
                self.assertEqual(product["description"], "Описание 2")
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_creates_separate_snapshots_for_different_stores(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            observed = datetime(2026, 2, 28, 12, 0, tzinfo=timezone.utc)
            first = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Йогурт",
                title_normalized="йогурт",
                title_original_no_stopwords="йогурт",
                title_normalized_no_stopwords="йогурт",
                brand="Brand",
                unit="PCE",
                available_count=3.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:store-split:1",
                sku="store-split-1",
                price=79.9,
                discount_price=69.9,
                loyal_price=59.9,
                price_unit="RUB",
                observed_at=observed,
                source_payload={
                    "receiver_run_id": "run-store-1",
                    "receiver_product_id": 1,
                    "receiver_artifact_id": 9101,
                    "receiver_source": "api",
                    "receiver_artifact": {
                        "source": "api",
                        "code": "store-1",
                        "address": "Store 1",
                    },
                },
            )
            second = NormalizedProductRecord(
                parser_name="fixprice",
                title_original="Йогурт",
                title_normalized="йогурт",
                title_original_no_stopwords="йогурт",
                title_normalized_no_stopwords="йогурт",
                brand="Brand",
                unit="PCE",
                available_count=3.0,
                package_quantity=None,
                package_unit=None,
                source_id="receiver:store-split:1",
                sku="store-split-1",
                price=79.9,
                discount_price=69.9,
                loyal_price=59.9,
                price_unit="RUB",
                observed_at=observed,
                source_payload={
                    "receiver_run_id": "run-store-2",
                    "receiver_product_id": 2,
                    "receiver_artifact_id": 9102,
                    "receiver_source": "api",
                    "receiver_artifact": {
                        "source": "api",
                        "code": "store-2",
                        "address": "Store 2",
                    },
                },
            )

            repo.upsert_many([first])
            repo.upsert_many([second])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                snapshots = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt, COUNT(DISTINCT store_id) AS store_cnt
                    FROM catalog_product_snapshots
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:store-split:1"),
                ).fetchone()
                self.assertIsNotNone(snapshots)
                assert snapshots is not None
                self.assertEqual(int(snapshots["cnt"]), 2)
                self.assertEqual(int(snapshots["store_cnt"]), 2)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_persists_settlements_and_categories(self) -> None:
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
                            "adult": True,
                            "icon": "https://cdn.example/icons/cat-root.svg",
                            "banner": "https://cdn.example/banners/cat-root.jpg",
                            "depth": 0,
                            "sort_order": 0,
                        },
                        {
                            "uid": "cat-plates",
                            "parent_uid": "cat-root",
                            "title": "Тарелки",
                            "adult": False,
                            "icon": "https://cdn.example/icons/cat-plates.svg",
                            "banner": "https://cdn.example/banners/cat-plates.jpg",
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
                    "SELECT id, name, region, country, latitude, longitude FROM catalog_settlements"
                ).fetchone()
                self.assertIsNotNone(settlement)
                self.assertEqual(settlement["name"], "Санкт-Петербург")
                self.assertEqual(settlement["region"], "Ленинградская область")
                self.assertEqual(settlement["country"], "RUS")
                self.assertAlmostEqual(float(settlement["latitude"]), 59.93863, places=5)
                self.assertAlmostEqual(float(settlement["longitude"]), 30.31413, places=5)
                store = conn.execute(
                    "SELECT settlement_id FROM catalog_stores ORDER BY id ASC LIMIT 1"
                ).fetchone()
                self.assertIsNotNone(store)
                assert store is not None
                self.assertEqual(int(store["settlement_id"]), int(settlement["id"]))

                category_rows = conn.execute(
                    "SELECT source_uid, title, adult, icon, banner, depth FROM catalog_categories ORDER BY depth ASC"
                ).fetchall()
                self.assertEqual(len(category_rows), 2)
                self.assertEqual(category_rows[0]["source_uid"], "cat-root")
                self.assertEqual(category_rows[1]["source_uid"], "cat-plates")
                self.assertEqual(int(category_rows[0]["adult"]), 1)
                self.assertEqual(int(category_rows[1]["adult"]), 0)
                self.assertEqual(category_rows[0]["icon"], "https://cdn.example/icons/cat-root.svg")
                self.assertEqual(category_rows[1]["icon"], "https://cdn.example/icons/cat-plates.svg")
                self.assertEqual(category_rows[0]["banner"], "https://cdn.example/banners/cat-root.jpg")
                self.assertEqual(category_rows[1]["banner"], "https://cdn.example/banners/cat-plates.jpg")

                legacy_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='catalog_product_category_links'"
                ).fetchone()
                self.assertIsNone(legacy_table)
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

    def test_upsert_persists_settlement_coordinates_from_artifact_fallback(self) -> None:
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
                settlement = conn.execute(
                    "SELECT latitude, longitude FROM catalog_settlements ORDER BY id DESC LIMIT 1"
                ).fetchone()
                self.assertIsNotNone(settlement)
                assert settlement is not None
                self.assertAlmostEqual(float(settlement["latitude"]), 59.93863, places=5)
                self.assertAlmostEqual(float(settlement["longitude"]), 30.31413, places=5)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_skips_invalid_settlement_coordinates_without_failing(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            pipeline = build_default_pipeline()

            raw = RawProductRecord(
                parser_name="fixprice",
                source_id="receiver:run-geo-invalid:1",
                sku="geo-invalid-1",
                title="Тарелка",
                geo="RUS, Ленинградская область, Санкт-Петербург",
                observed_at=datetime(2026, 2, 10, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-geo-invalid",
                    "receiver_artifact_id": 900,
                    "receiver_product_id": 99,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": "Ленинградская область",
                    "receiver_geo_name": "Санкт-Петербург",
                    "receiver_geo_latitude": 120.0,
                    "receiver_geo_longitude": 300.0,
                },
            )

            normalized = pipeline.process_one(raw)
            repo.upsert_many([normalized])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                settlement = conn.execute(
                    "SELECT latitude, longitude FROM catalog_settlements ORDER BY id DESC LIMIT 1"
                ).fetchone()
                self.assertIsNotNone(settlement)
                assert settlement is not None
                self.assertIsNone(settlement["latitude"])
                self.assertIsNone(settlement["longitude"])
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_settlement_keeps_initial_coordinates(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            pipeline = build_default_pipeline()

            raw_first = RawProductRecord(
                parser_name="fixprice",
                source_id="receiver:run-geo-history:1",
                sku="geo-history-1",
                title="Тарелка",
                geo="RUS, Ленинградская область, Санкт-Петербург",
                observed_at=datetime(2026, 2, 10, 10, 0, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-geo-history",
                    "receiver_artifact_id": 901,
                    "receiver_product_id": 1,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": "Ленинградская область",
                    "receiver_geo_name": "Санкт-Петербург",
                    "receiver_geo_latitude": 59.93863,
                    "receiver_geo_longitude": 30.31413,
                },
            )
            raw_second = RawProductRecord(
                parser_name="fixprice",
                source_id="receiver:run-geo-history:1",
                sku="geo-history-1",
                title="Тарелка",
                geo="RUS, Ленинградская область, Санкт-Петербург",
                observed_at=datetime(2026, 2, 10, 10, 5, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-geo-history",
                    "receiver_artifact_id": 902,
                    "receiver_product_id": 2,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": "Ленинградская область",
                    "receiver_geo_name": "Санкт-Петербург",
                    "receiver_geo_latitude": 59.93864,
                    "receiver_geo_longitude": 30.31414,
                },
            )

            repo.upsert_many([pipeline.process_one(raw_first)])
            repo.upsert_many([pipeline.process_one(raw_second)])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                settlement = conn.execute(
                    """
                    SELECT latitude, longitude
                    FROM catalog_settlements
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
                self.assertIsNotNone(settlement)
                assert settlement is not None
                self.assertAlmostEqual(float(settlement["latitude"]), 59.93863, places=5)
                self.assertAlmostEqual(float(settlement["longitude"]), 30.31413, places=5)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_merges_settlements_across_sources_when_region_is_missing_on_one_side(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            pipeline = build_default_pipeline()

            first = RawProductRecord(
                parser_name="fixprice",
                source_id="receiver:run-settlement-merge:1",
                sku="settlement-merge-1",
                title="Товар 1",
                observed_at=datetime(2026, 2, 10, 10, 0, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-settlement-merge",
                    "receiver_artifact_id": 1001,
                    "receiver_product_id": 1,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": "Ленинградская область",
                    "receiver_geo_name": "Санкт-Петербург",
                    "receiver_geo_settlement_type": "city",
                    "receiver_geo_latitude": 59.93863,
                    "receiver_geo_longitude": 30.31413,
                },
            )
            second = RawProductRecord(
                parser_name="chizhik",
                source_id="receiver:run-settlement-merge:2",
                sku="settlement-merge-2",
                title="Товар 2",
                observed_at=datetime(2026, 2, 10, 10, 5, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-settlement-merge",
                    "receiver_artifact_id": 1002,
                    "receiver_product_id": 2,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": None,
                    "receiver_geo_name": "Санкт-Петербург",
                    "receiver_geo_settlement_type": "city",
                },
            )

            repo.upsert_many([pipeline.process_one(first)])
            repo.upsert_many([pipeline.process_one(second)])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                settlements = conn.execute(
                    """
                    SELECT id, country, region, name, settlement_type
                    FROM catalog_settlements
                    ORDER BY id ASC
                    """
                ).fetchall()
                self.assertEqual(len(settlements), 1)
                self.assertEqual(settlements[0]["country"], "RUS")
                self.assertEqual(settlements[0]["region"], "Ленинградская область")
                self.assertEqual(settlements[0]["name"], "Санкт-Петербург")
                self.assertEqual(settlements[0]["settlement_type"], "city")

                linked = conn.execute(
                    """
                    SELECT settlement_id
                    FROM catalog_products
                    WHERE source_id IN (?, ?)
                    ORDER BY source_id ASC
                    """,
                    ("receiver:run-settlement-merge:1", "receiver:run-settlement-merge:2"),
                ).fetchall()
                self.assertEqual(len(linked), 2)
                self.assertEqual(int(linked[0]["settlement_id"]), int(settlements[0]["id"]))
                self.assertEqual(int(linked[1]["settlement_id"]), int(settlements[0]["id"]))
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_does_not_merge_settlements_when_regions_conflict(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            pipeline = build_default_pipeline()

            first = RawProductRecord(
                parser_name="fixprice",
                source_id="receiver:run-settlement-conflict:1",
                sku="settlement-conflict-1",
                title="Товар 1",
                observed_at=datetime(2026, 2, 10, 11, 0, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-settlement-conflict",
                    "receiver_artifact_id": 1101,
                    "receiver_product_id": 1,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": "Тверская область",
                    "receiver_geo_name": "Озерки",
                    "receiver_geo_settlement_type": "village",
                },
            )
            second = RawProductRecord(
                parser_name="chizhik",
                source_id="receiver:run-settlement-conflict:2",
                sku="settlement-conflict-2",
                title="Товар 2",
                observed_at=datetime(2026, 2, 10, 11, 5, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-settlement-conflict",
                    "receiver_artifact_id": 1102,
                    "receiver_product_id": 2,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": "Ленинградская область",
                    "receiver_geo_name": "Озерки",
                    "receiver_geo_settlement_type": "village",
                },
            )

            repo.upsert_many([pipeline.process_one(first)])
            repo.upsert_many([pipeline.process_one(second)])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                settlements = conn.execute(
                    """
                    SELECT id, region, name, settlement_type
                    FROM catalog_settlements
                    ORDER BY id ASC
                    """
                ).fetchall()
                self.assertEqual(len(settlements), 2)
                self.assertEqual({str(row["region"]) for row in settlements}, {"Тверская область", "Ленинградская область"})

                linked = conn.execute(
                    """
                    SELECT settlement_id
                    FROM catalog_products
                    WHERE source_id IN (?, ?)
                    ORDER BY source_id ASC
                    """,
                    ("receiver:run-settlement-conflict:1", "receiver:run-settlement-conflict:2"),
                ).fetchall()
                self.assertEqual(len(linked), 2)
                self.assertNotEqual(int(linked[0]["settlement_id"]), int(linked[1]["settlement_id"]))
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_does_not_merge_settlements_when_types_differ(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            pipeline = build_default_pipeline()

            first = RawProductRecord(
                parser_name="fixprice",
                source_id="receiver:run-settlement-type:1",
                sku="settlement-type-1",
                title="Товар 1",
                observed_at=datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-settlement-type",
                    "receiver_artifact_id": 1201,
                    "receiver_product_id": 1,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": "Ленинградская область",
                    "receiver_geo_name": "Кировск",
                    "receiver_geo_settlement_type": "city",
                },
            )
            second = RawProductRecord(
                parser_name="chizhik",
                source_id="receiver:run-settlement-type:2",
                sku="settlement-type-2",
                title="Товар 2",
                observed_at=datetime(2026, 2, 10, 12, 5, tzinfo=timezone.utc),
                payload={
                    "receiver_run_id": "run-settlement-type",
                    "receiver_artifact_id": 1202,
                    "receiver_product_id": 2,
                    "receiver_geo_country": "RUS",
                    "receiver_geo_region": "Ленинградская область",
                    "receiver_geo_name": "Кировск",
                    "receiver_geo_settlement_type": "village",
                },
            )

            repo.upsert_many([pipeline.process_one(first)])
            repo.upsert_many([pipeline.process_one(second)])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                settlements = conn.execute(
                    """
                    SELECT settlement_type
                    FROM catalog_settlements
                    ORDER BY id ASC
                    """
                ).fetchall()
                self.assertEqual(len(settlements), 2)
                self.assertEqual({str(row["settlement_type"]) for row in settlements}, {"city", "village"})
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_upsert_requests_storage_delete_for_duplicate_images(self) -> None:
        db_path = self._make_db()
        try:
            storage = _FakeStorageRepository()
            storage.size_map["http://storage.local/images-permanent/dup.webp"] = 321
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
            self.assertEqual(
                storage.persisted_batches,
                [[
                    "http://storage.local/images/dup.webp",
                    "http://storage.local/images/dup.webp",
                ]],
            )
            outbox_result = repo.process_storage_delete_outbox(limit=10)
            self.assertEqual(outbox_result["processed"], 1)
            self.assertEqual(outbox_result["deleted"], 1)
            self.assertEqual(outbox_result["failed"], 0)
            self.assertEqual(storage.deleted_batches, [["http://storage.local/images-permanent/dup.webp"]])
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                product_row = conn.execute(
                    """
                    SELECT id
                    FROM catalog_products
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-dup:1"),
                ).fetchone()
                self.assertIsNotNone(product_row)
                assert product_row is not None
                self.assertEqual(
                    self._asset_urls(
                        conn,
                        table="catalog_product_assets",
                        id_column="product_id",
                        row_id=int(product_row["id"]),
                    ),
                    ["http://storage.local/images-permanent/dup.webp"],
                )
                self.assertEqual(
                    self._asset_sizes(
                        conn,
                        table="catalog_product_assets",
                        id_column="product_id",
                        row_id=int(product_row["id"]),
                    ),
                    [321],
                )
            finally:
                conn.close()
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
                brand_normalized="o'kitchen",
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
                brand_normalized=None,
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
                    SELECT
                        id,
                        brand,
                        brand_normalized,
                        primary_category_id,
                        settlement_id,
                        composition_original,
                        composition_normalized
                    FROM catalog_products
                    WHERE parser_name = ? AND source_id = ?
                    """,
                    ("fixprice", "receiver:run-null:1"),
                ).fetchone()
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["brand"], "O'Kitchen")
                self.assertEqual(row["brand_normalized"], "o'kitchen")
                self.assertIsNotNone(row["primary_category_id"])
                self.assertIsNotNone(row["settlement_id"])
                self.assertEqual(row["composition_original"], "Сталь")
                self.assertEqual(row["composition_normalized"], "сталь")
                self.assertEqual(
                    self._asset_urls(
                        conn,
                        table="catalog_product_assets",
                        id_column="product_id",
                        row_id=int(row["id"]),
                    ),
                    ["https://cdn.example/spoons.jpg"],
                )
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)

    def test_apply_chunk_uses_sublinear_sql_growth(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)

            def _run_and_count(chunk: SyncChunkV2) -> int:
                statements: list[str] = []

                def _on_before_cursor_execute(
                    _conn,
                    _cursor,
                    statement,
                    _parameters,
                    _context,
                    _executemany,
                ) -> None:
                    statements.append(str(statement))

                event.listen(repo._engine, "before_cursor_execute", _on_before_cursor_execute)
                try:
                    repo.apply_chunk(chunk)
                finally:
                    event.remove(repo._engine, "before_cursor_execute", _on_before_cursor_execute)
                return len(statements)

            observed = datetime(2026, 3, 1, tzinfo=timezone.utc)
            small_chunk = SyncChunkV2(
                parser_name="fixprice",
                chunk_id="chunk-small",
                records=[self._make_chunk_record(0, observed_at=observed)],
            )
            large_chunk = SyncChunkV2(
                parser_name="fixprice",
                chunk_id="chunk-large",
                records=[self._make_chunk_record(i + 1, observed_at=observed) for i in range(20)],
            )

            small_count = _run_and_count(small_chunk)
            large_count = _run_and_count(large_chunk)

            self.assertGreater(small_count, 0)
            self.assertLessEqual(large_count, 250)
            self.assertLessEqual(large_count, small_count * 13)
        finally:
            db_path.unlink(missing_ok=True)

    def test_apply_chunk_batches_asset_deletes(self) -> None:
        db_path = self._make_db()
        try:
            repo = CatalogSQLiteRepository(db_path)
            observed_first = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
            observed_second = datetime(2026, 3, 1, 11, 0, tzinfo=timezone.utc)

            first_chunk = SyncChunkV2(
                parser_name="fixprice",
                chunk_id="chunk-assets-initial",
                records=[
                    self._make_chunk_record(
                        idx=i,
                        observed_at=observed_first,
                        image_url=f"https://cdn.example/initial-{i}.jpg",
                    )
                    for i in range(3)
                ],
            )
            repo.apply_chunk(first_chunk)

            second_chunk = SyncChunkV2(
                parser_name="fixprice",
                chunk_id="chunk-assets-update",
                records=[
                    self._make_chunk_record(
                        idx=i,
                        observed_at=observed_second,
                        image_url=f"https://cdn.example/updated-{i}.jpg",
                    )
                    for i in range(3)
                ],
            )

            statements: list[str] = []

            def _on_before_cursor_execute(
                _conn,
                _cursor,
                statement,
                _parameters,
                _context,
                _executemany,
            ) -> None:
                statements.append(str(statement))

            event.listen(repo._engine, "before_cursor_execute", _on_before_cursor_execute)
            try:
                repo.apply_chunk(second_chunk)
            finally:
                event.remove(repo._engine, "before_cursor_execute", _on_before_cursor_execute)

            asset_delete_sql = [
                token
                for token in statements
                if "DELETE FROM catalog_product_assets" in token
            ]
            self.assertEqual(len(asset_delete_sql), 1)
            self.assertIn(" IN ", asset_delete_sql[0].upper())
        finally:
            db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
