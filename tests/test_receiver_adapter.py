from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from converter import ReceiverSQLiteRepository, build_default_pipeline


def _create_schema(conn: sqlite3.Connection, *, include_artifact_parser_name: bool) -> None:
    cur = conn.cursor()

    parser_name_column = "parser_name TEXT," if include_artifact_parser_name else ""
    cur.execute(
        f"""
        CREATE TABLE run_artifacts (
            id INTEGER PRIMARY KEY,
            run_id TEXT,
            source TEXT,
            {parser_name_column}
            ingested_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE run_artifact_products (
            id INTEGER PRIMARY KEY,
            artifact_id INTEGER,
            sku TEXT,
            plu TEXT,
            title TEXT,
            composition TEXT,
            brand TEXT,
            unit TEXT,
            available_count REAL,
            package_quantity REAL,
            package_unit TEXT,
            categories_uid_json TEXT,
            main_image TEXT,
            sort_order INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE run_artifact_categories (
            id INTEGER PRIMARY KEY,
            artifact_id INTEGER,
            uid TEXT,
            title TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE run_artifact_administrative_units (
            id INTEGER PRIMARY KEY,
            artifact_id INTEGER,
            name TEXT,
            region TEXT,
            country TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE run_artifact_product_images (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            url TEXT,
            sort_order INTEGER
        )
        """
    )

    conn.commit()


class ReceiverSQLiteRepositoryTests(unittest.TestCase):
    def _make_db(self, *, include_artifact_parser_name: bool) -> Path:
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()

        db_path = Path(tmp.name)
        conn = sqlite3.connect(db_path)
        try:
            _create_schema(conn, include_artifact_parser_name=include_artifact_parser_name)
        finally:
            conn.close()
        return db_path

    def test_legacy_schema_is_rejected(self) -> None:
        db_path = self._make_db(include_artifact_parser_name=False)
        try:
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO run_artifacts(id, run_id, source, ingested_at) VALUES (?, ?, ?, ?)",
                    (10, "run-1", "output_json", "2026-02-27T10:00:00+00:00"),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_products(
                        id, artifact_id, sku, plu, title, composition, brand,
                        unit, available_count, package_quantity, package_unit,
                        categories_uid_json, main_image, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        100,
                        10,
                        "5092806",
                        None,
                        "Лопатка кулинарная, 26 см, в ассортименте",
                        None,
                        "O'Kitchen",
                        None,
                        52,
                        None,
                        None,
                        json.dumps(["cat-1"], ensure_ascii=False),
                        "images/main.jpg",
                        0,
                    ),
                )
                cur.execute(
                    "INSERT INTO run_artifact_categories(id, artifact_id, uid, title) VALUES (?, ?, ?, ?)",
                    (1, 10, "cat-1", "Посуда"),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_administrative_units(id, artifact_id, name, region, country)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (1, 10, "Москва", "г. Москва", "RUS"),
                )
                cur.execute(
                    "INSERT INTO run_artifact_product_images(id, product_id, url, sort_order) VALUES (?, ?, ?, ?)",
                    (1, 100, "images/main.jpg", 0),
                )
                cur.execute(
                    "INSERT INTO run_artifact_product_images(id, product_id, url, sort_order) VALUES (?, ?, ?, ?)",
                    (2, 100, "images/gallery_1.jpg", 1),
                )
                conn.commit()
            finally:
                conn.close()

            repository = ReceiverSQLiteRepository(db_path)
            with self.assertRaises(RuntimeError) as exc:
                repository.fetch_batch(limit=10)
            self.assertIn("run_artifacts.parser_name is missing", str(exc.exception))
        finally:
            db_path.unlink(missing_ok=True)

    def test_new_schema_maps_product_and_supports_filtering(self) -> None:
        db_path = self._make_db(include_artifact_parser_name=True)
        try:
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO run_artifacts(id, run_id, source, parser_name, ingested_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (11, "run-2", "output_json", "fixprice", "2026-02-27T12:00:00+00:00"),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_products(
                        id, artifact_id, sku, plu, title, composition, brand,
                        unit, available_count, package_quantity, package_unit,
                        categories_uid_json, main_image, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        101,
                        11,
                        "sku-101",
                        None,
                        "Тестовый товар",
                        None,
                        "Brand",
                        "PCE",
                        2,
                        None,
                        None,
                        json.dumps([], ensure_ascii=False),
                        "images/main.jpg",
                        0,
                    ),
                )
                conn.commit()
            finally:
                conn.close()

            repository = ReceiverSQLiteRepository(db_path)
            all_rows = repository.fetch_batch(limit=10)
            self.assertEqual(len(all_rows), 1)
            self.assertEqual(all_rows[0].parser_name, "fixprice")
            self.assertEqual(all_rows[0].brand, "Brand")
            self.assertEqual(all_rows[0].available_count, 2.0)

            fixprice_only = repository.fetch_batch(limit=10, parser_name="fixprice")
            self.assertEqual(len(fixprice_only), 1)

            perekrestok_only = repository.fetch_batch(limit=10, parser_name="perekrestok")
            self.assertEqual(perekrestok_only, [])

            pipeline = build_default_pipeline()
            normalized = pipeline.process_one(all_rows[0])
            self.assertEqual(normalized.parser_name, "fixprice")
            self.assertEqual(normalized.brand, "Brand")
            self.assertEqual(normalized.unit, "PCE")
        finally:
            db_path.unlink(missing_ok=True)

    def test_new_schema_supports_incremental_cursor(self) -> None:
        db_path = self._make_db(include_artifact_parser_name=True)
        try:
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO run_artifacts(id, run_id, source, parser_name, ingested_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (11, "run-1", "output_json", "fixprice", "2026-02-27T12:00:00+00:00"),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifacts(id, run_id, source, parser_name, ingested_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (12, "run-2", "output_json", "fixprice", "2026-02-27T12:01:00+00:00"),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifacts(id, run_id, source, parser_name, ingested_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (13, "run-3", "output_json", "perekrestok", "2026-02-27T12:02:00+00:00"),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_products(
                        id, artifact_id, sku, plu, title, composition, brand,
                        unit, available_count, package_quantity, package_unit,
                        categories_uid_json, main_image, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (101, 11, "sku-101", None, "Товар 1", None, "Brand", "PCE", 2, None, None, "[]", "main-1", 0),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_products(
                        id, artifact_id, sku, plu, title, composition, brand,
                        unit, available_count, package_quantity, package_unit,
                        categories_uid_json, main_image, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (102, 12, "sku-102", None, "Товар 2", None, "Brand", "PCE", 3, None, None, "[]", "main-2", 0),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_products(
                        id, artifact_id, sku, plu, title, composition, brand,
                        unit, available_count, package_quantity, package_unit,
                        categories_uid_json, main_image, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (103, 13, "sku-103", None, "Товар 3", None, "Brand", "PCE", 4, None, None, "[]", "main-3", 0),
                )
                conn.commit()
            finally:
                conn.close()

            repository = ReceiverSQLiteRepository(db_path)

            first_batch = repository.fetch_batch(limit=1, parser_name="fixprice")
            self.assertEqual(len(first_batch), 1)
            self.assertEqual(first_batch[0].sku, "sku-101")

            cursor_ingested_at = first_batch[0].observed_at.isoformat()
            cursor_product_id = int(first_batch[0].payload.get("receiver_product_id", 0))

            second_batch = repository.fetch_batch(
                limit=10,
                parser_name="fixprice",
                after_ingested_at=cursor_ingested_at,
                after_product_id=cursor_product_id,
            )
            self.assertEqual(len(second_batch), 1)
            self.assertEqual(second_batch[0].sku, "sku-102")
        finally:
            db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
