from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from converter import ReceiverSQLiteRepository, build_default_pipeline
from converter.adapters.receiver_mapping import map_receiver_row_to_raw_product


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
            retail_type TEXT,
            code TEXT,
            address TEXT,
            schedule_weekdays_open_from TEXT,
            schedule_weekdays_closed_from TEXT,
            schedule_saturday_open_from TEXT,
            schedule_saturday_closed_from TEXT,
            schedule_sunday_open_from TEXT,
            schedule_sunday_closed_from TEXT,
            temporarily_closed INTEGER,
            longitude REAL,
            latitude REAL,
            dataclass_validated INTEGER,
            dataclass_validation_error TEXT,
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
            source_page_url TEXT,
            title TEXT,
            description TEXT,
            adult INTEGER,
            is_new INTEGER,
            promo INTEGER,
            season INTEGER,
            hit INTEGER,
            data_matrix INTEGER,
            composition TEXT,
            brand TEXT,
            producer_name TEXT,
            producer_country TEXT,
            expiration_date_in_days INTEGER,
            rating REAL,
            reviews_count INTEGER,
            price REAL,
            discount_price REAL,
            loyal_price REAL,
            price_unit TEXT,
            unit_net TEXT,
            available_count REAL,
            package_quantity_net REAL,
            package_weight_gross REAL,
            package_unit TEXT,
            package_count REAL,
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
            parent_uid TEXT,
            alias TEXT,
            title TEXT,
            adult INTEGER,
            icon TEXT,
            banner TEXT,
            depth INTEGER,
            sort_order INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE run_artifact_administrative_units (
            id INTEGER PRIMARY KEY,
            artifact_id INTEGER,
            settlement_type TEXT,
            name TEXT,
            alias TEXT,
            region TEXT,
            country TEXT,
            longitude REAL,
            latitude REAL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE run_artifact_product_images (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            url TEXT,
            is_main INTEGER,
            sort_order INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE run_artifact_product_meta (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            name TEXT,
            alias TEXT,
            value_type TEXT,
            value_text TEXT,
            sort_order INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE run_artifact_product_wholesale_prices (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            from_items REAL,
            price REAL,
            sort_order INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE run_artifact_product_categories (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            category_uid TEXT,
            sort_order INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE crawl_tasks (
            id INTEGER PRIMARY KEY,
            city TEXT,
            store TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE task_runs (
            id TEXT PRIMARY KEY,
            task_id INTEGER,
            orchestrator_id TEXT,
            status TEXT,
            converter_elapsed_sec INTEGER DEFAULT 0,
            finish TEXT
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

    def test_schema_without_parser_name_is_rejected(self) -> None:
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
                        unit_net, available_count, package_quantity_net, package_unit,
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

            with self.assertRaises(RuntimeError) as exc:
                ReceiverSQLiteRepository(db_path)
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
                    INSERT INTO run_artifacts(
                        id, run_id, source, parser_name,
                        retail_type, code, address,
                        schedule_weekdays_open_from, schedule_weekdays_closed_from,
                        schedule_saturday_open_from, schedule_saturday_closed_from,
                        schedule_sunday_open_from, schedule_sunday_closed_from,
                        temporarily_closed, longitude, latitude,
                        dataclass_validated, dataclass_validation_error,
                        ingested_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        11,
                        "run-2",
                        "output_json",
                        "fixprice",
                        "store",
                        "FP-11",
                        "Тестовый адрес",
                        "08:00",
                        "22:00",
                        "09:00",
                        "21:00",
                        "10:00",
                        "20:00",
                        0,
                        37.6173,
                        55.7558,
                        1,
                        None,
                        "2026-02-27T12:00:00+00:00",
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_products(
                        id, artifact_id, sku, plu, source_page_url, title, description,
                        adult, is_new, promo, season, hit, data_matrix,
                        composition, brand, producer_name, producer_country,
                        expiration_date_in_days, rating, reviews_count,
                        price, discount_price, loyal_price, price_unit,
                        unit_net, available_count, package_quantity_net, package_unit,
                        categories_uid_json, main_image, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        101,
                        11,
                        "sku-101",
                        None,
                        "https://example.local/product/sku-101",
                        "Тестовый товар",
                        "Описание товара",
                        0,
                        1,
                        1,
                        0,
                        1,
                        0,
                        None,
                        "Brand",
                        "Producer",
                        "RU",
                        90,
                        4.7,
                        11,
                        199.9,
                        149.9,
                        129.9,
                        "RUB",
                        "PCE",
                        2,
                        None,
                        None,
                        json.dumps(["cat-101"], ensure_ascii=False),
                        "images/main.jpg",
                        0,
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_categories(
                        id, artifact_id, uid, parent_uid, alias, title, adult, icon, banner, depth, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (1, 11, "cat-101", None, "milk", "Молочные продукты", 0, "/i.png", "/b.png", 0, 0),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_administrative_units(
                        id, artifact_id, settlement_type, name, alias, region, country, longitude, latitude
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (1, 11, "city", "Москва", "moscow", "Москва", "RUS", 37.6173, 55.7558),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_product_meta(
                        id, product_id, name, alias, value_type, value_text, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (1, 101, "Жирность", "fat", "str", "2.5%", 0),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_product_wholesale_prices(
                        id, product_id, from_items, price, sort_order
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (1, 101, 10, 119.9, 0),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_product_categories(
                        id, product_id, category_uid, sort_order
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (1, 101, "cat-101", 0),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_product_images(
                        id, product_id, url, is_main, sort_order
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (1, 101, "https://example.local/img-101.jpg", 1, 0),
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
            self.assertAlmostEqual(all_rows[0].price or 0.0, 199.9, places=3)
            self.assertAlmostEqual(all_rows[0].discount_price or 0.0, 149.9, places=3)
            self.assertAlmostEqual(all_rows[0].loyal_price or 0.0, 129.9, places=3)
            self.assertEqual(all_rows[0].price_unit, "RUB")
            self.assertEqual(all_rows[0].producer_name, "Producer")
            self.assertTrue(all_rows[0].is_new)
            self.assertTrue(all_rows[0].promo)
            self.assertTrue(all_rows[0].hit)
            self.assertIn("receiver_product", all_rows[0].payload)
            self.assertIn("receiver_artifact", all_rows[0].payload)
            self.assertIn("receiver_admin_unit", all_rows[0].payload)
            self.assertIn("receiver_categories", all_rows[0].payload)
            self.assertIn("receiver_product_meta", all_rows[0].payload)
            self.assertIn("receiver_product_wholesale_prices", all_rows[0].payload)
            self.assertIn("receiver_product_categories", all_rows[0].payload)
            self.assertIn("receiver_product_images", all_rows[0].payload)
            self.assertEqual(all_rows[0].payload["receiver_parser_name"], "fixprice")
            self.assertEqual(all_rows[0].payload["receiver_artifact"]["code"], "FP-11")
            self.assertEqual(all_rows[0].payload["receiver_admin_unit"]["id"], 1)
            self.assertEqual(all_rows[0].payload["receiver_categories"][0]["id"], 1)
            self.assertEqual(len(all_rows[0].payload["receiver_product_meta"]), 1)
            self.assertEqual(len(all_rows[0].payload["receiver_product_wholesale_prices"]), 1)
            self.assertEqual(len(all_rows[0].payload["receiver_product_categories"]), 1)
            self.assertEqual(len(all_rows[0].payload["receiver_product_images"]), 1)
            self.assertEqual(all_rows[0].payload["receiver_product_meta"][0]["id"], 1)
            self.assertEqual(all_rows[0].payload["receiver_product_wholesale_prices"][0]["id"], 1)
            self.assertEqual(all_rows[0].payload["receiver_product_categories"][0]["id"], 1)
            self.assertEqual(all_rows[0].payload["receiver_product_images"][0]["id"], 1)

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

    def test_mapping_does_not_mix_gross_weight_into_package_quantity(self) -> None:
        raw = map_receiver_row_to_raw_product(
            {
                "parser_name": "fixprice",
                "product_title": "Товар",
                "product_unit_net": "KGM",
                "product_package_quantity_net": None,
                "product_package_weight_gross": 0.159,
                "product_package_unit": "KGM",
            }
        )
        self.assertEqual(raw.unit, "KGM")
        self.assertIsNone(raw.package_quantity)
        self.assertEqual(raw.package_unit, "KGM")
        self.assertAlmostEqual(raw.package_weight_gross or 0.0, 0.159)

    def test_delete_processed_products_cleans_orphans_and_keeps_scheduler_tables(self) -> None:
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
                    INSERT INTO run_artifact_products(
                        id, artifact_id, sku, plu, title, composition, brand,
                        unit_net, available_count, package_quantity_net, package_unit,
                        categories_uid_json, main_image, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (101, 11, "sku-101", None, "Товар 1", None, "Brand", "PCE", 2, None, None, "[]", "main-1", 0),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_products(
                        id, artifact_id, sku, plu, title, composition, brand,
                        unit_net, available_count, package_quantity_net, package_unit,
                        categories_uid_json, main_image, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (102, 11, "sku-102", None, "Товар 2", None, "Brand", "PCE", 3, None, None, "[]", "main-2", 0),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_products(
                        id, artifact_id, sku, plu, title, composition, brand,
                        unit_net, available_count, package_quantity_net, package_unit,
                        categories_uid_json, main_image, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (103, 12, "sku-103", None, "Товар 3", None, "Brand", "PCE", 4, None, None, "[]", "main-3", 0),
                )
                cur.execute(
                    "INSERT INTO run_artifact_categories(id, artifact_id, uid, title) VALUES (?, ?, ?, ?)",
                    (1, 11, "cat-1", "Категория 1"),
                )
                cur.execute(
                    "INSERT INTO run_artifact_categories(id, artifact_id, uid, title) VALUES (?, ?, ?, ?)",
                    (2, 12, "cat-2", "Категория 2"),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_administrative_units(id, artifact_id, name, region, country)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (1, 11, "Москва", "г. Москва", "RUS"),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_administrative_units(id, artifact_id, name, region, country)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (2, 12, "Санкт-Петербург", "г. Санкт-Петербург", "RUS"),
                )
                cur.execute(
                    "INSERT INTO run_artifact_product_images(id, product_id, url, sort_order) VALUES (?, ?, ?, ?)",
                    (1, 101, "img-101", 0),
                )
                cur.execute(
                    "INSERT INTO run_artifact_product_images(id, product_id, url, sort_order) VALUES (?, ?, ?, ?)",
                    (2, 102, "img-102", 0),
                )
                cur.execute(
                    "INSERT INTO run_artifact_product_meta(id, product_id, name, value_text, sort_order) VALUES (?, ?, ?, ?, ?)",
                    (1, 101, "meta", "m1", 0),
                )
                cur.execute(
                    "INSERT INTO run_artifact_product_meta(id, product_id, name, value_text, sort_order) VALUES (?, ?, ?, ?, ?)",
                    (2, 102, "meta", "m2", 0),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_product_wholesale_prices(id, product_id, from_items, price, sort_order)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (1, 101, 2, 10.0, 0),
                )
                cur.execute(
                    """
                    INSERT INTO run_artifact_product_categories(id, product_id, category_uid, sort_order)
                    VALUES (?, ?, ?, ?)
                    """,
                    (1, 101, "cat-1", 0),
                )
                cur.execute(
                    "INSERT INTO crawl_tasks(id, city, store) VALUES (?, ?, ?)",
                    (99, "Moscow", "S1"),
                )
                cur.execute(
                    "INSERT INTO task_runs(id, task_id, orchestrator_id, status) VALUES (?, ?, ?, ?)",
                    ("run-keep", 99, "orch", "success"),
                )
                cur.execute(
                    "INSERT INTO task_runs(id, task_id, orchestrator_id, status) VALUES (?, ?, ?, ?)",
                    ("run-1", 99, "orch", "success"),
                )
                cur.execute(
                    "INSERT INTO task_runs(id, task_id, orchestrator_id, status) VALUES (?, ?, ?, ?)",
                    ("run-2", 99, "orch", "success"),
                )
                conn.commit()
            finally:
                conn.close()

            repository = ReceiverSQLiteRepository(db_path)
            with patch.object(type(repository), "_elapsed_seconds_since", return_value=7):
                ack_partial = repository.delete_processed_products([101], chunk_started_at=123.0)
            self.assertEqual(ack_partial.requested_products, 1)
            self.assertEqual(ack_partial.deleted_products, 1)
            self.assertEqual(ack_partial.deleted_artifacts, 0)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                remaining_products = conn.execute(
                    "SELECT id FROM run_artifact_products ORDER BY id ASC"
                ).fetchall()
                self.assertEqual([int(row["id"]) for row in remaining_products], [102, 103])
                artifact_11 = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM run_artifacts WHERE id = 11"
                ).fetchone()
                self.assertEqual(int(artifact_11["cnt"]), 1)
                run_1_state = conn.execute(
                    "SELECT converter_elapsed_sec, finish FROM task_runs WHERE id = ?",
                    ("run-1",),
                ).fetchone()
                self.assertIsNotNone(run_1_state)
                self.assertEqual(int(run_1_state["converter_elapsed_sec"] or 0), 7)
                self.assertIsNone(run_1_state["finish"])
            finally:
                conn.close()

            with patch.object(type(repository), "_elapsed_seconds_since", return_value=5):
                ack_full = repository.delete_processed_products([102], chunk_started_at=124.0)
            self.assertEqual(ack_full.requested_products, 1)
            self.assertEqual(ack_full.deleted_products, 1)
            self.assertEqual(ack_full.deleted_artifacts, 1)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                artifact_ids = conn.execute(
                    "SELECT id FROM run_artifacts ORDER BY id ASC"
                ).fetchall()
                self.assertEqual([int(row["id"]) for row in artifact_ids], [12])

                categories = conn.execute(
                    "SELECT artifact_id FROM run_artifact_categories ORDER BY artifact_id ASC"
                ).fetchall()
                self.assertEqual([int(row["artifact_id"]) for row in categories], [12])

                admin_units = conn.execute(
                    "SELECT artifact_id FROM run_artifact_administrative_units ORDER BY artifact_id ASC"
                ).fetchall()
                self.assertEqual([int(row["artifact_id"]) for row in admin_units], [12])

                run_1_state = conn.execute(
                    "SELECT converter_elapsed_sec, finish FROM task_runs WHERE id = ?",
                    ("run-1",),
                ).fetchone()
                self.assertIsNotNone(run_1_state)
                self.assertEqual(int(run_1_state["converter_elapsed_sec"] or 0), 12)
                self.assertIsNotNone(run_1_state["finish"])

                run_keep_state = conn.execute(
                    "SELECT converter_elapsed_sec, finish FROM task_runs WHERE id = ?",
                    ("run-keep",),
                ).fetchone()
                self.assertIsNotNone(run_keep_state)
                self.assertEqual(int(run_keep_state["converter_elapsed_sec"] or 0), 0)
                self.assertIsNone(run_keep_state["finish"])

                runs_count = conn.execute("SELECT COUNT(*) AS cnt FROM task_runs").fetchone()
                tasks_count = conn.execute("SELECT COUNT(*) AS cnt FROM crawl_tasks").fetchone()
                self.assertEqual(int(runs_count["cnt"]), 3)
                self.assertEqual(int(tasks_count["cnt"]), 1)
            finally:
                conn.close()
        finally:
            db_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
