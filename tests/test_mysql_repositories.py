from __future__ import annotations

import unittest
from datetime import datetime, timezone
from typing import Any

from converter.adapters.catalog_mysql import CatalogMySQLRepository
from converter.adapters.receiver_mysql import ReceiverMySQLRepository
from converter.core.models import NormalizedProductRecord


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn
        self._result: Any = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, query: str, params: Any = None) -> None:
        self._conn.executed.append((query, params))
        self._result = self._conn.dispatch(query, params)

    def fetchone(self) -> Any:
        if isinstance(self._result, list):
            return self._result[0] if self._result else None
        return self._result

    def fetchall(self) -> Any:
        if isinstance(self._result, list):
            return self._result
        if self._result is None:
            return []
        return [self._result]


class _FakeConn:
    def __init__(self, dispatcher) -> None:
        self._dispatcher = dispatcher
        self.executed: list[tuple[str, Any]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def dispatch(self, query: str, params: Any) -> Any:
        return self._dispatcher(query, params)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class ReceiverMySQLRepositoryTests(unittest.TestCase):
    def test_fetch_batch_maps_rows(self) -> None:
        def dispatcher(query: str, _params: Any) -> Any:
            q = " ".join(query.lower().split())
            if "information_schema.columns" in q:
                return [{"ok": 1}]
            if "from run_artifact_products as p" in q:
                return [
                    {
                        "product_id": 1,
                        "artifact_id": 10,
                        "product_sku": "sku-1",
                        "product_plu": "plu-1",
                        "product_title": "Шоколад молочный, 200 г, 10 шт",
                        "product_composition": "Сахар",
                        "product_brand": "Brand",
                        "product_unit": "PCE",
                        "product_available_count": 10,
                        "product_package_quantity": 0.2,
                        "product_package_unit": "KGM",
                        "category_uids_json": '["cat-1"]',
                        "product_main_image": "images/main.jpg",
                        "product_sort_order": 0,
                        "run_id": "run-1",
                        "artifact_source": "output_json",
                        "ingested_at": datetime(2026, 2, 28, 10, 0, 0),
                        "parser_name": "fixprice",
                        "geo_name": "Москва",
                        "geo_region": "г. Москва",
                        "geo_country": "RUS",
                    }
                ]
            if "from run_artifact_categories" in q:
                return [{"artifact_id": 10, "uid": "cat-1", "title": "Продукты"}]
            if "from run_artifact_product_images" in q:
                return [{"product_id": 1, "url": "images/main.jpg"}]
            return []

        fake_conn = _FakeConn(dispatcher)
        repo = ReceiverMySQLRepository(
            {
                "host": "127.0.0.1",
                "port": 3306,
                "user": "u",
                "password": "p",
                "database": "receiver",
            },
            connect_fn=lambda **_: fake_conn,
        )

        rows = repo.fetch_batch(limit=10, parser_name="fixprice")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].sku, "sku-1")
        self.assertEqual(rows[0].category, "Продукты")
        self.assertEqual(rows[0].image_urls, ["images/main.jpg"])


class CatalogMySQLRepositoryTests(unittest.TestCase):
    def test_upsert_many_executes_without_mysql_server(self) -> None:
        def dispatcher(_query: str, _params: Any) -> Any:
            return []

        fake_conn = _FakeConn(dispatcher)
        repo = CatalogMySQLRepository(
            {
                "host": "127.0.0.1",
                "port": 3306,
                "user": "u",
                "password": "p",
                "database": "catalog",
            },
            connect_fn=lambda **_: fake_conn,
        )

        record = NormalizedProductRecord(
            parser_name="fixprice",
            raw_title="Шоколад молочный, 200 г, 10 шт",
            title_original="Шоколад молочный",
            title_normalized="шоколад молочный",
            title_original_no_stopwords="шоколад молочный",
            title_normalized_no_stopwords="шоколад молочный",
            brand="Brand",
            unit="PCE",
            available_count=10,
            package_quantity=0.2,
            package_unit="KGM",
            source_id="receiver:run-1:1",
            plu="plu-1",
            sku="sku-1",
            category_raw="Продукты",
            category_normalized="продукты",
            geo_raw="RUS, г. Москва, Москва",
            geo_normalized="rus, г. москва, москва",
            composition_raw="Сахар",
            composition_normalized="сахар",
            image_urls=["images/main.jpg"],
            observed_at=datetime(2026, 2, 28, 10, 0, 0, tzinfo=timezone.utc),
            raw_payload={"receiver_product_id": 1},
        )

        repo.upsert_many([record])
        self.assertIsNotNone(record.canonical_product_id)
        self.assertTrue(fake_conn.committed)
        self.assertGreater(len(fake_conn.executed), 5)


if __name__ == "__main__":
    unittest.main()
