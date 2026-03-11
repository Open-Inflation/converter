from __future__ import annotations

import unittest
from unittest.mock import patch

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from converter.adapters.catalog import _CatalogBase
from converter.adapters.catalog_postgres import CatalogPostgreSQLRepository
from converter.adapters.receiver import _ReceiverBase
from converter.adapters.receiver_postgres import ReceiverPostgreSQLRepository


class PostgreSQLRepositoryWrappersTests(unittest.TestCase):
    def test_receiver_from_dsn_builds_sqlalchemy_url(self) -> None:
        with patch.object(ReceiverPostgreSQLRepository, "__init__", return_value=None) as init_mock:
            ReceiverPostgreSQLRepository.from_dsn(
                "postgresql+psycopg://u:p@127.0.0.1:5432/receiver?sslmode=require"
            )
            init_mock.assert_called_once()
            url = init_mock.call_args.args[0]
            self.assertEqual(
                url,
                "postgresql+psycopg://u:p@127.0.0.1:5432/receiver?sslmode=require",
            )

    def test_catalog_from_dsn_builds_sqlalchemy_url(self) -> None:
        with patch.object(CatalogPostgreSQLRepository, "__init__", return_value=None) as init_mock:
            CatalogPostgreSQLRepository.from_dsn(
                "postgresql+psycopg://u:p@127.0.0.1:5432/catalog?sslmode=require"
            )
            init_mock.assert_called_once()
            url = init_mock.call_args.args[0]
            self.assertEqual(
                url,
                "postgresql+psycopg://u:p@127.0.0.1:5432/catalog?sslmode=require",
            )

    def test_receiver_models_compile_for_postgresql(self) -> None:
        dialect = postgresql.dialect()
        for table in _ReceiverBase.metadata.sorted_tables:
            sql = str(CreateTable(table).compile(dialect=dialect))
            self.assertIn("CREATE TABLE", sql)

    def test_catalog_models_compile_for_postgresql(self) -> None:
        dialect = postgresql.dialect()
        for table in _CatalogBase.metadata.sorted_tables:
            sql = str(CreateTable(table).compile(dialect=dialect))
            self.assertIn("CREATE TABLE", sql)


if __name__ == "__main__":
    unittest.main()
