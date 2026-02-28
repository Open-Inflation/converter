from __future__ import annotations

import unittest
from unittest.mock import patch

from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable

from converter.adapters.catalog import _CatalogBase
from converter.adapters.catalog_mysql import CatalogMySQLRepository
from converter.adapters.receiver import _ReceiverBase
from converter.adapters.receiver_mysql import ReceiverMySQLRepository


class MySQLRepositoryWrappersTests(unittest.TestCase):
    def test_receiver_from_dsn_builds_sqlalchemy_url(self) -> None:
        with patch.object(ReceiverMySQLRepository, "__init__", return_value=None) as init_mock:
            ReceiverMySQLRepository.from_dsn("mysql+pymysql://u:p@127.0.0.1:3306/receiver?charset=utf8mb4")
            init_mock.assert_called_once()
            url = init_mock.call_args.args[0]
            self.assertEqual(url, "mysql+pymysql://u:p@127.0.0.1:3306/receiver?charset=utf8mb4")

    def test_catalog_from_dsn_builds_sqlalchemy_url(self) -> None:
        with patch.object(CatalogMySQLRepository, "__init__", return_value=None) as init_mock:
            CatalogMySQLRepository.from_dsn("mysql+pymysql://u:p@127.0.0.1:3306/catalog?charset=utf8mb4")
            init_mock.assert_called_once()
            url = init_mock.call_args.args[0]
            self.assertEqual(url, "mysql+pymysql://u:p@127.0.0.1:3306/catalog?charset=utf8mb4")

    def test_receiver_models_compile_for_mysql(self) -> None:
        dialect = mysql.dialect()
        for table in _ReceiverBase.metadata.sorted_tables:
            sql = str(CreateTable(table).compile(dialect=dialect))
            self.assertIn("CREATE TABLE", sql)

    def test_catalog_models_compile_for_mysql(self) -> None:
        dialect = mysql.dialect()
        for table in _CatalogBase.metadata.sorted_tables:
            sql = str(CreateTable(table).compile(dialect=dialect))
            self.assertIn("CREATE TABLE", sql)


if __name__ == "__main__":
    unittest.main()
