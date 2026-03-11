from __future__ import annotations

import unittest

from converter.adapters import PostgreSQLDsnError, is_postgres_dsn, parse_postgres_dsn


class PostgreSQLCommonTests(unittest.TestCase):
    def test_parse_postgresql_dsn(self) -> None:
        cfg = parse_postgres_dsn("postgresql://user:pass@db.example:5433/catalog?sslmode=require")
        self.assertEqual(cfg["host"], "db.example")
        self.assertEqual(cfg["port"], 5433)
        self.assertEqual(cfg["user"], "user")
        self.assertEqual(cfg["password"], "pass")
        self.assertEqual(cfg["database"], "catalog")
        self.assertEqual(cfg["query"], {"sslmode": ["require"]})

    def test_parse_postgresql_psycopg_dsn(self) -> None:
        cfg = parse_postgres_dsn("postgresql+psycopg://u:p@127.0.0.1:5432/receiver")
        self.assertEqual(cfg["host"], "127.0.0.1")
        self.assertEqual(cfg["database"], "receiver")

    def test_parse_postgresql_dsn_requires_database(self) -> None:
        with self.assertRaises(PostgreSQLDsnError):
            parse_postgres_dsn("postgresql://user:pass@127.0.0.1")

    def test_is_postgresql_dsn(self) -> None:
        self.assertTrue(is_postgres_dsn("postgresql://u:p@h:5432/db"))
        self.assertTrue(is_postgres_dsn("postgresql+psycopg://u:p@h:5432/db"))
        self.assertTrue(is_postgres_dsn("postgres://u:p@h:5432/db"))
        self.assertFalse(is_postgres_dsn("sqlite:///tmp/test.db"))
        self.assertFalse(is_postgres_dsn("/tmp/test.db"))


if __name__ == "__main__":
    unittest.main()
