from __future__ import annotations

import unittest

from converter.adapters import MySQLDsnError, is_mysql_dsn, parse_mysql_dsn


class MySQLCommonTests(unittest.TestCase):
    def test_parse_mysql_dsn(self) -> None:
        cfg = parse_mysql_dsn("mysql://user:pass@db.example:3307/catalog?charset=utf8mb4")
        self.assertEqual(cfg["host"], "db.example")
        self.assertEqual(cfg["port"], 3307)
        self.assertEqual(cfg["user"], "user")
        self.assertEqual(cfg["password"], "pass")
        self.assertEqual(cfg["database"], "catalog")
        self.assertEqual(cfg["charset"], "utf8mb4")

    def test_parse_mysql_pymysql_dsn(self) -> None:
        cfg = parse_mysql_dsn("mysql+pymysql://u:p@127.0.0.1:3306/receiver")
        self.assertEqual(cfg["host"], "127.0.0.1")
        self.assertEqual(cfg["database"], "receiver")

    def test_parse_mysql_dsn_requires_database(self) -> None:
        with self.assertRaises(MySQLDsnError):
            parse_mysql_dsn("mysql://user:pass@127.0.0.1")

    def test_is_mysql_dsn(self) -> None:
        self.assertTrue(is_mysql_dsn("mysql://u:p@h:3306/db"))
        self.assertTrue(is_mysql_dsn("mysql+pymysql://u:p@h:3306/db"))
        self.assertFalse(is_mysql_dsn("sqlite:///tmp/test.db"))
        self.assertFalse(is_mysql_dsn("/tmp/test.db"))


if __name__ == "__main__":
    unittest.main()
