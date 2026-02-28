from __future__ import annotations

from .catalog import CatalogRepository
from .mysql_common import parse_mysql_dsn


class CatalogMySQLRepository(CatalogRepository):
    @classmethod
    def from_dsn(cls, dsn: str) -> "CatalogMySQLRepository":
        kwargs = parse_mysql_dsn(dsn)
        user = kwargs.get("user", "")
        password = kwargs.get("password", "")
        host = kwargs.get("host", "127.0.0.1")
        port = kwargs.get("port", 3306)
        database = kwargs.get("database", "")
        charset = kwargs.get("charset", "utf8mb4")

        database_url = (
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset={charset}"
        )
        return cls(database_url)
