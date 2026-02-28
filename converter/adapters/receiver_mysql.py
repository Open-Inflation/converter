from __future__ import annotations

from .mysql_common import parse_mysql_dsn
from .receiver import ReceiverRepository


class ReceiverMySQLRepository(ReceiverRepository):
    @classmethod
    def from_dsn(cls, dsn: str, *, default_parser_name: str = "fixprice") -> "ReceiverMySQLRepository":
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
        return cls(database_url, default_parser_name=default_parser_name)
