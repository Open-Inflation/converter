from __future__ import annotations

from urllib.parse import parse_qs, unquote, urlparse


class MySQLDsnError(ValueError):
    pass


def parse_mysql_dsn(dsn: str) -> dict[str, object]:
    token = dsn.strip()
    if token.startswith("mysql+pymysql://"):
        token = "mysql://" + token[len("mysql+pymysql://") :]

    parsed = urlparse(token)
    if parsed.scheme != "mysql":
        raise MySQLDsnError(f"Unsupported DSN scheme: {parsed.scheme!r}")

    database = parsed.path.lstrip("/")
    if not database:
        raise MySQLDsnError("MySQL DSN must include database name")

    query = parse_qs(parsed.query)
    charset = query.get("charset", ["utf8mb4"])[0]

    connect_kwargs: dict[str, object] = {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 3306,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": database,
        "charset": charset,
        "autocommit": False,
    }

    return connect_kwargs


def is_mysql_dsn(value: str) -> bool:
    token = value.strip().lower()
    return token.startswith("mysql://") or token.startswith("mysql+pymysql://")
