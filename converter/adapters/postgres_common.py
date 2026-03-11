from __future__ import annotations

from urllib.parse import parse_qs, unquote, urlparse


class PostgreSQLDsnError(ValueError):
    pass


def parse_postgres_dsn(dsn: str) -> dict[str, object]:
    token = dsn.strip()
    if token.startswith("postgres://"):
        token = "postgresql://" + token[len("postgres://") :]
    if token.startswith("postgresql+psycopg://"):
        token = "postgresql://" + token[len("postgresql+psycopg://") :]

    parsed = urlparse(token)
    if parsed.scheme != "postgresql":
        raise PostgreSQLDsnError(f"Unsupported DSN scheme: {parsed.scheme!r}")

    database = parsed.path.lstrip("/")
    if not database:
        raise PostgreSQLDsnError("PostgreSQL DSN must include database name")

    query = parse_qs(parsed.query)
    connect_kwargs: dict[str, object] = {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 5432,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": database,
        "query": query,
    }
    return connect_kwargs


def is_postgres_dsn(value: str) -> bool:
    token = value.strip().lower()
    return (
        token.startswith("postgresql://")
        or token.startswith("postgresql+psycopg://")
        or token.startswith("postgres://")
    )
