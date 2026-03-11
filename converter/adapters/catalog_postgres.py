from __future__ import annotations

from urllib.parse import urlencode, quote

from .catalog import CatalogRepository
from .postgres_common import parse_postgres_dsn


class CatalogPostgreSQLRepository(CatalogRepository):
    @classmethod
    def from_dsn(cls, dsn: str) -> "CatalogPostgreSQLRepository":
        kwargs = parse_postgres_dsn(dsn)
        user = quote(str(kwargs.get("user", "")), safe="")
        password = quote(str(kwargs.get("password", "")), safe="")
        host = kwargs.get("host", "127.0.0.1")
        port = kwargs.get("port", 5432)
        database = kwargs.get("database", "")
        query = kwargs.get("query", {})
        query_token = urlencode(query, doseq=True) if isinstance(query, dict) and query else ""

        auth = f"{user}:{password}@" if user or password else ""
        database_url = f"postgresql+psycopg://{auth}{host}:{port}/{database}"
        if query_token:
            database_url = f"{database_url}?{query_token}"
        return cls(database_url)
