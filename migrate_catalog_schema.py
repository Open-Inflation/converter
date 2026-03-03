from __future__ import annotations

import argparse
from pathlib import Path

from converter.adapters.catalog import CatalogRepository
from converter.adapters.mysql_common import is_mysql_dsn, parse_mysql_dsn


def _database_url(catalog_db: str) -> str:
    token = catalog_db.strip()
    if is_mysql_dsn(token):
        kwargs = parse_mysql_dsn(token)
        user = str(kwargs.get("user", ""))
        password = str(kwargs.get("password", ""))
        host = str(kwargs.get("host", "127.0.0.1"))
        port = int(kwargs.get("port", 3306))
        database = str(kwargs.get("database", ""))
        charset = str(kwargs.get("charset", "utf8mb4"))
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset={charset}"

    path = Path(token)
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{path.resolve()}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run one-way manual migration for catalog snapshot schema."
    )
    parser.add_argument(
        "--catalog-db",
        required=True,
        help="SQLite path or MySQL DSN (mysql://... or mysql+pymysql://...).",
    )
    args = parser.parse_args()

    repo = CatalogRepository(_database_url(args.catalog_db), validate_schema=False)
    repo.migrate_schema()
    print("Catalog schema migration completed")


if __name__ == "__main__":
    main()
