from __future__ import annotations

from .adapters import (
    CatalogMySQLRepository,
    CatalogSQLiteRepository,
    ReceiverMySQLRepository,
    ReceiverSQLiteRepository,
    is_mysql_dsn,
)
from .core.registry import HandlerRegistry
from .parsers import register_builtin_handlers
from .pipeline import ConverterPipeline


def build_default_pipeline() -> ConverterPipeline:
    registry = HandlerRegistry()
    register_builtin_handlers(registry)
    return ConverterPipeline(registry=registry)


__all__ = [
    "CatalogMySQLRepository",
    "CatalogSQLiteRepository",
    "ConverterPipeline",
    "ReceiverMySQLRepository",
    "ReceiverSQLiteRepository",
    "build_default_pipeline",
    "is_mysql_dsn",
]
