from __future__ import annotations

from .adapters import (
    CatalogMySQLRepository,
    CatalogSQLiteRepository,
    ReceiverMySQLRepository,
    ReceiverSQLiteRepository,
    is_mysql_dsn,
)
from .core.registry import HandlerRegistry
from .daemon import ConverterDaemon, PollingJob
from .parsers import register_builtin_handlers
from .pipeline import ConverterPipeline
from .sync import ConverterSyncService, SyncBatchEvent, SyncJob, SyncOutcome


def build_default_pipeline() -> ConverterPipeline:
    registry = HandlerRegistry()
    register_builtin_handlers(registry)
    return ConverterPipeline(registry=registry)


__all__ = [
    "CatalogMySQLRepository",
    "CatalogSQLiteRepository",
    "ConverterDaemon",
    "ConverterPipeline",
    "ConverterSyncService",
    "PollingJob",
    "ReceiverMySQLRepository",
    "ReceiverSQLiteRepository",
    "SyncBatchEvent",
    "SyncJob",
    "SyncOutcome",
    "build_default_pipeline",
    "is_mysql_dsn",
]
