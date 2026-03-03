from __future__ import annotations

import logging
import os

from sqlalchemy import inspect, text

from converter.core.ports import StorageRepository


LOGGER = logging.getLogger(__name__)


class _CatalogSchemaMigrationMixin:
    def _validate_catalog_products_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_products"):
            LOGGER.debug("Catalog schema validation skipped: table catalog_products not found yet")
            return

        columns = {item["name"] for item in inspector.get_columns("catalog_products")}
        required = (
            "primary_category_id",
            "settlement_id",
            "price",
            "discount_price",
            "loyal_price",
            "price_unit",
            "composition_original",
        )
        missing = [name for name in required if name not in columns]
        if missing:
            raise RuntimeError(
                "Schema mismatch in `catalog_products`: missing columns "
                f"{', '.join(missing)}. Use the current converter schema."
            )
        forbidden_product_json_columns = [
            name
            for name in (
                "image_urls_json",
                "duplicate_image_urls_json",
                "image_fingerprints_json",
                "source_payload_json",
            )
            if name in columns
        ]
        if forbidden_product_json_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_products`: JSON columns are not allowed "
                f"({', '.join(forbidden_product_json_columns)}). Use normalized tables."
            )

        if not inspector.has_table("catalog_product_snapshots"):
            LOGGER.debug("Catalog schema validation partially skipped: table catalog_product_snapshots not found yet")
            return
        snapshot_columns = {item["name"] for item in inspector.get_columns("catalog_product_snapshots")}
        snapshot_required = (
            "price",
            "discount_price",
            "loyal_price",
            "price_unit",
            "composition_original",
        )
        snapshot_missing = [name for name in snapshot_required if name not in snapshot_columns]
        if snapshot_missing:
            raise RuntimeError(
                "Schema mismatch in `catalog_product_snapshots`: missing columns "
                f"{', '.join(snapshot_missing)}. Use the current converter schema."
            )
        forbidden_snapshot_json_columns = [
            name
            for name in (
                "image_urls_json",
                "duplicate_image_urls_json",
                "image_fingerprints_json",
                "source_payload_json",
            )
            if name in snapshot_columns
        ]
        if forbidden_snapshot_json_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_product_snapshots`: JSON columns are not allowed "
                f"({', '.join(forbidden_snapshot_json_columns)}). Use normalized tables."
            )

        expected_tables = (
            "catalog_product_assets",
            "catalog_snapshot_assets",
        )
        for table_name in expected_tables:
            if not inspector.has_table(table_name):
                raise RuntimeError(
                    f"Schema mismatch: missing table `{table_name}`. Use the current converter schema."
                )
        LOGGER.debug("Catalog schema validation passed")

    def _ensure_snapshot_interval_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_product_snapshots"):
            return

        snapshot_columns = {item["name"] for item in inspector.get_columns("catalog_product_snapshots")}
        missing_columns = [
            column_name
            for column_name in ("content_fingerprint", "valid_from_at", "valid_to_at")
            if column_name not in snapshot_columns
        ]
        if not missing_columns:
            return

        dialect = self._engine.dialect.name
        with self._engine.begin() as connection:
            for column_name in missing_columns:
                if dialect == "mysql":
                    if column_name == "content_fingerprint":
                        ddl = (
                            "ALTER TABLE catalog_product_snapshots "
                            "ADD COLUMN content_fingerprint VARCHAR(64) NULL"
                        )
                    else:
                        ddl = (
                            f"ALTER TABLE catalog_product_snapshots "
                            f"ADD COLUMN {column_name} DATETIME(6) NULL"
                        )
                elif dialect == "sqlite":
                    if column_name == "content_fingerprint":
                        ddl = (
                            "ALTER TABLE catalog_product_snapshots "
                            "ADD COLUMN content_fingerprint VARCHAR(64)"
                        )
                    else:
                        ddl = (
                            f"ALTER TABLE catalog_product_snapshots "
                            f"ADD COLUMN {column_name} DATETIME"
                        )
                else:
                    raise RuntimeError(
                        f"Unsupported dialect for snapshot schema migration: {dialect}"
                    )
                connection.execute(text(ddl))

        LOGGER.info(
            "Catalog snapshot schema migrated: added_columns=%s",
            ",".join(missing_columns),
        )

    def _ensure_product_sources_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_product_sources"):
            return

        columns = {item["name"] for item in inspector.get_columns("catalog_product_sources")}
        if "latest_content_fingerprint" not in columns:
            dialect = self._engine.dialect.name
            with self._engine.begin() as connection:
                if dialect == "mysql":
                    ddl = (
                        "ALTER TABLE catalog_product_sources "
                        "ADD COLUMN latest_content_fingerprint VARCHAR(64) NULL"
                    )
                else:
                    ddl = (
                        "ALTER TABLE catalog_product_sources "
                        "ADD COLUMN latest_content_fingerprint VARCHAR(64)"
                    )
                connection.execute(text(ddl))
            LOGGER.info("Catalog product_sources schema migrated: added latest_content_fingerprint")

        indexes = {idx.get("name") for idx in inspector.get_indexes("catalog_product_sources")}
        if "idx_cps_last_seen" not in indexes:
            with self._engine.begin() as connection:
                connection.execute(
                    text(
                        "CREATE INDEX idx_cps_last_seen ON catalog_product_sources (last_seen_at)"
                    )
                )
            LOGGER.info("Catalog product_sources schema migrated: added idx_cps_last_seen")

    def _ensure_snapshot_event_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_product_snapshots"):
            return

        columns = {item["name"] for item in inspector.get_columns("catalog_product_snapshots")}
        if "source_event_uid" not in columns:
            dialect = self._engine.dialect.name
            with self._engine.begin() as connection:
                if dialect == "mysql":
                    ddl = (
                        "ALTER TABLE catalog_product_snapshots "
                        "ADD COLUMN source_event_uid VARCHAR(191) NULL"
                    )
                else:
                    ddl = (
                        "ALTER TABLE catalog_product_snapshots "
                        "ADD COLUMN source_event_uid VARCHAR(191)"
                    )
                connection.execute(text(ddl))
            LOGGER.info("Catalog snapshots schema migrated: added source_event_uid")

        indexes = {idx.get("name") for idx in inspector.get_indexes("catalog_product_snapshots")}
        if "idx_cps_latest" not in indexes:
            with self._engine.begin() as connection:
                connection.execute(
                    text(
                        "CREATE INDEX idx_cps_latest ON catalog_product_snapshots (parser_name, source_id, id)"
                    )
                )
            LOGGER.info("Catalog snapshots schema migrated: added idx_cps_latest")

        unique_constraints = {
            item.get("name")
            for item in inspector.get_unique_constraints("catalog_product_snapshots")
        }
        if "uq_cps_event" not in unique_constraints:
            dialect = self._engine.dialect.name
            with self._engine.begin() as connection:
                if dialect == "mysql":
                    connection.execute(
                        text(
                            "ALTER TABLE catalog_product_snapshots "
                            "ADD CONSTRAINT uq_cps_event UNIQUE (source_event_uid)"
                        )
                    )
                else:
                    connection.execute(
                        text(
                            "CREATE UNIQUE INDEX uq_cps_event "
                            "ON catalog_product_snapshots (source_event_uid)"
                        )
                    )
            LOGGER.info("Catalog snapshots schema migrated: added uq_cps_event")

    def _ensure_catalog_products_constraints(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_products"):
            return

        unique_constraints = {
            item.get("name")
            for item in inspector.get_unique_constraints("catalog_products")
        }
        indexes = {idx.get("name") for idx in inspector.get_indexes("catalog_products")}
        if "uq_catalog_products_source" in unique_constraints or "uq_catalog_products_source" in indexes:
            return

        dialect = self._engine.dialect.name
        try:
            with self._engine.begin() as connection:
                if dialect == "mysql":
                    connection.execute(
                        text(
                            "ALTER TABLE catalog_products "
                            "ADD CONSTRAINT uq_catalog_products_source UNIQUE (parser_name, source_id)"
                        )
                    )
                else:
                    connection.execute(
                        text(
                            "CREATE UNIQUE INDEX uq_catalog_products_source "
                            "ON catalog_products (parser_name, source_id)"
                        )
                    )
        except Exception as exc:
            raise RuntimeError(
                "Unable to add unique constraint uq_catalog_products_source on catalog_products(parser_name, source_id). "
                "Check and deduplicate existing rows before running converter v2."
            ) from exc
        LOGGER.info("Catalog products schema migrated: added uq_catalog_products_source")

    @staticmethod
    def _build_storage_repository_from_env() -> StorageRepository | None:
        base_url = (
            (os.getenv("CONVERTER_STORAGE_BASE_URL") or "").strip()
            or (os.getenv("STORAGE_BASE_URL") or "").strip()
        )
        api_token = (
            (os.getenv("CONVERTER_STORAGE_API_TOKEN") or "").strip()
            or (os.getenv("STORAGE_API_TOKEN") or "").strip()
        )
        if not base_url or not api_token:
            LOGGER.debug("Catalog storage adapter disabled: missing base_url or api_token")
            return None

        strict_raw = (os.getenv("CONVERTER_STORAGE_DELETE_STRICT") or "0").strip().lower()
        fail_on_error = strict_raw in {"1", "true", "yes", "y", "on"}

        timeout_raw = (os.getenv("CONVERTER_STORAGE_DELETE_TIMEOUT_SEC") or "10").strip()
        try:
            timeout_seconds = max(0.1, float(timeout_raw))
        except ValueError:
            timeout_seconds = 10.0

        from .storage_http import StorageHTTPRepository

        LOGGER.info(
            "Catalog storage adapter enabled: base_url=%s fail_on_error=%s timeout_seconds=%.1f",
            base_url,
            fail_on_error,
            timeout_seconds,
        )
        return StorageHTTPRepository(
            base_url=base_url,
            api_token=api_token,
            timeout_seconds=timeout_seconds,
            fail_on_error=fail_on_error,
        )
