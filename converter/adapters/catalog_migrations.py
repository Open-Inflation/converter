from __future__ import annotations

import logging
import os

from sqlalchemy import inspect

from converter.core.ports import StorageRepository


LOGGER = logging.getLogger(__name__)


class _CatalogSchemaMigrationMixin:
    _SNAPSHOT_COLUMNS = (
        "id",
        "canonical_product_id",
        "parser_name",
        "source_id",
        "source_run_id",
        "receiver_product_id",
        "receiver_artifact_id",
        "receiver_sort_order",
        "source_event_uid",
        "content_fingerprint",
        "valid_from_at",
        "valid_to_at",
        "observed_at",
        "created_at",
        "price",
        "discount_price",
        "loyal_price",
        "price_unit",
        "available_count",
    )
    _LEGACY_SNAPSHOT_TABLES = (
        "catalog_snapshot_events",
        "catalog_snapshot_available_counts",
        "catalog_snapshot_assets",
    )

    def _validate_catalog_products_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_products"):
            LOGGER.debug("Catalog schema validation skipped: table catalog_products not found yet")
            return

        product_columns = {item["name"] for item in inspector.get_columns("catalog_products")}
        required_product_columns = (
            "primary_category_id",
            "settlement_id",
            "price",
            "discount_price",
            "loyal_price",
            "price_unit",
            "composition_original",
        )
        missing_product_columns = [
            name for name in required_product_columns if name not in product_columns
        ]
        if missing_product_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_products`: missing columns "
                f"{', '.join(missing_product_columns)}. Use current SQL schema migration."
            )

        forbidden_product_json_columns = [
            name
            for name in (
                "image_urls_json",
                "duplicate_image_urls_json",
                "image_fingerprints_json",
                "source_payload_json",
            )
            if name in product_columns
        ]
        if forbidden_product_json_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_products`: JSON columns are not allowed "
                f"({', '.join(forbidden_product_json_columns)}). Use normalized schema."
            )
        self._ensure_decimal_columns(
            inspector=inspector,
            table_name="catalog_products",
            column_names=("price", "discount_price", "loyal_price"),
        )

        if not inspector.has_table("catalog_product_snapshots"):
            raise RuntimeError(
                "Schema mismatch: missing table `catalog_product_snapshots`. "
                "Run the current PostgreSQL schema migration."
            )
        snapshot_columns = {item["name"] for item in inspector.get_columns("catalog_product_snapshots")}
        missing_snapshot_columns = [name for name in self._SNAPSHOT_COLUMNS if name not in snapshot_columns]
        if missing_snapshot_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_product_snapshots`: missing columns "
                f"{', '.join(missing_snapshot_columns)}. Run SQL migration."
            )

        unexpected_snapshot_columns = [
            name for name in snapshot_columns if name not in set(self._SNAPSHOT_COLUMNS)
        ]
        if unexpected_snapshot_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_product_snapshots`: unexpected legacy columns "
                f"({', '.join(sorted(unexpected_snapshot_columns))}). Run SQL migration."
            )
        self._ensure_decimal_columns(
            inspector=inspector,
            table_name="catalog_product_snapshots",
            column_names=("price", "discount_price", "loyal_price"),
        )

        for table_name in self._LEGACY_SNAPSHOT_TABLES:
            if inspector.has_table(table_name):
                raise RuntimeError(
                    f"Schema mismatch: legacy table `{table_name}` is not allowed. Run SQL migration."
                )

        if not inspector.has_table("catalog_product_assets"):
            raise RuntimeError(
                "Schema mismatch: missing table `catalog_product_assets`. Use current schema."
            )

        if inspector.has_table("catalog_settlements"):
            settlement_columns = {item["name"] for item in inspector.get_columns("catalog_settlements")}
            if "geo_point" not in settlement_columns:
                raise RuntimeError(
                    "Schema mismatch in `catalog_settlements`: missing column geo_point."
                )
        if inspector.has_table("catalog_settlement_geodata"):
            geodata_columns = {item["name"] for item in inspector.get_columns("catalog_settlement_geodata")}
            if "geo_point" not in geodata_columns:
                raise RuntimeError(
                    "Schema mismatch in `catalog_settlement_geodata`: missing column geo_point."
                )

        if self._engine.dialect.name == "postgresql":
            self._ensure_postgresql_types(inspector=inspector)

        LOGGER.debug("Catalog schema validation passed")

    @staticmethod
    def _ensure_decimal_columns(
        *,
        inspector,
        table_name: str,
        column_names: tuple[str, ...],
    ) -> None:
        columns = {item["name"]: item for item in inspector.get_columns(table_name)}
        invalid_columns: list[str] = []
        for column_name in column_names:
            info = columns.get(column_name)
            if info is None:
                continue
            type_token = str(info.get("type") or "").upper()
            if "DECIMAL" not in type_token and "NUMERIC" not in type_token:
                invalid_columns.append(f"{column_name}={type_token or 'UNKNOWN'}")
        if invalid_columns:
            raise RuntimeError(
                f"Schema mismatch in `{table_name}`: money columns must be DECIMAL/NUMERIC "
                f"({', '.join(invalid_columns)}). Run SQL migration."
            )

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

    @staticmethod
    def _ensure_postgresql_types(*, inspector) -> None:
        checks = (
            ("catalog_products", "canonical_product_id", ("UUID",)),
            ("catalog_product_sources", "canonical_product_id", ("UUID",)),
            ("catalog_identity_map", "canonical_product_id", ("UUID",)),
            ("catalog_storage_delete_outbox", "status", ("ENUM",)),
            ("catalog_product_assets", "asset_kind", ("ENUM",)),
            ("catalog_settlements", "geo_point", ("GEOGRAPHY", "USER-DEFINED")),
            ("catalog_settlement_geodata", "geo_point", ("GEOGRAPHY", "USER-DEFINED")),
        )
        problems: list[str] = []
        for table_name, column_name, expected_tokens in checks:
            if not inspector.has_table(table_name):
                continue
            columns = {item["name"]: item for item in inspector.get_columns(table_name)}
            info = columns.get(column_name)
            if info is None:
                continue
            type_token = str(info.get("type") or "").upper()
            if not any(expected in type_token for expected in expected_tokens):
                problems.append(f"{table_name}.{column_name}={type_token or 'UNKNOWN'}")
        if problems:
            raise RuntimeError(
                "Schema mismatch in PostgreSQL type mapping: "
                f"{', '.join(problems)}"
            )
