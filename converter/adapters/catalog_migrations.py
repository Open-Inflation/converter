from __future__ import annotations

import logging
import os

from sqlalchemy import inspect, text
from sqlalchemy.exc import OperationalError

from converter.core.ports import StorageRepository


LOGGER = logging.getLogger(__name__)


class _CatalogSchemaMigrationMixin:
    _SNAPSHOT_EVENT_COLUMNS = (
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
    )
    _SNAPSHOT_PRICE_COLUMNS = (
        "id",
        "price",
        "discount_price",
        "loyal_price",
        "price_unit",
    )
    _SNAPSHOT_AVAILABLE_COLUMNS = (
        "snapshot_id",
        "available_count",
        "created_at",
    )

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

        expected_tables = (
            "catalog_snapshot_events",
            "catalog_product_snapshots",
            "catalog_snapshot_available_counts",
            "catalog_product_assets",
        )
        for table_name in expected_tables:
            if not inspector.has_table(table_name):
                raise RuntimeError(
                    f"Schema mismatch: missing table `{table_name}`. Use the current converter schema."
                )

        event_columns = {item["name"] for item in inspector.get_columns("catalog_snapshot_events")}
        missing_event_columns = [
            name for name in self._SNAPSHOT_EVENT_COLUMNS if name not in event_columns
        ]
        if missing_event_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_snapshot_events`: missing columns "
                f"{', '.join(missing_event_columns)}. Use the current converter schema."
            )
        unexpected_event_columns = [
            name for name in event_columns if name not in set(self._SNAPSHOT_EVENT_COLUMNS)
        ]
        if unexpected_event_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_snapshot_events`: legacy columns are not allowed "
                f"({', '.join(sorted(unexpected_event_columns))}). Run one-way migration."
            )

        price_columns = {item["name"] for item in inspector.get_columns("catalog_product_snapshots")}
        missing_price_columns = [
            name for name in self._SNAPSHOT_PRICE_COLUMNS if name not in price_columns
        ]
        if missing_price_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_product_snapshots`: missing columns "
                f"{', '.join(missing_price_columns)}. Use the current converter schema."
            )
        unexpected_price_columns = [
            name for name in price_columns if name not in set(self._SNAPSHOT_PRICE_COLUMNS)
        ]
        if unexpected_price_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_product_snapshots`: legacy columns are not allowed "
                f"({', '.join(sorted(unexpected_price_columns))}). Run one-way migration."
            )

        available_columns = {
            item["name"] for item in inspector.get_columns("catalog_snapshot_available_counts")
        }
        missing_available_columns = [
            name for name in self._SNAPSHOT_AVAILABLE_COLUMNS if name not in available_columns
        ]
        if missing_available_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_snapshot_available_counts`: missing columns "
                f"{', '.join(missing_available_columns)}. Use the current converter schema."
            )
        unexpected_available_columns = [
            name for name in available_columns if name not in set(self._SNAPSHOT_AVAILABLE_COLUMNS)
        ]
        if unexpected_available_columns:
            raise RuntimeError(
                "Schema mismatch in `catalog_snapshot_available_counts`: legacy columns are not allowed "
                f"({', '.join(sorted(unexpected_available_columns))}). Run one-way migration."
            )

        has_price_fk = self._table_has_fk(
            inspector,
            table_name="catalog_product_snapshots",
            constrained_columns=("id",),
            referred_table="catalog_snapshot_events",
            referred_columns=("id",),
        )
        if not has_price_fk:
            LOGGER.debug(
                "Catalog schema validation: FK catalog_product_snapshots.id -> "
                "catalog_snapshot_events.id is not enforced"
            )

        has_available_fk = self._table_has_fk(
            inspector,
            table_name="catalog_snapshot_available_counts",
            constrained_columns=("snapshot_id",),
            referred_table="catalog_snapshot_events",
            referred_columns=("id",),
        )
        if not has_available_fk:
            LOGGER.debug(
                "Catalog schema validation: FK catalog_snapshot_available_counts.snapshot_id -> "
                "catalog_snapshot_events.id is not enforced"
            )

        if inspector.has_table("catalog_snapshot_assets"):
            raise RuntimeError(
                "Schema mismatch: legacy table `catalog_snapshot_assets` is not allowed. "
                "Run one-way migration."
            )

        LOGGER.debug("Catalog schema validation passed")

    @staticmethod
    def _table_has_fk(
        inspector,
        *,
        table_name: str,
        constrained_columns: tuple[str, ...],
        referred_table: str,
        referred_columns: tuple[str, ...],
    ) -> bool:
        expected_constrained = list(constrained_columns)
        expected_referred = list(referred_columns)
        for fk in inspector.get_foreign_keys(table_name):
            if fk.get("referred_table") != referred_table:
                continue
            if (fk.get("constrained_columns") or []) != expected_constrained:
                continue
            referred = fk.get("referred_columns") or []
            if referred and referred != expected_referred:
                continue
            return True
        return False

    @staticmethod
    def _is_mysql_references_denied_error(exc: OperationalError) -> bool:
        orig = getattr(exc, "orig", None)
        code: int | None = None
        args = getattr(orig, "args", None)
        if isinstance(args, tuple) and args:
            try:
                code = int(args[0])
            except (TypeError, ValueError):
                code = None
        message = str(orig or exc).lower()
        return code == 1142 and "references command denied" in message

    def _enforce_snapshot_contract_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_product_snapshots"):
            return
        if not inspector.has_table("catalog_snapshot_events"):
            return

        dialect = self._engine.dialect.name
        price_columns = {item["name"] for item in inspector.get_columns("catalog_product_snapshots")}
        event_columns = {item["name"] for item in inspector.get_columns("catalog_snapshot_events")}

        has_legacy_snapshot_event_payload = {
            "canonical_product_id",
            "parser_name",
            "source_id",
            "observed_at",
            "created_at",
        }.issubset(price_columns)

        if has_legacy_snapshot_event_payload:
            with self._engine.begin() as connection:
                if dialect == "mysql":
                    connection.execute(
                        text(
                            """
                            INSERT INTO catalog_snapshot_events (
                                id,
                                canonical_product_id,
                                parser_name,
                                source_id,
                                source_run_id,
                                receiver_product_id,
                                receiver_artifact_id,
                                receiver_sort_order,
                                source_event_uid,
                                content_fingerprint,
                                valid_from_at,
                                valid_to_at,
                                observed_at,
                                created_at
                            )
                            SELECT
                                cps.id,
                                cps.canonical_product_id,
                                cps.parser_name,
                                cps.source_id,
                                cps.source_run_id,
                                cps.receiver_product_id,
                                cps.receiver_artifact_id,
                                cps.receiver_sort_order,
                                cps.source_event_uid,
                                cps.content_fingerprint,
                                cps.valid_from_at,
                                cps.valid_to_at,
                                cps.observed_at,
                                cps.created_at
                            FROM catalog_product_snapshots AS cps
                            LEFT JOIN catalog_snapshot_events AS cse
                                ON cse.id = cps.id
                            WHERE cse.id IS NULL
                            """
                        )
                    )
                elif dialect == "sqlite":
                    connection.execute(
                        text(
                            """
                            INSERT INTO catalog_snapshot_events (
                                id,
                                canonical_product_id,
                                parser_name,
                                source_id,
                                source_run_id,
                                receiver_product_id,
                                receiver_artifact_id,
                                receiver_sort_order,
                                source_event_uid,
                                content_fingerprint,
                                valid_from_at,
                                valid_to_at,
                                observed_at,
                                created_at
                            )
                            SELECT
                                cps.id,
                                cps.canonical_product_id,
                                cps.parser_name,
                                cps.source_id,
                                cps.source_run_id,
                                cps.receiver_product_id,
                                cps.receiver_artifact_id,
                                cps.receiver_sort_order,
                                cps.source_event_uid,
                                cps.content_fingerprint,
                                cps.valid_from_at,
                                cps.valid_to_at,
                                cps.observed_at,
                                cps.created_at
                            FROM catalog_product_snapshots AS cps
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM catalog_snapshot_events AS cse
                                WHERE cse.id = cps.id
                            )
                            """
                        )
                    )
                else:
                    raise RuntimeError(f"Unsupported dialect for snapshot event migration: {dialect}")
            LOGGER.info("Catalog snapshot schema migrated: backfilled catalog_snapshot_events")

        if "available_count" in price_columns and inspector.has_table("catalog_snapshot_available_counts"):
            with self._engine.begin() as connection:
                if dialect == "mysql":
                    connection.execute(
                        text(
                            """
                            INSERT INTO catalog_snapshot_available_counts (
                                snapshot_id,
                                available_count,
                                created_at
                            )
                            SELECT
                                cps.id,
                                cps.available_count,
                                COALESCE(cps.created_at, cps.observed_at, UTC_TIMESTAMP(6))
                            FROM catalog_product_snapshots AS cps
                            INNER JOIN catalog_snapshot_events AS cse
                                ON cse.id = cps.id
                            LEFT JOIN catalog_snapshot_available_counts AS csac
                                ON csac.snapshot_id = cps.id
                            WHERE csac.snapshot_id IS NULL
                            """
                        )
                    )
                elif dialect == "sqlite":
                    connection.execute(
                        text(
                            """
                            INSERT INTO catalog_snapshot_available_counts (
                                snapshot_id,
                                available_count,
                                created_at
                            )
                            SELECT
                                cps.id,
                                cps.available_count,
                                COALESCE(cps.created_at, cps.observed_at, CURRENT_TIMESTAMP)
                            FROM catalog_product_snapshots AS cps
                            WHERE EXISTS (
                                SELECT 1
                                FROM catalog_snapshot_events AS cse
                                WHERE cse.id = cps.id
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                FROM catalog_snapshot_available_counts AS csac
                                WHERE csac.snapshot_id = cps.id
                            )
                            """
                        )
                    )
                else:
                    raise RuntimeError(f"Unsupported dialect for snapshot available migration: {dialect}")

        inspector = inspect(self._engine)
        price_columns = {item["name"] for item in inspector.get_columns("catalog_product_snapshots")}
        unexpected_price_columns = [
            name for name in price_columns if name not in set(self._SNAPSHOT_PRICE_COLUMNS)
        ]
        has_price_fk = self._table_has_fk(
            inspector,
            table_name="catalog_product_snapshots",
            constrained_columns=("id",),
            referred_table="catalog_snapshot_events",
            referred_columns=("id",),
        )

        if unexpected_price_columns or not has_price_fk:
            with self._engine.begin() as connection:
                if dialect == "sqlite":
                    connection.execute(text("DROP TABLE IF EXISTS catalog_product_snapshots__new"))
                    connection.execute(
                        text(
                            """
                            CREATE TABLE catalog_product_snapshots__new (
                                id INTEGER NOT NULL PRIMARY KEY,
                                price FLOAT NULL,
                                discount_price FLOAT NULL,
                                loyal_price FLOAT NULL,
                                price_unit VARCHAR(32) NULL,
                                FOREIGN KEY(id) REFERENCES catalog_snapshot_events(id) ON DELETE CASCADE
                            )
                            """
                        )
                    )
                    connection.execute(
                        text(
                            """
                            INSERT INTO catalog_product_snapshots__new (id, price, discount_price, loyal_price, price_unit)
                            SELECT
                                id,
                                price,
                                discount_price,
                                loyal_price,
                                price_unit
                            FROM catalog_product_snapshots
                            WHERE EXISTS (
                                SELECT 1
                                FROM catalog_snapshot_events AS cse
                                WHERE cse.id = catalog_product_snapshots.id
                            )
                            """
                        )
                    )
                    connection.execute(text("DROP TABLE catalog_product_snapshots"))
                    connection.execute(
                        text(
                            "ALTER TABLE catalog_product_snapshots__new RENAME TO catalog_product_snapshots"
                        )
                    )
                elif dialect == "mysql":
                    price_expr = "cps.price" if "price" in price_columns else "NULL"
                    discount_price_expr = "cps.discount_price" if "discount_price" in price_columns else "NULL"
                    loyal_price_expr = "cps.loyal_price" if "loyal_price" in price_columns else "NULL"
                    price_unit_expr = "cps.price_unit" if "price_unit" in price_columns else "NULL"

                    connection.execute(text("DROP TABLE IF EXISTS `catalog_product_snapshots__new`"))
                    connection.execute(
                        text(
                            """
                            CREATE TABLE `catalog_product_snapshots__new` (
                                id INTEGER NOT NULL PRIMARY KEY,
                                price FLOAT NULL,
                                discount_price FLOAT NULL,
                                loyal_price FLOAT NULL,
                                price_unit VARCHAR(32) NULL
                            )
                            """
                        )
                    )
                    connection.execute(
                        text(
                            f"""
                            INSERT INTO catalog_product_snapshots__new (
                                id,
                                price,
                                discount_price,
                                loyal_price,
                                price_unit
                            )
                            SELECT
                                cps.id,
                                {price_expr},
                                {discount_price_expr},
                                {loyal_price_expr},
                                {price_unit_expr}
                            FROM catalog_product_snapshots AS cps
                            INNER JOIN catalog_snapshot_events AS cse
                                ON cse.id = cps.id
                            """
                        )
                    )
                    connection.execute(text("DROP TABLE IF EXISTS `catalog_product_snapshots__old`"))
                    connection.execute(
                        text(
                            """
                            RENAME TABLE
                                catalog_product_snapshots TO catalog_product_snapshots__old,
                                catalog_product_snapshots__new TO catalog_product_snapshots
                            """
                        )
                    )
                    connection.execute(text("DROP TABLE `catalog_product_snapshots__old`"))

                    inspector_after_rebuild = inspect(self._engine)
                    has_price_fk_after_rebuild = self._table_has_fk(
                        inspector_after_rebuild,
                        table_name="catalog_product_snapshots",
                        constrained_columns=("id",),
                        referred_table="catalog_snapshot_events",
                        referred_columns=("id",),
                    )
                    if not has_price_fk_after_rebuild:
                        try:
                            connection.execute(
                                text(
                                    """
                                    ALTER TABLE catalog_product_snapshots
                                    ADD CONSTRAINT fk_cps_event
                                    FOREIGN KEY (id)
                                    REFERENCES catalog_snapshot_events (id)
                                    ON DELETE CASCADE
                                    """
                                )
                            )
                        except OperationalError as exc:
                            if self._is_mysql_references_denied_error(exc):
                                setattr(self, "_snapshot_fk_supported", False)
                                LOGGER.warning(
                                    "Catalog snapshot schema migrated without FK fk_cps_event: "
                                    "MySQL user lacks REFERENCES privilege"
                                )
                            else:
                                raise
                else:
                    raise RuntimeError(f"Unsupported dialect for snapshot price migration: {dialect}")
            LOGGER.info("Catalog snapshot schema migrated: normalized catalog_product_snapshots")

        inspector = inspect(self._engine)
        if inspector.has_table("catalog_snapshot_assets"):
            with self._engine.begin() as connection:
                if dialect == "mysql":
                    connection.execute(text("DROP TABLE `catalog_snapshot_assets`"))
                elif dialect == "sqlite":
                    connection.execute(text("DROP TABLE catalog_snapshot_assets"))
                else:
                    raise RuntimeError(
                        f"Unsupported dialect for snapshot contract migration: {dialect}"
                    )
            LOGGER.info("Catalog snapshot schema migrated: dropped table catalog_snapshot_assets")

        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_snapshot_available_counts"):
            return

        available_columns = {
            item["name"] for item in inspector.get_columns("catalog_snapshot_available_counts")
        }
        unexpected_available_columns = [
            name for name in available_columns if name not in set(self._SNAPSHOT_AVAILABLE_COLUMNS)
        ]
        has_available_fk = self._table_has_fk(
            inspector,
            table_name="catalog_snapshot_available_counts",
            constrained_columns=("snapshot_id",),
            referred_table="catalog_snapshot_events",
            referred_columns=("id",),
        )

        if unexpected_available_columns or not has_available_fk:
            with self._engine.begin() as connection:
                if dialect == "sqlite":
                    has_created_at = "created_at" in available_columns
                    created_expr = "COALESCE(created_at, CURRENT_TIMESTAMP)" if has_created_at else "CURRENT_TIMESTAMP"
                    connection.execute(text("DROP TABLE IF EXISTS catalog_snapshot_available_counts__new"))
                    connection.execute(
                        text(
                            """
                            CREATE TABLE catalog_snapshot_available_counts__new (
                                snapshot_id INTEGER NOT NULL PRIMARY KEY,
                                available_count FLOAT NULL,
                                created_at DATETIME NOT NULL,
                                FOREIGN KEY(snapshot_id) REFERENCES catalog_snapshot_events(id) ON DELETE CASCADE
                            )
                            """
                        )
                    )
                    connection.execute(
                        text(
                            f"""
                            INSERT INTO catalog_snapshot_available_counts__new (
                                snapshot_id,
                                available_count,
                                created_at
                            )
                            SELECT
                                snapshot_id,
                                available_count,
                                {created_expr}
                            FROM catalog_snapshot_available_counts
                            WHERE EXISTS (
                                SELECT 1
                                FROM catalog_snapshot_events AS cse
                                WHERE cse.id = catalog_snapshot_available_counts.snapshot_id
                            )
                            """
                        )
                    )
                    connection.execute(text("DROP TABLE catalog_snapshot_available_counts"))
                    connection.execute(
                        text(
                            "ALTER TABLE catalog_snapshot_available_counts__new RENAME TO catalog_snapshot_available_counts"
                        )
                    )
                elif dialect == "mysql":
                    available_count_expr = (
                        "csac.available_count" if "available_count" in available_columns else "NULL"
                    )
                    created_at_expr = (
                        "COALESCE(csac.created_at, UTC_TIMESTAMP(6))"
                        if "created_at" in available_columns
                        else "UTC_TIMESTAMP(6)"
                    )

                    connection.execute(text("DROP TABLE IF EXISTS `catalog_snapshot_available_counts__new`"))
                    connection.execute(
                        text(
                            """
                            CREATE TABLE `catalog_snapshot_available_counts__new` (
                                snapshot_id INTEGER NOT NULL PRIMARY KEY,
                                available_count FLOAT NULL,
                                created_at DATETIME(6) NOT NULL
                            )
                            """
                        )
                    )
                    connection.execute(
                        text(
                            f"""
                            INSERT INTO catalog_snapshot_available_counts__new (
                                snapshot_id,
                                available_count,
                                created_at
                            )
                            SELECT
                                csac.snapshot_id,
                                {available_count_expr},
                                {created_at_expr}
                            FROM catalog_snapshot_available_counts AS csac
                            INNER JOIN catalog_snapshot_events AS cse
                                ON cse.id = csac.snapshot_id
                            """
                        )
                    )
                    connection.execute(text("DROP TABLE IF EXISTS `catalog_snapshot_available_counts__old`"))
                    connection.execute(
                        text(
                            """
                            RENAME TABLE
                                catalog_snapshot_available_counts TO catalog_snapshot_available_counts__old,
                                catalog_snapshot_available_counts__new TO catalog_snapshot_available_counts
                            """
                        )
                    )
                    connection.execute(text("DROP TABLE `catalog_snapshot_available_counts__old`"))

                    inspector_after_rebuild = inspect(self._engine)
                    has_available_fk_after = self._table_has_fk(
                        inspector_after_rebuild,
                        table_name="catalog_snapshot_available_counts",
                        constrained_columns=("snapshot_id",),
                        referred_table="catalog_snapshot_events",
                        referred_columns=("id",),
                    )
                    if not has_available_fk_after:
                        try:
                            connection.execute(
                                text(
                                    """
                                    ALTER TABLE catalog_snapshot_available_counts
                                    ADD CONSTRAINT fk_csac_event
                                    FOREIGN KEY (snapshot_id)
                                    REFERENCES catalog_snapshot_events (id)
                                    ON DELETE CASCADE
                                    """
                                )
                            )
                        except OperationalError as exc:
                            if self._is_mysql_references_denied_error(exc):
                                setattr(self, "_snapshot_fk_supported", False)
                                LOGGER.warning(
                                    "Catalog snapshot schema migrated without FK fk_csac_event: "
                                    "MySQL user lacks REFERENCES privilege"
                                )
                            else:
                                raise
                else:
                    raise RuntimeError(f"Unsupported dialect for available_count snapshot migration: {dialect}")
            LOGGER.info("Catalog snapshot schema migrated: normalized catalog_snapshot_available_counts")

    def _ensure_snapshot_interval_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_snapshot_events"):
            return

        event_columns = {item["name"] for item in inspector.get_columns("catalog_snapshot_events")}
        missing_columns = [
            column_name
            for column_name in ("content_fingerprint", "valid_from_at", "valid_to_at")
            if column_name not in event_columns
        ]
        if not missing_columns:
            return

        dialect = self._engine.dialect.name
        with self._engine.begin() as connection:
            for column_name in missing_columns:
                if dialect == "mysql":
                    if column_name == "content_fingerprint":
                        ddl = (
                            "ALTER TABLE catalog_snapshot_events "
                            "ADD COLUMN content_fingerprint VARCHAR(64) NULL"
                        )
                    else:
                        ddl = (
                            "ALTER TABLE catalog_snapshot_events "
                            f"ADD COLUMN {column_name} DATETIME(6) NULL"
                        )
                elif dialect == "sqlite":
                    if column_name == "content_fingerprint":
                        ddl = (
                            "ALTER TABLE catalog_snapshot_events "
                            "ADD COLUMN content_fingerprint VARCHAR(64)"
                        )
                    else:
                        ddl = (
                            "ALTER TABLE catalog_snapshot_events "
                            f"ADD COLUMN {column_name} DATETIME"
                        )
                else:
                    raise RuntimeError(
                        f"Unsupported dialect for snapshot event schema migration: {dialect}"
                    )
                connection.execute(text(ddl))

        LOGGER.info(
            "Catalog snapshot event schema migrated: added_columns=%s",
            ",".join(missing_columns),
        )

    def _ensure_snapshot_available_counts_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_product_snapshots"):
            return
        if not inspector.has_table("catalog_snapshot_available_counts"):
            return

        available_columns = {
            item["name"] for item in inspector.get_columns("catalog_snapshot_available_counts")
        }
        snapshot_columns = {item["name"] for item in inspector.get_columns("catalog_product_snapshots")}
        dialect = self._engine.dialect.name

        with self._engine.begin() as connection:
            if "created_at" not in available_columns:
                if dialect == "mysql":
                    connection.execute(
                        text(
                            "ALTER TABLE catalog_snapshot_available_counts "
                            "ADD COLUMN created_at DATETIME(6) NULL"
                        )
                    )
                elif dialect == "sqlite":
                    connection.execute(
                        text(
                            "ALTER TABLE catalog_snapshot_available_counts "
                            "ADD COLUMN created_at DATETIME"
                        )
                    )
                else:
                    raise RuntimeError(
                        f"Unsupported dialect for available_count snapshot migration: {dialect}"
                    )

            if "available_count" in snapshot_columns:
                if dialect == "mysql":
                    connection.execute(
                        text(
                            """
                            INSERT INTO catalog_snapshot_available_counts (
                                snapshot_id,
                                available_count,
                                created_at
                            )
                            SELECT
                                cps.id,
                                cps.available_count,
                                COALESCE(cps.created_at, cps.observed_at, UTC_TIMESTAMP(6))
                            FROM catalog_product_snapshots AS cps
                            INNER JOIN catalog_snapshot_events AS cse
                                ON cse.id = cps.id
                            LEFT JOIN catalog_snapshot_available_counts AS csac
                                ON csac.snapshot_id = cps.id
                            WHERE csac.snapshot_id IS NULL
                            """
                        )
                    )
                elif dialect == "sqlite":
                    connection.execute(
                        text(
                            """
                            INSERT INTO catalog_snapshot_available_counts (
                                snapshot_id,
                                available_count,
                                created_at
                            )
                            SELECT
                                cps.id,
                                cps.available_count,
                                COALESCE(cps.created_at, cps.observed_at, CURRENT_TIMESTAMP)
                            FROM catalog_product_snapshots AS cps
                            WHERE EXISTS (
                                SELECT 1
                                FROM catalog_snapshot_events AS cse
                                WHERE cse.id = cps.id
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                FROM catalog_snapshot_available_counts AS csac
                                WHERE csac.snapshot_id = cps.id
                            )
                            """
                        )
                    )
                else:
                    raise RuntimeError(
                        f"Unsupported dialect for available_count snapshot migration: {dialect}"
                    )

            if dialect == "mysql":
                connection.execute(
                    text(
                        "UPDATE catalog_snapshot_available_counts "
                        "SET created_at = COALESCE(created_at, UTC_TIMESTAMP(6))"
                    )
                )
            elif dialect == "sqlite":
                connection.execute(
                    text(
                        "UPDATE catalog_snapshot_available_counts "
                        "SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)"
                    )
                )
            else:
                raise RuntimeError(f"Unsupported dialect for available_count snapshot migration: {dialect}")

        LOGGER.info("Catalog snapshot schema migrated: backfilled available_count snapshots")

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
        if not inspector.has_table("catalog_snapshot_events"):
            return

        event_columns = {item["name"] for item in inspector.get_columns("catalog_snapshot_events")}
        if "source_event_uid" not in event_columns:
            dialect = self._engine.dialect.name
            with self._engine.begin() as connection:
                if dialect == "mysql":
                    ddl = (
                        "ALTER TABLE catalog_snapshot_events "
                        "ADD COLUMN source_event_uid VARCHAR(191) NULL"
                    )
                else:
                    ddl = (
                        "ALTER TABLE catalog_snapshot_events "
                        "ADD COLUMN source_event_uid VARCHAR(191)"
                    )
                connection.execute(text(ddl))
            LOGGER.info("Catalog snapshot events schema migrated: added source_event_uid")

        indexes = {idx.get("name") for idx in inspector.get_indexes("catalog_snapshot_events")}
        if "idx_cse_latest" not in indexes:
            with self._engine.begin() as connection:
                connection.execute(
                    text(
                        "CREATE INDEX idx_cse_latest ON catalog_snapshot_events (parser_name, source_id, id)"
                    )
                )
            LOGGER.info("Catalog snapshot events schema migrated: added idx_cse_latest")

        unique_constraints = {
            item.get("name")
            for item in inspector.get_unique_constraints("catalog_snapshot_events")
        }
        if "uq_cse_event" not in unique_constraints:
            dialect = self._engine.dialect.name
            with self._engine.begin() as connection:
                if dialect == "mysql":
                    connection.execute(
                        text(
                            "ALTER TABLE catalog_snapshot_events "
                            "ADD CONSTRAINT uq_cse_event UNIQUE (source_event_uid)"
                        )
                    )
                else:
                    connection.execute(
                        text(
                            "CREATE UNIQUE INDEX uq_cse_event "
                            "ON catalog_snapshot_events (source_event_uid)"
                        )
                    )
            LOGGER.info("Catalog snapshot events schema migrated: added uq_cse_event")

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
