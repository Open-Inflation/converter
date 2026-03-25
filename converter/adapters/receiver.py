from __future__ import annotations

import logging
from datetime import datetime, timezone
import math
from pathlib import Path
from time import monotonic
from typing import Any
from collections.abc import Sequence

from sqlalchemy import delete, func, inspect, select, text, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from converter.core.models import AckResult, RawProductRecord
from .receiver_mapping import (
    _as_bool,
    _as_float,
    _as_int,
    _as_string_list,
    _safe_str,
    map_receiver_row_to_raw_product,
)
from .receiver_schema import (
    _ReceiverBase,
    _RunArtifact,
    _RunArtifactAdministrativeUnit,
    _RunArtifactCategory,
    _RunArtifactProduct,
    _RunArtifactProductCategory,
    _RunArtifactProductImage,
    _RunArtifactProductMeta,
    _RunArtifactProductWholesalePrice,
    _TaskRun,
)


LOGGER = logging.getLogger(__name__)


def _create_engine(database_url: str) -> Engine:
    from sqlalchemy import create_engine

    connect_args: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    return create_engine(
        database_url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
    )


class ReceiverRepository:
    """
    SQLAlchemy-based reader for receiver DB normalized tables.
    """

    def __init__(
        self,
        database_url: str,
        *,
        default_parser_name: str = "fixprice",
        engine: Engine | None = None,
    ) -> None:
        self._database_url = database_url
        self._default_parser_name = default_parser_name
        self._engine = engine or _create_engine(database_url)
        self._session_factory = sessionmaker(
            bind=self._engine,
            class_=Session,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        self._validate_schema()
        self._ensure_read_indexes()

    def fetch_batch(
        self,
        limit: int = 100,
        *,
        parser_name: str | None = None,
    ) -> list[RawProductRecord]:
        parser_filter = parser_name.strip().lower() if isinstance(parser_name, str) else None

        with self._session_factory() as session:
            stmt = (
                select(
                    _RunArtifactProduct,
                    _RunArtifact,
                    _RunArtifactAdministrativeUnit,
                )
                .join(_RunArtifact, _RunArtifact.id == _RunArtifactProduct.artifact_id)
                .outerjoin(
                    _RunArtifactAdministrativeUnit,
                    _RunArtifactAdministrativeUnit.artifact_id == _RunArtifact.id,
                )
            )

            if parser_filter:
                stmt = stmt.where(func.lower(_RunArtifact.parser_name) == parser_filter)

            stmt = stmt.order_by(_RunArtifact.ingested_at.asc(), _RunArtifactProduct.id.asc()).limit(max(1, int(limit)))
            rows = session.execute(stmt).all()
            if not rows:
                return []

            artifact_ids = sorted({product.artifact_id for product, _, _ in rows})
            product_ids = sorted({product.id for product, _, _ in rows})

            category_lookup = self._load_category_lookup(session, artifact_ids)
            image_lookup = self._load_image_lookup(session, product_ids)
            image_details_lookup = self._load_image_details_lookup(session, product_ids)
            meta_lookup = self._load_product_meta_lookup(session, product_ids)
            wholesale_lookup = self._load_product_wholesale_lookup(session, product_ids)
            product_category_lookup = self._load_product_category_lookup(session, product_ids)

            out: list[RawProductRecord] = []
            for product, artifact, admin in rows:
                package_quantity_net = _as_float(product.package_quantity_net)
                package_weight_gross = _as_float(product.package_weight_gross)
                package_unit = _safe_str(product.package_unit)

                row_data: dict[str, Any] = {
                    "product_id": product.id,
                    "artifact_id": product.artifact_id,
                    "product_sku": product.sku,
                    "product_plu": product.plu,
                    "product_source_page_url": product.source_page_url,
                    "product_title": product.title,
                    "product_description": product.description,
                    "product_adult": product.adult,
                    "product_is_new": product.is_new,
                    "product_promo": product.promo,
                    "product_season": product.season,
                    "product_hit": product.hit,
                    "product_data_matrix": product.data_matrix,
                    "product_composition": product.composition,
                    "product_brand": product.brand,
                    "product_producer_name": product.producer_name,
                    "product_producer_country": product.producer_country,
                    "product_expiration_date_in_days": product.expiration_date_in_days,
                    "product_rating": product.rating,
                    "product_reviews_count": product.reviews_count,
                    "product_price": product.price,
                    "product_discount_price": product.discount_price,
                    "product_loyal_price": product.loyal_price,
                    "product_price_unit": product.price_unit,
                    "product_unit": product.unit_net,
                    "product_available_count": product.available_count,
                    "product_package_quantity": package_quantity_net,
                    "product_package_unit": package_unit,
                    "product_package_quantity_net": package_quantity_net,
                    "product_package_weight_gross": package_weight_gross,
                    "product_package_count": _as_float(product.package_count),
                    "category_uids_json": product.categories_uid_json,
                    "product_main_image": product.main_image,
                    "product_sort_order": product.sort_order,
                    "run_id": artifact.run_id,
                    "artifact_source": artifact.source,
                    "ingested_at": artifact.ingested_at,
                    "parser_name": artifact.parser_name,
                    "artifact_retail_type": artifact.retail_type,
                    "artifact_code": artifact.code,
                    "artifact_address": artifact.address,
                    "artifact_schedule_weekdays_open_from": artifact.schedule_weekdays_open_from,
                    "artifact_schedule_weekdays_closed_from": artifact.schedule_weekdays_closed_from,
                    "artifact_schedule_saturday_open_from": artifact.schedule_saturday_open_from,
                    "artifact_schedule_saturday_closed_from": artifact.schedule_saturday_closed_from,
                    "artifact_schedule_sunday_open_from": artifact.schedule_sunday_open_from,
                    "artifact_schedule_sunday_closed_from": artifact.schedule_sunday_closed_from,
                    "artifact_temporarily_closed": artifact.temporarily_closed,
                    "artifact_longitude": artifact.longitude,
                    "artifact_latitude": artifact.latitude,
                    "artifact_rating": artifact.rating,
                    "artifact_reviews_count": artifact.reviews_count,
                    "artifact_open_date": artifact.open_date,
                    "artifact_dataclass_validated": artifact.dataclass_validated,
                    "artifact_dataclass_validation_error": artifact.dataclass_validation_error,
                    "geo_settlement_type": admin.settlement_type if admin else None,
                    "geo_name": admin.name if admin else None,
                    "geo_alias": admin.alias if admin else None,
                    "geo_region": admin.region if admin else None,
                    "geo_country": admin.country if admin else None,
                    "geo_longitude": admin.longitude if admin else None,
                    "geo_latitude": admin.latitude if admin else None,
                }

                category_uids = _as_string_list(row_data.get("category_uids_json"))
                row_data["category_titles"] = self._resolve_category_titles(
                    category_uids,
                    category_lookup.get(product.artifact_id, {}),
                )
                row_data["receiver_categories"] = self._resolve_categories_payload(
                    category_uids,
                    category_lookup.get(product.artifact_id, {}),
                )
                row_data["image_urls_json"] = image_lookup.get(product.id, [])
                row_data["receiver_product_images"] = image_details_lookup.get(product.id, [])
                row_data["receiver_product_meta"] = meta_lookup.get(product.id, [])
                row_data["receiver_product_wholesale_prices"] = wholesale_lookup.get(product.id, [])
                row_data["receiver_product_categories"] = product_category_lookup.get(product.id, [])
                row_data["receiver_product"] = {
                    "id": product.id,
                    "artifact_id": product.artifact_id,
                    "sku": product.sku,
                    "plu": product.plu,
                    "source_page_url": product.source_page_url,
                    "title": product.title,
                    "description": product.description,
                    "adult": product.adult,
                    "is_new": product.is_new,
                    "promo": product.promo,
                    "season": product.season,
                    "hit": product.hit,
                    "data_matrix": product.data_matrix,
                    "brand": product.brand,
                    "producer_name": product.producer_name,
                    "producer_country": product.producer_country,
                    "composition": product.composition,
                    "expiration_date_in_days": product.expiration_date_in_days,
                    "rating": product.rating,
                    "reviews_count": product.reviews_count,
                    "price": product.price,
                    "discount_price": product.discount_price,
                    "loyal_price": product.loyal_price,
                    "price_unit": product.price_unit,
                    "unit_net": product.unit_net,
                    "available_count": product.available_count,
                    "package_quantity_net": package_quantity_net,
                    "package_weight_gross": package_weight_gross,
                    "package_unit": package_unit,
                    "package_count": _as_float(product.package_count),
                    "categories_uid_json": _as_string_list(product.categories_uid_json),
                    "main_image": product.main_image,
                    "sort_order": product.sort_order,
                }
                row_data["receiver_artifact"] = {
                    "id": artifact.id,
                    "run_id": artifact.run_id,
                    "source": artifact.source,
                    "parser_name": artifact.parser_name,
                    "retail_type": artifact.retail_type,
                    "code": artifact.code,
                    "address": artifact.address,
                    "schedule_weekdays_open_from": artifact.schedule_weekdays_open_from,
                    "schedule_weekdays_closed_from": artifact.schedule_weekdays_closed_from,
                    "schedule_saturday_open_from": artifact.schedule_saturday_open_from,
                    "schedule_saturday_closed_from": artifact.schedule_saturday_closed_from,
                    "schedule_sunday_open_from": artifact.schedule_sunday_open_from,
                    "schedule_sunday_closed_from": artifact.schedule_sunday_closed_from,
                    "temporarily_closed": artifact.temporarily_closed,
                    "longitude": artifact.longitude,
                    "latitude": artifact.latitude,
                    "rating": artifact.rating,
                    "reviews_count": artifact.reviews_count,
                    "open_date": artifact.open_date,
                    "dataclass_validated": artifact.dataclass_validated,
                    "dataclass_validation_error": artifact.dataclass_validation_error,
                    "ingested_at": artifact.ingested_at,
                }
                row_data["receiver_admin_unit"] = {
                    "id": admin.id if admin else None,
                    "artifact_id": admin.artifact_id if admin else None,
                    "settlement_type": admin.settlement_type if admin else None,
                    "name": admin.name if admin else None,
                    "alias": admin.alias if admin else None,
                    "region": admin.region if admin else None,
                    "country": admin.country if admin else None,
                    "longitude": admin.longitude if admin else None,
                    "latitude": admin.latitude if admin else None,
                }

                parsed = map_receiver_row_to_raw_product(
                    row_data,
                    default_parser_name=self._default_parser_name,
                )
                out.append(parsed)

            return out

    def delete_processed_products(
        self,
        product_ids: Sequence[int],
        *,
        chunk_started_at: float,
    ) -> AckResult:
        normalized_ids = self._normalize_product_ids(product_ids)
        if not normalized_ids:
            return AckResult(requested_products=0, deleted_products=0, deleted_artifacts=0)

        with self._session_factory() as session:
            product_rows = session.execute(
                select(
                    _RunArtifactProduct.id,
                    _RunArtifactProduct.artifact_id,
                    _RunArtifact.run_id,
                )
                .join(_RunArtifact, _RunArtifact.id == _RunArtifactProduct.artifact_id)
                .where(_RunArtifactProduct.id.in_(normalized_ids))
            ).all()
            if not product_rows:
                return AckResult(
                    requested_products=len(normalized_ids),
                    deleted_products=0,
                    deleted_artifacts=0,
                )

            existing_product_ids = sorted({int(row.id) for row in product_rows})
            touched_artifact_ids = sorted({int(row.artifact_id) for row in product_rows})
            touched_run_counts: dict[str, int] = {}
            for row in product_rows:
                run_id = _safe_str(row.run_id)
                if run_id is None:
                    continue
                touched_run_counts[run_id] = touched_run_counts.get(run_id, 0) + 1

            session.execute(
                delete(_RunArtifactProductImage).where(_RunArtifactProductImage.product_id.in_(existing_product_ids))
            )
            session.execute(
                delete(_RunArtifactProductMeta).where(_RunArtifactProductMeta.product_id.in_(existing_product_ids))
            )
            session.execute(
                delete(_RunArtifactProductWholesalePrice).where(
                    _RunArtifactProductWholesalePrice.product_id.in_(existing_product_ids)
                )
            )
            session.execute(
                delete(_RunArtifactProductCategory).where(
                    _RunArtifactProductCategory.product_id.in_(existing_product_ids)
                )
            )
            session.execute(
                delete(_RunArtifactProduct).where(_RunArtifactProduct.id.in_(existing_product_ids))
            )

            empty_artifact_ids = self._find_empty_artifacts(session, touched_artifact_ids)
            if empty_artifact_ids:
                session.execute(
                    delete(_RunArtifactCategory).where(_RunArtifactCategory.artifact_id.in_(empty_artifact_ids))
                )
                session.execute(
                    delete(_RunArtifactAdministrativeUnit).where(
                        _RunArtifactAdministrativeUnit.artifact_id.in_(empty_artifact_ids)
                    )
                )
                session.execute(
                    delete(_RunArtifact).where(_RunArtifact.id.in_(empty_artifact_ids))
                )

            normalized_elapsed_seconds = self._elapsed_seconds_since(chunk_started_at)
            elapsed_by_run = self._distribute_elapsed_seconds(normalized_elapsed_seconds, touched_run_counts)
            if elapsed_by_run:
                for run_id, delta in elapsed_by_run.items():
                    if delta <= 0:
                        continue
                    session.execute(
                        update(_TaskRun)
                        .where(_TaskRun.id == run_id)
                        .values(converter_elapsed_sec=func.coalesce(_TaskRun.converter_elapsed_sec, 0) + delta)
                    )

            finished_run_ids = self._find_finished_runs(session, tuple(touched_run_counts.keys()))
            if finished_run_ids:
                session.execute(
                    update(_TaskRun)
                    .where(_TaskRun.id.in_(finished_run_ids))
                    .where(_TaskRun.finish.is_(None))
                    .values(finish=datetime.now(tz=timezone.utc))
                )

            session.commit()
            ack = AckResult(
                requested_products=len(normalized_ids),
                deleted_products=len(existing_product_ids),
                deleted_artifacts=len(empty_artifact_ids),
            )
            LOGGER.debug(
                "Receiver consume-delete acknowledged: requested_products=%s deleted_products=%s deleted_artifacts=%s elapsed_seconds=%s touched_runs=%s finished_runs=%s",
                ack.requested_products,
                ack.deleted_products,
                ack.deleted_artifacts,
                normalized_elapsed_seconds,
                len(touched_run_counts),
                len(finished_run_ids),
            )
            return ack

    def _ensure_read_indexes(self) -> None:
        dialect = self._engine.dialect.name
        if dialect == "postgresql":
            LOGGER.debug(
                "Receiver read-index auto-create is skipped for PostgreSQL: schema DDL is managed externally"
            )
            return
        if dialect != "sqlite":
            return

        required_indexes = {
            "run_artifacts": (
                ("idx_ra_parser_ingested_id", "parser_name, ingested_at, id"),
            ),
            "run_artifact_products": (
                ("idx_rap_artifact_product", "artifact_id, id"),
            ),
            "run_artifact_administrative_units": (
                ("idx_raau_artifact", "artifact_id"),
            ),
            "run_artifact_product_images": (
                ("idx_rapi_product_sort", "product_id, sort_order"),
            ),
            "run_artifact_product_meta": (
                ("idx_rapm_product_sort", "product_id, sort_order"),
            ),
            "run_artifact_product_wholesale_prices": (
                ("idx_rapwp_product_sort", "product_id, sort_order"),
            ),
            "run_artifact_product_categories": (
                ("idx_rapc_product_sort", "product_id, sort_order"),
            ),
        }

        inspector = inspect(self._engine)
        with self._engine.begin() as connection:
            for table_name, index_defs in required_indexes.items():
                if not inspector.has_table(table_name):
                    continue
                existing = {item.get("name") for item in inspector.get_indexes(table_name)}
                for index_name, columns in index_defs:
                    if index_name in existing:
                        continue
                    ddl = f"CREATE INDEX {index_name} ON {table_name} ({columns})"
                    connection.execute(text(ddl))
                    LOGGER.info("Receiver schema optimized: created index %s on %s", index_name, table_name)

    def _validate_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("run_artifacts"):
            raise RuntimeError("Receiver schema is missing table run_artifacts")
        if not inspector.has_table("run_artifact_products"):
            raise RuntimeError("Receiver schema is missing table run_artifact_products")
        if not inspector.has_table("run_artifact_categories"):
            raise RuntimeError("Receiver schema is missing table run_artifact_categories")
        if not inspector.has_table("run_artifact_administrative_units"):
            raise RuntimeError("Receiver schema is missing table run_artifact_administrative_units")
        if not inspector.has_table("run_artifact_product_images"):
            raise RuntimeError("Receiver schema is missing table run_artifact_product_images")
        if not inspector.has_table("run_artifact_product_meta"):
            raise RuntimeError("Receiver schema is missing table run_artifact_product_meta")
        if not inspector.has_table("run_artifact_product_wholesale_prices"):
            raise RuntimeError("Receiver schema is missing table run_artifact_product_wholesale_prices")
        if not inspector.has_table("run_artifact_product_categories"):
            raise RuntimeError("Receiver schema is missing table run_artifact_product_categories")
        if not inspector.has_table("task_runs"):
            raise RuntimeError("Receiver schema is missing table task_runs")

        columns = {item["name"] for item in inspector.get_columns("run_artifacts")}
        if "parser_name" not in columns:
            raise RuntimeError(
                "Unsupported receiver schema: run_artifacts.parser_name is missing. "
                "Use the current receiver schema."
            )
        required_artifact = {
            "retail_type",
            "code",
            "address",
            "schedule_weekdays_open_from",
            "schedule_weekdays_closed_from",
            "schedule_saturday_open_from",
            "schedule_saturday_closed_from",
            "schedule_sunday_open_from",
            "schedule_sunday_closed_from",
            "temporarily_closed",
            "longitude",
            "latitude",
            "rating",
            "reviews_count",
            "open_date",
            "dataclass_validated",
            "dataclass_validation_error",
        }
        missing_artifact = sorted(required_artifact - columns)
        if missing_artifact:
            raise RuntimeError(
                "Unsupported receiver schema: run_artifacts is missing columns "
                f"{', '.join(missing_artifact)}"
            )

        product_columns = {item["name"] for item in inspector.get_columns("run_artifact_products")}
        required_product = {
            "source_page_url",
            "description",
            "adult",
            "is_new",
            "promo",
            "season",
            "hit",
            "data_matrix",
            "producer_name",
            "producer_country",
            "expiration_date_in_days",
            "rating",
            "reviews_count",
            "price",
            "discount_price",
            "loyal_price",
            "price_unit",
            "unit_net",
            "package_quantity_net",
            "package_weight_gross",
            "package_count",
        }
        missing_product = sorted(required_product - product_columns)
        if missing_product:
            raise RuntimeError(
                "Unsupported receiver schema: run_artifact_products is missing columns "
                f"{', '.join(missing_product)}"
            )

        category_columns = {item["name"] for item in inspector.get_columns("run_artifact_categories")}
        missing_category = sorted({"adult", "icon", "banner"} - category_columns)
        if missing_category:
            raise RuntimeError(
                "Unsupported receiver schema: run_artifact_categories is missing columns "
                f"{', '.join(missing_category)}"
            )

        image_columns = {item["name"] for item in inspector.get_columns("run_artifact_product_images")}
        if "is_main" not in image_columns:
            raise RuntimeError(
                "Unsupported receiver schema: run_artifact_product_images is missing column is_main"
            )

        task_run_columns = {item["name"] for item in inspector.get_columns("task_runs")}
        missing_task_run = sorted({"converter_elapsed_sec", "finish"} - task_run_columns)
        if missing_task_run:
            raise RuntimeError(
                "Unsupported receiver schema: task_runs is missing columns "
                f"{', '.join(missing_task_run)}"
            )

    def _load_category_lookup(
        self,
        session: Session,
        artifact_ids: list[int],
    ) -> dict[int, dict[str, dict[str, Any]]]:
        if not artifact_ids:
            return {}

        stmt = select(
            _RunArtifactCategory.id,
            _RunArtifactCategory.artifact_id,
            _RunArtifactCategory.uid,
            _RunArtifactCategory.parent_uid,
            _RunArtifactCategory.alias,
            _RunArtifactCategory.title,
            _RunArtifactCategory.adult,
            _RunArtifactCategory.icon,
            _RunArtifactCategory.banner,
            _RunArtifactCategory.depth,
            _RunArtifactCategory.sort_order,
        ).where(_RunArtifactCategory.artifact_id.in_(artifact_ids))

        out: dict[int, dict[str, dict[str, Any]]] = {}
        for category_id, artifact_id, uid, parent_uid, alias, title, adult, icon, banner, depth, sort_order in session.execute(stmt):
            u = _safe_str(uid)
            if u is None:
                continue
            out.setdefault(int(artifact_id), {})[u] = {
                "id": int(category_id),
                "artifact_id": int(artifact_id),
                "uid": u,
                "parent_uid": _safe_str(parent_uid),
                "alias": _safe_str(alias),
                "title": _safe_str(title),
                "adult": _as_bool(adult),
                "icon": _safe_str(icon),
                "banner": _safe_str(banner),
                "depth": int(depth) if isinstance(depth, int) else None,
                "sort_order": int(sort_order) if isinstance(sort_order, int) else None,
            }

        return out

    def _load_image_lookup(self, session: Session, product_ids: list[int]) -> dict[int, list[str]]:
        if not product_ids:
            return {}

        stmt = (
            select(_RunArtifactProductImage.product_id, _RunArtifactProductImage.url)
            .where(_RunArtifactProductImage.product_id.in_(product_ids))
            .order_by(_RunArtifactProductImage.product_id.asc(), _RunArtifactProductImage.sort_order.asc())
        )

        out: dict[int, list[str]] = {}
        for product_id, url in session.execute(stmt):
            token = _safe_str(url)
            if token is None:
                continue
            bucket = out.setdefault(int(product_id), [])
            if token not in bucket:
                bucket.append(token)

        return out

    def _load_image_details_lookup(self, session: Session, product_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        if not product_ids:
            return {}

        stmt = (
            select(
                _RunArtifactProductImage.product_id,
                _RunArtifactProductImage.id,
                _RunArtifactProductImage.url,
                _RunArtifactProductImage.is_main,
                _RunArtifactProductImage.sort_order,
            )
            .where(_RunArtifactProductImage.product_id.in_(product_ids))
            .order_by(_RunArtifactProductImage.product_id.asc(), _RunArtifactProductImage.sort_order.asc())
        )

        out: dict[int, list[dict[str, Any]]] = {}
        for product_id, image_id, url, is_main, sort_order in session.execute(stmt):
            out.setdefault(int(product_id), []).append(
                {
                    "id": int(image_id),
                    "product_id": int(product_id),
                    "url": _safe_str(url),
                    "is_main": _as_bool(is_main),
                    "sort_order": int(sort_order) if isinstance(sort_order, int) else None,
                }
            )

        return out

    def _load_product_meta_lookup(self, session: Session, product_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        if not product_ids:
            return {}

        stmt = (
            select(
                _RunArtifactProductMeta.product_id,
                _RunArtifactProductMeta.id,
                _RunArtifactProductMeta.name,
                _RunArtifactProductMeta.alias,
                _RunArtifactProductMeta.value_type,
                _RunArtifactProductMeta.value_text,
                _RunArtifactProductMeta.sort_order,
            )
            .where(_RunArtifactProductMeta.product_id.in_(product_ids))
            .order_by(_RunArtifactProductMeta.product_id.asc(), _RunArtifactProductMeta.sort_order.asc())
        )

        out: dict[int, list[dict[str, Any]]] = {}
        for product_id, meta_id, name, alias, value_type, value_text, sort_order in session.execute(stmt):
            out.setdefault(int(product_id), []).append(
                {
                    "id": int(meta_id),
                    "product_id": int(product_id),
                    "name": _safe_str(name),
                    "alias": _safe_str(alias),
                    "value_type": _safe_str(value_type),
                    "value_text": _safe_str(value_text),
                    "sort_order": int(sort_order) if isinstance(sort_order, int) else None,
                }
            )
        return out

    def _load_product_wholesale_lookup(self, session: Session, product_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        if not product_ids:
            return {}

        stmt = (
            select(
                _RunArtifactProductWholesalePrice.product_id,
                _RunArtifactProductWholesalePrice.id,
                _RunArtifactProductWholesalePrice.from_items,
                _RunArtifactProductWholesalePrice.price,
                _RunArtifactProductWholesalePrice.sort_order,
            )
            .where(_RunArtifactProductWholesalePrice.product_id.in_(product_ids))
            .order_by(_RunArtifactProductWholesalePrice.product_id.asc(), _RunArtifactProductWholesalePrice.sort_order.asc())
        )

        out: dict[int, list[dict[str, Any]]] = {}
        for product_id, wholesale_id, from_items, price, sort_order in session.execute(stmt):
            out.setdefault(int(product_id), []).append(
                {
                    "id": int(wholesale_id),
                    "product_id": int(product_id),
                    "from_items": _as_float(from_items),
                    "price": _as_float(price),
                    "sort_order": int(sort_order) if isinstance(sort_order, int) else None,
                }
            )
        return out

    def _load_product_category_lookup(self, session: Session, product_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        if not product_ids:
            return {}

        stmt = (
            select(
                _RunArtifactProductCategory.product_id,
                _RunArtifactProductCategory.id,
                _RunArtifactProductCategory.category_uid,
                _RunArtifactProductCategory.sort_order,
            )
            .where(_RunArtifactProductCategory.product_id.in_(product_ids))
            .order_by(_RunArtifactProductCategory.product_id.asc(), _RunArtifactProductCategory.sort_order.asc())
        )

        out: dict[int, list[dict[str, Any]]] = {}
        for product_id, link_id, category_uid, sort_order in session.execute(stmt):
            out.setdefault(int(product_id), []).append(
                {
                    "id": int(link_id),
                    "product_id": int(product_id),
                    "category_uid": _safe_str(category_uid),
                    "sort_order": int(sort_order) if isinstance(sort_order, int) else None,
                }
            )
        return out

    @staticmethod
    def _resolve_category_titles(
        category_uids: list[str],
        category_lookup: dict[str, dict[str, Any]],
    ) -> str | None:
        if not category_uids:
            return None

        titles: list[str] = []
        seen: set[str] = set()
        for uid in category_uids:
            title = _safe_str((category_lookup.get(uid) or {}).get("title"))
            if title is None:
                continue
            lowered = title.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            titles.append(title)

        if not titles:
            return None
        return " / ".join(titles)

    @staticmethod
    def _resolve_categories_payload(
        category_uids: list[str],
        category_lookup: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for idx, uid in enumerate(category_uids):
            entry = category_lookup.get(uid)
            if not isinstance(entry, dict):
                out.append(
                    {
                        "id": None,
                        "artifact_id": None,
                        "uid": uid,
                        "title": None,
                        "adult": None,
                        "icon": None,
                        "banner": None,
                        "sort_order": idx,
                    }
                )
                continue

            out.append(
                {
                    "id": entry.get("id") if isinstance(entry.get("id"), int) else None,
                    "artifact_id": entry.get("artifact_id") if isinstance(entry.get("artifact_id"), int) else None,
                    "uid": _safe_str(entry.get("uid")) or uid,
                    "parent_uid": _safe_str(entry.get("parent_uid")),
                    "alias": _safe_str(entry.get("alias")),
                    "title": _safe_str(entry.get("title")),
                    "adult": _as_bool(entry.get("adult")),
                    "icon": _safe_str(entry.get("icon")),
                    "banner": _safe_str(entry.get("banner")),
                    "depth": entry.get("depth") if isinstance(entry.get("depth"), int) else None,
                    "sort_order": entry.get("sort_order") if isinstance(entry.get("sort_order"), int) else idx,
                }
            )

        return out

    @staticmethod
    def _normalize_product_ids(product_ids: Sequence[int]) -> list[int]:
        out: list[int] = []
        seen: set[int] = set()
        for raw in product_ids:
            value = _as_int(raw)
            if value is None or value <= 0:
                continue
            if value in seen:
                continue
            seen.add(value)
            out.append(int(value))
        return out

    @staticmethod
    def _find_empty_artifacts(session: Session, artifact_ids: Sequence[int]) -> list[int]:
        if not artifact_ids:
            return []
        rows = session.execute(
            select(_RunArtifact.id)
            .where(_RunArtifact.id.in_(artifact_ids))
            .where(
                ~select(_RunArtifactProduct.id)
                .where(_RunArtifactProduct.artifact_id == _RunArtifact.id)
                .exists()
            )
        ).all()
        return sorted({int(row.id) for row in rows})

    @staticmethod
    def _find_finished_runs(session: Session, run_ids: Sequence[str]) -> list[str]:
        if not run_ids:
            return []
        existing_rows = session.execute(
            select(_RunArtifact.run_id).where(_RunArtifact.run_id.in_(run_ids))
        ).all()
        existing_run_ids = {row.run_id for row in existing_rows if isinstance(row.run_id, str) and row.run_id}
        return sorted({run_id for run_id in run_ids if run_id not in existing_run_ids})

    @staticmethod
    def _elapsed_seconds_since(chunk_started_at: float) -> int:
        if isinstance(chunk_started_at, bool):
            return 0
        try:
            started = float(chunk_started_at)
        except (TypeError, ValueError):
            return 0
        elapsed = monotonic() - started
        if elapsed <= 0:
            return 0
        return int(math.ceil(elapsed))

    @staticmethod
    def _distribute_elapsed_seconds(
        elapsed_seconds: int,
        run_counts: dict[str, int],
    ) -> dict[str, int]:
        if elapsed_seconds <= 0 or not run_counts:
            return {}

        total_count = sum(max(0, int(count)) for count in run_counts.values())
        if total_count <= 0:
            return {}

        allocations: dict[str, int] = {}
        remainders: list[tuple[float, str]] = []
        assigned = 0
        for run_id in sorted(run_counts.keys()):
            count = max(0, int(run_counts[run_id]))
            if count == 0:
                allocations[run_id] = 0
                continue
            quota = elapsed_seconds * count / total_count
            whole = int(quota)
            allocations[run_id] = whole
            assigned += whole
            remainders.append((quota - whole, run_id))

        remaining = elapsed_seconds - assigned
        if remaining > 0:
            remainders.sort(key=lambda item: (-item[0], item[1]))
            for _, run_id in remainders[:remaining]:
                allocations[run_id] = allocations.get(run_id, 0) + 1

        return allocations


class ReceiverSQLiteRepository(ReceiverRepository):
    def __init__(self, db_path: str | Path, *, default_parser_name: str = "fixprice") -> None:
        path = Path(db_path)
        if not path.is_file():
            raise FileNotFoundError(f"receiver sqlite db not found: {path}")
        super().__init__(
            f"sqlite:///{path.resolve()}",
            default_parser_name=default_parser_name,
        )
