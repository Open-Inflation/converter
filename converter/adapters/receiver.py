from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import JSON, String, Text, and_, func, inspect, or_, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from converter.core.models import PackageUnit, RawProductRecord, Unit


class _ReceiverBase(DeclarativeBase):
    pass


class _RunArtifact(_ReceiverBase):
    __tablename__ = "run_artifacts"

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    parser_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    retail_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    schedule_weekdays_open_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_weekdays_closed_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_saturday_open_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_saturday_closed_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_sunday_open_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_sunday_closed_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    temporarily_closed: Mapped[bool | None] = mapped_column(nullable=True)
    longitude: Mapped[float | None] = mapped_column(nullable=True)
    latitude: Mapped[float | None] = mapped_column(nullable=True)
    dataclass_validated: Mapped[bool | None] = mapped_column(nullable=True)
    dataclass_validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    ingested_at: Mapped[str | None] = mapped_column(String(64), nullable=True)


class _RunArtifactProduct(_ReceiverBase):
    __tablename__ = "run_artifact_products"

    id: Mapped[int] = mapped_column(primary_key=True)
    artifact_id: Mapped[int] = mapped_column(nullable=False)

    sku: Mapped[str | None] = mapped_column(String(128), nullable=True)
    plu: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_page_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    adult: Mapped[bool | None] = mapped_column(nullable=True)
    is_new: Mapped[bool | None] = mapped_column(nullable=True)
    promo: Mapped[bool | None] = mapped_column(nullable=True)
    season: Mapped[bool | None] = mapped_column(nullable=True)
    hit: Mapped[bool | None] = mapped_column(nullable=True)
    data_matrix: Mapped[bool | None] = mapped_column(nullable=True)
    composition: Mapped[str | None] = mapped_column(Text, nullable=True)

    brand: Mapped[str | None] = mapped_column(String(255), nullable=True)
    producer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    producer_country: Mapped[str | None] = mapped_column(String(32), nullable=True)
    expiration_date_in_days: Mapped[int | None] = mapped_column(nullable=True)
    rating: Mapped[float | None] = mapped_column(nullable=True)
    reviews_count: Mapped[int | None] = mapped_column(nullable=True)
    price: Mapped[float | None] = mapped_column(nullable=True)
    discount_price: Mapped[float | None] = mapped_column(nullable=True)
    loyal_price: Mapped[float | None] = mapped_column(nullable=True)
    price_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    available_count: Mapped[float | None] = mapped_column(nullable=True)
    package_quantity: Mapped[float | None] = mapped_column(nullable=True)
    package_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)

    categories_uid_json: Mapped[Any] = mapped_column(JSON, nullable=True)
    main_image: Mapped[str | None] = mapped_column(Text, nullable=True)
    sort_order: Mapped[int | None] = mapped_column(nullable=True)


class _RunArtifactCategory(_ReceiverBase):
    __tablename__ = "run_artifact_categories"

    id: Mapped[int] = mapped_column(primary_key=True)
    artifact_id: Mapped[int] = mapped_column(nullable=False)
    uid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    parent_uid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    alias: Mapped[str | None] = mapped_column(String(255), nullable=True)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    adult: Mapped[bool | None] = mapped_column(nullable=True)
    icon: Mapped[str | None] = mapped_column(Text, nullable=True)
    banner: Mapped[str | None] = mapped_column(Text, nullable=True)
    depth: Mapped[int | None] = mapped_column(nullable=True)
    sort_order: Mapped[int | None] = mapped_column(nullable=True)


class _RunArtifactAdministrativeUnit(_ReceiverBase):
    __tablename__ = "run_artifact_administrative_units"

    id: Mapped[int] = mapped_column(primary_key=True)
    artifact_id: Mapped[int] = mapped_column(nullable=False)

    settlement_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    alias: Mapped[str | None] = mapped_column(String(255), nullable=True)
    region: Mapped[str | None] = mapped_column(String(255), nullable=True)
    country: Mapped[str | None] = mapped_column(String(64), nullable=True)
    longitude: Mapped[float | None] = mapped_column(nullable=True)
    latitude: Mapped[float | None] = mapped_column(nullable=True)


class _RunArtifactProductImage(_ReceiverBase):
    __tablename__ = "run_artifact_product_images"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(nullable=False)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_main: Mapped[bool | None] = mapped_column(nullable=True)
    sort_order: Mapped[int | None] = mapped_column(nullable=True)


class _RunArtifactProductMeta(_ReceiverBase):
    __tablename__ = "run_artifact_product_meta"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(nullable=False)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    alias: Mapped[str | None] = mapped_column(String(255), nullable=True)
    value_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    value_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    sort_order: Mapped[int | None] = mapped_column(nullable=True)


class _RunArtifactProductWholesalePrice(_ReceiverBase):
    __tablename__ = "run_artifact_product_wholesale_prices"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(nullable=False)
    from_items: Mapped[float | None] = mapped_column(nullable=True)
    price: Mapped[float | None] = mapped_column(nullable=True)
    sort_order: Mapped[int | None] = mapped_column(nullable=True)


class _RunArtifactProductCategory(_ReceiverBase):
    __tablename__ = "run_artifact_product_categories"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(nullable=False)
    category_uid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sort_order: Mapped[int | None] = mapped_column(nullable=True)


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    token = _safe_str(value)
    if token is None:
        return None
    token = token.replace(",", ".")
    try:
        return float(token)
    except ValueError:
        return None


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None

    token = _safe_str(value)
    if token is None:
        return None
    try:
        return int(token)
    except ValueError:
        return None


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    token = _safe_str(value)
    if token is None:
        return None
    lowered = token.lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []

    raw_list: list[Any]
    if isinstance(value, list):
        raw_list = value
    elif isinstance(value, str):
        token = value.strip()
        if not token:
            return []
        if token.startswith("["):
            try:
                parsed = json.loads(token)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                raw_list = parsed
            else:
                raw_list = [token]
        else:
            raw_list = [item.strip() for item in token.split(",") if item.strip()]
    else:
        raw_list = [value]

    out: list[str] = []
    for item in raw_list:
        token = _safe_str(item)
        if token is not None:
            out.append(token)
    return out


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    token = _safe_str(value)
    if token is None:
        return None

    normalized = token.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _normalize_unit(value: Any) -> Unit | None:
    token = _safe_str(value)
    if token is None:
        return None
    token = token.upper()
    if token in {"PCE", "KGM", "LTR"}:
        return token  # type: ignore[return-value]
    return None


def _normalize_package_unit(value: Any) -> PackageUnit | None:
    token = _safe_str(value)
    if token is None:
        return None
    token = token.upper()
    if token in {"KGM", "LTR"}:
        return token  # type: ignore[return-value]
    return None


def _join_non_empty(parts: Iterable[str | None], *, sep: str) -> str | None:
    seen: set[str] = set()
    out: list[str] = []
    for part in parts:
        token = _safe_str(part)
        if token is None:
            continue
        lowered = token.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(token)
    if not out:
        return None
    return sep.join(out)


def map_receiver_row_to_raw_product(
    row: Mapping[str, Any],
    *,
    default_parser_name: str = "fixprice",
) -> RawProductRecord:
    parser_name = _safe_str(row.get("parser_name")) or default_parser_name

    title = _safe_str(row.get("product_title")) or ""
    if not title:
        title = "Unnamed product"

    run_id = _safe_str(row.get("run_id"))
    product_id = row.get("product_id")
    source_id = _safe_str(row.get("source_id"))
    if source_id is None and run_id is not None and product_id is not None:
        source_id = f"receiver:{run_id}:{product_id}"

    category = _safe_str(row.get("category_titles"))

    geo = _join_non_empty(
        (
            _safe_str(row.get("geo_country")),
            _safe_str(row.get("geo_region")),
            _safe_str(row.get("geo_name")),
        ),
        sep=", ",
    )

    images = _as_string_list(row.get("image_urls_json"))
    if not images:
        main_image = _safe_str(row.get("product_main_image"))
        if main_image is not None:
            images = [main_image]

    observed_at = _parse_datetime(row.get("ingested_at")) or datetime.now(tz=timezone.utc)

    category_uids = _as_string_list(row.get("category_uids_json"))

    receiver_categories = row.get("receiver_categories")
    if not isinstance(receiver_categories, list):
        receiver_categories = []

    receiver_product = row.get("receiver_product")
    if not isinstance(receiver_product, dict):
        receiver_product = {}

    receiver_artifact = row.get("receiver_artifact")
    if not isinstance(receiver_artifact, dict):
        receiver_artifact = {}

    receiver_admin = row.get("receiver_admin_unit")
    if not isinstance(receiver_admin, dict):
        receiver_admin = {}

    receiver_product_images = row.get("receiver_product_images")
    if not isinstance(receiver_product_images, list):
        receiver_product_images = []

    receiver_product_meta = row.get("receiver_product_meta")
    if not isinstance(receiver_product_meta, list):
        receiver_product_meta = []

    receiver_product_wholesale_prices = row.get("receiver_product_wholesale_prices")
    if not isinstance(receiver_product_wholesale_prices, list):
        receiver_product_wholesale_prices = []

    receiver_product_categories = row.get("receiver_product_categories")
    if not isinstance(receiver_product_categories, list):
        receiver_product_categories = []

    payload = {
        "receiver_product_id": row.get("product_id"),
        "receiver_artifact_id": row.get("artifact_id"),
        "receiver_run_id": run_id,
        "receiver_parser_name": _safe_str(row.get("parser_name")),
        "receiver_source": _safe_str(row.get("artifact_source")),
        "receiver_ingested_at": _safe_str(row.get("ingested_at")),
        "receiver_sort_order": row.get("product_sort_order"),
        "receiver_categories_uid": category_uids,
        "receiver_categories": receiver_categories,
        "receiver_geo_country": _safe_str(row.get("geo_country")),
        "receiver_geo_region": _safe_str(row.get("geo_region")),
        "receiver_geo_name": _safe_str(row.get("geo_name")),
        "receiver_geo_settlement_type": _safe_str(row.get("geo_settlement_type")),
        "receiver_geo_alias": _safe_str(row.get("geo_alias")),
        "receiver_geo_longitude": _as_float(row.get("geo_longitude")),
        "receiver_geo_latitude": _as_float(row.get("geo_latitude")),
        "receiver_product": receiver_product,
        "receiver_artifact": receiver_artifact,
        "receiver_admin_unit": receiver_admin,
        "receiver_product_images": receiver_product_images,
        "receiver_product_meta": receiver_product_meta,
        "receiver_product_wholesale_prices": receiver_product_wholesale_prices,
        "receiver_product_categories": receiver_product_categories,
    }

    return RawProductRecord(
        parser_name=parser_name,
        title=title,
        source_id=source_id,
        plu=_safe_str(row.get("product_plu")),
        sku=_safe_str(row.get("product_sku")),
        brand=_safe_str(row.get("product_brand")),
        unit=_normalize_unit(row.get("product_unit")),
        available_count=_as_float(row.get("product_available_count")),
        package_quantity=_as_float(row.get("product_package_quantity")),
        package_unit=_normalize_package_unit(row.get("product_package_unit")),
        price=_as_float(row.get("product_price")),
        discount_price=_as_float(row.get("product_discount_price")),
        loyal_price=_as_float(row.get("product_loyal_price")),
        price_unit=_safe_str(row.get("product_price_unit")),
        source_page_url=_safe_str(row.get("product_source_page_url")),
        description=_safe_str(row.get("product_description")),
        producer_name=_safe_str(row.get("product_producer_name")),
        producer_country=_safe_str(row.get("product_producer_country")),
        expiration_date_in_days=_as_int(row.get("product_expiration_date_in_days")),
        rating=_as_float(row.get("product_rating")),
        reviews_count=_as_int(row.get("product_reviews_count")),
        adult=_as_bool(row.get("product_adult")),
        is_new=_as_bool(row.get("product_is_new")),
        promo=_as_bool(row.get("product_promo")),
        season=_as_bool(row.get("product_season")),
        hit=_as_bool(row.get("product_hit")),
        data_matrix=_as_bool(row.get("product_data_matrix")),
        category=category,
        geo=geo,
        composition=_safe_str(row.get("product_composition")),
        image_urls=images,
        observed_at=observed_at,
        payload=payload,
    )


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

    def fetch_batch(
        self,
        limit: int = 100,
        *,
        parser_name: str | None = None,
        after_ingested_at: str | datetime | None = None,
        after_product_id: int | None = None,
    ) -> list[RawProductRecord]:
        parser_filter = parser_name.strip().lower() if isinstance(parser_name, str) else None
        watermark = self._normalize_watermark(after_ingested_at)
        after_id = int(after_product_id or 0)

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

            if watermark is not None:
                stmt = stmt.where(
                    or_(
                        _RunArtifact.ingested_at > watermark,
                        and_(
                            _RunArtifact.ingested_at == watermark,
                            _RunArtifactProduct.id > after_id,
                        ),
                    )
                )

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
                    "product_unit": product.unit,
                    "product_available_count": product.available_count,
                    "product_package_quantity": product.package_quantity,
                    "product_package_unit": product.package_unit,
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
                    "unit": product.unit,
                    "available_count": product.available_count,
                    "package_quantity": product.package_quantity,
                    "package_unit": product.package_unit,
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
                if parser_filter and parsed.parser_name.strip().lower() != parser_filter:
                    continue
                out.append(parsed)

            return out

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
    def _normalize_watermark(value: str | datetime | None) -> str | datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        token = _safe_str(value)
        return token


class ReceiverSQLiteRepository(ReceiverRepository):
    def __init__(self, db_path: str | Path, *, default_parser_name: str = "fixprice") -> None:
        path = Path(db_path)
        if not path.is_file():
            raise FileNotFoundError(f"receiver sqlite db not found: {path}")
        super().__init__(
            f"sqlite:///{path.resolve()}",
            default_parser_name=default_parser_name,
        )
