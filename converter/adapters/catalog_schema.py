from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Float,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _safe_str(value: object) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _as_float(value: object) -> float | None:
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


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


class _CatalogBase(DeclarativeBase):
    pass


def _bigint_sqlite() -> BigInteger:
    return BigInteger().with_variant(Integer(), "sqlite")


def _uuid_text() -> String:
    return String(36).with_variant(UUID(as_uuid=False), "postgresql")


_STORAGE_DELETE_STATUS_ENUM = Enum(
    "pending",
    "done",
    "failed",
    name="catalog_storage_delete_status_enum",
    native_enum=True,
)


class _CatalogProduct(_CatalogBase):
    __tablename__ = "catalog_products"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)

    canonical_product_id: Mapped[str] = mapped_column(_uuid_text(), nullable=False, index=True)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)

    plu: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sku: Mapped[str | None] = mapped_column(String(128), nullable=True)

    title_original: Mapped[str] = mapped_column(Text, nullable=False)
    title_normalized_no_stopwords: Mapped[str] = mapped_column(Text, nullable=False)

    brand: Mapped[str | None] = mapped_column(String(255), nullable=True)
    brand_normalized: Mapped[str | None] = mapped_column(String(255), nullable=True)

    source_page_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    producer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    producer_country: Mapped[str | None] = mapped_column(String(32), nullable=True)
    expiration_date_in_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    reviews_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    adult: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_new: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    promo: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    season: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    hit: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    data_matrix: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    unit: Mapped[str] = mapped_column(String(32), nullable=False)
    package_quantity: Mapped[float | None] = mapped_column(nullable=True)
    package_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    package_weight_gross: Mapped[float | None] = mapped_column(Float, nullable=True)
    package_count: Mapped[float | None] = mapped_column(Float, nullable=True)
    dimension_height_m: Mapped[float | None] = mapped_column(Float, nullable=True)
    dimension_width_m: Mapped[float | None] = mapped_column(Float, nullable=True)
    dimension_depth_m: Mapped[float | None] = mapped_column(Float, nullable=True)

    primary_category_id: Mapped[int | None] = mapped_column(_bigint_sqlite(), nullable=True, index=True)
    settlement_id: Mapped[int | None] = mapped_column(_bigint_sqlite(), nullable=True, index=True)

    composition_original: Mapped[str | None] = mapped_column(Text, nullable=True)
    composition_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)

    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "parser_name",
            "source_id",
            name="uq_catalog_products_source",
        ),
    )


class _CatalogProductSnapshot(_CatalogBase):
    __tablename__ = "catalog_product_snapshots"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)

    canonical_product_id: Mapped[str] = mapped_column(_uuid_text(), nullable=False, index=True)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    source_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    receiver_product_id: Mapped[int | None] = mapped_column(_bigint_sqlite(), nullable=True)
    receiver_artifact_id: Mapped[int | None] = mapped_column(_bigint_sqlite(), nullable=True)
    store_id: Mapped[int | None] = mapped_column(_bigint_sqlite(), nullable=True, index=True)
    receiver_sort_order: Mapped[int | None] = mapped_column(_bigint_sqlite(), nullable=True)

    source_event_uid: Mapped[str | None] = mapped_column(String(191), nullable=True, index=True)
    content_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    valid_from_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    valid_to_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    price: Mapped[float | None] = mapped_column(Numeric(12, 4, asdecimal=False), nullable=True)
    discount_price: Mapped[float | None] = mapped_column(Numeric(12, 4, asdecimal=False), nullable=True)
    loyal_price: Mapped[float | None] = mapped_column(Numeric(12, 4, asdecimal=False), nullable=True)
    price_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    available_count: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "source_event_uid",
            name="uq_cps_event",
        ),
    )


class _CatalogProductSource(_CatalogBase):
    __tablename__ = "catalog_product_sources"

    parser_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    canonical_product_id: Mapped[str] = mapped_column(_uuid_text(), nullable=False, index=True)
    latest_snapshot_id: Mapped[int | None] = mapped_column(_bigint_sqlite(), nullable=True)
    latest_content_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogSettlement(_CatalogBase):
    __tablename__ = "catalog_settlements"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    geo_key: Mapped[str] = mapped_column(String(191), nullable=False, unique=True)
    country: Mapped[str | None] = mapped_column(String(64), nullable=True)
    country_normalized: Mapped[str | None] = mapped_column(String(128), nullable=True)
    region: Mapped[str | None] = mapped_column(Text, nullable=True)
    region_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    name_normalized: Mapped[str | None] = mapped_column(String(255), nullable=True)
    settlement_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    alias: Mapped[str | None] = mapped_column(String(255), nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogStore(_CatalogBase):
    __tablename__ = "catalog_stores"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    store_key: Mapped[str] = mapped_column(String(191), nullable=False, unique=True)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    retail_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    code: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    schedule_weekdays_open_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_weekdays_closed_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_saturday_open_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_saturday_closed_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_sunday_open_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_sunday_closed_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    temporarily_closed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    reviews_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    open_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    settlement_id: Mapped[int | None] = mapped_column(
        _bigint_sqlite(),
        ForeignKey("catalog_settlements.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index(
            "ix_catalog_stores_parser_source_code",
            "parser_name",
            "source",
            "code",
        ),
    )


class _CatalogCategory(_CatalogBase):
    __tablename__ = "catalog_categories"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    category_key: Mapped[str] = mapped_column(String(191), nullable=False, unique=True)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_uid: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    parent_source_uid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    title_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)
    alias: Mapped[str | None] = mapped_column(Text, nullable=True)
    adult: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    icon: Mapped[str | None] = mapped_column(Text, nullable=True)
    banner: Mapped[str | None] = mapped_column(Text, nullable=True)
    depth: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sort_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogIdentityMap(_CatalogBase):
    __tablename__ = "catalog_identity_map"

    parser_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    identity_type: Mapped[str] = mapped_column(String(64), primary_key=True)
    identity_value: Mapped[str] = mapped_column(String(255), primary_key=True)
    canonical_product_id: Mapped[str] = mapped_column(_uuid_text(), nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogProductGroup(_CatalogBase):
    __tablename__ = "catalog_product_groups"

    group_uid: Mapped[str] = mapped_column(_uuid_text(), primary_key=True)
    product_id: Mapped[int] = mapped_column(
        _bigint_sqlite(),
        ForeignKey("catalog_products.id", ondelete="CASCADE"),
        primary_key=True,
    )
    source: Mapped[str] = mapped_column(String(64), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index(
            "ix_catalog_product_groups_product_id",
            "product_id",
        ),
    )


class _CatalogProductAsset(_CatalogBase):
    __tablename__ = "catalog_product_assets"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(_bigint_sqlite(), nullable=False, index=True)
    sort_order: Mapped[int] = mapped_column(_bigint_sqlite(), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    size: Mapped[int | None] = mapped_column(_bigint_sqlite(), nullable=True)
    fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "product_id",
            "sort_order",
            name="uq_catalog_product_assets_slot",
        ),
    )


class _CatalogStorageDeleteOutbox(_CatalogBase):
    __tablename__ = "catalog_storage_delete_outbox"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    dedupe_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    image_url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(_STORAGE_DELETE_STATUS_ENUM, nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(_bigint_sqlite(), nullable=False, default=0)
    enqueued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


__all__ = [
    "_CatalogBase",
    "_CatalogCategory",
    "_CatalogIdentityMap",
    "_CatalogProduct",
    "_CatalogProductGroup",
    "_CatalogProductAsset",
    "_CatalogProductSnapshot",
    "_CatalogProductSource",
    "_CatalogSettlement",
    "_CatalogStore",
    "_CatalogStorageDeleteOutbox",
    "_as_float",
    "_is_missing",
    "_safe_str",
    "_utc_now",
]
