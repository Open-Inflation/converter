from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
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


class _CatalogProduct(_CatalogBase):
    __tablename__ = "catalog_products"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    canonical_product_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)

    plu: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sku: Mapped[str | None] = mapped_column(String(128), nullable=True)

    title_original: Mapped[str] = mapped_column(Text, nullable=False)
    title_normalized_no_stopwords: Mapped[str] = mapped_column(Text, nullable=False)

    brand: Mapped[str | None] = mapped_column(String(255), nullable=True)

    source_page_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    producer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    producer_country: Mapped[str | None] = mapped_column(String(32), nullable=True)
    expiration_date_in_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    reviews_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    discount_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    loyal_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    adult: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_new: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    promo: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    season: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    hit: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    data_matrix: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    unit: Mapped[str] = mapped_column(String(32), nullable=False)
    available_count: Mapped[float | None] = mapped_column(nullable=True)
    package_quantity: Mapped[float | None] = mapped_column(nullable=True)
    package_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)

    primary_category_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    settlement_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

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

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    canonical_product_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    source_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    receiver_product_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    receiver_artifact_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    receiver_sort_order: Mapped[int | None] = mapped_column(Integer, nullable=True)

    title_original: Mapped[str] = mapped_column(Text, nullable=False)
    title_normalized_no_stopwords: Mapped[str] = mapped_column(Text, nullable=False)

    brand: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_page_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    producer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    producer_country: Mapped[str | None] = mapped_column(String(32), nullable=True)
    expiration_date_in_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    reviews_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    discount_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    loyal_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    adult: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_new: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    promo: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    season: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    hit: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    data_matrix: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    unit: Mapped[str] = mapped_column(String(32), nullable=False)
    available_count: Mapped[float | None] = mapped_column(Float, nullable=True)
    package_quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    package_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)

    category_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)
    composition_original: Mapped[str | None] = mapped_column(Text, nullable=True)
    composition_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)
    settlement_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    source_event_uid: Mapped[str | None] = mapped_column(String(191), nullable=True, index=True)
    content_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    valid_from_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    valid_to_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

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
    canonical_product_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    latest_snapshot_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latest_content_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogSettlement(_CatalogBase):
    __tablename__ = "catalog_settlements"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
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


class _CatalogSettlementGeodata(_CatalogBase):
    __tablename__ = "catalog_settlement_geodata"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    geo_fingerprint: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    settlement_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    source_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    receiver_artifact_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    receiver_product_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogCategory(_CatalogBase):
    __tablename__ = "catalog_categories"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    category_key: Mapped[str] = mapped_column(String(191), nullable=False, unique=True)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_uid: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    parent_source_uid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    title_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)
    alias: Mapped[str | None] = mapped_column(Text, nullable=True)
    depth: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sort_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogProductCategoryLink(_CatalogBase):
    __tablename__ = "catalog_product_category_links"

    snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    category_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sort_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogIdentityMap(_CatalogBase):
    __tablename__ = "catalog_identity_map"

    parser_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    identity_type: Mapped[str] = mapped_column(String(64), primary_key=True)
    identity_value: Mapped[str] = mapped_column(String(255), primary_key=True)
    canonical_product_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogImageFingerprint(_CatalogBase):
    __tablename__ = "catalog_image_fingerprints"

    fingerprint: Mapped[str] = mapped_column(String(64), primary_key=True)
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogProductAsset(_CatalogBase):
    __tablename__ = "catalog_product_assets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    asset_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "product_id",
            "asset_kind",
            "sort_order",
            name="uq_catalog_product_assets_slot",
        ),
    )


class _CatalogSnapshotAsset(_CatalogBase):
    __tablename__ = "catalog_snapshot_assets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    snapshot_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    asset_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "snapshot_id",
            "asset_kind",
            "sort_order",
            name="uq_catalog_snapshot_assets_slot",
        ),
    )


class _ConverterSyncState(_CatalogBase):
    __tablename__ = "converter_sync_state"

    state_key: Mapped[str] = mapped_column("key", String(191), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogIngestStageProduct(_CatalogBase):
    __tablename__ = "catalog_ingest_stage_products"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chunk_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    row_no: Mapped[int] = mapped_column(Integer, nullable=False)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    canonical_product_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    content_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    source_event_uid: Mapped[str] = mapped_column(String(191), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    receiver_product_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    receiver_artifact_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "chunk_id",
            "row_no",
            name="uq_catalog_stage_products_chunk_row",
        ),
    )


class _CatalogIngestStageAsset(_CatalogBase):
    __tablename__ = "catalog_ingest_stage_assets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chunk_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    asset_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogIngestStageCategory(_CatalogBase):
    __tablename__ = "catalog_ingest_stage_categories"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chunk_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    uid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    parent_uid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    depth: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False)
    alias: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogStorageDeleteOutbox(_CatalogBase):
    __tablename__ = "catalog_storage_delete_outbox"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dedupe_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    image_url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    enqueued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


__all__ = [
    "_CatalogBase",
    "_CatalogCategory",
    "_CatalogIdentityMap",
    "_CatalogImageFingerprint",
    "_CatalogIngestStageAsset",
    "_CatalogIngestStageCategory",
    "_CatalogIngestStageProduct",
    "_CatalogProduct",
    "_CatalogProductAsset",
    "_CatalogProductCategoryLink",
    "_CatalogProductSnapshot",
    "_CatalogProductSource",
    "_CatalogSettlement",
    "_CatalogSettlementGeodata",
    "_CatalogSnapshotAsset",
    "_CatalogStorageDeleteOutbox",
    "_ConverterSyncState",
    "_as_float",
    "_is_missing",
    "_safe_str",
    "_utc_now",
]
