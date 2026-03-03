from __future__ import annotations

from typing import Any

from sqlalchemy import JSON, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


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


__all__ = [
    "_ReceiverBase",
    "_RunArtifact",
    "_RunArtifactAdministrativeUnit",
    "_RunArtifactCategory",
    "_RunArtifactProduct",
    "_RunArtifactProductCategory",
    "_RunArtifactProductImage",
    "_RunArtifactProductMeta",
    "_RunArtifactProductWholesalePrice",
]
