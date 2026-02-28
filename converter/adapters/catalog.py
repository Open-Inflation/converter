from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
    inspect,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from converter.core.models import NormalizedProductRecord


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

    raw_title: Mapped[str] = mapped_column(Text, nullable=False)
    title_original: Mapped[str] = mapped_column(Text, nullable=False)
    title_normalized: Mapped[str] = mapped_column(Text, nullable=False)
    title_original_no_stopwords: Mapped[str] = mapped_column(Text, nullable=False)
    title_normalized_no_stopwords: Mapped[str] = mapped_column(Text, nullable=False)

    brand: Mapped[str | None] = mapped_column(String(255), nullable=True)

    unit: Mapped[str] = mapped_column(String(32), nullable=False)
    available_count: Mapped[float | None] = mapped_column(nullable=True)
    package_quantity: Mapped[float | None] = mapped_column(nullable=True)
    package_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)

    primary_category_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    settlement_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    composition_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    composition_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)

    image_urls_json: Mapped[Any] = mapped_column(JSON, nullable=False)
    duplicate_image_urls_json: Mapped[Any] = mapped_column(JSON, nullable=False)
    image_fingerprints_json: Mapped[Any] = mapped_column(JSON, nullable=False)

    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    raw_payload_json: Mapped[Any] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


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

    raw_title: Mapped[str] = mapped_column(Text, nullable=False)
    title_original: Mapped[str] = mapped_column(Text, nullable=False)
    title_normalized: Mapped[str] = mapped_column(Text, nullable=False)
    title_original_no_stopwords: Mapped[str] = mapped_column(Text, nullable=False)
    title_normalized_no_stopwords: Mapped[str] = mapped_column(Text, nullable=False)

    brand: Mapped[str | None] = mapped_column(String(255), nullable=True)

    unit: Mapped[str] = mapped_column(String(32), nullable=False)
    available_count: Mapped[float | None] = mapped_column(Float, nullable=True)
    package_quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    package_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)

    category_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    category_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)

    geo_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)

    composition_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    composition_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)

    settlement_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    image_urls_json: Mapped[Any] = mapped_column(JSON, nullable=False)
    duplicate_image_urls_json: Mapped[Any] = mapped_column(JSON, nullable=False)
    image_fingerprints_json: Mapped[Any] = mapped_column(JSON, nullable=False)

    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    raw_payload_json: Mapped[Any] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)


class _CatalogProductSource(_CatalogBase):
    __tablename__ = "catalog_product_sources"

    parser_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_id: Mapped[str] = mapped_column(String(255), primary_key=True)

    canonical_product_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    latest_snapshot_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogSettlement(_CatalogBase):
    __tablename__ = "catalog_settlements"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    geo_key: Mapped[str] = mapped_column(String(191), nullable=False, unique=True)

    country_raw: Mapped[str | None] = mapped_column(String(64), nullable=True)
    country_normalized: Mapped[str | None] = mapped_column(String(128), nullable=True)

    region_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    region_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)

    name_raw: Mapped[str | None] = mapped_column(String(255), nullable=True)
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
    raw_payload_json: Mapped[Any] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _CatalogCategory(_CatalogBase):
    __tablename__ = "catalog_categories"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    category_key: Mapped[str] = mapped_column(String(191), nullable=False, unique=True)

    parser_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_uid: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    parent_source_uid: Mapped[str | None] = mapped_column(String(128), nullable=True)

    title_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
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


class _ConverterSyncState(_CatalogBase):
    __tablename__ = "converter_sync_state"

    state_key: Mapped[str] = mapped_column("key", String(191), primary_key=True)
    value: Mapped[Any] = mapped_column(JSON, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CatalogRepository:
    """
    SQLAlchemy-based persistent sink for normalized catalog products.

    Policy:
    - append-only history to `catalog_product_snapshots`;
    - additive updates for dimensions (settlements/categories/geodata);
    - non-destructive merge for `catalog_products` current projection.
    """

    BACKFILL_FIELDS = (
        "brand",
        "category_normalized",
        "geo_normalized",
        "composition_normalized",
        "package_quantity",
        "package_unit",
    )

    def __init__(self, database_url: str, *, engine: Engine | None = None) -> None:
        self._database_url = database_url
        self._engine = engine or self._create_engine(database_url)
        self._session_factory = sessionmaker(
            bind=self._engine,
            class_=Session,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        _CatalogBase.metadata.create_all(self._engine)
        self._validate_catalog_products_schema()

    def upsert_many(self, records: list[NormalizedProductRecord]) -> None:
        if not records:
            return

        with self._session_factory() as session:
            for record in records:
                canonical_product_id = self._resolve_canonical_product_id(session, record)
                record.canonical_product_id = canonical_product_id

                self._apply_persistent_image_dedup(session, record)
                self._apply_backfill(session, record)

                settlement = self._upsert_settlement(session, record)
                snapshot = self._insert_product_snapshot(session, record, settlement=settlement)
                self._append_settlement_geodata(session, settlement=settlement, record=record)

                categories = self._upsert_categories(session, record)
                self._link_snapshot_categories(session, snapshot=snapshot, categories=categories)

                self._upsert_product_source(session, record, snapshot=snapshot)
                self._upsert_product_row(
                    session,
                    record,
                    settlement=settlement,
                    categories=categories,
                )

            session.commit()

    def get_receiver_cursor(self, parser_name: str) -> tuple[str | None, int | None]:
        with self._session_factory() as session:
            key = self._cursor_key(parser_name)
            row = session.get(_ConverterSyncState, key)
            if row is None:
                return None, None

            raw_value = row.value
            if isinstance(raw_value, str):
                try:
                    parsed = json.loads(raw_value)
                except json.JSONDecodeError:
                    return None, None
            elif isinstance(raw_value, dict):
                parsed = raw_value
            else:
                return None, None

            ingested_at = _safe_str(parsed.get("ingested_at"))
            product_id_raw = parsed.get("product_id")
            product_id = int(product_id_raw) if isinstance(product_id_raw, int) or str(product_id_raw).isdigit() else None
            return ingested_at, product_id

    def set_receiver_cursor(
        self,
        parser_name: str,
        *,
        ingested_at: str,
        product_id: int,
    ) -> None:
        payload = {
            "ingested_at": ingested_at,
            "product_id": int(product_id),
        }
        now = _utc_now()

        with self._session_factory() as session:
            key = self._cursor_key(parser_name)
            row = session.get(_ConverterSyncState, key)
            if row is None:
                row = _ConverterSyncState(
                    state_key=key,
                    value=payload,
                    updated_at=now,
                )
                session.add(row)
            else:
                row.value = payload
                row.updated_at = now
            session.commit()

    @staticmethod
    def _create_engine(database_url: str) -> Engine:
        connect_args: dict[str, object] = {}
        if database_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False

        return create_engine(
            database_url,
            future=True,
            pool_pre_ping=True,
            connect_args=connect_args,
        )

    def _resolve_canonical_product_id(self, session: Session, record: NormalizedProductRecord) -> str:
        parser_name = record.parser_name.strip().lower()
        identity_keys = record.identity_candidates()

        chosen_id: str | None = None
        for identity_type, identity_value in identity_keys:
            row = self._get_identity_map_row(
                session,
                parser_name=parser_name,
                identity_type=identity_type,
                identity_value=identity_value,
            )
            if row is not None and _safe_str(row.canonical_product_id):
                chosen_id = row.canonical_product_id
                break

        fallback_identity = self._fallback_identity_value(record)
        if chosen_id is None and fallback_identity is not None:
            row = self._get_identity_map_row(
                session,
                parser_name=parser_name,
                identity_type="normalized_name",
                identity_value=fallback_identity,
            )
            if row is not None and _safe_str(row.canonical_product_id):
                chosen_id = row.canonical_product_id

        if chosen_id is None:
            chosen_id = str(uuid4())

        identity_values = list(identity_keys)
        if fallback_identity is not None:
            identity_values.append(("normalized_name", fallback_identity))
        identity_values = list(dict.fromkeys(identity_values))

        now = _utc_now()
        for identity_type, identity_value in identity_values:
            row = self._get_identity_map_row(
                session,
                parser_name=parser_name,
                identity_type=identity_type,
                identity_value=identity_value,
            )
            if row is None:
                row = _CatalogIdentityMap(
                    parser_name=parser_name,
                    identity_type=identity_type,
                    identity_value=identity_value,
                    canonical_product_id=chosen_id,
                    updated_at=now,
                )
                session.add(row)
            else:
                row.canonical_product_id = chosen_id
                row.updated_at = now

        return chosen_id

    @staticmethod
    def _get_identity_map_row(
        session: Session,
        *,
        parser_name: str,
        identity_type: str,
        identity_value: str,
    ) -> _CatalogIdentityMap | None:
        for pending in session.new:
            if not isinstance(pending, _CatalogIdentityMap):
                continue
            if (
                pending.parser_name == parser_name
                and pending.identity_type == identity_type
                and pending.identity_value == identity_value
            ):
                return pending
        return session.get(_CatalogIdentityMap, (parser_name, identity_type, identity_value))

    @staticmethod
    def _fallback_identity_value(record: NormalizedProductRecord) -> str | None:
        fallback = _safe_str(record.title_normalized_no_stopwords)
        if fallback:
            return fallback
        return _safe_str(record.title_normalized)

    def _apply_persistent_image_dedup(self, session: Session, record: NormalizedProductRecord) -> None:
        unique_urls: list[str] = []
        duplicate_urls: list[str] = []
        fingerprints: list[str] = []

        seen_in_record: set[str] = set()
        now = _utc_now()

        for raw_url in record.image_urls:
            url = raw_url.strip()
            if not url:
                continue

            fingerprint = hashlib.sha256(url.encode("utf-8")).hexdigest()
            row = session.get(_CatalogImageFingerprint, fingerprint)

            if row is None:
                canonical_url = url
                row = _CatalogImageFingerprint(
                    fingerprint=fingerprint,
                    canonical_url=canonical_url,
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
            else:
                canonical_url = row.canonical_url
                row.updated_at = now
                if canonical_url != url:
                    duplicate_urls.append(url)

            if fingerprint in seen_in_record:
                duplicate_urls.append(url)
                continue

            seen_in_record.add(fingerprint)
            unique_urls.append(canonical_url)
            fingerprints.append(fingerprint)

        record.image_urls = unique_urls
        record.duplicate_image_urls = duplicate_urls
        record.image_fingerprints = fingerprints

    def _apply_backfill(self, session: Session, record: NormalizedProductRecord) -> None:
        canonical_product_id = _safe_str(record.canonical_product_id)
        if canonical_product_id is None:
            return

        history = session.scalars(
            select(_CatalogProductSnapshot).where(_CatalogProductSnapshot.canonical_product_id == canonical_product_id)
        ).all()
        if not history:
            history = session.scalars(
                select(_CatalogProduct).where(_CatalogProduct.canonical_product_id == canonical_product_id)
            ).all()
        if not history:
            return

        target_time = self._to_utc(record.observed_at)

        for field_name in self.BACKFILL_FIELDS:
            current_value = getattr(record, field_name)
            if not _is_missing(current_value):
                continue

            replacement = self._closest_non_missing(history, field_name, target_time)
            if replacement is not None:
                setattr(record, field_name, replacement)

    @staticmethod
    def _closest_non_missing(
        history: list[Any],
        field_name: str,
        target_time: datetime,
    ) -> object | None:
        nearest_value: object | None = None
        nearest_delta: float | None = None

        for item in history:
            value = getattr(item, field_name, None)
            if _is_missing(value):
                continue

            observed = CatalogRepository._to_utc(getattr(item, "observed_at"))
            delta = abs((observed - target_time).total_seconds())
            if nearest_delta is None or delta < nearest_delta:
                nearest_delta = delta
                nearest_value = value

        return nearest_value

    def _insert_product_snapshot(
        self,
        session: Session,
        record: NormalizedProductRecord,
        *,
        settlement: _CatalogSettlement | None,
    ) -> _CatalogProductSnapshot:
        now = _utc_now()
        payload = dict(record.raw_payload) if isinstance(record.raw_payload, dict) else {}

        snapshot = _CatalogProductSnapshot(
            canonical_product_id=record.canonical_product_id or str(uuid4()),
            parser_name=record.parser_name,
            source_id=self._source_id(record),
            source_run_id=_safe_str(payload.get("receiver_run_id")),
            receiver_product_id=self._to_int(payload.get("receiver_product_id")),
            receiver_artifact_id=self._to_int(payload.get("receiver_artifact_id")),
            receiver_sort_order=self._to_int(payload.get("receiver_sort_order")),
            raw_title=record.raw_title,
            title_original=record.title_original,
            title_normalized=record.title_normalized,
            title_original_no_stopwords=record.title_original_no_stopwords,
            title_normalized_no_stopwords=record.title_normalized_no_stopwords,
            brand=record.brand,
            unit=record.unit,
            available_count=record.available_count,
            package_quantity=record.package_quantity,
            package_unit=record.package_unit,
            category_raw=record.category_raw,
            category_normalized=record.category_normalized,
            geo_raw=record.geo_raw,
            geo_normalized=record.geo_normalized,
            composition_raw=record.composition_raw,
            composition_normalized=record.composition_normalized,
            settlement_id=settlement.id if settlement is not None else None,
            image_urls_json=list(record.image_urls),
            duplicate_image_urls_json=list(record.duplicate_image_urls),
            image_fingerprints_json=list(record.image_fingerprints),
            observed_at=self._to_utc(record.observed_at),
            raw_payload_json=payload,
            created_at=now,
        )
        session.add(snapshot)
        session.flush([snapshot])
        return snapshot

    def _upsert_product_source(
        self,
        session: Session,
        record: NormalizedProductRecord,
        *,
        snapshot: _CatalogProductSnapshot,
    ) -> None:
        parser_name = record.parser_name.strip().lower()
        source_id = self._source_id(record)
        observed_at = self._to_utc(record.observed_at)
        now = _utc_now()

        row = session.get(_CatalogProductSource, (parser_name, source_id))
        if row is None:
            row = _CatalogProductSource(
                parser_name=parser_name,
                source_id=source_id,
                canonical_product_id=record.canonical_product_id or str(uuid4()),
                latest_snapshot_id=snapshot.id,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                updated_at=now,
            )
            session.add(row)
            return

        if _safe_str(row.canonical_product_id):
            record.canonical_product_id = row.canonical_product_id
        else:
            row.canonical_product_id = record.canonical_product_id or row.canonical_product_id

        row.latest_snapshot_id = snapshot.id
        row.last_seen_at = self._max_datetime(row.last_seen_at, observed_at)
        row.updated_at = now

    def _upsert_settlement(
        self,
        session: Session,
        record: NormalizedProductRecord,
    ) -> _CatalogSettlement | None:
        geo = self._extract_geo_components(record)
        if geo is None:
            return None

        key = self._geo_key(geo)
        if key is None:
            return None

        observed_at = self._to_utc(record.observed_at)
        now = _utc_now()
        row = self._get_settlement_row(session, key)

        if row is None:
            row = _CatalogSettlement(
                geo_key=key,
                country_raw=geo.get("country_raw"),
                country_normalized=geo.get("country_normalized"),
                region_raw=geo.get("region_raw"),
                region_normalized=geo.get("region_normalized"),
                name_raw=geo.get("name_raw"),
                name_normalized=geo.get("name_normalized"),
                settlement_type=geo.get("settlement_type"),
                alias=geo.get("alias"),
                latitude=geo.get("latitude"),
                longitude=geo.get("longitude"),
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                updated_at=now,
            )
            session.add(row)
            session.flush([row])
            return row

        row.last_seen_at = self._max_datetime(row.last_seen_at, observed_at)
        row.updated_at = now

        self._fill_missing(row, "country_raw", geo.get("country_raw"))
        self._fill_missing(row, "country_normalized", geo.get("country_normalized"))
        self._fill_missing(row, "region_raw", geo.get("region_raw"))
        self._fill_missing(row, "region_normalized", geo.get("region_normalized"))
        self._fill_missing(row, "name_raw", geo.get("name_raw"))
        self._fill_missing(row, "name_normalized", geo.get("name_normalized"))
        self._fill_missing(row, "settlement_type", geo.get("settlement_type"))
        self._fill_missing(row, "alias", geo.get("alias"))
        self._fill_missing(row, "latitude", geo.get("latitude"))
        self._fill_missing(row, "longitude", geo.get("longitude"))

        return row

    def _append_settlement_geodata(
        self,
        session: Session,
        *,
        settlement: _CatalogSettlement | None,
        record: NormalizedProductRecord,
    ) -> None:
        if settlement is None or settlement.id is None:
            return

        payload = dict(record.raw_payload) if isinstance(record.raw_payload, dict) else {}
        latitude = _as_float(payload.get("receiver_geo_latitude"))
        longitude = _as_float(payload.get("receiver_geo_longitude"))
        if latitude is None or longitude is None:
            return

        fingerprint = hashlib.sha256(f"{settlement.id}:{latitude:.8f}:{longitude:.8f}".encode("utf-8")).hexdigest()
        for pending in session.new:
            if not isinstance(pending, _CatalogSettlementGeodata):
                continue
            if pending.geo_fingerprint == fingerprint:
                return

        existing = session.scalar(
            select(_CatalogSettlementGeodata.id).where(_CatalogSettlementGeodata.geo_fingerprint == fingerprint)
        )
        if existing is not None:
            return

        row = _CatalogSettlementGeodata(
            geo_fingerprint=fingerprint,
            settlement_id=settlement.id,
            latitude=latitude,
            longitude=longitude,
            observed_at=self._to_utc(record.observed_at),
            source_run_id=_safe_str(payload.get("receiver_run_id")),
            raw_payload_json={
                "receiver_run_id": _safe_str(payload.get("receiver_run_id")),
                "receiver_artifact_id": self._to_int(payload.get("receiver_artifact_id")),
                "receiver_product_id": self._to_int(payload.get("receiver_product_id")),
            },
            created_at=_utc_now(),
        )
        session.add(row)

    def _upsert_categories(
        self,
        session: Session,
        record: NormalizedProductRecord,
    ) -> list[tuple[_CatalogCategory, int]]:
        candidates = self._extract_category_candidates(record)
        if not candidates:
            return []

        parser_name = record.parser_name.strip().lower()
        observed_at = self._to_utc(record.observed_at)
        now = _utc_now()

        out: list[tuple[_CatalogCategory, int]] = []
        for idx, item in enumerate(candidates):
            source_uid = _safe_str(item.get("uid"))
            title_raw = _safe_str(item.get("title"))
            title_normalized = self._normalize_text(title_raw)

            category_key = self._category_key(
                parser_name=parser_name,
                source_uid=source_uid,
                title_normalized=title_normalized,
            )
            if category_key is None:
                continue

            row = self._get_category_row(session, category_key)
            if row is None:
                row = _CatalogCategory(
                    category_key=category_key,
                    parser_name=parser_name,
                    source_uid=source_uid,
                    parent_source_uid=_safe_str(item.get("parent_uid")),
                    title_raw=title_raw,
                    title_normalized=title_normalized,
                    alias=_safe_str(item.get("alias")),
                    depth=self._to_int(item.get("depth")),
                    sort_order=self._to_int(item.get("sort_order")),
                    first_seen_at=observed_at,
                    last_seen_at=observed_at,
                    updated_at=now,
                )
                session.add(row)
            else:
                row.last_seen_at = self._max_datetime(row.last_seen_at, observed_at)
                row.updated_at = now
                self._fill_missing(row, "source_uid", source_uid)
                self._fill_missing(row, "parent_source_uid", _safe_str(item.get("parent_uid")))
                self._fill_missing(row, "title_raw", title_raw)
                self._fill_missing(row, "title_normalized", title_normalized)
                self._fill_missing(row, "alias", _safe_str(item.get("alias")))
                self._fill_missing(row, "depth", self._to_int(item.get("depth")))
                self._fill_missing(row, "sort_order", self._to_int(item.get("sort_order")))

            sort_order = self._to_int(item.get("sort_order"))
            out.append((row, sort_order if sort_order is not None else idx))

        return out

    def _link_snapshot_categories(
        self,
        session: Session,
        *,
        snapshot: _CatalogProductSnapshot,
        categories: list[tuple[_CatalogCategory, int]],
    ) -> None:
        if snapshot.id is None or not categories:
            return

        session.flush()

        seen: set[int] = set()
        now = _utc_now()
        for idx, (category, sort_order) in enumerate(categories):
            if category.id is None or category.id in seen:
                continue
            seen.add(category.id)

            key = (int(snapshot.id), int(category.id))
            link = session.get(_CatalogProductCategoryLink, key)
            if link is None:
                link = _CatalogProductCategoryLink(
                    snapshot_id=key[0],
                    category_id=key[1],
                    sort_order=sort_order,
                    is_primary=(idx == 0),
                    created_at=now,
                )
                session.add(link)
            else:
                if link.sort_order is None:
                    link.sort_order = sort_order
                if idx == 0:
                    link.is_primary = True

    def _extract_geo_components(self, record: NormalizedProductRecord) -> dict[str, object] | None:
        payload = dict(record.raw_payload) if isinstance(record.raw_payload, dict) else {}

        country_raw = _safe_str(payload.get("receiver_geo_country"))
        region_raw = _safe_str(payload.get("receiver_geo_region"))
        name_raw = _safe_str(payload.get("receiver_geo_name"))
        settlement_type = _safe_str(payload.get("receiver_geo_settlement_type"))
        alias = _safe_str(payload.get("receiver_geo_alias"))
        latitude = _as_float(payload.get("receiver_geo_latitude"))
        longitude = _as_float(payload.get("receiver_geo_longitude"))

        if name_raw is None and record.geo_raw:
            parts = [segment.strip() for segment in str(record.geo_raw).split(",") if segment.strip()]
            if len(parts) >= 1 and country_raw is None:
                country_raw = parts[0]
            if len(parts) >= 2 and region_raw is None:
                region_raw = parts[1]
            if len(parts) >= 3 and name_raw is None:
                name_raw = parts[2]

        country_normalized = self._normalize_text(country_raw)
        region_normalized = self._normalize_text(region_raw)
        name_normalized = self._normalize_text(name_raw)

        if all(token is None for token in (country_normalized, region_normalized, name_normalized)):
            return None

        return {
            "country_raw": country_raw,
            "country_normalized": country_normalized,
            "region_raw": region_raw,
            "region_normalized": region_normalized,
            "name_raw": name_raw,
            "name_normalized": name_normalized,
            "settlement_type": settlement_type,
            "alias": alias,
            "latitude": latitude,
            "longitude": longitude,
        }

    def _extract_category_candidates(self, record: NormalizedProductRecord) -> list[dict[str, object]]:
        payload = dict(record.raw_payload) if isinstance(record.raw_payload, dict) else {}
        raw = payload.get("receiver_categories")

        out: list[dict[str, object]] = []
        if isinstance(raw, list):
            for idx, item in enumerate(raw):
                if isinstance(item, dict):
                    candidate = dict(item)
                else:
                    token = _safe_str(item)
                    if token is None:
                        continue
                    candidate = {"title": token}
                if "sort_order" not in candidate:
                    candidate["sort_order"] = idx
                out.append(candidate)

        if out:
            return out

        category_raw = _safe_str(record.category_raw)
        if category_raw is None:
            return []

        parts = [segment.strip() for segment in category_raw.split("/") if segment.strip()]
        return [{"title": title, "sort_order": idx} for idx, title in enumerate(parts)]

    @staticmethod
    def _get_settlement_row(session: Session, geo_key: str) -> _CatalogSettlement | None:
        for pending in session.new:
            if not isinstance(pending, _CatalogSettlement):
                continue
            if pending.geo_key == geo_key:
                return pending
        return session.scalar(select(_CatalogSettlement).where(_CatalogSettlement.geo_key == geo_key))

    @staticmethod
    def _get_category_row(session: Session, category_key: str) -> _CatalogCategory | None:
        for pending in session.new:
            if not isinstance(pending, _CatalogCategory):
                continue
            if pending.category_key == category_key:
                return pending
        return session.scalar(select(_CatalogCategory).where(_CatalogCategory.category_key == category_key))

    @staticmethod
    def _geo_key(geo: dict[str, object]) -> str | None:
        country = _safe_str(geo.get("country_normalized")) or ""
        region = _safe_str(geo.get("region_normalized")) or ""
        name = _safe_str(geo.get("name_normalized")) or ""
        combined = "|".join((country, region, name)).strip("|")
        return combined or None

    @staticmethod
    def _category_key(
        *,
        parser_name: str,
        source_uid: str | None,
        title_normalized: str | None,
    ) -> str | None:
        if source_uid:
            return f"{parser_name}:uid:{source_uid.lower()}"
        if title_normalized:
            digest = hashlib.sha256(title_normalized.encode("utf-8")).hexdigest()[:40]
            return f"{parser_name}:title:{digest}"
        return None

    def _upsert_product_row(
        self,
        session: Session,
        record: NormalizedProductRecord,
        *,
        settlement: _CatalogSettlement | None,
        categories: list[tuple[_CatalogCategory, int]],
    ) -> None:
        now = _utc_now()
        source_id = self._source_id(record)
        primary_category_id = self._primary_category_id(categories)
        settlement_id = int(settlement.id) if settlement is not None and settlement.id is not None else None

        existing = session.scalar(
            select(_CatalogProduct).where(
                _CatalogProduct.parser_name == record.parser_name,
                _CatalogProduct.source_id == source_id,
            )
        )

        if existing is None:
            existing = _CatalogProduct(
                parser_name=record.parser_name,
                source_id=source_id,
                created_at=now,
                updated_at=now,
                canonical_product_id=record.canonical_product_id or str(uuid4()),
                plu=record.plu,
                sku=record.sku,
                raw_title=record.raw_title,
                title_original=record.title_original,
                title_normalized=record.title_normalized,
                title_original_no_stopwords=record.title_original_no_stopwords,
                title_normalized_no_stopwords=record.title_normalized_no_stopwords,
                brand=record.brand,
                unit=record.unit,
                available_count=record.available_count,
                package_quantity=record.package_quantity,
                package_unit=record.package_unit,
                primary_category_id=primary_category_id,
                settlement_id=settlement_id,
                composition_raw=record.composition_raw,
                composition_normalized=record.composition_normalized,
                image_urls_json=list(record.image_urls),
                duplicate_image_urls_json=list(record.duplicate_image_urls),
                image_fingerprints_json=list(record.image_fingerprints),
                observed_at=self._to_utc(record.observed_at),
                raw_payload_json=dict(record.raw_payload),
            )
            session.add(existing)
            return

        existing.updated_at = now
        self._fill_missing(existing, "canonical_product_id", record.canonical_product_id)

        # keep identity tokens up-to-date only when provided
        if not _is_missing(record.plu):
            existing.plu = record.plu
        if not _is_missing(record.sku):
            existing.sku = record.sku

        # title fields are authoritative per source snapshot
        existing.raw_title = record.raw_title
        existing.title_original = record.title_original
        existing.title_normalized = record.title_normalized
        existing.title_original_no_stopwords = record.title_original_no_stopwords
        existing.title_normalized_no_stopwords = record.title_normalized_no_stopwords

        if not _is_missing(record.brand):
            existing.brand = record.brand
        existing.unit = record.unit

        if not _is_missing(record.available_count):
            existing.available_count = record.available_count
        if not _is_missing(record.package_quantity):
            existing.package_quantity = record.package_quantity
        if not _is_missing(record.package_unit):
            existing.package_unit = record.package_unit

        if primary_category_id is not None:
            existing.primary_category_id = primary_category_id
        if settlement_id is not None:
            existing.settlement_id = settlement_id

        if not _is_missing(record.composition_raw):
            existing.composition_raw = record.composition_raw
        if not _is_missing(record.composition_normalized):
            existing.composition_normalized = record.composition_normalized

        if record.image_urls:
            existing.image_urls_json = list(record.image_urls)
            existing.duplicate_image_urls_json = list(record.duplicate_image_urls)
            existing.image_fingerprints_json = list(record.image_fingerprints)

        existing.observed_at = self._max_datetime(existing.observed_at, self._to_utc(record.observed_at))
        existing.raw_payload_json = self._merge_payload(existing.raw_payload_json, record.raw_payload)

    @staticmethod
    def _primary_category_id(categories: list[tuple[_CatalogCategory, int]]) -> int | None:
        for category, _ in categories:
            if category.id is not None:
                return int(category.id)
        return None

    @staticmethod
    def _fill_missing(target: Any, field_name: str, value: object) -> None:
        if _is_missing(value):
            return
        current = getattr(target, field_name)
        if _is_missing(current):
            setattr(target, field_name, value)

    @staticmethod
    def _merge_payload(existing: object, incoming: object) -> dict[str, Any]:
        base = dict(existing) if isinstance(existing, dict) else {}
        if isinstance(incoming, dict):
            base.update(incoming)
        return base

    @staticmethod
    def _normalize_text(value: str | None) -> str | None:
        token = _safe_str(value)
        if token is None:
            return None
        return " ".join(token.replace("ё", "е").lower().split())

    @staticmethod
    def _source_id(record: NormalizedProductRecord) -> str:
        source_id = _safe_str(record.source_id)
        if source_id is not None:
            return source_id

        sku = _safe_str(record.sku)
        if sku is not None:
            return f"sku:{sku}"

        plu = _safe_str(record.plu)
        if plu is not None:
            return f"plu:{plu}"

        canonical = _safe_str(record.canonical_product_id) or "unknown"
        return f"generated:{canonical}:{CatalogRepository._to_utc(record.observed_at).isoformat()}"

    @staticmethod
    def _cursor_key(parser_name: str) -> str:
        return f"receiver_cursor:{parser_name.strip().lower()}"

    @staticmethod
    def _to_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _max_datetime(left: datetime, right: datetime) -> datetime:
        left_utc = CatalogRepository._to_utc(left)
        right_utc = CatalogRepository._to_utc(right)
        return left_utc if left_utc >= right_utc else right_utc

    @staticmethod
    def _to_int(value: object) -> int | None:
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

    def _validate_catalog_products_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("catalog_products"):
            return

        columns = {item["name"] for item in inspector.get_columns("catalog_products")}
        required = ("primary_category_id", "settlement_id")
        missing = [name for name in required if name not in columns]
        if missing:
            raise RuntimeError(
                "Schema mismatch in `catalog_products`: missing columns "
                f"{', '.join(missing)}. Apply DB migrations before starting converter."
            )


class CatalogSQLiteRepository(CatalogRepository):
    def __init__(self, db_path: str | Path) -> None:
        path = Path(db_path)
        if path.parent and not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        super().__init__(f"sqlite:///{path.resolve()}")
