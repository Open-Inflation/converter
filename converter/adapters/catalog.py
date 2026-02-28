from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, String, Text, create_engine, func, select
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

    category_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    category_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)

    geo_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)

    composition_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    composition_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)

    image_urls_json: Mapped[Any] = mapped_column(JSON, nullable=False)
    duplicate_image_urls_json: Mapped[Any] = mapped_column(JSON, nullable=False)
    image_fingerprints_json: Mapped[Any] = mapped_column(JSON, nullable=False)

    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    raw_payload_json: Mapped[Any] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


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

    def upsert_many(self, records: list[NormalizedProductRecord]) -> None:
        if not records:
            return

        with self._session_factory() as session:
            for record in records:
                canonical_product_id = self._resolve_canonical_product_id(session, record)
                record.canonical_product_id = canonical_product_id

                self._apply_persistent_image_dedup(session, record)
                self._apply_backfill(session, record)
                self._upsert_product_row(session, record)

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
            row = session.get(_CatalogIdentityMap, (parser_name, identity_type, identity_value))
            if row is not None and _safe_str(row.canonical_product_id):
                chosen_id = row.canonical_product_id
                break

        fallback_identity = self._fallback_identity_value(record)
        if chosen_id is None and fallback_identity is not None:
            row = session.get(_CatalogIdentityMap, (parser_name, "normalized_name", fallback_identity))
            if row is not None and _safe_str(row.canonical_product_id):
                chosen_id = row.canonical_product_id

        if chosen_id is None:
            chosen_id = str(uuid4())

        identity_values = list(identity_keys)
        if fallback_identity is not None:
            identity_values.append(("normalized_name", fallback_identity))

        now = _utc_now()
        for identity_type, identity_value in identity_values:
            row = session.get(_CatalogIdentityMap, (parser_name, identity_type, identity_value))
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
        history: list[_CatalogProduct],
        field_name: str,
        target_time: datetime,
    ) -> object | None:
        nearest_value: object | None = None
        nearest_delta: float | None = None

        for item in history:
            value = getattr(item, field_name)
            if _is_missing(value):
                continue

            observed = CatalogRepository._to_utc(item.observed_at)
            delta = abs((observed - target_time).total_seconds())
            if nearest_delta is None or delta < nearest_delta:
                nearest_delta = delta
                nearest_value = value

        return nearest_value

    def _upsert_product_row(self, session: Session, record: NormalizedProductRecord) -> None:
        now = _utc_now()
        source_id = self._source_id(record)

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
                category_raw=record.category_raw,
                category_normalized=record.category_normalized,
                geo_raw=record.geo_raw,
                geo_normalized=record.geo_normalized,
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
        existing.canonical_product_id = record.canonical_product_id or existing.canonical_product_id
        existing.plu = record.plu
        existing.sku = record.sku
        existing.raw_title = record.raw_title
        existing.title_original = record.title_original
        existing.title_normalized = record.title_normalized
        existing.title_original_no_stopwords = record.title_original_no_stopwords
        existing.title_normalized_no_stopwords = record.title_normalized_no_stopwords
        existing.brand = record.brand
        existing.unit = record.unit
        existing.available_count = record.available_count
        existing.package_quantity = record.package_quantity
        existing.package_unit = record.package_unit
        existing.category_raw = record.category_raw
        existing.category_normalized = record.category_normalized
        existing.geo_raw = record.geo_raw
        existing.geo_normalized = record.geo_normalized
        existing.composition_raw = record.composition_raw
        existing.composition_normalized = record.composition_normalized
        existing.image_urls_json = list(record.image_urls)
        existing.duplicate_image_urls_json = list(record.duplicate_image_urls)
        existing.image_fingerprints_json = list(record.image_fingerprints)
        existing.observed_at = self._to_utc(record.observed_at)
        existing.raw_payload_json = dict(record.raw_payload)

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


class CatalogSQLiteRepository(CatalogRepository):
    def __init__(self, db_path: str | Path) -> None:
        path = Path(db_path)
        if path.parent and not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        super().__init__(f"sqlite:///{path.resolve()}")
