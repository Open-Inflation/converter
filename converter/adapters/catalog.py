from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import logging
from time import sleep
from time import monotonic
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from sqlalchemy import (
    create_engine,
    delete,
    insert,
    select,
    text,
    tuple_,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from converter.core.models import ChunkApplyResultV2, NormalizedProductRecord, SyncChunkV2
from converter.core.ports import StorageRepository
from converter.parsers.category_normalization import normalize_category_text
from converter.parsers.normalizers import RussianTextNormalizer
from .catalog_migrations import _CatalogSchemaMigrationMixin
from .catalog_schema import (
    _CatalogBase,
    _CatalogCategory,
    _CatalogIdentityMap,
    _CatalogProduct,
    _CatalogProductGroup,
    _CatalogProductAsset,
    _CatalogProductSnapshot,
    _CatalogProductSource,
    _CatalogSettlement,
    _CatalogStore,
    _CatalogStorageDeleteOutbox,
    _as_float,
    _is_missing,
    _safe_str,
    _utc_now,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _PreparedChunkRecord:
    record: NormalizedProductRecord
    parser_name: str
    source_id: str
    payload: dict[str, Any]
    store_data: dict[str, object] | None
    snapshot_fingerprint: str
    source_event_uid: str


class CatalogRepository(_CatalogSchemaMigrationMixin):
    """
    SQLAlchemy-based persistent sink for normalized catalog products.

    Policy:
    - append-only history to `catalog_product_snapshots`;
    - additive updates for dimensions (settlements/categories);
    - non-destructive merge for `catalog_products` current projection.
    """

    BACKFILL_FIELDS = (
        "brand",
        "brand_normalized",
        "category_normalized",
        "geo_normalized",
        "composition_original",
        "composition_normalized",
        "package_quantity",
        "package_unit",
        "package_weight_gross",
        "package_count",
        "dimension_height_m",
        "dimension_width_m",
        "dimension_depth_m",
    )
    _RETRYABLE_POSTGRESQL_SQLSTATES = {"40P01", "40001"}
    _TXN_RETRY_ATTEMPTS = 5
    _TXN_RETRY_BASE_DELAY_SEC = 0.2
    _TXN_RETRY_MAX_DELAY_SEC = 2.0
    _PRODUCT_GROUP_SOURCE = "converter"

    def __init__(
        self,
        database_url: str,
        *,
        engine: Engine | None = None,
        storage_repository: StorageRepository | None = None,
        validate_schema: bool = True,
    ) -> None:
        self._database_url = database_url
        self._engine = engine or self._create_engine(database_url)
        self._session_factory = sessionmaker(
            bind=self._engine,
            class_=Session,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        self._storage_repository: StorageRepository | None = (
            storage_repository or self._build_storage_repository_from_env()
        )
        self._category_text_normalizer = RussianTextNormalizer()
        _CatalogBase.metadata.create_all(self._engine)
        if validate_schema:
            self._validate_catalog_products_schema()
        LOGGER.info(
            "Catalog repository initialized: dialect=%s storage_delete_enabled=%s",
            self._engine.dialect.name,
            self._storage_repository is not None,
        )

    def upsert_many(self, records: list[NormalizedProductRecord]) -> None:
        if not records:
            LOGGER.debug("Catalog upsert_many skipped: empty records")
            return

        started_at = monotonic()
        LOGGER.debug("Catalog upsert_many started: records=%s", len(records))
        self._run_write_transaction(
            lambda session: self._upsert_many_in_session(session, records),
            operation_name="upsert_many",
        )
        LOGGER.debug(
            "Catalog upsert_many finished: records=%s elapsed_sec=%.3f",
            len(records),
            monotonic() - started_at,
        )

    def apply_chunk(self, chunk: SyncChunkV2) -> ChunkApplyResultV2:
        started_at = monotonic()
        counters = {
            "inserted_snapshots": 0,
            "reused_snapshots": 0,
            "upserted_products": 0,
        }
        LOGGER.debug(
            "Catalog apply_chunk started: parser=%s chunk_id=%s records=%s",
            chunk.parser_name,
            chunk.chunk_id,
            len(chunk.records),
        )

        def _work(session: Session) -> None:
            prepared_records = self._prepare_chunk_records(session, chunk.records)
            self._prime_apply_chunk_caches(session, prepared_records)
            self._enable_asset_batch_mode(session)
            try:
                for prepared in prepared_records:
                    record = prepared.record
                    counters["upserted_products"] += 1

                    settlement = self._upsert_settlement(session, record, payload=prepared.payload)
                    store = self._upsert_store(
                        session,
                        record,
                        payload=prepared.payload,
                        store_data=prepared.store_data,
                        settlement=settlement,
                    )
                    touched_snapshot = self._touch_latest_snapshot_if_unchanged(
                        session,
                        record,
                        snapshot_fingerprint=prepared.snapshot_fingerprint,
                        store=store,
                    )
                    categories = self._upsert_categories(session, record, payload=prepared.payload)

                    if touched_snapshot:
                        counters["reused_snapshots"] += 1
                        self._update_source_fingerprint_in_session(
                            session,
                            record=record,
                            snapshot_fingerprint=prepared.snapshot_fingerprint,
                        )
                    else:
                        snapshot, inserted = self._insert_product_snapshot(
                            session,
                            record,
                            payload=prepared.payload,
                            store=store,
                            snapshot_fingerprint=prepared.snapshot_fingerprint,
                            source_event_uid=prepared.source_event_uid,
                        )
                        if inserted:
                            counters["inserted_snapshots"] += 1
                        self._upsert_product_source(
                            session,
                            record,
                            snapshot=snapshot,
                            snapshot_fingerprint=prepared.snapshot_fingerprint,
                        )

                    self._upsert_product_row(
                        session,
                        record,
                        settlement=settlement,
                        categories=categories,
                    )
                self._flush_buffered_product_assets(session)
            finally:
                self._disable_asset_batch_mode(session)

        self._run_write_transaction(
            _work,
            operation_name="apply_chunk",
        )
        elapsed_ms = int((monotonic() - started_at) * 1000)
        result = ChunkApplyResultV2(
            inserted_snapshots=int(counters["inserted_snapshots"]),
            reused_snapshots=int(counters["reused_snapshots"]),
            upserted_products=int(counters["upserted_products"]),
            elapsed_ms=elapsed_ms,
        )
        LOGGER.info(
            "Catalog apply_chunk finished: parser=%s chunk_id=%s inserted_snapshots=%s reused_snapshots=%s upserted_products=%s elapsed_ms=%s",
            chunk.parser_name,
            chunk.chunk_id,
            result.inserted_snapshots,
            result.reused_snapshots,
            result.upserted_products,
            result.elapsed_ms,
        )
        return result

    def _prepare_chunk_records(
        self,
        session: Session,
        records: list[NormalizedProductRecord],
    ) -> list[_PreparedChunkRecord]:
        self._prime_identity_map_cache(session, records)
        prepared: list[_PreparedChunkRecord] = []
        for record in records:
            canonical_product_id = self._resolve_canonical_product_id(session, record)
            record.canonical_product_id = canonical_product_id

            self._apply_persistent_image_dedup(session, record)
            payload = self._source_payload(record)
            store_data = self._extract_store_components(record, payload=payload)
            prepared.append(
                _PreparedChunkRecord(
                    record=record,
                    parser_name=record.parser_name.strip().lower(),
                    source_id=self._source_id(record),
                    payload=payload,
                    store_data=store_data,
                    snapshot_fingerprint=self._snapshot_content_fingerprint(
                        record,
                        payload=payload,
                        store_key=_safe_str(store_data.get("store_key")) if isinstance(store_data, dict) else None,
                    ),
                    source_event_uid=self._source_event_uid(
                        record,
                        payload=payload,
                        store_key=_safe_str(store_data.get("store_key")) if isinstance(store_data, dict) else None,
                    ),
                )
            )
        return prepared

    def _prime_identity_map_cache(
        self,
        session: Session,
        records: list[NormalizedProductRecord],
    ) -> None:
        identity_keys: set[tuple[str, str, str]] = set()
        for record in records:
            parser_name = record.parser_name.strip().lower()
            for identity_type, identity_value in record.identity_candidates():
                token = _safe_str(identity_value)
                if token is None:
                    continue
                identity_keys.add((parser_name, identity_type, token))
            if not self._has_strong_identity(record):
                fallback_identity = self._fallback_identity_value(record)
                if fallback_identity is not None:
                    identity_keys.add((parser_name, "normalized_name", fallback_identity))

        key_list = sorted(identity_keys)
        cache: dict[tuple[str, str, str], _CatalogIdentityMap | None] = {
            key: None for key in key_list
        }
        if key_list:
            rows = session.scalars(
                select(_CatalogIdentityMap).where(
                    tuple_(
                        _CatalogIdentityMap.parser_name,
                        _CatalogIdentityMap.identity_type,
                        _CatalogIdentityMap.identity_value,
                    ).in_(key_list)
                )
            ).all()
            for row in rows:
                cache[(row.parser_name, row.identity_type, row.identity_value)] = row
        session.info["_catalog_identity_cache"] = cache

    def _prime_apply_chunk_caches(
        self,
        session: Session,
        prepared_records: list[_PreparedChunkRecord],
    ) -> None:
        source_keys = sorted({(item.parser_name, item.source_id) for item in prepared_records})
        source_cache: dict[tuple[str, str], _CatalogProductSource | None] = {
            key: None for key in source_keys
        }
        if source_keys:
            rows = session.scalars(
                select(_CatalogProductSource).where(
                    tuple_(_CatalogProductSource.parser_name, _CatalogProductSource.source_id).in_(source_keys)
                )
            ).all()
            for row in rows:
                source_cache[(row.parser_name, row.source_id)] = row
        session.info["_catalog_product_source_cache"] = source_cache

        product_cache: dict[tuple[str, str], _CatalogProduct | None] = {
            key: None for key in source_keys
        }
        if source_keys:
            product_rows = session.scalars(
                select(_CatalogProduct).where(
                    tuple_(_CatalogProduct.parser_name, _CatalogProduct.source_id).in_(source_keys)
                )
            ).all()
            for row in product_rows:
                product_cache[(row.parser_name.strip().lower(), row.source_id)] = row
        session.info["_catalog_product_cache"] = product_cache

        latest_snapshot_ids = sorted(
            {
                int(source.latest_snapshot_id)
                for source in source_cache.values()
                if isinstance(source, _CatalogProductSource) and source.latest_snapshot_id is not None
            }
        )
        snapshot_id_cache: dict[int, _CatalogProductSnapshot | None] = {
            snapshot_id: None for snapshot_id in latest_snapshot_ids
        }
        if latest_snapshot_ids:
            snapshot_rows = session.scalars(
                select(_CatalogProductSnapshot).where(_CatalogProductSnapshot.id.in_(latest_snapshot_ids))
            ).all()
            for row in snapshot_rows:
                snapshot_id_cache[int(row.id)] = row
        session.info["_catalog_snapshot_id_cache"] = snapshot_id_cache

        event_uids = sorted(
            {item.source_event_uid for item in prepared_records if _safe_str(item.source_event_uid) is not None}
        )
        snapshot_event_cache: dict[str, _CatalogProductSnapshot | None] = {
            event_uid: None for event_uid in event_uids
        }
        if event_uids:
            event_rows = session.scalars(
                select(_CatalogProductSnapshot).where(_CatalogProductSnapshot.source_event_uid.in_(event_uids))
            ).all()
            for row in event_rows:
                event_uid = _safe_str(row.source_event_uid)
                if event_uid is not None:
                    snapshot_event_cache[event_uid] = row
        session.info["_catalog_snapshot_event_cache"] = snapshot_event_cache

        store_keys = sorted(
            {
                store_key
                for item in prepared_records
                for store_key in [
                    _safe_str(item.store_data.get("store_key"))
                    if isinstance(item.store_data, dict)
                    else None
                ]
                if store_key is not None
            }
        )
        store_cache: dict[str, _CatalogStore | None] = {key: None for key in store_keys}
        if store_keys:
            store_rows = session.scalars(
                select(_CatalogStore).where(_CatalogStore.store_key.in_(store_keys))
            ).all()
            for row in store_rows:
                store_cache[row.store_key] = row
        session.info["_catalog_store_cache"] = store_cache

        settlement_keys_set: set[str] = set()
        settlement_match_keys_set: set[tuple[str, str]] = set()
        for item in prepared_records:
            geo = self._extract_geo_components(item.record, payload=item.payload)
            if geo is None:
                continue
            key = self._geo_key(geo)
            if key is not None:
                settlement_keys_set.add(key)
            match_key = self._settlement_match_key(geo)
            if match_key is not None:
                settlement_match_keys_set.add(match_key)
        settlement_keys = sorted(settlement_keys_set)
        settlement_cache: dict[str, _CatalogSettlement | None] = {key: None for key in settlement_keys}
        if settlement_keys:
            settlement_rows = session.scalars(
                select(_CatalogSettlement).where(_CatalogSettlement.geo_key.in_(settlement_keys))
            ).all()
            for row in settlement_rows:
                settlement_cache[row.geo_key] = row
        session.info["_catalog_settlement_cache"] = settlement_cache
        settlement_match_keys = sorted(settlement_match_keys_set)
        settlement_candidate_cache: dict[tuple[str, str], list[_CatalogSettlement]] = {
            key: [] for key in settlement_match_keys
        }
        if settlement_match_keys:
            settlement_candidate_rows = session.scalars(
                select(_CatalogSettlement).where(
                    tuple_(
                        _CatalogSettlement.name_normalized,
                        _CatalogSettlement.settlement_type,
                    ).in_(settlement_match_keys)
                )
            ).all()
            for row in settlement_candidate_rows:
                match_key = self._settlement_match_key_from_row(row)
                if match_key is None:
                    continue
                settlement_candidate_cache.setdefault(match_key, []).append(row)
        session.info["_catalog_settlement_candidate_cache"] = settlement_candidate_cache

        category_keys: set[str] = set()
        for item in prepared_records:
            for candidate in self._extract_category_candidates(item.record, payload=item.payload):
                source_uid = _safe_str(candidate.get("uid"))
                title = _safe_str(candidate.get("title"))
                title_normalized = self._normalize_category_title(title)
                key = self._category_key(
                    parser_name=item.parser_name,
                    source_uid=source_uid,
                    title_normalized=title_normalized,
                )
                if key is not None:
                    category_keys.add(key)
        category_key_list = sorted(category_keys)
        category_cache: dict[str, _CatalogCategory | None] = {key: None for key in category_key_list}
        if category_key_list:
            category_rows = session.scalars(
                select(_CatalogCategory).where(_CatalogCategory.category_key.in_(category_key_list))
            ).all()
            for row in category_rows:
                category_cache[row.category_key] = row
        session.info["_catalog_category_cache"] = category_cache

    @staticmethod
    def _enable_asset_batch_mode(session: Session) -> None:
        session.info["_catalog_asset_batch_mode"] = True
        session.info["_catalog_asset_replace_buffer"] = {}

    @staticmethod
    def _disable_asset_batch_mode(session: Session) -> None:
        session.info.pop("_catalog_asset_batch_mode", None)
        session.info.pop("_catalog_asset_replace_buffer", None)

    @staticmethod
    def _asset_batch_mode_enabled(session: Session) -> bool:
        return bool(session.info.get("_catalog_asset_batch_mode"))

    def _flush_buffered_product_assets(self, session: Session) -> None:
        buffered = session.info.get("_catalog_asset_replace_buffer")
        if not isinstance(buffered, dict) or not buffered:
            return

        product_ids = sorted({int(product_id) for product_id in buffered.keys()})
        if product_ids:
            session.execute(
                delete(_CatalogProductAsset).where(_CatalogProductAsset.product_id.in_(product_ids))
            )

        rows: list[dict[str, Any]] = []
        for bucket in buffered.values():
            if not isinstance(bucket, list):
                continue
            rows.extend(bucket)
        if rows:
            session.execute(insert(_CatalogProductAsset), rows)

        buffered.clear()

    @staticmethod
    def _get_cached_product_source(
        session: Session,
        *,
        parser_name: str,
        source_id: str,
    ) -> _CatalogProductSource | None:
        key = (parser_name, source_id)
        cached = session.info.get("_catalog_product_source_cache")
        if isinstance(cached, dict) and key in cached:
            value = cached[key]
            return value if isinstance(value, _CatalogProductSource) else None
        row = session.get(_CatalogProductSource, key)
        if isinstance(cached, dict):
            cached[key] = row
        return row

    @staticmethod
    def _cache_product_source(
        session: Session,
        *,
        parser_name: str,
        source_id: str,
        row: _CatalogProductSource | None,
    ) -> None:
        cached = session.info.get("_catalog_product_source_cache")
        if isinstance(cached, dict):
            cached[(parser_name, source_id)] = row

    @staticmethod
    def _get_cached_product_row(
        session: Session,
        *,
        parser_name: str,
        source_id: str,
    ) -> _CatalogProduct | None:
        key = (parser_name, source_id)
        cached = session.info.get("_catalog_product_cache")
        if isinstance(cached, dict) and key in cached:
            value = cached[key]
            return value if isinstance(value, _CatalogProduct) else None
        row = session.scalar(
            select(_CatalogProduct).where(
                _CatalogProduct.parser_name == parser_name,
                _CatalogProduct.source_id == source_id,
            )
        )
        if isinstance(cached, dict):
            cached[key] = row
        return row

    @staticmethod
    def _cache_product_row(
        session: Session,
        *,
        parser_name: str,
        source_id: str,
        row: _CatalogProduct | None,
    ) -> None:
        cached = session.info.get("_catalog_product_cache")
        if isinstance(cached, dict):
            cached[(parser_name, source_id)] = row

    @staticmethod
    def _get_cached_product_group_row(
        session: Session,
        *,
        group_uid: str,
        product_id: int,
        source: str,
    ) -> _CatalogProductGroup | None:
        key = (group_uid, product_id, source)
        cached = session.info.setdefault("_catalog_product_group_cache", {})
        if isinstance(cached, dict) and key in cached:
            value = cached[key]
            return value if isinstance(value, _CatalogProductGroup) else None
        row = session.get(_CatalogProductGroup, key)
        if isinstance(cached, dict):
            cached[key] = row
        return row

    @staticmethod
    def _cache_product_group_row(
        session: Session,
        *,
        group_uid: str,
        product_id: int,
        source: str,
        row: _CatalogProductGroup | None,
    ) -> None:
        cached = session.info.setdefault("_catalog_product_group_cache", {})
        if isinstance(cached, dict):
            cached[(group_uid, product_id, source)] = row

    @staticmethod
    def _get_cached_snapshot_by_id(
        session: Session,
        snapshot_id: int,
    ) -> _CatalogProductSnapshot | None:
        cached = session.info.get("_catalog_snapshot_id_cache")
        if isinstance(cached, dict) and snapshot_id in cached:
            value = cached[snapshot_id]
            return value if isinstance(value, _CatalogProductSnapshot) else None
        row = session.get(_CatalogProductSnapshot, snapshot_id)
        if isinstance(cached, dict):
            cached[snapshot_id] = row
        return row

    @staticmethod
    def _cache_snapshot_by_id(
        session: Session,
        snapshot_id: int,
        row: _CatalogProductSnapshot | None,
    ) -> None:
        cached = session.info.get("_catalog_snapshot_id_cache")
        if isinstance(cached, dict):
            cached[snapshot_id] = row

    @staticmethod
    def _get_cached_snapshot_by_event_uid(
        session: Session,
        *,
        event_uid: str,
    ) -> _CatalogProductSnapshot | None:
        cached = session.info.get("_catalog_snapshot_event_cache")
        if isinstance(cached, dict) and event_uid in cached:
            value = cached[event_uid]
            return value if isinstance(value, _CatalogProductSnapshot) else None
        row = session.scalar(
            select(_CatalogProductSnapshot).where(_CatalogProductSnapshot.source_event_uid == event_uid)
        )
        if isinstance(cached, dict):
            cached[event_uid] = row
        return row

    @staticmethod
    def _cache_snapshot_by_event_uid(
        session: Session,
        *,
        event_uid: str,
        row: _CatalogProductSnapshot | None,
    ) -> None:
        cached = session.info.get("_catalog_snapshot_event_cache")
        if isinstance(cached, dict):
            cached[event_uid] = row

    @staticmethod
    def _get_cached_store_row(
        session: Session,
        *,
        store_key: str,
    ) -> _CatalogStore | None:
        cached = session.info.get("_catalog_store_cache")
        if isinstance(cached, dict) and store_key in cached:
            value = cached[store_key]
            return value if isinstance(value, _CatalogStore) else None

        row = CatalogRepository._get_store_row(session, store_key)
        if isinstance(cached, dict):
            cached[store_key] = row
        return row

    @staticmethod
    def _cache_store_row(
        session: Session,
        *,
        store_key: str,
        row: _CatalogStore | None,
    ) -> None:
        cached = session.info.get("_catalog_store_cache")
        if isinstance(cached, dict):
            cached[store_key] = row

    def _run_write_transaction(
        self,
        work: Callable[[Session], None],
        *,
        operation_name: str,
    ) -> None:
        for attempt in range(1, self._TXN_RETRY_ATTEMPTS + 1):
            try:
                with self._session_factory() as session:
                    work(session)
                    session.commit()
                return
            except (OperationalError, IntegrityError) as exc:
                code = self._extract_db_error_code(exc)
                is_retryable = (
                    isinstance(code, str)
                    and code.upper() in self._RETRYABLE_POSTGRESQL_SQLSTATES
                    and isinstance(exc, (OperationalError, IntegrityError))
                )
                if (not is_retryable) or (attempt >= self._TXN_RETRY_ATTEMPTS):
                    raise

                delay = min(
                    self._TXN_RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)),
                    self._TXN_RETRY_MAX_DELAY_SEC,
                )
                LOGGER.warning(
                    "Transient DB error on %s (error_type=%s, db_code=%s, attempt=%s/%s). Retrying in %.2fs.",
                    operation_name,
                    type(exc).__name__,
                    code,
                    attempt,
                    self._TXN_RETRY_ATTEMPTS,
                    delay,
                )
                sleep(delay)

    @staticmethod
    def _extract_db_error_code(exc: Exception) -> str | int | None:
        original = getattr(exc, "orig", None)
        pg_code = getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)
        if isinstance(pg_code, str) and pg_code.strip():
            return pg_code.strip().upper()
        args = getattr(original, "args", None)
        if args is None:
            args = getattr(exc, "args", None)
        if not isinstance(args, tuple) or not args:
            return None
        code = args[0]
        if isinstance(code, int):
            return code
        if isinstance(code, str):
            token = code.strip()
            if token.isdigit():
                return int(token)
            if token:
                return token.upper()
        return None

    def _upsert_many_in_session(
        self,
        session: Session,
        records: list[NormalizedProductRecord],
    ) -> None:
        LOGGER.debug("Catalog session upsert started: records=%s", len(records))
        for record in records:
            canonical_product_id = self._resolve_canonical_product_id(session, record)
            record.canonical_product_id = canonical_product_id

            self._apply_persistent_image_dedup(session, record)
            self._apply_backfill(session, record)

            payload = self._source_payload(record)
            store_data = self._extract_store_components(record, payload=payload)
            store_key = _safe_str(store_data.get("store_key")) if isinstance(store_data, dict) else None
            settlement = self._upsert_settlement(session, record, payload=payload)
            store = self._upsert_store(
                session,
                record,
                payload=payload,
                store_data=store_data,
                settlement=settlement,
            )
            snapshot_fingerprint = self._snapshot_content_fingerprint(
                record,
                payload=payload,
                store_key=store_key,
            )
            source_event_uid = self._source_event_uid(
                record,
                payload=payload,
                store_key=store_key,
            )

            touched_snapshot = self._touch_latest_snapshot_if_unchanged(
                session,
                record,
                snapshot_fingerprint=snapshot_fingerprint,
                store=store,
            )

            categories = self._upsert_categories(session, record, payload=payload)
            if touched_snapshot:
                self._update_source_fingerprint_in_session(
                    session,
                    record=record,
                    snapshot_fingerprint=snapshot_fingerprint,
                )
            else:
                snapshot, _ = self._insert_product_snapshot(
                    session,
                    record,
                    payload=payload,
                    store=store,
                    snapshot_fingerprint=snapshot_fingerprint,
                    source_event_uid=source_event_uid,
                )
                self._upsert_product_source(
                    session,
                    record,
                    snapshot=snapshot,
                    snapshot_fingerprint=snapshot_fingerprint,
                )
            self._upsert_product_row(
                session,
                record,
                settlement=settlement,
                categories=categories,
            )
        LOGGER.debug("Catalog session upsert completed: records=%s", len(records))

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
        allow_normalized_fallback = not self._has_strong_identity(record)

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
        if chosen_id is None and allow_normalized_fallback and fallback_identity is not None:
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
        if allow_normalized_fallback and fallback_identity is not None:
            identity_values.append(("normalized_name", fallback_identity))
        identity_values = list(dict.fromkeys(identity_values))

        now = _utc_now()
        for identity_type, identity_value in identity_values:
            row = self._ensure_identity_map_row(
                session,
                parser_name=parser_name,
                identity_type=identity_type,
                identity_value=identity_value,
                canonical_product_id=chosen_id,
                updated_at=now,
            )
            current_canonical = _safe_str(row.canonical_product_id)
            if current_canonical is not None:
                chosen_id = current_canonical

        for identity_type, identity_value in identity_values:
            row = self._get_identity_map_row(
                session,
                parser_name=parser_name,
                identity_type=identity_type,
                identity_value=identity_value,
            )
            if row is not None:
                row.canonical_product_id = chosen_id
                row.updated_at = now

        return chosen_id

    def _snapshot_content_fingerprint(
        self,
        record: NormalizedProductRecord,
        *,
        payload: dict[str, Any],
        store_key: str | None = None,
    ) -> str:
        source_id = self._source_id(record)
        resolved_store_key = store_key or self._store_key(record, payload=payload)
        fingerprint_input = {
            "parser_name": record.parser_name.strip().lower(),
            "source_id": source_id,
            "store_key": resolved_store_key,
            "price": record.price,
            "discount_price": record.discount_price,
            "loyal_price": record.loyal_price,
            "price_unit": record.price_unit,
            "available_count": record.available_count,
        }
        encoded = json.dumps(
            self._to_json_safe(fingerprint_input),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _source_event_uid(
        self,
        record: NormalizedProductRecord,
        *,
        payload: dict[str, Any],
        store_key: str | None = None,
    ) -> str:
        receiver_product_id = self._to_int(payload.get("receiver_product_id")) or 0
        receiver_artifact_id = self._to_int(payload.get("receiver_artifact_id")) or 0
        resolved_store_key = store_key or self._store_key(record, payload=payload) or ""
        seed = "|".join(
            (
                record.parser_name.strip().lower(),
                self._source_id(record),
                resolved_store_key,
                self._to_utc(record.observed_at).isoformat(),
                str(receiver_product_id),
                str(receiver_artifact_id),
            )
        )
        return hashlib.sha256(seed.encode("utf-8")).hexdigest()

    def _update_source_fingerprint_in_session(
        self,
        session: Session,
        *,
        record: NormalizedProductRecord,
        snapshot_fingerprint: str,
    ) -> None:
        parser_name = record.parser_name.strip().lower()
        source_id = self._source_id(record)
        row = self._get_cached_product_source(
            session,
            parser_name=parser_name,
            source_id=source_id,
        )
        if row is None:
            return
        if _is_missing(row.latest_content_fingerprint):
            row.latest_content_fingerprint = snapshot_fingerprint

    def _touch_latest_snapshot_if_unchanged(
        self,
        session: Session,
        record: NormalizedProductRecord,
        *,
        snapshot_fingerprint: str,
        store: _CatalogStore | None = None,
    ) -> bool:
        parser_name = record.parser_name.strip().lower()
        source_id = self._source_id(record)
        source = self._get_cached_product_source(
            session,
            parser_name=parser_name,
            source_id=source_id,
        )
        if source is None or source.latest_snapshot_id is None:
            return False

        latest_source_fingerprint = _safe_str(source.latest_content_fingerprint)
        if latest_source_fingerprint is not None and latest_source_fingerprint != snapshot_fingerprint:
            return False

        snapshot = self._get_cached_snapshot_by_id(session, int(source.latest_snapshot_id))
        if snapshot is None:
            return False

        existing_fingerprint = _safe_str(snapshot.content_fingerprint)
        if existing_fingerprint is None or existing_fingerprint != snapshot_fingerprint:
            return False

        observed_at = self._to_utc(record.observed_at)
        now = _utc_now()

        if snapshot.valid_from_at is None:
            snapshot.valid_from_at = snapshot.observed_at
        if snapshot.valid_to_at is None:
            snapshot.valid_to_at = snapshot.observed_at
        snapshot.valid_to_at = self._max_datetime(snapshot.valid_to_at, observed_at)
        snapshot.observed_at = self._max_datetime(snapshot.observed_at, observed_at)
        if store is not None:
            if store.id is None:
                session.flush([store])
            if store.id is not None:
                snapshot.store_id = int(store.id)
        snapshot.price = record.price
        snapshot.discount_price = record.discount_price
        snapshot.loyal_price = record.loyal_price
        snapshot.price_unit = record.price_unit
        snapshot.available_count = record.available_count

        if _safe_str(source.canonical_product_id):
            record.canonical_product_id = source.canonical_product_id
        else:
            source.canonical_product_id = record.canonical_product_id or source.canonical_product_id
        source.last_seen_at = self._max_datetime(source.last_seen_at, observed_at)
        source.latest_content_fingerprint = snapshot_fingerprint
        source.updated_at = now

        projection = self._get_cached_product_row(
            session,
            parser_name=record.parser_name,
            source_id=source_id,
        )
        if projection is not None:
            projection.observed_at = self._max_datetime(projection.observed_at, observed_at)
            projection.updated_at = now

        LOGGER.debug(
            "Catalog snapshot reused: parser=%s source_id=%s snapshot_id=%s valid_from_at=%s valid_to_at=%s",
            record.parser_name,
            source_id,
            snapshot.id,
            snapshot.valid_from_at,
            snapshot.valid_to_at,
        )
        return True

    def _ensure_identity_map_row(
        self,
        session: Session,
        *,
        parser_name: str,
        identity_type: str,
        identity_value: str,
        canonical_product_id: str,
        updated_at: datetime,
    ) -> _CatalogIdentityMap:
        row = self._get_identity_map_row(
            session,
            parser_name=parser_name,
            identity_type=identity_type,
            identity_value=identity_value,
        )
        if row is not None:
            if _is_missing(row.canonical_product_id):
                row.canonical_product_id = canonical_product_id
            row.updated_at = updated_at
            identity_cache = session.info.get("_catalog_identity_cache")
            if isinstance(identity_cache, dict):
                identity_cache[(parser_name, identity_type, identity_value)] = row
            return row

        pk = {
            "parser_name": parser_name,
            "identity_type": identity_type,
            "identity_value": identity_value,
        }
        values = {
            **pk,
            "canonical_product_id": canonical_product_id,
            "updated_at": updated_at,
        }

        dialect_name = session.get_bind().dialect.name
        if dialect_name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as postgres_insert

            stmt = postgres_insert(_CatalogIdentityMap).values(**values).on_conflict_do_nothing(
                index_elements=("parser_name", "identity_type", "identity_value"),
            )
            session.execute(stmt)
        elif dialect_name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(_CatalogIdentityMap).values(**values).on_conflict_do_nothing(
                index_elements=("parser_name", "identity_type", "identity_value"),
            )
            session.execute(stmt)
        else:
            row = _CatalogIdentityMap(**values)
            session.add(row)
            identity_cache = session.info.get("_catalog_identity_cache")
            if isinstance(identity_cache, dict):
                identity_cache[(parser_name, identity_type, identity_value)] = row
            return row

        row = session.get(_CatalogIdentityMap, (parser_name, identity_type, identity_value))
        if row is None:
            row = _CatalogIdentityMap(**values)
            session.add(row)
            identity_cache = session.info.get("_catalog_identity_cache")
            if isinstance(identity_cache, dict):
                identity_cache[(parser_name, identity_type, identity_value)] = row
            return row

        if _is_missing(row.canonical_product_id):
            row.canonical_product_id = canonical_product_id
        row.updated_at = updated_at
        identity_cache = session.info.get("_catalog_identity_cache")
        if isinstance(identity_cache, dict):
            identity_cache[(parser_name, identity_type, identity_value)] = row
        return row

    @staticmethod
    def _get_identity_map_row(
        session: Session,
        *,
        parser_name: str,
        identity_type: str,
        identity_value: str,
    ) -> _CatalogIdentityMap | None:
        cache_key = (parser_name, identity_type, identity_value)
        identity_cache = session.info.get("_catalog_identity_cache")
        if isinstance(identity_cache, dict) and cache_key in identity_cache:
            cached = identity_cache[cache_key]
            return cached if isinstance(cached, _CatalogIdentityMap) else None

        for pending in session.new:
            if not isinstance(pending, _CatalogIdentityMap):
                continue
            if (
                pending.parser_name == parser_name
                and pending.identity_type == identity_type
                and pending.identity_value == identity_value
            ):
                if isinstance(identity_cache, dict):
                    identity_cache[cache_key] = pending
                return pending
        row = session.get(_CatalogIdentityMap, cache_key)
        if isinstance(identity_cache, dict):
            identity_cache[cache_key] = row
        return row

    @staticmethod
    def _fallback_identity_value(record: NormalizedProductRecord) -> str | None:
        fallback = _safe_str(record.title_normalized_no_stopwords)
        if fallback:
            return fallback
        return _safe_str(record.title_normalized)

    @staticmethod
    def _has_strong_identity(record: NormalizedProductRecord) -> bool:
        return _safe_str(record.plu) is not None or _safe_str(record.sku) is not None

    def _apply_persistent_image_dedup(self, session: Session, record: NormalizedProductRecord) -> None:
        original_image_count = len(record.image_urls)
        self._persist_record_images(record)
        unique_urls: list[str] = []
        duplicate_urls: list[str] = []
        fingerprints: list[str] = []

        seen_in_record: set[str] = set()

        for raw_url in record.image_urls:
            url = raw_url.strip()
            if not url:
                continue

            fingerprint = hashlib.sha256(url.encode("utf-8")).hexdigest()
            if fingerprint in seen_in_record:
                duplicate_urls.append(url)
                continue

            seen_in_record.add(fingerprint)
            unique_urls.append(url)
            fingerprints.append(fingerprint)

        duplicates_to_delete = list(dict.fromkeys(duplicate_urls))
        record.image_urls = unique_urls
        self._populate_record_image_sizes(record)
        record.duplicate_image_urls = []
        record.image_fingerprints = fingerprints
        LOGGER.debug(
            "Catalog image dedup: parser=%s source_id=%s input=%s unique=%s duplicates=%s",
            record.parser_name,
            self._source_id(record),
            original_image_count,
            len(unique_urls),
            len(duplicates_to_delete),
        )
        self._enqueue_duplicate_images(session, duplicates_to_delete)

    def _persist_record_images(self, record: NormalizedProductRecord) -> None:
        storage = self._storage_repository
        if storage is None or not record.image_urls:
            return

        persist_handler = getattr(storage, "persist_images", None)
        if not callable(persist_handler):
            return

        try:
            persisted = persist_handler(list(record.image_urls))
        except Exception as exc:
            LOGGER.warning(
                "Catalog image persist failed (best effort): parser=%s source_id=%s error=%s",
                record.parser_name,
                self._source_id(record),
                exc,
            )
            return

        if not isinstance(persisted, list):
            return
        normalized: list[str] = []
        for idx, value in enumerate(persisted):
            token = _safe_str(value)
            if token is None:
                original = _safe_str(record.image_urls[idx]) if idx < len(record.image_urls) else None
                if original is not None:
                    normalized.append(original)
                continue
            normalized.append(token)
        if normalized:
            record.image_urls = normalized

    def _populate_record_image_sizes(self, record: NormalizedProductRecord) -> None:
        record.image_sizes = [None] * len(record.image_urls)
        storage = self._storage_repository
        if storage is None or not record.image_urls:
            return

        size_handler = getattr(storage, "get_image_sizes", None)
        if not callable(size_handler):
            return

        try:
            raw_sizes = size_handler(list(record.image_urls))
        except Exception as exc:
            LOGGER.warning(
                "Catalog image size fetch failed (best effort): parser=%s source_id=%s error=%s",
                record.parser_name,
                self._source_id(record),
                exc,
            )
            return

        if not isinstance(raw_sizes, list):
            return

        normalized_sizes: list[int | None] = []
        for idx, _ in enumerate(record.image_urls):
            raw = raw_sizes[idx] if idx < len(raw_sizes) else None
            parsed = self._to_int(raw)
            normalized_sizes.append(parsed if parsed is not None and parsed >= 0 else None)
        record.image_sizes = normalized_sizes

    def _enqueue_duplicate_images(self, session: Session, duplicate_urls: list[str]) -> None:
        if not duplicate_urls:
            return
        now = _utc_now()
        unique_urls = list(dict.fromkeys(duplicate_urls))
        for image_url in unique_urls:
            token = _safe_str(image_url)
            if token is None:
                continue
            dedupe_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
            row = session.scalar(
                select(_CatalogStorageDeleteOutbox).where(_CatalogStorageDeleteOutbox.dedupe_key == dedupe_key)
            )
            if row is None:
                row = _CatalogStorageDeleteOutbox(
                    dedupe_key=dedupe_key,
                    image_url=token,
                    status="pending",
                    attempts=0,
                    enqueued_at=now,
                    available_at=now,
                    processed_at=None,
                    last_error=None,
                )
                session.add(row)
            else:
                if row.status != "done":
                    row.status = "pending"
                    row.available_at = now
                    row.last_error = None
        LOGGER.debug("Catalog duplicate images enqueued to outbox: count=%s", len(unique_urls))

    def process_storage_delete_outbox(self, *, limit: int = 100) -> dict[str, int]:
        if self._storage_repository is None:
            return {"processed": 0, "deleted": 0, "failed": 0}

        processed = 0
        deleted = 0
        failed = 0
        now = _utc_now()
        with self._session_factory() as session:
            rows = session.scalars(
                select(_CatalogStorageDeleteOutbox)
                .where(
                    _CatalogStorageDeleteOutbox.status == "pending",
                    _CatalogStorageDeleteOutbox.available_at <= now,
                )
                .order_by(_CatalogStorageDeleteOutbox.id.asc())
                .limit(max(1, int(limit)))
            ).all()

            for row in rows:
                processed += 1
                try:
                    self._storage_repository.delete_images([row.image_url])
                    row.status = "done"
                    row.processed_at = _utc_now()
                    row.last_error = None
                    deleted += 1
                except Exception as exc:
                    row.attempts = int(row.attempts) + 1
                    row.last_error = str(exc)
                    if row.attempts >= 10:
                        row.status = "failed"
                        failed += 1
                    else:
                        delay_sec = min(300, 2 ** max(0, row.attempts - 1))
                        row.available_at = _utc_now().replace(
                            microsecond=0
                        ) + timedelta(seconds=delay_sec)
                        row.status = "pending"
                        failed += 1
            session.commit()
        return {"processed": processed, "deleted": deleted, "failed": failed}

    def _apply_backfill(self, session: Session, record: NormalizedProductRecord) -> None:
        canonical_product_id = _safe_str(record.canonical_product_id)
        if canonical_product_id is None:
            return

        missing_fields = [field_name for field_name in self.BACKFILL_FIELDS if _is_missing(getattr(record, field_name))]
        if not missing_fields:
            return

        product_history = session.scalars(
            select(_CatalogProduct).where(_CatalogProduct.canonical_product_id == canonical_product_id)
        ).all()
        history = [*product_history]
        if not history:
            return

        target_time = self._to_utc(record.observed_at)
        filled = 0

        for field_name in missing_fields:
            replacement = self._closest_non_missing(history, field_name, target_time)
            if replacement is not None:
                setattr(record, field_name, replacement)
                filled += 1
        if filled:
            LOGGER.debug(
                "Catalog backfill applied: canonical_product_id=%s missing_fields=%s filled_fields=%s",
                canonical_product_id,
                len(missing_fields),
                filled,
            )

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
        payload: dict[str, Any],
        store: _CatalogStore | None,
        snapshot_fingerprint: str,
        source_event_uid: str | None,
    ) -> tuple[_CatalogProductSnapshot, bool]:
        observed_at = self._to_utc(record.observed_at)
        event_uid = _safe_str(source_event_uid)
        store_id: int | None = None
        if store is not None:
            if store.id is None:
                session.flush([store])
            if store.id is not None:
                store_id = int(store.id)

        if event_uid is not None:
            existing = self._get_cached_snapshot_by_event_uid(
                session,
                event_uid=event_uid,
            )
            if existing is not None:
                if existing.valid_to_at is None:
                    existing.valid_to_at = existing.observed_at
                existing.valid_to_at = self._max_datetime(existing.valid_to_at, observed_at)
                existing.observed_at = self._max_datetime(existing.observed_at, observed_at)
                existing.price = record.price
                existing.discount_price = record.discount_price
                existing.loyal_price = record.loyal_price
                existing.price_unit = record.price_unit
                existing.available_count = record.available_count
                if store_id is not None:
                    existing.store_id = store_id
                if existing.id is not None:
                    self._cache_snapshot_by_id(session, int(existing.id), existing)
                return existing, False

        snapshot = _CatalogProductSnapshot(
            canonical_product_id=record.canonical_product_id or str(uuid4()),
            parser_name=record.parser_name,
            source_id=self._source_id(record),
            source_run_id=_safe_str(payload.get("receiver_run_id")),
            receiver_product_id=self._to_int(payload.get("receiver_product_id")),
            receiver_artifact_id=self._to_int(payload.get("receiver_artifact_id")),
            store_id=store_id,
            receiver_sort_order=self._to_int(payload.get("receiver_sort_order")),
            source_event_uid=event_uid,
            content_fingerprint=snapshot_fingerprint,
            valid_from_at=observed_at,
            valid_to_at=observed_at,
            observed_at=observed_at,
            created_at=observed_at,
            price=record.price,
            discount_price=record.discount_price,
            loyal_price=record.loyal_price,
            price_unit=record.price_unit,
            available_count=record.available_count,
        )
        session.add(snapshot)
        session.flush([snapshot])
        if snapshot.id is not None:
            self._cache_snapshot_by_id(session, int(snapshot.id), snapshot)
        if event_uid is not None:
            self._cache_snapshot_by_event_uid(
                session,
                event_uid=event_uid,
                row=snapshot,
            )
        return snapshot, True

    def _upsert_product_source(
        self,
        session: Session,
        record: NormalizedProductRecord,
        *,
        snapshot: _CatalogProductSnapshot,
        snapshot_fingerprint: str | None = None,
    ) -> None:
        parser_name = record.parser_name.strip().lower()
        source_id = self._source_id(record)
        observed_at = self._to_utc(record.observed_at)
        now = _utc_now()

        row = self._get_cached_product_source(
            session,
            parser_name=parser_name,
            source_id=source_id,
        )
        if row is None:
            row = _CatalogProductSource(
                parser_name=parser_name,
                source_id=source_id,
                canonical_product_id=record.canonical_product_id or str(uuid4()),
                latest_snapshot_id=snapshot.id,
                latest_content_fingerprint=snapshot_fingerprint,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                updated_at=now,
            )
            session.add(row)
            self._cache_product_source(
                session,
                parser_name=parser_name,
                source_id=source_id,
                row=row,
            )
            return

        if _safe_str(row.canonical_product_id):
            record.canonical_product_id = row.canonical_product_id
        else:
            row.canonical_product_id = record.canonical_product_id or row.canonical_product_id

        row.latest_snapshot_id = snapshot.id
        if snapshot_fingerprint is not None:
            row.latest_content_fingerprint = snapshot_fingerprint
        row.last_seen_at = self._max_datetime(row.last_seen_at, observed_at)
        row.updated_at = now

    def _upsert_store(
        self,
        session: Session,
        record: NormalizedProductRecord,
        *,
        payload: dict[str, Any],
        store_data: dict[str, object] | None = None,
        settlement: _CatalogSettlement | None = None,
    ) -> _CatalogStore | None:
        data = store_data if isinstance(store_data, dict) else self._extract_store_components(record, payload=payload)
        if not isinstance(data, dict):
            return None

        store_key = _safe_str(data.get("store_key"))
        if store_key is None:
            return None

        observed_at = self._to_utc(record.observed_at)
        now = _utc_now()
        settlement_id = int(settlement.id) if settlement is not None and settlement.id is not None else None
        row = self._get_cached_store_row(session, store_key=store_key)

        if row is None:
            row = _CatalogStore(
                store_key=store_key,
                parser_name=record.parser_name.strip().lower(),
                source=_safe_str(data.get("source")),
                retail_type=_safe_str(data.get("retail_type")),
                code=_safe_str(data.get("code")),
                address=_safe_str(data.get("address")),
                schedule_weekdays_open_from=_safe_str(data.get("schedule_weekdays_open_from")),
                schedule_weekdays_closed_from=_safe_str(data.get("schedule_weekdays_closed_from")),
                schedule_saturday_open_from=_safe_str(data.get("schedule_saturday_open_from")),
                schedule_saturday_closed_from=_safe_str(data.get("schedule_saturday_closed_from")),
                schedule_sunday_open_from=_safe_str(data.get("schedule_sunday_open_from")),
                schedule_sunday_closed_from=_safe_str(data.get("schedule_sunday_closed_from")),
                temporarily_closed=self._to_bool(data.get("temporarily_closed")),
                longitude=_as_float(data.get("longitude")),
                latitude=_as_float(data.get("latitude")),
                rating=_as_float(data.get("rating")),
                reviews_count=self._to_int(data.get("reviews_count")),
                open_date=self._to_date(data.get("open_date")),
                settlement_id=settlement_id,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                updated_at=now,
            )
            session.add(row)
            self._cache_store_row(session, store_key=store_key, row=row)
            return row

        row.last_seen_at = self._max_datetime(row.last_seen_at, observed_at)
        row.updated_at = now
        self._fill_missing(row, "source", _safe_str(data.get("source")))
        self._fill_missing(row, "retail_type", _safe_str(data.get("retail_type")))
        self._fill_missing(row, "code", _safe_str(data.get("code")))
        self._fill_missing(row, "address", _safe_str(data.get("address")))
        self._fill_missing(
            row,
            "schedule_weekdays_open_from",
            _safe_str(data.get("schedule_weekdays_open_from")),
        )
        self._fill_missing(
            row,
            "schedule_weekdays_closed_from",
            _safe_str(data.get("schedule_weekdays_closed_from")),
        )
        self._fill_missing(
            row,
            "schedule_saturday_open_from",
            _safe_str(data.get("schedule_saturday_open_from")),
        )
        self._fill_missing(
            row,
            "schedule_saturday_closed_from",
            _safe_str(data.get("schedule_saturday_closed_from")),
        )
        self._fill_missing(
            row,
            "schedule_sunday_open_from",
            _safe_str(data.get("schedule_sunday_open_from")),
        )
        self._fill_missing(
            row,
            "schedule_sunday_closed_from",
            _safe_str(data.get("schedule_sunday_closed_from")),
        )
        if _is_missing(row.temporarily_closed):
            row.temporarily_closed = self._to_bool(data.get("temporarily_closed"))
        if _is_missing(row.longitude):
            row.longitude = _as_float(data.get("longitude"))
        if _is_missing(row.latitude):
            row.latitude = _as_float(data.get("latitude"))
        rating = _as_float(data.get("rating"))
        if not _is_missing(rating):
            row.rating = rating
        reviews_count = self._to_int(data.get("reviews_count"))
        if not _is_missing(reviews_count):
            row.reviews_count = reviews_count
        self._fill_missing(row, "open_date", self._to_date(data.get("open_date")))
        self._fill_missing(row, "settlement_id", settlement_id)
        return row

    def _upsert_settlement(
        self,
        session: Session,
        record: NormalizedProductRecord,
        *,
        payload: dict[str, Any],
    ) -> _CatalogSettlement | None:
        geo = self._extract_geo_components(record, payload=payload)
        if geo is None:
            return None

        key = self._geo_key(geo)
        if key is None:
            return None

        observed_at = self._to_utc(record.observed_at)
        now = _utc_now()
        settlement_cache = session.info.setdefault("_catalog_settlement_cache", {})
        if isinstance(settlement_cache, dict) and key in settlement_cache:
            cached = settlement_cache[key]
            row = cached if isinstance(cached, _CatalogSettlement) else None
        else:
            row = self._get_settlement_row(session, key)
            if isinstance(settlement_cache, dict):
                settlement_cache[key] = row
        if row is None:
            row = self._find_compatible_settlement_row(session, geo)
            if isinstance(settlement_cache, dict):
                settlement_cache[key] = row

        if row is None:
            row = _CatalogSettlement(
                geo_key=key,
                country=geo.get("country"),
                country_normalized=geo.get("country_normalized"),
                region=geo.get("region"),
                region_normalized=geo.get("region_normalized"),
                name=geo.get("name"),
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
            if isinstance(settlement_cache, dict):
                settlement_cache[key] = row
            self._cache_settlement_candidate_row(session, row=row)
            LOGGER.debug(
                "Catalog settlement created: parser=%s source_id=%s settlement_id=%s geo_key=%s",
                record.parser_name,
                self._source_id(record),
                row.id,
                key,
            )
            return row

        row.last_seen_at = self._max_datetime(row.last_seen_at, observed_at)
        row.updated_at = now

        self._fill_missing(row, "country", geo.get("country"))
        self._fill_missing(row, "country_normalized", geo.get("country_normalized"))
        self._fill_missing(row, "region", geo.get("region"))
        self._fill_missing(row, "region_normalized", geo.get("region_normalized"))
        self._fill_missing(row, "name", geo.get("name"))
        self._fill_missing(row, "name_normalized", geo.get("name_normalized"))
        self._fill_missing(row, "settlement_type", geo.get("settlement_type"))
        self._fill_missing(row, "alias", geo.get("alias"))
        self._fill_missing(row, "latitude", geo.get("latitude"))
        self._fill_missing(row, "longitude", geo.get("longitude"))
        self._cache_settlement_candidate_row(session, row=row)

        LOGGER.debug(
            "Catalog settlement updated: parser=%s source_id=%s settlement_id=%s geo_key=%s",
            record.parser_name,
            self._source_id(record),
            row.id,
            key,
        )
        return row

    def _upsert_categories(
        self,
        session: Session,
        record: NormalizedProductRecord,
        *,
        payload: dict[str, Any],
    ) -> list[tuple[_CatalogCategory, int]]:
        candidates = self._extract_category_candidates(record, payload=payload)
        if not candidates:
            return []

        parser_name = record.parser_name.strip().lower()
        observed_at = self._to_utc(record.observed_at)
        now = _utc_now()
        category_cache = session.info.setdefault("_catalog_category_cache", {})

        out: list[tuple[_CatalogCategory, int]] = []
        for idx, item in enumerate(candidates):
            source_uid = _safe_str(item.get("uid"))
            title = _safe_str(item.get("title"))
            title_normalized = self._normalize_category_title(title)

            category_key = self._category_key(
                parser_name=parser_name,
                source_uid=source_uid,
                title_normalized=title_normalized,
            )
            if category_key is None:
                continue

            if isinstance(category_cache, dict) and category_key in category_cache:
                cached = category_cache[category_key]
                row = cached if isinstance(cached, _CatalogCategory) else None
            else:
                row = self._get_category_row(session, category_key)
                if isinstance(category_cache, dict):
                    category_cache[category_key] = row
            if row is None:
                row = _CatalogCategory(
                    category_key=category_key,
                    parser_name=parser_name,
                    source_uid=source_uid,
                    parent_source_uid=_safe_str(item.get("parent_uid")),
                    title=title,
                    title_normalized=title_normalized,
                    alias=_safe_str(item.get("alias")),
                    adult=self._to_bool(item.get("adult")),
                    icon=_safe_str(item.get("icon")),
                    banner=_safe_str(item.get("banner")),
                    depth=self._to_int(item.get("depth")),
                    sort_order=self._to_int(item.get("sort_order")),
                    first_seen_at=observed_at,
                    last_seen_at=observed_at,
                    updated_at=now,
                )
                session.add(row)
                if isinstance(category_cache, dict):
                    category_cache[category_key] = row
            else:
                row.last_seen_at = self._max_datetime(row.last_seen_at, observed_at)
                row.updated_at = now
                self._fill_missing(row, "source_uid", source_uid)
                self._fill_missing(row, "parent_source_uid", _safe_str(item.get("parent_uid")))
                self._fill_missing(row, "title", title)
                self._fill_missing(row, "title_normalized", title_normalized)
                self._fill_missing(row, "alias", _safe_str(item.get("alias")))
                self._fill_missing(row, "adult", self._to_bool(item.get("adult")))
                self._fill_missing(row, "icon", _safe_str(item.get("icon")))
                self._fill_missing(row, "banner", _safe_str(item.get("banner")))
                self._fill_missing(row, "depth", self._to_int(item.get("depth")))
                self._fill_missing(row, "sort_order", self._to_int(item.get("sort_order")))

            sort_order = self._to_int(item.get("sort_order"))
            out.append((row, sort_order if sort_order is not None else idx))

        LOGGER.debug(
            "Catalog categories resolved: parser=%s source_id=%s candidates=%s linked=%s",
            record.parser_name,
            self._source_id(record),
            len(candidates),
            len(out),
        )
        return out

    def _normalize_category_title(self, value: str | None) -> str | None:
        token = _safe_str(value)
        if token is None:
            return None

        normalized = normalize_category_text(token, text_normalizer=self._category_text_normalizer)
        if normalized is not None:
            return normalized
        return self._normalize_text(token)

    def _extract_store_components(
        self,
        record: NormalizedProductRecord,
        *,
        payload: dict[str, Any],
    ) -> dict[str, object] | None:
        artifact = payload.get("receiver_artifact")
        artifact_data = artifact if isinstance(artifact, dict) else {}

        source = _safe_str(payload.get("receiver_source")) or _safe_str(artifact_data.get("source"))
        retail_type = _safe_str(artifact_data.get("retail_type"))
        code = _safe_str(artifact_data.get("code"))
        address = _safe_str(artifact_data.get("address"))

        latitude = _as_float(artifact_data.get("latitude"))
        longitude = _as_float(artifact_data.get("longitude"))
        normalized_geo = self._normalize_geo_coordinates(latitude, longitude)
        if normalized_geo is not None:
            latitude, longitude = normalized_geo

        fallback_artifact_id = self._to_int(payload.get("receiver_artifact_id"))
        store_key = self._store_key(
            record,
            source=source,
            retail_type=retail_type,
            code=code,
            address=address,
            latitude=latitude,
            longitude=longitude,
            fallback_artifact_id=fallback_artifact_id,
        )
        if store_key is None:
            return None

        return {
            "store_key": store_key,
            "source": source,
            "retail_type": retail_type,
            "code": code,
            "address": address,
            "schedule_weekdays_open_from": _safe_str(artifact_data.get("schedule_weekdays_open_from")),
            "schedule_weekdays_closed_from": _safe_str(artifact_data.get("schedule_weekdays_closed_from")),
            "schedule_saturday_open_from": _safe_str(artifact_data.get("schedule_saturday_open_from")),
            "schedule_saturday_closed_from": _safe_str(artifact_data.get("schedule_saturday_closed_from")),
            "schedule_sunday_open_from": _safe_str(artifact_data.get("schedule_sunday_open_from")),
            "schedule_sunday_closed_from": _safe_str(artifact_data.get("schedule_sunday_closed_from")),
            "temporarily_closed": artifact_data.get("temporarily_closed"),
            "longitude": longitude,
            "latitude": latitude,
            "rating": _as_float(artifact_data.get("rating")),
            "reviews_count": self._to_int(artifact_data.get("reviews_count")),
            "open_date": self._to_date(artifact_data.get("open_date")),
        }

    @staticmethod
    def _build_store_key(
        *,
        parser_name: str,
        source: str | None,
        retail_type: str | None,
        code: str | None,
        address: str | None,
        latitude: float | None,
        longitude: float | None,
        fallback_artifact_id: int | None,
    ) -> str | None:
        parser_token = parser_name.strip().lower()
        source_token = (source or "").strip().lower()
        retail_token = (retail_type or "").strip().lower()
        code_token = (code or "").strip().lower()
        address_token = (address or "").strip().lower()
        coord_token = ""
        normalized_geo = CatalogRepository._normalize_geo_coordinates(latitude, longitude)
        if normalized_geo is not None:
            lat, lon = normalized_geo
            coord_token = f"{lat:.8f},{lon:.8f}"

        identity_parts = [parser_token, source_token, retail_token]
        if code_token:
            identity_parts.extend(["code", code_token])
        elif address_token:
            identity_parts.extend(["address", address_token])
            if coord_token:
                identity_parts.extend(["coords", coord_token])
        elif coord_token:
            identity_parts.extend(["coords", coord_token])
        elif fallback_artifact_id is not None:
            identity_parts.extend(["artifact", str(fallback_artifact_id)])
        else:
            return None

        seed = "|".join(identity_parts)
        digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:40]
        return f"{parser_token}:store:{digest}"

    def _store_key(
        self,
        record: NormalizedProductRecord,
        *,
        source: str | None = None,
        retail_type: str | None = None,
        code: str | None = None,
        address: str | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        fallback_artifact_id: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> str | None:
        if payload is not None:
            store_data = self._extract_store_components(record, payload=payload)
            if not isinstance(store_data, dict):
                return None
            return _safe_str(store_data.get("store_key"))
        return self._build_store_key(
            parser_name=record.parser_name,
            source=source,
            retail_type=retail_type,
            code=code,
            address=address,
            latitude=latitude,
            longitude=longitude,
            fallback_artifact_id=fallback_artifact_id,
        )

    def _extract_geo_components(
        self,
        record: NormalizedProductRecord,
        *,
        payload: dict[str, Any],
    ) -> dict[str, object] | None:
        country = _safe_str(payload.get("receiver_geo_country"))
        region = _safe_str(payload.get("receiver_geo_region"))
        name = _safe_str(payload.get("receiver_geo_name"))
        settlement_type = _safe_str(payload.get("receiver_geo_settlement_type"))
        alias = _safe_str(payload.get("receiver_geo_alias"))
        latitude, longitude = self._extract_geo_coordinates(payload)
        normalized_geo = self._normalize_geo_coordinates(latitude, longitude)
        if latitude is not None and longitude is not None and normalized_geo is None:
            LOGGER.warning(
                "Catalog settlement coordinates skipped due to invalid range: parser=%s source_id=%s latitude=%s longitude=%s",
                record.parser_name,
                self._source_id(record),
                latitude,
                longitude,
            )
            latitude = None
            longitude = None
        elif normalized_geo is not None:
            latitude, longitude = normalized_geo

        if name is None and record.geo_normalized:
            parts = [segment.strip() for segment in str(record.geo_normalized).split(",") if segment.strip()]
            if len(parts) >= 1 and country is None:
                country = parts[0]
            if len(parts) >= 2 and region is None:
                region = parts[1]
            if len(parts) >= 3 and name is None:
                name = parts[2]

        country_normalized = self._normalize_text(country)
        region_normalized = self._normalize_text(region)
        name_normalized = self._normalize_text(name)

        if all(token is None for token in (country_normalized, region_normalized, name_normalized)):
            return None

        return {
            "country": country,
            "country_normalized": country_normalized,
            "region": region,
            "region_normalized": region_normalized,
            "name": name,
            "name_normalized": name_normalized,
            "settlement_type": settlement_type,
            "alias": alias,
            "latitude": latitude,
            "longitude": longitude,
        }

    @staticmethod
    def _extract_geo_coordinates(payload: dict[str, Any]) -> tuple[float | None, float | None]:
        # Prefer admin-unit coordinates, then fallback to artifact-level coordinates.
        latitude = _as_float(payload.get("receiver_geo_latitude"))
        longitude = _as_float(payload.get("receiver_geo_longitude"))
        if latitude is not None and longitude is not None:
            return latitude, longitude

        admin = payload.get("receiver_admin_unit")
        if isinstance(admin, dict):
            latitude = _as_float(admin.get("latitude"))
            longitude = _as_float(admin.get("longitude"))
            if latitude is not None and longitude is not None:
                return latitude, longitude

        artifact = payload.get("receiver_artifact")
        if isinstance(artifact, dict):
            latitude = _as_float(artifact.get("latitude"))
            longitude = _as_float(artifact.get("longitude"))
            if latitude is not None and longitude is not None:
                return latitude, longitude

        return None, None

    @staticmethod
    def _normalize_geo_coordinates(
        latitude: float | None,
        longitude: float | None,
    ) -> tuple[float, float] | None:
        if latitude is None or longitude is None:
            return None
        lat = float(latitude)
        lon = float(longitude)
        if lat < -90.0 or lat > 90.0:
            return None
        if lon < -180.0 or lon > 180.0:
            return None
        return round(lat, 8), round(lon, 8)

    def _extract_category_candidates(
        self,
        record: NormalizedProductRecord,
        *,
        payload: dict[str, Any],
    ) -> list[dict[str, object]]:
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

        category_normalized = _safe_str(record.category_normalized)
        if category_normalized is None:
            return []

        parts = [segment.strip() for segment in category_normalized.split("/") if segment.strip()]
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
    def _settlement_match_key(geo: dict[str, object]) -> tuple[str, str] | None:
        name = _safe_str(geo.get("name_normalized"))
        settlement_type = _safe_str(geo.get("settlement_type"))
        if name is None or settlement_type is None:
            return None
        return name, settlement_type.strip().lower()

    @staticmethod
    def _settlement_match_key_from_row(row: _CatalogSettlement) -> tuple[str, str] | None:
        name = _safe_str(row.name_normalized)
        settlement_type = _safe_str(row.settlement_type)
        if name is None or settlement_type is None:
            return None
        return name, settlement_type.strip().lower()

    @staticmethod
    def _compatible_settlement_value(left: str | None, right: str | None) -> bool:
        left_token = _safe_str(left)
        right_token = _safe_str(right)
        if left_token is None or right_token is None:
            return True
        return left_token == right_token

    @classmethod
    def _is_settlement_row_compatible(
        cls,
        row: _CatalogSettlement,
        geo: dict[str, object],
    ) -> bool:
        row_match_key = cls._settlement_match_key_from_row(row)
        incoming_match_key = cls._settlement_match_key(geo)
        if row_match_key is None or incoming_match_key is None or row_match_key != incoming_match_key:
            return False
        if not cls._compatible_settlement_value(row.country_normalized, _safe_str(geo.get("country_normalized"))):
            return False
        if not cls._compatible_settlement_value(row.region_normalized, _safe_str(geo.get("region_normalized"))):
            return False
        return True

    @classmethod
    def _settlement_match_score(
        cls,
        row: _CatalogSettlement,
        geo: dict[str, object],
    ) -> tuple[int, int, int, int, int, int]:
        incoming_country = _safe_str(geo.get("country_normalized"))
        incoming_region = _safe_str(geo.get("region_normalized"))
        row_country = _safe_str(row.country_normalized)
        row_region = _safe_str(row.region_normalized)
        row_id = int(row.id) if row.id is not None else 0
        return (
            1 if row_country is not None and incoming_country is not None and row_country == incoming_country else 0,
            1 if row_region is not None and incoming_region is not None and row_region == incoming_region else 0,
            1 if row_country is not None else 0,
            1 if row_region is not None else 0,
            1 if row.latitude is not None and row.longitude is not None else 0,
            -row_id,
        )

    def _find_compatible_settlement_row(
        self,
        session: Session,
        geo: dict[str, object],
    ) -> _CatalogSettlement | None:
        match_key = self._settlement_match_key(geo)
        if match_key is None:
            return None

        candidate_cache = session.info.setdefault("_catalog_settlement_candidate_cache", {})
        candidates: list[_CatalogSettlement]
        if isinstance(candidate_cache, dict) and match_key in candidate_cache:
            cached = candidate_cache.get(match_key)
            candidates = [row for row in cached if isinstance(row, _CatalogSettlement)] if isinstance(cached, list) else []
        else:
            candidates = session.scalars(
                select(_CatalogSettlement).where(
                    _CatalogSettlement.name_normalized == match_key[0],
                    _CatalogSettlement.settlement_type == match_key[1],
                )
            ).all()
            if isinstance(candidate_cache, dict):
                candidate_cache[match_key] = list(candidates)

        compatible = [row for row in candidates if self._is_settlement_row_compatible(row, geo)]
        if not compatible:
            return None
        return max(compatible, key=lambda row: self._settlement_match_score(row, geo))

    @staticmethod
    def _cache_settlement_candidate_row(
        session: Session,
        *,
        row: _CatalogSettlement,
    ) -> None:
        match_key = CatalogRepository._settlement_match_key_from_row(row)
        if match_key is None:
            return
        candidate_cache = session.info.setdefault("_catalog_settlement_candidate_cache", {})
        if not isinstance(candidate_cache, dict):
            return
        bucket = candidate_cache.setdefault(match_key, [])
        if not isinstance(bucket, list):
            candidate_cache[match_key] = [row]
            return
        for existing in bucket:
            if isinstance(existing, _CatalogSettlement) and existing is row:
                return
            if (
                isinstance(existing, _CatalogSettlement)
                and existing.id is not None
                and row.id is not None
                and int(existing.id) == int(row.id)
            ):
                return
        bucket.append(row)

    @staticmethod
    def _get_store_row(session: Session, store_key: str) -> _CatalogStore | None:
        for pending in session.new:
            if not isinstance(pending, _CatalogStore):
                continue
            if pending.store_key == store_key:
                return pending
        return session.scalar(select(_CatalogStore).where(_CatalogStore.store_key == store_key))

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
        settlement_type = _safe_str(geo.get("settlement_type")) or ""
        combined = "|".join((country, region, name, settlement_type)).strip("|")
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
        if any(category.id is None for category, _ in categories):
            session.flush()
        primary_category_id = self._primary_category_id(categories)
        settlement_id = int(settlement.id) if settlement is not None and settlement.id is not None else None
        parser_name = record.parser_name
        brand_normalized = self._normalized_brand_value(record.brand_normalized, record.brand)

        existing = self._get_cached_product_row(
            session,
            parser_name=parser_name,
            source_id=source_id,
        )

        if existing is None:
            existing = _CatalogProduct(
                parser_name=parser_name,
                source_id=source_id,
                created_at=now,
                updated_at=now,
                canonical_product_id=record.canonical_product_id or str(uuid4()),
                plu=record.plu,
                sku=record.sku,
                title_original=record.title_original,
                title_normalized_no_stopwords=record.title_normalized_no_stopwords,
                brand=record.brand,
                brand_normalized=brand_normalized,
                source_page_url=record.source_page_url,
                description=record.description,
                producer_name=record.producer_name,
                producer_country=record.producer_country,
                expiration_date_in_days=record.expiration_date_in_days,
                rating=record.rating,
                reviews_count=record.reviews_count,
                adult=record.adult,
                is_new=record.is_new,
                promo=record.promo,
                season=record.season,
                hit=record.hit,
                data_matrix=record.data_matrix,
                unit=record.unit,
                package_quantity=record.package_quantity,
                package_unit=record.package_unit,
                package_weight_gross=record.package_weight_gross,
                package_count=record.package_count,
                dimension_height_m=record.dimension_height_m,
                dimension_width_m=record.dimension_width_m,
                dimension_depth_m=record.dimension_depth_m,
                primary_category_id=primary_category_id,
                settlement_id=settlement_id,
                composition_original=record.composition_original,
                composition_normalized=record.composition_normalized,
                observed_at=self._to_utc(record.observed_at),
            )
            session.add(existing)
            session.flush([existing])
            self._cache_product_row(
                session,
                parser_name=parser_name,
                source_id=source_id,
                row=existing,
            )
            if existing.id is not None and self._has_any_assets(record):
                self._replace_product_assets(session, int(existing.id), record, now=now)
            self._ensure_product_group_membership(session, product=existing)
            LOGGER.debug(
                "Catalog product created: parser=%s source_id=%s product_id=%s canonical_product_id=%s",
                record.parser_name,
                source_id,
                existing.id,
                existing.canonical_product_id,
            )
            return

        existing.updated_at = now
        self._fill_missing(existing, "canonical_product_id", record.canonical_product_id)

        # keep identity tokens up-to-date only when provided
        if not _is_missing(record.plu):
            existing.plu = record.plu
        if not _is_missing(record.sku):
            existing.sku = record.sku

        # title fields are authoritative per source snapshot
        existing.title_original = record.title_original
        existing.title_normalized_no_stopwords = record.title_normalized_no_stopwords

        if not _is_missing(record.brand):
            existing.brand = record.brand
        if not _is_missing(brand_normalized):
            existing.brand_normalized = brand_normalized
        if not _is_missing(record.source_page_url):
            existing.source_page_url = record.source_page_url
        if not _is_missing(record.description):
            existing.description = record.description
        if not _is_missing(record.producer_name):
            existing.producer_name = record.producer_name
        if not _is_missing(record.producer_country):
            existing.producer_country = record.producer_country
        if not _is_missing(record.expiration_date_in_days):
            existing.expiration_date_in_days = record.expiration_date_in_days
        if not _is_missing(record.rating):
            existing.rating = record.rating
        if not _is_missing(record.reviews_count):
            existing.reviews_count = record.reviews_count
        if not _is_missing(record.adult):
            existing.adult = record.adult
        if not _is_missing(record.is_new):
            existing.is_new = record.is_new
        if not _is_missing(record.promo):
            existing.promo = record.promo
        if not _is_missing(record.season):
            existing.season = record.season
        if not _is_missing(record.hit):
            existing.hit = record.hit
        if not _is_missing(record.data_matrix):
            existing.data_matrix = record.data_matrix
        existing.unit = record.unit

        if not _is_missing(record.package_quantity):
            existing.package_quantity = record.package_quantity
        if not _is_missing(record.package_unit):
            existing.package_unit = record.package_unit
        if not _is_missing(record.package_weight_gross):
            existing.package_weight_gross = record.package_weight_gross
        if not _is_missing(record.package_count):
            existing.package_count = record.package_count
        if not _is_missing(record.dimension_height_m):
            existing.dimension_height_m = record.dimension_height_m
        if not _is_missing(record.dimension_width_m):
            existing.dimension_width_m = record.dimension_width_m
        if not _is_missing(record.dimension_depth_m):
            existing.dimension_depth_m = record.dimension_depth_m

        if primary_category_id is not None:
            existing.primary_category_id = primary_category_id
        if settlement_id is not None:
            existing.settlement_id = settlement_id

        if not _is_missing(record.composition_original):
            existing.composition_original = record.composition_original
        if not _is_missing(record.composition_normalized):
            existing.composition_normalized = record.composition_normalized

        if existing.id is not None and record.image_urls:
            self._replace_product_assets(session, int(existing.id), record, now=now)

        existing.observed_at = self._max_datetime(existing.observed_at, self._to_utc(record.observed_at))
        self._ensure_product_group_membership(session, product=existing)
        LOGGER.debug(
            "Catalog product updated: parser=%s source_id=%s product_id=%s canonical_product_id=%s",
            record.parser_name,
            source_id,
            existing.id,
            existing.canonical_product_id,
        )

    def _ensure_product_group_membership(
        self,
        session: Session,
        *,
        product: _CatalogProduct,
    ) -> None:
        group_uid = _safe_str(product.canonical_product_id)
        source = self._PRODUCT_GROUP_SOURCE
        if group_uid is None:
            return
        if product.id is None:
            session.flush([product])
        if product.id is None:
            return

        product_id = int(product.id)
        existing = self._get_cached_product_group_row(
            session,
            group_uid=group_uid,
            product_id=product_id,
            source=source,
        )
        if existing is not None:
            return

        row = _CatalogProductGroup(
            group_uid=group_uid,
            product_id=product_id,
            source=source,
            created_at=self._to_utc(product.created_at),
        )
        session.add(row)
        self._cache_product_group_row(
            session,
            group_uid=group_uid,
            product_id=product_id,
            source=source,
            row=row,
        )

    @staticmethod
    def _iter_asset_urls(record: NormalizedProductRecord) -> list[str]:
        return list(record.image_urls)

    @staticmethod
    def _has_any_assets(record: NormalizedProductRecord) -> bool:
        return bool(CatalogRepository._iter_asset_urls(record))

    def _replace_product_assets(
        self,
        session: Session,
        product_id: int,
        record: NormalizedProductRecord,
        *,
        now: datetime,
    ) -> None:
        if self._asset_batch_mode_enabled(session):
            buffered = session.info.setdefault("_catalog_asset_replace_buffer", {})
            if isinstance(buffered, dict):
                buffered[int(product_id)] = self._build_product_asset_rows(
                    product_id=int(product_id),
                    record=record,
                    now=now,
                )
            LOGGER.debug("Catalog product assets buffered: product_id=%s", product_id)
            return

        session.execute(
            delete(_CatalogProductAsset).where(_CatalogProductAsset.product_id == int(product_id))
        )
        rows = self._build_product_asset_rows(
            product_id=int(product_id),
            record=record,
            now=now,
        )
        if rows:
            session.execute(insert(_CatalogProductAsset), rows)
        LOGGER.debug("Catalog product assets replaced: product_id=%s rows=%s", product_id, len(rows))

    def _build_product_asset_rows(
        self,
        *,
        product_id: int,
        record: NormalizedProductRecord,
        now: datetime,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for idx, url in enumerate(self._iter_asset_urls(record)):
            size = record.image_sizes[idx] if idx < len(record.image_sizes) else None
            fingerprint = record.image_fingerprints[idx] if idx < len(record.image_fingerprints) else None
            rows.append(
                {
                    "product_id": int(product_id),
                    "sort_order": idx,
                    "url": str(url),
                    "size": size,
                    "fingerprint": fingerprint,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        return rows

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
    def _normalized_brand_value(brand_normalized: object, brand: object) -> str | None:
        source = brand if not _is_missing(brand) else brand_normalized
        token = _safe_str(source)
        if token is None:
            return None
        lowered = token.lower()
        return lowered or None

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

    @staticmethod
    def _to_date(value: object) -> date | None:
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        token = _safe_str(value)
        if token is None:
            return None
        try:
            return date.fromisoformat(token)
        except ValueError:
            try:
                return datetime.fromisoformat(token.replace("Z", "+00:00")).date()
            except ValueError:
                return None

    @staticmethod
    def _to_bool(value: object) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if value == 1:
                return True
            if value == 0:
                return False
            return None
        token = _safe_str(value)
        if token is None:
            return None
        lowered = token.lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
        return None

    def _source_payload(self, record: NormalizedProductRecord) -> dict[str, Any]:
        raw_payload = record.source_payload if isinstance(record.source_payload, dict) else {}
        normalized = self._to_json_safe(raw_payload)
        return normalized if isinstance(normalized, dict) else {}

    @classmethod
    def _to_json_safe(cls, value: object) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, datetime):
            return cls._to_utc(value).isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for key, item in value.items():
                out[str(key)] = cls._to_json_safe(item)
            return out
        if isinstance(value, (list, tuple, set)):
            return [cls._to_json_safe(item) for item in value]
        return str(value)


class CatalogSQLiteRepository(CatalogRepository):
    def __init__(
        self,
        db_path: str | Path,
        *,
        storage_repository: StorageRepository | None = None,
        validate_schema: bool = True,
    ) -> None:
        path = Path(db_path)
        if path.parent and not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        super().__init__(
            f"sqlite:///{path.resolve()}",
            storage_repository=storage_repository,
            validate_schema=validate_schema,
        )
