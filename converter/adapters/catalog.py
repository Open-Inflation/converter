from __future__ import annotations

import hashlib
import json
import logging
from time import sleep
from time import monotonic
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from sqlalchemy import (
    create_engine,
    delete,
    insert,
    select,
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
    _CatalogImageFingerprint,
    _CatalogIngestStageAsset,
    _CatalogIngestStageCategory,
    _CatalogIngestStageProduct,
    _CatalogProduct,
    _CatalogProductAsset,
    _CatalogProductCategoryLink,
    _CatalogProductSnapshot,
    _CatalogSnapshotEvent,
    _CatalogSnapshotAvailableCount,
    _CatalogProductSource,
    _CatalogSettlement,
    _CatalogSettlementGeodata,
    _CatalogStorageDeleteOutbox,
    _ConverterSyncState,
    _as_float,
    _is_missing,
    _safe_str,
    _utc_now,
)

LOGGER = logging.getLogger(__name__)


class CatalogRepository(_CatalogSchemaMigrationMixin):
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
        "composition_original",
        "composition_normalized",
        "package_quantity",
        "package_unit",
    )
    _RETRYABLE_MYSQL_OPERATIONAL_ERROR_CODES = {1205, 1213}
    _RETRYABLE_MYSQL_INTEGRITY_ERROR_CODES: set[int] = set()
    _TXN_RETRY_ATTEMPTS = 5
    _TXN_RETRY_BASE_DELAY_SEC = 0.2
    _TXN_RETRY_MAX_DELAY_SEC = 2.0

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
        self._snapshot_fk_supported = True
        _CatalogBase.metadata.create_all(self._engine)
        if validate_schema:
            self._validate_catalog_products_schema()
        LOGGER.info(
            "Catalog repository initialized: dialect=%s storage_delete_enabled=%s",
            self._engine.dialect.name,
            self._storage_repository is not None,
        )

    def migrate_schema(self) -> None:
        self._ensure_snapshot_interval_schema()
        self._ensure_product_sources_schema()
        self._ensure_snapshot_event_schema()
        self._ensure_snapshot_available_counts_schema()
        self._enforce_snapshot_contract_schema()
        self._ensure_catalog_products_constraints()
        self._validate_catalog_products_schema()
        LOGGER.info("Catalog schema migration completed: dialect=%s", self._engine.dialect.name)

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

    def upsert_many_with_cursor(
        self,
        records: list[NormalizedProductRecord],
        *,
        parser_name: str,
        cursor_ingested_at: str,
        cursor_product_id: int,
    ) -> None:
        started_at = monotonic()
        LOGGER.debug(
            "Catalog upsert_many_with_cursor started: records=%s parser=%s cursor_ingested_at=%s cursor_product_id=%s",
            len(records),
            parser_name,
            cursor_ingested_at,
            cursor_product_id,
        )

        def _work(session: Session) -> None:
            if records:
                self._upsert_many_in_session(session, records)
            self._set_receiver_cursor_in_session(
                session,
                parser_name,
                ingested_at=cursor_ingested_at,
                product_id=cursor_product_id,
            )

        self._run_write_transaction(
            _work,
            operation_name="upsert_many_with_cursor",
        )
        LOGGER.debug(
            "Catalog upsert_many_with_cursor finished: records=%s parser=%s elapsed_sec=%.3f",
            len(records),
            parser_name,
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
            "Catalog apply_chunk started: parser=%s chunk_id=%s records=%s cursor_ingested_at=%s cursor_product_id=%s",
            chunk.parser_name,
            chunk.chunk_id,
            len(chunk.records),
            chunk.cursor_ingested_at,
            chunk.cursor_product_id,
        )

        def _work(session: Session) -> None:
            self._stage_chunk_in_session(session, chunk)
            for record in chunk.records:
                counters["upserted_products"] += 1
                canonical_product_id = self._resolve_canonical_product_id(session, record)
                record.canonical_product_id = canonical_product_id

                self._apply_persistent_image_dedup(session, record)
                payload = self._source_payload(record)
                snapshot_fingerprint = self._snapshot_content_fingerprint(record, payload=payload)
                source_event_uid = self._source_event_uid(record, payload=payload)

                touched_snapshot = self._touch_latest_snapshot_if_unchanged(
                    session,
                    record,
                    snapshot_fingerprint=snapshot_fingerprint,
                )
                settlement = self._upsert_settlement(session, record, payload=payload)
                categories = self._upsert_categories(session, record, payload=payload)

                if touched_snapshot:
                    counters["reused_snapshots"] += 1
                    self._update_source_fingerprint_in_session(
                        session,
                        record=record,
                        snapshot_fingerprint=snapshot_fingerprint,
                    )
                else:
                    snapshot, inserted = self._insert_product_snapshot(
                        session,
                        record,
                        payload=payload,
                        snapshot_fingerprint=snapshot_fingerprint,
                        source_event_uid=source_event_uid,
                    )
                    if inserted:
                        counters["inserted_snapshots"] += 1
                    self._link_snapshot_categories(session, snapshot=snapshot, categories=categories)
                    self._upsert_product_source(
                        session,
                        record,
                        snapshot=snapshot,
                        snapshot_fingerprint=snapshot_fingerprint,
                    )

                self._append_settlement_geodata(
                    session,
                    settlement=settlement,
                    record=record,
                    payload=payload,
                )
                self._upsert_product_row(
                    session,
                    record,
                    settlement=settlement,
                    categories=categories,
                )

            self._set_receiver_cursor_in_session(
                session,
                chunk.parser_name,
                ingested_at=chunk.cursor_ingested_at,
                product_id=chunk.cursor_product_id,
            )
            self._clear_stage_chunk_in_session(session, chunk.chunk_id)

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

    def get_receiver_cursor(self, parser_name: str) -> tuple[str | None, int | None]:
        with self._session_factory() as session:
            key = self._cursor_key(parser_name)
            row = session.get(_ConverterSyncState, key)
            if row is None:
                LOGGER.debug("Catalog cursor missing: parser=%s", parser_name)
                return None, None

            token = _safe_str(row.value)
            if token is None:
                LOGGER.debug("Catalog cursor invalid empty value: parser=%s", parser_name)
                return None, None

            if "\t" not in token:
                LOGGER.debug("Catalog cursor invalid format: parser=%s value=%s", parser_name, token)
                return None, None
            ingested_at_raw, product_id_raw = token.rsplit("\t", 1)
            ingested_at = _safe_str(ingested_at_raw)
            product_id = self._to_int(product_id_raw)
            LOGGER.debug(
                "Catalog cursor loaded: parser=%s ingested_at=%s product_id=%s",
                parser_name,
                ingested_at,
                product_id,
            )
            return ingested_at, product_id

    def set_receiver_cursor(
        self,
        parser_name: str,
        *,
        ingested_at: str,
        product_id: int,
    ) -> None:
        LOGGER.debug(
            "Catalog set_receiver_cursor requested: parser=%s ingested_at=%s product_id=%s",
            parser_name,
            ingested_at,
            product_id,
        )

        def _work(session: Session) -> None:
            self._set_receiver_cursor_in_session(
                session,
                parser_name,
                ingested_at=ingested_at,
                product_id=product_id,
            )

        self._run_write_transaction(
            _work,
            operation_name="set_receiver_cursor",
        )

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
                code = self._extract_mysql_error_code(exc)
                is_retryable_operational = (
                    isinstance(exc, OperationalError)
                    and code in self._RETRYABLE_MYSQL_OPERATIONAL_ERROR_CODES
                )
                is_retryable_integrity = (
                    isinstance(exc, IntegrityError)
                    and code in self._RETRYABLE_MYSQL_INTEGRITY_ERROR_CODES
                )
                if (not is_retryable_operational and not is_retryable_integrity) or (
                    attempt >= self._TXN_RETRY_ATTEMPTS
                ):
                    raise

                delay = min(
                    self._TXN_RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)),
                    self._TXN_RETRY_MAX_DELAY_SEC,
                )
                LOGGER.warning(
                    "Transient DB error on %s (error_type=%s, mysql_code=%s, attempt=%s/%s). Retrying in %.2fs.",
                    operation_name,
                    type(exc).__name__,
                    code,
                    attempt,
                    self._TXN_RETRY_ATTEMPTS,
                    delay,
                )
                sleep(delay)

    @staticmethod
    def _extract_mysql_error_code(exc: Exception) -> int | None:
        original = getattr(exc, "orig", None)
        args = getattr(original, "args", None)
        if args is None:
            args = getattr(exc, "args", None)
        if not isinstance(args, tuple) or not args:
            return None
        code = args[0]
        if isinstance(code, int):
            return code
        if isinstance(code, str) and code.isdigit():
            return int(code)
        return None

    def _set_receiver_cursor_in_session(
        self,
        session: Session,
        parser_name: str,
        *,
        ingested_at: str,
        product_id: int,
    ) -> None:
        encoded = f"{ingested_at}\t{int(product_id)}"
        now = _utc_now()
        key = self._cursor_key(parser_name)
        row = session.get(_ConverterSyncState, key)
        if row is None:
            row = _ConverterSyncState(
                state_key=key,
                value=encoded,
                updated_at=now,
            )
            session.add(row)
            LOGGER.debug(
                "Catalog cursor created: parser=%s ingested_at=%s product_id=%s",
                parser_name,
                ingested_at,
                product_id,
            )
            return

        current_ingested_at: str | None = None
        current_product_id: int | None = None
        token = _safe_str(row.value)
        if token and "\t" in token:
            current_ingested_at_raw, current_product_id_raw = token.rsplit("\t", 1)
            current_ingested_at = _safe_str(current_ingested_at_raw)
            current_product_id = self._to_int(current_product_id_raw)

        if not self._is_cursor_after(
            new_ingested_at=ingested_at,
            new_product_id=product_id,
            current_ingested_at=current_ingested_at,
            current_product_id=current_product_id,
        ):
            LOGGER.debug(
                "Catalog cursor update skipped (non-monotonic): parser=%s current=(%s,%s) new=(%s,%s)",
                parser_name,
                current_ingested_at,
                current_product_id,
                ingested_at,
                product_id,
            )
            return

        row.value = encoded
        row.updated_at = now
        LOGGER.debug(
            "Catalog cursor updated: parser=%s ingested_at=%s product_id=%s",
            parser_name,
            ingested_at,
            product_id,
        )

    @staticmethod
    def _is_cursor_after(
        *,
        new_ingested_at: str,
        new_product_id: int,
        current_ingested_at: str | None,
        current_product_id: int | None,
    ) -> bool:
        if current_ingested_at is None:
            return True
        if new_ingested_at > current_ingested_at:
            return True
        if new_ingested_at < current_ingested_at:
            return False
        return int(new_product_id) > int(current_product_id or 0)

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
            snapshot_fingerprint = self._snapshot_content_fingerprint(record, payload=payload)
            source_event_uid = self._source_event_uid(record, payload=payload)

            touched_snapshot = self._touch_latest_snapshot_if_unchanged(
                session,
                record,
                snapshot_fingerprint=snapshot_fingerprint,
            )

            settlement = self._upsert_settlement(session, record, payload=payload)
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
                    snapshot_fingerprint=snapshot_fingerprint,
                    source_event_uid=source_event_uid,
                )
                self._link_snapshot_categories(session, snapshot=snapshot, categories=categories)
                self._upsert_product_source(
                    session,
                    record,
                    snapshot=snapshot,
                    snapshot_fingerprint=snapshot_fingerprint,
                )
            self._append_settlement_geodata(
                session,
                settlement=settlement,
                record=record,
                payload=payload,
            )
            self._upsert_product_row(
                session,
                record,
                settlement=settlement,
                categories=categories,
            )
        LOGGER.debug("Catalog session upsert completed: records=%s", len(records))

    def _stage_chunk_in_session(self, session: Session, chunk: SyncChunkV2) -> None:
        self._clear_stage_chunk_in_session(session, chunk.chunk_id)

        now = _utc_now()
        product_rows: list[dict[str, Any]] = []
        asset_rows: list[dict[str, Any]] = []
        category_rows: list[dict[str, Any]] = []

        for row_no, record in enumerate(chunk.records):
            payload = self._source_payload(record)
            source_id = self._source_id(record)
            product_rows.append(
                {
                    "chunk_id": chunk.chunk_id,
                    "row_no": int(row_no),
                    "parser_name": record.parser_name,
                    "source_id": source_id,
                    "canonical_product_id": _safe_str(record.canonical_product_id),
                    "content_fingerprint": self._snapshot_content_fingerprint(record, payload=payload),
                    "source_event_uid": self._source_event_uid(record, payload=payload),
                    "observed_at": self._to_utc(record.observed_at),
                    "receiver_product_id": self._to_int(payload.get("receiver_product_id")),
                    "receiver_artifact_id": self._to_int(payload.get("receiver_artifact_id")),
                    "created_at": now,
                }
            )

            for asset_kind, values in self._iter_asset_values(record):
                for idx, value in enumerate(values):
                    asset_rows.append(
                        {
                            "chunk_id": chunk.chunk_id,
                            "parser_name": record.parser_name,
                            "source_id": source_id,
                            "asset_kind": asset_kind,
                            "sort_order": int(idx),
                            "value": str(value),
                            "created_at": now,
                        }
                    )

            candidates = self._extract_category_candidates(record, payload=payload)
            for idx, item in enumerate(candidates):
                category_rows.append(
                    {
                        "chunk_id": chunk.chunk_id,
                        "parser_name": record.parser_name,
                        "source_id": source_id,
                        "uid": _safe_str(item.get("uid")),
                        "title": _safe_str(item.get("title")),
                        "parent_uid": _safe_str(item.get("parent_uid")),
                        "depth": self._to_int(item.get("depth")),
                        "sort_order": self._to_int(item.get("sort_order")) or int(idx),
                        "alias": _safe_str(item.get("alias")),
                        "created_at": now,
                    }
                )

        if product_rows:
            session.execute(insert(_CatalogIngestStageProduct), product_rows)
        if asset_rows:
            session.execute(insert(_CatalogIngestStageAsset), asset_rows)
        if category_rows:
            session.execute(insert(_CatalogIngestStageCategory), category_rows)
        LOGGER.debug(
            "Catalog chunk staged: chunk_id=%s products=%s assets=%s categories=%s",
            chunk.chunk_id,
            len(product_rows),
            len(asset_rows),
            len(category_rows),
        )

    def _clear_stage_chunk_in_session(self, session: Session, chunk_id: str) -> None:
        session.execute(
            delete(_CatalogIngestStageAsset).where(_CatalogIngestStageAsset.chunk_id == chunk_id)
        )
        session.execute(
            delete(_CatalogIngestStageCategory).where(_CatalogIngestStageCategory.chunk_id == chunk_id)
        )
        session.execute(
            delete(_CatalogIngestStageProduct).where(_CatalogIngestStageProduct.chunk_id == chunk_id)
        )

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
    ) -> str:
        source_id = self._source_id(record)
        fingerprint_input = {
            "parser_name": record.parser_name.strip().lower(),
            "source_id": source_id,
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
    ) -> str:
        receiver_product_id = self._to_int(payload.get("receiver_product_id")) or 0
        receiver_artifact_id = self._to_int(payload.get("receiver_artifact_id")) or 0
        seed = "|".join(
            (
                record.parser_name.strip().lower(),
                self._source_id(record),
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
        row = session.get(_CatalogProductSource, (parser_name, source_id))
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
    ) -> bool:
        parser_name = record.parser_name.strip().lower()
        source_id = self._source_id(record)
        source = session.get(_CatalogProductSource, (parser_name, source_id))
        if source is None or source.latest_snapshot_id is None:
            return False

        latest_source_fingerprint = _safe_str(source.latest_content_fingerprint)
        if latest_source_fingerprint is not None and latest_source_fingerprint != snapshot_fingerprint:
            return False

        snapshot_event = session.get(_CatalogSnapshotEvent, int(source.latest_snapshot_id))
        if snapshot_event is None:
            return False

        existing_fingerprint = _safe_str(snapshot_event.content_fingerprint)
        if existing_fingerprint is None or existing_fingerprint != snapshot_fingerprint:
            return False

        observed_at = self._to_utc(record.observed_at)
        now = _utc_now()

        if snapshot_event.valid_from_at is None:
            snapshot_event.valid_from_at = snapshot_event.observed_at
        if snapshot_event.valid_to_at is None:
            snapshot_event.valid_to_at = snapshot_event.observed_at
        snapshot_event.valid_to_at = self._max_datetime(snapshot_event.valid_to_at, observed_at)
        snapshot_event.observed_at = self._max_datetime(snapshot_event.observed_at, observed_at)
        if snapshot_event.id is not None:
            self._upsert_snapshot_available_count(
                session,
                snapshot_id=int(snapshot_event.id),
                available_count=record.available_count,
                created_at=observed_at,
            )

        if _safe_str(source.canonical_product_id):
            record.canonical_product_id = source.canonical_product_id
        else:
            source.canonical_product_id = record.canonical_product_id or source.canonical_product_id
        source.last_seen_at = self._max_datetime(source.last_seen_at, observed_at)
        source.latest_content_fingerprint = snapshot_fingerprint
        source.updated_at = now

        projection = session.scalar(
            select(_CatalogProduct).where(
                _CatalogProduct.parser_name == record.parser_name,
                _CatalogProduct.source_id == source_id,
            )
        )
        if projection is not None:
            projection.observed_at = self._max_datetime(projection.observed_at, observed_at)
            projection.updated_at = now

        LOGGER.debug(
            "Catalog snapshot reused: parser=%s source_id=%s snapshot_id=%s valid_from_at=%s valid_to_at=%s",
            record.parser_name,
            source_id,
            snapshot_event.id,
            snapshot_event.valid_from_at,
            snapshot_event.valid_to_at,
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
        if dialect_name == "mysql":
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            stmt = mysql_insert(_CatalogIdentityMap).values(**values).on_duplicate_key_update(
                updated_at=updated_at
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
            return row

        row = session.get(_CatalogIdentityMap, (parser_name, identity_type, identity_value))
        if row is None:
            row = _CatalogIdentityMap(**values)
            session.add(row)
            return row

        if _is_missing(row.canonical_product_id):
            row.canonical_product_id = canonical_product_id
        row.updated_at = updated_at
        return row

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
        original_image_count = len(record.image_urls)
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
            row = self._get_image_fingerprint_row(session, fingerprint)

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

        duplicates_to_delete = list(dict.fromkeys(duplicate_urls))
        record.image_urls = unique_urls
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

    @staticmethod
    def _get_image_fingerprint_row(
        session: Session,
        fingerprint: str,
    ) -> _CatalogImageFingerprint | None:
        for pending in session.new:
            if not isinstance(pending, _CatalogImageFingerprint):
                continue
            if pending.fingerprint == fingerprint:
                return pending
        return session.get(_CatalogImageFingerprint, fingerprint)

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
        snapshot_fingerprint: str,
        source_event_uid: str | None,
    ) -> tuple[_CatalogSnapshotEvent, bool]:
        now = _utc_now()
        observed_at = self._to_utc(record.observed_at)
        event_uid = _safe_str(source_event_uid)

        if event_uid is not None:
            existing = session.scalar(
                select(_CatalogSnapshotEvent).where(_CatalogSnapshotEvent.source_event_uid == event_uid)
            )
            if existing is not None:
                if existing.valid_to_at is None:
                    existing.valid_to_at = existing.observed_at
                existing.valid_to_at = self._max_datetime(existing.valid_to_at, observed_at)
                existing.observed_at = self._max_datetime(existing.observed_at, observed_at)
                if existing.id is not None:
                    self._upsert_price_snapshot(
                        session,
                        snapshot_id=int(existing.id),
                        price=record.price,
                        discount_price=record.discount_price,
                        loyal_price=record.loyal_price,
                        price_unit=record.price_unit,
                    )
                    self._upsert_snapshot_available_count(
                        session,
                        snapshot_id=int(existing.id),
                        available_count=record.available_count,
                        created_at=observed_at,
                    )
                return existing, False

        snapshot = _CatalogSnapshotEvent(
            canonical_product_id=record.canonical_product_id or str(uuid4()),
            parser_name=record.parser_name,
            source_id=self._source_id(record),
            source_run_id=_safe_str(payload.get("receiver_run_id")),
            receiver_product_id=self._to_int(payload.get("receiver_product_id")),
            receiver_artifact_id=self._to_int(payload.get("receiver_artifact_id")),
            receiver_sort_order=self._to_int(payload.get("receiver_sort_order")),
            source_event_uid=event_uid,
            content_fingerprint=snapshot_fingerprint,
            valid_from_at=observed_at,
            valid_to_at=observed_at,
            observed_at=observed_at,
            created_at=now,
        )
        session.add(snapshot)
        session.flush([snapshot])
        if snapshot.id is not None:
            self._upsert_price_snapshot(
                session,
                snapshot_id=int(snapshot.id),
                price=record.price,
                discount_price=record.discount_price,
                loyal_price=record.loyal_price,
                price_unit=record.price_unit,
            )
            self._upsert_snapshot_available_count(
                session,
                snapshot_id=int(snapshot.id),
                available_count=record.available_count,
                created_at=observed_at,
            )
        return snapshot, True

    def _upsert_product_source(
        self,
        session: Session,
        record: NormalizedProductRecord,
        *,
        snapshot: _CatalogSnapshotEvent,
        snapshot_fingerprint: str | None = None,
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
                latest_content_fingerprint=snapshot_fingerprint,
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
        if snapshot_fingerprint is not None:
            row.latest_content_fingerprint = snapshot_fingerprint
        row.last_seen_at = self._max_datetime(row.last_seen_at, observed_at)
        row.updated_at = now

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

        LOGGER.debug(
            "Catalog settlement updated: parser=%s source_id=%s settlement_id=%s geo_key=%s",
            record.parser_name,
            self._source_id(record),
            row.id,
            key,
        )
        return row

    def _append_settlement_geodata(
        self,
        session: Session,
        *,
        settlement: _CatalogSettlement | None,
        record: NormalizedProductRecord,
        payload: dict[str, Any],
    ) -> None:
        if settlement is None or settlement.id is None:
            return

        latitude, longitude = self._extract_geo_coordinates(payload)
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
            receiver_artifact_id=self._to_int(payload.get("receiver_artifact_id")),
            receiver_product_id=self._to_int(payload.get("receiver_product_id")),
            created_at=_utc_now(),
        )
        session.add(row)
        LOGGER.debug(
            "Catalog settlement geodata appended: settlement_id=%s latitude=%.8f longitude=%.8f",
            settlement.id,
            latitude,
            longitude,
        )

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

    def _link_snapshot_categories(
        self,
        session: Session,
        *,
        snapshot: _CatalogSnapshotEvent,
        categories: list[tuple[_CatalogCategory, int]],
    ) -> None:
        if snapshot.id is None or not categories:
            return

        if any(category.id is None for category, _ in categories):
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
                title_original=record.title_original,
                title_normalized_no_stopwords=record.title_normalized_no_stopwords,
                brand=record.brand,
                source_page_url=record.source_page_url,
                description=record.description,
                producer_name=record.producer_name,
                producer_country=record.producer_country,
                expiration_date_in_days=record.expiration_date_in_days,
                rating=record.rating,
                reviews_count=record.reviews_count,
                price=record.price,
                discount_price=record.discount_price,
                loyal_price=record.loyal_price,
                price_unit=record.price_unit,
                adult=record.adult,
                is_new=record.is_new,
                promo=record.promo,
                season=record.season,
                hit=record.hit,
                data_matrix=record.data_matrix,
                unit=record.unit,
                available_count=record.available_count,
                package_quantity=record.package_quantity,
                package_unit=record.package_unit,
                primary_category_id=primary_category_id,
                settlement_id=settlement_id,
                composition_original=record.composition_original,
                composition_normalized=record.composition_normalized,
                observed_at=self._to_utc(record.observed_at),
            )
            session.add(existing)
            session.flush([existing])
            if existing.id is not None and self._has_any_assets(record):
                self._replace_product_assets(session, int(existing.id), record, now=now)
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
        if not _is_missing(record.price):
            existing.price = record.price
        if not _is_missing(record.discount_price):
            existing.discount_price = record.discount_price
        if not _is_missing(record.loyal_price):
            existing.loyal_price = record.loyal_price
        if not _is_missing(record.price_unit):
            existing.price_unit = record.price_unit
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

        if not _is_missing(record.composition_original):
            existing.composition_original = record.composition_original
        if not _is_missing(record.composition_normalized):
            existing.composition_normalized = record.composition_normalized

        if existing.id is not None and record.image_urls:
            self._replace_product_assets(session, int(existing.id), record, now=now)

        existing.observed_at = self._max_datetime(existing.observed_at, self._to_utc(record.observed_at))
        LOGGER.debug(
            "Catalog product updated: parser=%s source_id=%s product_id=%s canonical_product_id=%s",
            record.parser_name,
            source_id,
            existing.id,
            existing.canonical_product_id,
        )

    @staticmethod
    def _iter_asset_values(record: NormalizedProductRecord) -> list[tuple[str, list[str]]]:
        return [
            ("image_url", list(record.image_urls)),
            ("duplicate_image_url", list(record.duplicate_image_urls)),
            ("image_fingerprint", list(record.image_fingerprints)),
        ]

    @staticmethod
    def _has_any_assets(record: NormalizedProductRecord) -> bool:
        for _, values in CatalogRepository._iter_asset_values(record):
            if values:
                return True
        return False

    def _replace_product_assets(
        self,
        session: Session,
        product_id: int,
        record: NormalizedProductRecord,
        *,
        now: datetime,
    ) -> None:
        session.execute(
            delete(_CatalogProductAsset).where(_CatalogProductAsset.product_id == int(product_id))
        )
        rows: list[dict[str, Any]] = []
        for asset_kind, values in self._iter_asset_values(record):
            for idx, value in enumerate(values):
                rows.append(
                    {
                        "product_id": int(product_id),
                        "asset_kind": asset_kind,
                        "sort_order": idx,
                        "value": str(value),
                        "created_at": now,
                        "updated_at": now,
                    }
                )
        if rows:
            session.execute(insert(_CatalogProductAsset), rows)
        LOGGER.debug("Catalog product assets replaced: product_id=%s rows=%s", product_id, len(rows))

    def _upsert_price_snapshot(
        self,
        session: Session,
        snapshot_id: int,
        *,
        price: float | None,
        discount_price: float | None,
        loyal_price: float | None,
        price_unit: str | None,
    ) -> None:
        row = session.get(_CatalogProductSnapshot, int(snapshot_id))
        if row is None:
            session.add(
                _CatalogProductSnapshot(
                    id=int(snapshot_id),
                    price=price,
                    discount_price=discount_price,
                    loyal_price=loyal_price,
                    price_unit=price_unit,
                )
            )
            return
        row.price = price
        row.discount_price = discount_price
        row.loyal_price = loyal_price
        row.price_unit = price_unit

    def _upsert_snapshot_available_count(
        self,
        session: Session,
        snapshot_id: int,
        *,
        available_count: float | None,
        created_at: datetime,
    ) -> None:
        row = session.get(_CatalogSnapshotAvailableCount, int(snapshot_id))
        if row is None:
            session.add(
                _CatalogSnapshotAvailableCount(
                    snapshot_id=int(snapshot_id),
                    available_count=available_count,
                    created_at=created_at,
                )
            )
            return
        row.available_count = available_count

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
