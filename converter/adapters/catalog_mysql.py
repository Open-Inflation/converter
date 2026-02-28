from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from converter.core.models import NormalizedProductRecord

from .mysql_common import parse_mysql_dsn


def _import_pymysql() -> Any:
    try:
        import pymysql  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "pymysql is required for MySQL repositories. Install with: pip install pymysql"
        ) from exc
    return pymysql


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _mysql_dt(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


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


class CatalogMySQLRepository:
    """
    Persistent sink for normalized catalog products in MySQL.
    """

    BACKFILL_FIELDS = (
        "brand",
        "category_normalized",
        "geo_normalized",
        "composition_normalized",
        "package_quantity",
        "package_unit",
    )

    def __init__(
        self,
        connect_kwargs: dict[str, object],
        *,
        connect_fn: Callable[..., Any] | None = None,
    ) -> None:
        self._connect_kwargs = dict(connect_kwargs)

        if connect_fn is None:
            pymysql = _import_pymysql()
            connect_fn = pymysql.connect
            self._dict_cursor_cls = pymysql.cursors.DictCursor
        else:
            self._dict_cursor_cls = None

        self._connect_fn = connect_fn
        self._ensure_schema()

    @classmethod
    def from_dsn(cls, dsn: str) -> "CatalogMySQLRepository":
        return cls(parse_mysql_dsn(dsn))

    def upsert_many(self, records: list[NormalizedProductRecord]) -> None:
        if not records:
            return

        conn = self._connect()
        try:
            for record in records:
                canonical_product_id = self._resolve_canonical_product_id(conn, record)
                record.canonical_product_id = canonical_product_id

                self._apply_persistent_image_dedup(conn, record)
                self._apply_backfill(conn, record)
                self._upsert_product_row(conn, record)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_receiver_cursor(self, parser_name: str) -> tuple[str | None, int | None]:
        conn = self._connect()
        try:
            row = self._fetch_one(
                conn,
                "SELECT value_json FROM converter_sync_state WHERE state_key = %s",
                [self._cursor_key(parser_name)],
            )
            if row is None:
                return None, None

            raw_value = row.get("value_json")
            if raw_value is None:
                return None, None

            if isinstance(raw_value, str):
                parsed = json.loads(raw_value)
            else:
                parsed = raw_value

            if not isinstance(parsed, dict):
                return None, None

            ingested_at = _safe_str(parsed.get("ingested_at"))
            product_id_raw = parsed.get("product_id")
            product_id = int(product_id_raw) if isinstance(product_id_raw, int) or str(product_id_raw).isdigit() else None
            return ingested_at, product_id
        finally:
            conn.close()

    def set_receiver_cursor(self, parser_name: str, *, ingested_at: str, product_id: int) -> None:
        payload = json.dumps(
            {
                "ingested_at": ingested_at,
                "product_id": int(product_id),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        now = _mysql_dt(_utc_now())

        conn = self._connect()
        try:
            self._execute(
                conn,
                """
                INSERT INTO converter_sync_state(state_key, value_json, updated_at)
                VALUES (%s, CAST(%s AS JSON), %s)
                ON DUPLICATE KEY UPDATE
                    value_json = VALUES(value_json),
                    updated_at = VALUES(updated_at)
                """,
                [self._cursor_key(parser_name), payload, now],
            )
            conn.commit()
        finally:
            conn.close()

    def _resolve_canonical_product_id(self, conn: Any, record: NormalizedProductRecord) -> str:
        parser_name = record.parser_name.strip().lower()
        chosen_id: str | None = None

        for identity_type, identity_value in record.identity_candidates():
            row = self._fetch_one(
                conn,
                """
                SELECT canonical_product_id
                FROM catalog_identity_map
                WHERE parser_name = %s AND identity_type = %s AND identity_value = %s
                """,
                [parser_name, identity_type, identity_value],
            )
            if row is not None and _safe_str(row.get("canonical_product_id")):
                chosen_id = str(row["canonical_product_id"])
                break

        fallback_identity = self._fallback_identity_value(record)
        if chosen_id is None and fallback_identity is not None:
            row = self._fetch_one(
                conn,
                """
                SELECT canonical_product_id
                FROM catalog_identity_map
                WHERE parser_name = %s AND identity_type = 'normalized_name' AND identity_value = %s
                """,
                [parser_name, fallback_identity],
            )
            if row is not None and _safe_str(row.get("canonical_product_id")):
                chosen_id = str(row["canonical_product_id"])

        if chosen_id is None:
            chosen_id = str(uuid4())

        now = _mysql_dt(_utc_now())
        identities = list(record.identity_candidates())
        if fallback_identity is not None:
            identities.append(("normalized_name", fallback_identity))

        for identity_type, identity_value in identities:
            self._execute(
                conn,
                """
                INSERT INTO catalog_identity_map(
                    parser_name,
                    identity_type,
                    identity_value,
                    canonical_product_id,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    canonical_product_id = VALUES(canonical_product_id),
                    updated_at = VALUES(updated_at)
                """,
                [parser_name, identity_type, identity_value, chosen_id, now],
            )

        return chosen_id

    @staticmethod
    def _fallback_identity_value(record: NormalizedProductRecord) -> str | None:
        fallback = _safe_str(record.title_normalized_no_stopwords)
        if fallback:
            return fallback
        return _safe_str(record.title_normalized)

    def _apply_persistent_image_dedup(self, conn: Any, record: NormalizedProductRecord) -> None:
        unique_urls: list[str] = []
        duplicate_urls: list[str] = []
        fingerprints: list[str] = []

        seen_in_record: set[str] = set()
        now = _mysql_dt(_utc_now())

        for raw_url in record.image_urls:
            url = raw_url.strip()
            if not url:
                continue

            fingerprint = hashlib.sha256(url.encode("utf-8")).hexdigest()
            row = self._fetch_one(
                conn,
                "SELECT canonical_url FROM catalog_image_fingerprints WHERE fingerprint = %s",
                [fingerprint],
            )
            if row is None:
                canonical_url = url
                self._execute(
                    conn,
                    """
                    INSERT INTO catalog_image_fingerprints(fingerprint, canonical_url, created_at, updated_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    [fingerprint, canonical_url, now, now],
                )
            else:
                canonical_url = str(row["canonical_url"])
                self._execute(
                    conn,
                    "UPDATE catalog_image_fingerprints SET updated_at = %s WHERE fingerprint = %s",
                    [now, fingerprint],
                )
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

    def _apply_backfill(self, conn: Any, record: NormalizedProductRecord) -> None:
        canonical_product_id = _safe_str(record.canonical_product_id)
        if canonical_product_id is None:
            return

        observed_at = _mysql_dt(record.observed_at if isinstance(record.observed_at, datetime) else _utc_now())

        for field_name in self.BACKFILL_FIELDS:
            current_value = getattr(record, field_name)
            if not _is_missing(current_value):
                continue

            replacement = self._lookup_backfill_value(
                conn,
                canonical_product_id=canonical_product_id,
                field_name=field_name,
                observed_at=observed_at,
            )
            if replacement is not None:
                setattr(record, field_name, replacement)

    def _lookup_backfill_value(
        self,
        conn: Any,
        *,
        canonical_product_id: str,
        field_name: str,
        observed_at: datetime,
    ) -> object | None:
        if field_name not in self.BACKFILL_FIELDS:
            return None

        condition = f"{field_name} IS NOT NULL"
        if field_name in {"brand", "category_normalized", "geo_normalized", "composition_normalized", "package_unit"}:
            condition += f" AND TRIM({field_name}) <> ''"

        row = self._fetch_one(
            conn,
            f"""
            SELECT {field_name} AS value
            FROM catalog_products
            WHERE canonical_product_id = %s AND {condition}
            ORDER BY ABS(TIMESTAMPDIFF(SECOND, observed_at, %s)) ASC
            LIMIT 1
            """,
            [canonical_product_id, observed_at],
        )
        if row is None:
            return None
        return row.get("value")

    def _upsert_product_row(self, conn: Any, record: NormalizedProductRecord) -> None:
        now = _mysql_dt(_utc_now())
        observed_at = _mysql_dt(record.observed_at)

        payload: dict[str, object] = {
            "canonical_product_id": record.canonical_product_id,
            "parser_name": record.parser_name,
            "source_id": self._source_id(record),
            "plu": record.plu,
            "sku": record.sku,
            "raw_title": record.raw_title,
            "title_original": record.title_original,
            "title_normalized": record.title_normalized,
            "title_original_no_stopwords": record.title_original_no_stopwords,
            "title_normalized_no_stopwords": record.title_normalized_no_stopwords,
            "brand": record.brand,
            "unit": record.unit,
            "available_count": record.available_count,
            "package_quantity": record.package_quantity,
            "package_unit": record.package_unit,
            "category_raw": record.category_raw,
            "category_normalized": record.category_normalized,
            "geo_raw": record.geo_raw,
            "geo_normalized": record.geo_normalized,
            "composition_raw": record.composition_raw,
            "composition_normalized": record.composition_normalized,
            "image_urls_json": json.dumps(record.image_urls, ensure_ascii=False, separators=(",", ":")),
            "duplicate_image_urls_json": json.dumps(record.duplicate_image_urls, ensure_ascii=False, separators=(",", ":")),
            "image_fingerprints_json": json.dumps(record.image_fingerprints, ensure_ascii=False, separators=(",", ":")),
            "observed_at": observed_at,
            "raw_payload_json": json.dumps(record.raw_payload, ensure_ascii=False, separators=(",", ":")),
            "created_at": now,
            "updated_at": now,
        }

        columns = ", ".join(payload.keys())
        placeholders = ", ".join(["%s"] * len(payload))
        updates = ", ".join(
            [
                f"{key} = VALUES({key})"
                for key in payload.keys()
                if key not in {"created_at", "parser_name", "source_id"}
            ]
        )

        self._execute(
            conn,
            f"""
            INSERT INTO catalog_products({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
                {updates},
                updated_at = VALUES(updated_at)
            """,
            list(payload.values()),
        )

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
        observed = _mysql_dt(record.observed_at).isoformat()
        return f"generated:{canonical}:{observed}"

    @staticmethod
    def _cursor_key(parser_name: str) -> str:
        return f"receiver_cursor:{parser_name.strip().lower()}"

    def _connect(self) -> Any:
        kwargs = dict(self._connect_kwargs)
        if self._dict_cursor_cls is not None:
            kwargs.setdefault("cursorclass", self._dict_cursor_cls)
        return self._connect_fn(**kwargs)

    @staticmethod
    def _execute(conn: Any, query: str, params: list[object] | tuple[object, ...] | None = None) -> None:
        with conn.cursor() as cursor:
            cursor.execute(query, params or [])

    @staticmethod
    def _fetch_one(conn: Any, query: str, params: list[object]) -> dict[str, object] | None:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
        if row is None:
            return None
        return dict(row)

    def _ensure_schema(self) -> None:
        conn = self._connect()
        statements = [
            """
            CREATE TABLE IF NOT EXISTS catalog_products (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                canonical_product_id VARCHAR(36) NOT NULL,
                parser_name VARCHAR(64) NOT NULL,
                source_id VARCHAR(255) NOT NULL,
                plu VARCHAR(128) NULL,
                sku VARCHAR(128) NULL,
                raw_title TEXT NOT NULL,
                title_original TEXT NOT NULL,
                title_normalized TEXT NOT NULL,
                title_original_no_stopwords TEXT NOT NULL,
                title_normalized_no_stopwords TEXT NOT NULL,
                brand VARCHAR(255) NULL,
                unit VARCHAR(32) NOT NULL,
                available_count DOUBLE NULL,
                package_quantity DOUBLE NULL,
                package_unit VARCHAR(32) NULL,
                category_raw TEXT NULL,
                category_normalized TEXT NULL,
                geo_raw TEXT NULL,
                geo_normalized TEXT NULL,
                composition_raw TEXT NULL,
                composition_normalized TEXT NULL,
                image_urls_json JSON NOT NULL,
                duplicate_image_urls_json JSON NOT NULL,
                image_fingerprints_json JSON NOT NULL,
                observed_at DATETIME(6) NOT NULL,
                raw_payload_json JSON NOT NULL,
                created_at DATETIME(6) NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                PRIMARY KEY(id),
                UNIQUE KEY uq_catalog_products_parser_source (parser_name, source_id),
                KEY ix_catalog_products_canonical_observed (canonical_product_id, observed_at),
                KEY ix_catalog_products_parser_plu (parser_name, plu),
                KEY ix_catalog_products_parser_sku (parser_name, sku)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_identity_map (
                parser_name VARCHAR(64) NOT NULL,
                identity_type VARCHAR(64) NOT NULL,
                identity_value VARCHAR(255) NOT NULL,
                canonical_product_id VARCHAR(36) NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                PRIMARY KEY(parser_name, identity_type, identity_value),
                KEY ix_catalog_identity_map_canonical (canonical_product_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS catalog_image_fingerprints (
                fingerprint CHAR(64) NOT NULL,
                canonical_url TEXT NOT NULL,
                created_at DATETIME(6) NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                PRIMARY KEY(fingerprint)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS converter_sync_state (
                state_key VARCHAR(191) NOT NULL,
                value_json JSON NOT NULL,
                updated_at DATETIME(6) NOT NULL,
                PRIMARY KEY(state_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
        ]

        try:
            for statement in statements:
                self._execute(conn, statement)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
