from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from converter.core.models import NormalizedProductRecord


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


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


class CatalogSQLiteRepository:
    """
    Persistent sink for normalized catalog products.

    Stores:
    - normalized product rows;
    - canonical product id mapping;
    - persistent image fingerprint map;
    - receiver sync cursor.
    """

    BACKFILL_FIELDS = (
        "brand",
        "category_normalized",
        "geo_normalized",
        "composition_normalized",
        "package_quantity",
        "package_unit",
    )

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        if self._db_path.parent and not self._db_path.parent.exists():
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def upsert_many(self, records: list[NormalizedProductRecord]) -> None:
        if not records:
            return

        conn = self._connect()
        try:
            conn.execute("BEGIN")
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
            key = self._cursor_key(parser_name)
            row = conn.execute(
                "SELECT value FROM converter_sync_state WHERE key = ?",
                [key],
            ).fetchone()
            if row is None:
                return None, None

            raw_value = _safe_str(row["value"])
            if raw_value is None:
                return None, None

            try:
                parsed = json.loads(raw_value)
            except json.JSONDecodeError:
                return None, None

            if not isinstance(parsed, dict):
                return None, None

            ingested_at = _safe_str(parsed.get("ingested_at"))
            product_id_raw = parsed.get("product_id")
            product_id = int(product_id_raw) if isinstance(product_id_raw, int) or str(product_id_raw).isdigit() else None
            return ingested_at, product_id
        finally:
            conn.close()

    def set_receiver_cursor(
        self,
        parser_name: str,
        *,
        ingested_at: str,
        product_id: int,
    ) -> None:
        payload = json.dumps(
            {
                "ingested_at": ingested_at,
                "product_id": int(product_id),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        now = _utc_now_iso()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO converter_sync_state(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                [self._cursor_key(parser_name), payload, now],
            )
            conn.commit()
        finally:
            conn.close()

    def _resolve_canonical_product_id(self, conn: sqlite3.Connection, record: NormalizedProductRecord) -> str:
        parser_name = record.parser_name.strip().lower()
        identity_keys = record.identity_candidates()

        chosen_id: str | None = None

        for identity_type, identity_value in identity_keys:
            row = conn.execute(
                """
                SELECT canonical_product_id
                FROM catalog_identity_map
                WHERE parser_name = ? AND identity_type = ? AND identity_value = ?
                """,
                [parser_name, identity_type, identity_value],
            ).fetchone()
            if row is not None and _safe_str(row["canonical_product_id"]):
                chosen_id = str(row["canonical_product_id"])
                break

        fallback_identity = self._fallback_identity_value(record)
        if chosen_id is None and fallback_identity is not None:
            row = conn.execute(
                """
                SELECT canonical_product_id
                FROM catalog_identity_map
                WHERE parser_name = ? AND identity_type = 'normalized_name' AND identity_value = ?
                """,
                [parser_name, fallback_identity],
            ).fetchone()
            if row is not None and _safe_str(row["canonical_product_id"]):
                chosen_id = str(row["canonical_product_id"])

        if chosen_id is None:
            chosen_id = str(uuid4())

        identity_values = list(identity_keys)
        if fallback_identity is not None:
            identity_values.append(("normalized_name", fallback_identity))

        now = _utc_now_iso()
        for identity_type, identity_value in identity_values:
            conn.execute(
                """
                INSERT INTO catalog_identity_map(
                    parser_name,
                    identity_type,
                    identity_value,
                    canonical_product_id,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(parser_name, identity_type, identity_value) DO UPDATE SET
                    canonical_product_id = excluded.canonical_product_id,
                    updated_at = excluded.updated_at
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

    def _apply_persistent_image_dedup(self, conn: sqlite3.Connection, record: NormalizedProductRecord) -> None:
        unique_urls: list[str] = []
        duplicate_urls: list[str] = []
        fingerprints: list[str] = []

        seen_in_record: set[str] = set()
        now = _utc_now_iso()

        for raw_url in record.image_urls:
            url = raw_url.strip()
            if not url:
                continue

            fingerprint = hashlib.sha256(url.encode("utf-8")).hexdigest()
            row = conn.execute(
                "SELECT canonical_url FROM catalog_image_fingerprints WHERE fingerprint = ?",
                [fingerprint],
            ).fetchone()

            if row is None:
                canonical_url = url
                conn.execute(
                    """
                    INSERT INTO catalog_image_fingerprints(fingerprint, canonical_url, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    [fingerprint, canonical_url, now, now],
                )
            else:
                canonical_url = str(row["canonical_url"])
                conn.execute(
                    """
                    UPDATE catalog_image_fingerprints
                    SET updated_at = ?
                    WHERE fingerprint = ?
                    """,
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

    def _apply_backfill(self, conn: sqlite3.Connection, record: NormalizedProductRecord) -> None:
        canonical_product_id = _safe_str(record.canonical_product_id)
        if canonical_product_id is None:
            return

        observed_at = self._observed_at_iso(record)
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
        conn: sqlite3.Connection,
        *,
        canonical_product_id: str,
        field_name: str,
        observed_at: str,
    ) -> object | None:
        if field_name not in self.BACKFILL_FIELDS:
            return None

        if field_name in {"package_quantity"}:
            condition = f"{field_name} IS NOT NULL"
        else:
            condition = f"{field_name} IS NOT NULL"
            if field_name in {"brand", "category_normalized", "geo_normalized", "composition_normalized", "package_unit"}:
                condition += f" AND TRIM({field_name}) <> ''"

        query = f"""
            SELECT {field_name} AS value
            FROM catalog_products
            WHERE canonical_product_id = ? AND {condition}
            ORDER BY ABS(julianday(observed_at) - julianday(?)) ASC
            LIMIT 1
        """

        row = conn.execute(query, [canonical_product_id, observed_at]).fetchone()
        if row is None:
            return None
        return row["value"]

    def _upsert_product_row(self, conn: sqlite3.Connection, record: NormalizedProductRecord) -> None:
        now = _utc_now_iso()
        source_id = self._source_id(record)

        payload = {
            "canonical_product_id": record.canonical_product_id,
            "parser_name": record.parser_name,
            "source_id": source_id,
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
            "observed_at": self._observed_at_iso(record),
            "raw_payload_json": json.dumps(record.raw_payload, ensure_ascii=False, separators=(",", ":")),
            "created_at": now,
            "updated_at": now,
        }

        columns = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        updates = ", ".join(
            [
                f"{key} = excluded.{key}"
                for key in payload.keys()
                if key not in {"created_at", "parser_name", "source_id"}
            ]
        )

        conn.execute(
            f"""
            INSERT INTO catalog_products({columns})
            VALUES ({placeholders})
            ON CONFLICT(parser_name, source_id) DO UPDATE SET
                {updates},
                updated_at = excluded.updated_at
            """,
            list(payload.values()),
        )

    @staticmethod
    def _observed_at_iso(record: NormalizedProductRecord) -> str:
        dt = record.observed_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

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
        return f"generated:{canonical}:{CatalogSQLiteRepository._observed_at_iso(record)}"

    @staticmethod
    def _cursor_key(parser_name: str) -> str:
        return f"receiver_cursor:{parser_name.strip().lower()}"

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS catalog_products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_product_id TEXT NOT NULL,
                    parser_name TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    plu TEXT,
                    sku TEXT,
                    raw_title TEXT NOT NULL,
                    title_original TEXT NOT NULL,
                    title_normalized TEXT NOT NULL,
                    title_original_no_stopwords TEXT NOT NULL,
                    title_normalized_no_stopwords TEXT NOT NULL,
                    brand TEXT,
                    unit TEXT NOT NULL,
                    available_count REAL,
                    package_quantity REAL,
                    package_unit TEXT,
                    category_raw TEXT,
                    category_normalized TEXT,
                    geo_raw TEXT,
                    geo_normalized TEXT,
                    composition_raw TEXT,
                    composition_normalized TEXT,
                    image_urls_json TEXT NOT NULL,
                    duplicate_image_urls_json TEXT NOT NULL,
                    image_fingerprints_json TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    raw_payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(parser_name, source_id)
                );

                CREATE INDEX IF NOT EXISTS ix_catalog_products_canonical_observed
                ON catalog_products(canonical_product_id, observed_at);

                CREATE INDEX IF NOT EXISTS ix_catalog_products_parser_plu
                ON catalog_products(parser_name, plu);

                CREATE INDEX IF NOT EXISTS ix_catalog_products_parser_sku
                ON catalog_products(parser_name, sku);

                CREATE TABLE IF NOT EXISTS catalog_identity_map (
                    parser_name TEXT NOT NULL,
                    identity_type TEXT NOT NULL,
                    identity_value TEXT NOT NULL,
                    canonical_product_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(parser_name, identity_type, identity_value)
                );

                CREATE INDEX IF NOT EXISTS ix_catalog_identity_map_canonical
                ON catalog_identity_map(canonical_product_id);

                CREATE TABLE IF NOT EXISTS catalog_image_fingerprints (
                    fingerprint TEXT PRIMARY KEY,
                    canonical_url TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS converter_sync_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.commit()
        finally:
            conn.close()
