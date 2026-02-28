from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from converter.core.models import PackageUnit, RawProductRecord, Unit


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

    payload = {
        "receiver_product_id": row.get("product_id"),
        "receiver_artifact_id": row.get("artifact_id"),
        "receiver_run_id": run_id,
        "receiver_source": _safe_str(row.get("artifact_source")),
        "receiver_sort_order": row.get("product_sort_order"),
        "receiver_categories_uid": category_uids,
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
        category=category,
        geo=geo,
        composition=_safe_str(row.get("product_composition")),
        image_urls=images,
        observed_at=observed_at,
        payload=payload,
    )


class ReceiverSQLiteRepository:
    """
    Reader for receiver SQLite DB normalized tables.

    Requires receiver schema with `run_artifacts.parser_name`.
    Legacy schema is intentionally unsupported.
    """

    def __init__(self, db_path: str | Path, *, default_parser_name: str = "fixprice") -> None:
        self._db_path = Path(db_path)
        self._default_parser_name = default_parser_name

    def fetch_batch(
        self,
        limit: int = 100,
        *,
        parser_name: str | None = None,
        after_ingested_at: str | datetime | None = None,
        after_product_id: int | None = None,
    ) -> list[RawProductRecord]:
        if not self._db_path.is_file():
            raise FileNotFoundError(f"receiver sqlite db not found: {self._db_path}")

        conn = sqlite3.connect(self._db_path)
        try:
            conn.row_factory = sqlite3.Row

            has_parser_name = self._has_column(conn, "run_artifacts", "parser_name")
            if not has_parser_name:
                raise RuntimeError(
                    "Unsupported receiver schema: run_artifacts.parser_name is missing. "
                    "Apply receiver manual migrations from 2026-02-26."
                )

            where_clauses: list[str] = []
            params: list[object] = []
            parser_filter = parser_name.strip().lower() if isinstance(parser_name, str) else None
            if parser_filter:
                where_clauses.append("LOWER(a.parser_name) = ?")
                params.append(parser_filter)

            watermark = self._normalize_watermark(after_ingested_at)
            if watermark is not None:
                where_clauses.append("(a.ingested_at > ? OR (a.ingested_at = ? AND p.id > ?))")
                params.extend([watermark, watermark, int(after_product_id or 0)])

            rows = conn.execute(
                self._base_query(where_clauses),
                [*params, max(1, int(limit))],
            ).fetchall()

            if not rows:
                return []

            artifact_ids = sorted(
                {
                    int(row["artifact_id"])
                    for row in rows
                    if row["artifact_id"] is not None
                }
            )
            product_ids = sorted(
                {
                    int(row["product_id"])
                    for row in rows
                    if row["product_id"] is not None
                }
            )

            category_title_lookup = self._load_category_title_lookup(conn, artifact_ids)
            image_lookup = self._load_image_lookup(conn, product_ids)

            out: list[RawProductRecord] = []
            for row in rows:
                row_data = dict(row)

                row_category_uids = _as_string_list(row_data.get("category_uids_json"))
                row_data["category_titles"] = self._resolve_category_titles(
                    row_category_uids,
                    category_title_lookup.get(int(row_data["artifact_id"]), {}),
                )

                row_product_id = int(row_data["product_id"])
                row_images = image_lookup.get(row_product_id, [])
                row_data["image_urls_json"] = row_images

                parsed = map_receiver_row_to_raw_product(
                    row_data,
                    default_parser_name=self._default_parser_name,
                )
                if parser_filter and parsed.parser_name.strip().lower() != parser_filter:
                    continue

                out.append(parsed)

            return out
        finally:
            conn.close()

    @staticmethod
    def _has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(row[1] == column_name for row in rows)

    @staticmethod
    def _normalize_watermark(value: str | datetime | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc).isoformat()
            return value.isoformat()
        token = _safe_str(value)
        return token

    @staticmethod
    def _base_query(where_clauses: list[str]) -> str:
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)
        return f"""
        SELECT
            p.id AS product_id,
            p.artifact_id AS artifact_id,
            p.sku AS product_sku,
            p.plu AS product_plu,
            p.title AS product_title,
            p.composition AS product_composition,
            p.brand AS product_brand,
            p.unit AS product_unit,
            p.available_count AS product_available_count,
            p.package_quantity AS product_package_quantity,
            p.package_unit AS product_package_unit,
            p.categories_uid_json AS category_uids_json,
            p.main_image AS product_main_image,
            p.sort_order AS product_sort_order,
            a.run_id AS run_id,
            a.source AS artifact_source,
            a.ingested_at AS ingested_at,
            a.parser_name AS parser_name,
            au.name AS geo_name,
            au.region AS geo_region,
            au.country AS geo_country
        FROM run_artifact_products AS p
        JOIN run_artifacts AS a ON a.id = p.artifact_id
        LEFT JOIN run_artifact_administrative_units AS au ON au.artifact_id = a.id
        {where_sql}
        ORDER BY a.ingested_at ASC, p.id ASC
        LIMIT ?
        """

    @staticmethod
    def _load_category_title_lookup(
        conn: sqlite3.Connection,
        artifact_ids: list[int],
    ) -> dict[int, dict[str, str]]:
        if not artifact_ids:
            return {}

        placeholders = ",".join(["?"] * len(artifact_ids))
        rows = conn.execute(
            f"""
            SELECT artifact_id, uid, title
            FROM run_artifact_categories
            WHERE artifact_id IN ({placeholders})
            """,
            artifact_ids,
        ).fetchall()

        by_artifact: dict[int, dict[str, str]] = {}
        for row in rows:
            artifact_id = int(row[0])
            uid = _safe_str(row[1])
            title = _safe_str(row[2])
            if uid is None or title is None:
                continue

            if artifact_id not in by_artifact:
                by_artifact[artifact_id] = {}
            by_artifact[artifact_id][uid] = title

        return by_artifact

    @staticmethod
    def _load_image_lookup(conn: sqlite3.Connection, product_ids: list[int]) -> dict[int, list[str]]:
        if not product_ids:
            return {}

        placeholders = ",".join(["?"] * len(product_ids))
        rows = conn.execute(
            f"""
            SELECT product_id, url
            FROM run_artifact_product_images
            WHERE product_id IN ({placeholders})
            ORDER BY product_id ASC, sort_order ASC
            """,
            product_ids,
        ).fetchall()

        by_product: dict[int, list[str]] = {}
        for row in rows:
            product_id = int(row[0])
            url = _safe_str(row[1])
            if url is None:
                continue

            bucket = by_product.setdefault(product_id, [])
            if url not in bucket:
                bucket.append(url)

        return by_product

    @staticmethod
    def _resolve_category_titles(
        category_uids: list[str],
        category_lookup: dict[str, str],
    ) -> str | None:
        if not category_uids:
            return None

        titles: list[str] = []
        seen: set[str] = set()
        for uid in category_uids:
            title = _safe_str(category_lookup.get(uid))
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
