from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from converter.core.models import RawProductRecord

from .mysql_common import parse_mysql_dsn
from .receiver import map_receiver_row_to_raw_product


def _import_pymysql() -> Any:
    try:
        import pymysql  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "pymysql is required for MySQL repositories. Install with: pip install pymysql"
        ) from exc
    return pymysql


class ReceiverMySQLRepository:
    """
    Reader for receiver MySQL DB normalized tables.

    Requires schema with `run_artifacts.parser_name`.
    """

    def __init__(
        self,
        connect_kwargs: dict[str, object],
        *,
        default_parser_name: str = "fixprice",
        connect_fn: Callable[..., Any] | None = None,
    ) -> None:
        self._connect_kwargs = dict(connect_kwargs)
        self._default_parser_name = default_parser_name

        if connect_fn is None:
            pymysql = _import_pymysql()
            connect_fn = pymysql.connect
            self._dict_cursor_cls = pymysql.cursors.DictCursor
        else:
            self._dict_cursor_cls = None

        self._connect_fn = connect_fn

    @classmethod
    def from_dsn(cls, dsn: str, *, default_parser_name: str = "fixprice") -> "ReceiverMySQLRepository":
        return cls(parse_mysql_dsn(dsn), default_parser_name=default_parser_name)

    def fetch_batch(
        self,
        limit: int = 100,
        *,
        parser_name: str | None = None,
        after_ingested_at: str | datetime | None = None,
        after_product_id: int | None = None,
    ) -> list[RawProductRecord]:
        conn = self._connect()
        try:
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
                where_clauses.append("LOWER(a.parser_name) = %s")
                params.append(parser_filter)

            watermark = self._normalize_watermark(after_ingested_at)
            if watermark is not None:
                where_clauses.append("(a.ingested_at > %s OR (a.ingested_at = %s AND p.id > %s))")
                params.extend([watermark, watermark, int(after_product_id or 0)])

            query = self._base_query(where_clauses)
            rows = self._fetch_all(conn, query, [*params, max(1, int(limit))])
            if not rows:
                return []

            artifact_ids = sorted({int(row["artifact_id"]) for row in rows if row.get("artifact_id") is not None})
            product_ids = sorted({int(row["product_id"]) for row in rows if row.get("product_id") is not None})

            category_title_lookup = self._load_category_title_lookup(conn, artifact_ids)
            image_lookup = self._load_image_lookup(conn, product_ids)

            out: list[RawProductRecord] = []
            for row_data in rows:
                row_category_uids = self._as_string_list(row_data.get("category_uids_json"))
                row_data["category_titles"] = self._resolve_category_titles(
                    row_category_uids,
                    category_title_lookup.get(int(row_data["artifact_id"]), {}),
                )

                row_product_id = int(row_data["product_id"])
                row_data["image_urls_json"] = image_lookup.get(row_product_id, [])

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

    def _connect(self) -> Any:
        kwargs = dict(self._connect_kwargs)
        if self._dict_cursor_cls is not None:
            kwargs.setdefault("cursorclass", self._dict_cursor_cls)
        return self._connect_fn(**kwargs)

    def _has_column(self, conn: Any, table_name: str, column_name: str) -> bool:
        database = str(self._connect_kwargs.get("database") or "")
        rows = self._fetch_all(
            conn,
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            [database, table_name, column_name],
        )
        return bool(rows)

    @staticmethod
    def _normalize_watermark(value: str | datetime | None) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        else:
            token = str(value).strip()
            if not token:
                return None
            dt = datetime.fromisoformat(token.replace("Z", "+00:00"))

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

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
        LIMIT %s
        """

    def _load_category_title_lookup(self, conn: Any, artifact_ids: list[int]) -> dict[int, dict[str, str]]:
        if not artifact_ids:
            return {}

        placeholders = ",".join(["%s"] * len(artifact_ids))
        rows = self._fetch_all(
            conn,
            f"""
            SELECT artifact_id, uid, title
            FROM run_artifact_categories
            WHERE artifact_id IN ({placeholders})
            """,
            artifact_ids,
        )

        out: dict[int, dict[str, str]] = {}
        for row in rows:
            artifact_id = int(row["artifact_id"])
            uid = self._safe_str(row.get("uid"))
            title = self._safe_str(row.get("title"))
            if uid is None or title is None:
                continue
            out.setdefault(artifact_id, {})[uid] = title
        return out

    def _load_image_lookup(self, conn: Any, product_ids: list[int]) -> dict[int, list[str]]:
        if not product_ids:
            return {}

        placeholders = ",".join(["%s"] * len(product_ids))
        rows = self._fetch_all(
            conn,
            f"""
            SELECT product_id, url
            FROM run_artifact_product_images
            WHERE product_id IN ({placeholders})
            ORDER BY product_id ASC, sort_order ASC
            """,
            product_ids,
        )

        out: dict[int, list[str]] = {}
        for row in rows:
            product_id = int(row["product_id"])
            url = self._safe_str(row.get("url"))
            if url is None:
                continue
            bucket = out.setdefault(product_id, [])
            if url not in bucket:
                bucket.append(url)
        return out

    @staticmethod
    def _resolve_category_titles(category_uids: list[str], category_lookup: dict[str, str]) -> str | None:
        if not category_uids:
            return None

        titles: list[str] = []
        seen: set[str] = set()
        for uid in category_uids:
            title = ReceiverMySQLRepository._safe_str(category_lookup.get(uid))
            if title is None:
                continue
            lowered = title.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            titles.append(title)

        return " / ".join(titles) if titles else None

    @staticmethod
    def _safe_str(value: object) -> str | None:
        if value is None:
            return None
        token = str(value).strip()
        return token or None

    @staticmethod
    def _as_string_list(value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            raw_list = value
        elif isinstance(value, str):
            token = value.strip()
            if not token:
                return []
            if token.startswith("["):
                import json

                try:
                    parsed = json.loads(token)
                except Exception:
                    parsed = None
                raw_list = parsed if isinstance(parsed, list) else [token]
            else:
                raw_list = [item.strip() for item in token.split(",") if item.strip()]
        else:
            raw_list = [value]

        out: list[str] = []
        for item in raw_list:
            token = ReceiverMySQLRepository._safe_str(item)
            if token is not None:
                out.append(token)
        return out

    @staticmethod
    def _fetch_all(conn: Any, query: str, params: list[object]) -> list[dict[str, object]]:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        if isinstance(rows, list):
            return [dict(row) for row in rows]
        return []
