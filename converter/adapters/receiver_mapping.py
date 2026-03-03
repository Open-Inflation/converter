from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
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


def _as_int(value: Any) -> int | None:
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


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    token = _safe_str(value)
    if token is None:
        return None
    lowered = token.lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
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

    receiver_categories = row.get("receiver_categories")
    if not isinstance(receiver_categories, list):
        receiver_categories = []

    receiver_product = row.get("receiver_product")
    if not isinstance(receiver_product, dict):
        receiver_product = {}

    receiver_artifact = row.get("receiver_artifact")
    if not isinstance(receiver_artifact, dict):
        receiver_artifact = {}

    receiver_admin = row.get("receiver_admin_unit")
    if not isinstance(receiver_admin, dict):
        receiver_admin = {}

    receiver_product_images = row.get("receiver_product_images")
    if not isinstance(receiver_product_images, list):
        receiver_product_images = []

    receiver_product_meta = row.get("receiver_product_meta")
    if not isinstance(receiver_product_meta, list):
        receiver_product_meta = []

    receiver_product_wholesale_prices = row.get("receiver_product_wholesale_prices")
    if not isinstance(receiver_product_wholesale_prices, list):
        receiver_product_wholesale_prices = []

    receiver_product_categories = row.get("receiver_product_categories")
    if not isinstance(receiver_product_categories, list):
        receiver_product_categories = []

    payload = {
        "receiver_product_id": row.get("product_id"),
        "receiver_artifact_id": row.get("artifact_id"),
        "receiver_run_id": run_id,
        "receiver_parser_name": _safe_str(row.get("parser_name")),
        "receiver_source": _safe_str(row.get("artifact_source")),
        "receiver_ingested_at": _safe_str(row.get("ingested_at")),
        "receiver_sort_order": row.get("product_sort_order"),
        "receiver_categories_uid": category_uids,
        "receiver_categories": receiver_categories,
        "receiver_geo_country": _safe_str(row.get("geo_country")),
        "receiver_geo_region": _safe_str(row.get("geo_region")),
        "receiver_geo_name": _safe_str(row.get("geo_name")),
        "receiver_geo_settlement_type": _safe_str(row.get("geo_settlement_type")),
        "receiver_geo_alias": _safe_str(row.get("geo_alias")),
        "receiver_geo_longitude": _as_float(row.get("geo_longitude")),
        "receiver_geo_latitude": _as_float(row.get("geo_latitude")),
        "receiver_product": receiver_product,
        "receiver_artifact": receiver_artifact,
        "receiver_admin_unit": receiver_admin,
        "receiver_product_images": receiver_product_images,
        "receiver_product_meta": receiver_product_meta,
        "receiver_product_wholesale_prices": receiver_product_wholesale_prices,
        "receiver_product_categories": receiver_product_categories,
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
        price=_as_float(row.get("product_price")),
        discount_price=_as_float(row.get("product_discount_price")),
        loyal_price=_as_float(row.get("product_loyal_price")),
        price_unit=_safe_str(row.get("product_price_unit")),
        source_page_url=_safe_str(row.get("product_source_page_url")),
        description=_safe_str(row.get("product_description")),
        producer_name=_safe_str(row.get("product_producer_name")),
        producer_country=_safe_str(row.get("product_producer_country")),
        expiration_date_in_days=_as_int(row.get("product_expiration_date_in_days")),
        rating=_as_float(row.get("product_rating")),
        reviews_count=_as_int(row.get("product_reviews_count")),
        adult=_as_bool(row.get("product_adult")),
        is_new=_as_bool(row.get("product_is_new")),
        promo=_as_bool(row.get("product_promo")),
        season=_as_bool(row.get("product_season")),
        hit=_as_bool(row.get("product_hit")),
        data_matrix=_as_bool(row.get("product_data_matrix")),
        category=category,
        geo=geo,
        composition=_safe_str(row.get("product_composition")),
        image_urls=images,
        observed_at=observed_at,
        payload=payload,
    )


__all__ = [
    "_safe_str",
    "_as_float",
    "_as_int",
    "_as_bool",
    "_as_string_list",
    "_parse_datetime",
    "_normalize_unit",
    "_normalize_package_unit",
    "_join_non_empty",
    "map_receiver_row_to_raw_product",
]
