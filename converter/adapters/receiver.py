from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import JSON, String, Text, and_, func, inspect, or_, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from converter.core.models import PackageUnit, RawProductRecord, Unit


class _ReceiverBase(DeclarativeBase):
    pass


class _RunArtifact(_ReceiverBase):
    __tablename__ = "run_artifacts"

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    parser_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ingested_at: Mapped[str | None] = mapped_column(String(64), nullable=True)


class _RunArtifactProduct(_ReceiverBase):
    __tablename__ = "run_artifact_products"

    id: Mapped[int] = mapped_column(primary_key=True)
    artifact_id: Mapped[int] = mapped_column(nullable=False)

    sku: Mapped[str | None] = mapped_column(String(128), nullable=True)
    plu: Mapped[str | None] = mapped_column(String(128), nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    composition: Mapped[str | None] = mapped_column(Text, nullable=True)

    brand: Mapped[str | None] = mapped_column(String(255), nullable=True)
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
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)


class _RunArtifactAdministrativeUnit(_ReceiverBase):
    __tablename__ = "run_artifact_administrative_units"

    id: Mapped[int] = mapped_column(primary_key=True)
    artifact_id: Mapped[int] = mapped_column(nullable=False)

    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    region: Mapped[str | None] = mapped_column(String(255), nullable=True)
    country: Mapped[str | None] = mapped_column(String(64), nullable=True)


class _RunArtifactProductImage(_ReceiverBase):
    __tablename__ = "run_artifact_product_images"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(nullable=False)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    sort_order: Mapped[int | None] = mapped_column(nullable=True)


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


def _create_engine(database_url: str) -> Engine:
    from sqlalchemy import create_engine

    connect_args: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    return create_engine(
        database_url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
    )


class ReceiverRepository:
    """
    SQLAlchemy-based reader for receiver DB normalized tables.
    """

    def __init__(
        self,
        database_url: str,
        *,
        default_parser_name: str = "fixprice",
        engine: Engine | None = None,
    ) -> None:
        self._database_url = database_url
        self._default_parser_name = default_parser_name
        self._engine = engine or _create_engine(database_url)
        self._session_factory = sessionmaker(
            bind=self._engine,
            class_=Session,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        self._validate_schema()

    def fetch_batch(
        self,
        limit: int = 100,
        *,
        parser_name: str | None = None,
        after_ingested_at: str | datetime | None = None,
        after_product_id: int | None = None,
    ) -> list[RawProductRecord]:
        parser_filter = parser_name.strip().lower() if isinstance(parser_name, str) else None
        watermark = self._normalize_watermark(after_ingested_at)
        after_id = int(after_product_id or 0)

        with self._session_factory() as session:
            stmt = (
                select(
                    _RunArtifactProduct,
                    _RunArtifact,
                    _RunArtifactAdministrativeUnit,
                )
                .join(_RunArtifact, _RunArtifact.id == _RunArtifactProduct.artifact_id)
                .outerjoin(
                    _RunArtifactAdministrativeUnit,
                    _RunArtifactAdministrativeUnit.artifact_id == _RunArtifact.id,
                )
            )

            if parser_filter:
                stmt = stmt.where(func.lower(_RunArtifact.parser_name) == parser_filter)

            if watermark is not None:
                stmt = stmt.where(
                    or_(
                        _RunArtifact.ingested_at > watermark,
                        and_(
                            _RunArtifact.ingested_at == watermark,
                            _RunArtifactProduct.id > after_id,
                        ),
                    )
                )

            stmt = stmt.order_by(_RunArtifact.ingested_at.asc(), _RunArtifactProduct.id.asc()).limit(max(1, int(limit)))
            rows = session.execute(stmt).all()
            if not rows:
                return []

            artifact_ids = sorted({product.artifact_id for product, _, _ in rows})
            product_ids = sorted({product.id for product, _, _ in rows})

            category_lookup = self._load_category_title_lookup(session, artifact_ids)
            image_lookup = self._load_image_lookup(session, product_ids)

            out: list[RawProductRecord] = []
            for product, artifact, admin in rows:
                row_data: dict[str, Any] = {
                    "product_id": product.id,
                    "artifact_id": product.artifact_id,
                    "product_sku": product.sku,
                    "product_plu": product.plu,
                    "product_title": product.title,
                    "product_composition": product.composition,
                    "product_brand": product.brand,
                    "product_unit": product.unit,
                    "product_available_count": product.available_count,
                    "product_package_quantity": product.package_quantity,
                    "product_package_unit": product.package_unit,
                    "category_uids_json": product.categories_uid_json,
                    "product_main_image": product.main_image,
                    "product_sort_order": product.sort_order,
                    "run_id": artifact.run_id,
                    "artifact_source": artifact.source,
                    "ingested_at": artifact.ingested_at,
                    "parser_name": artifact.parser_name,
                    "geo_name": admin.name if admin else None,
                    "geo_region": admin.region if admin else None,
                    "geo_country": admin.country if admin else None,
                }

                category_uids = _as_string_list(row_data.get("category_uids_json"))
                row_data["category_titles"] = self._resolve_category_titles(
                    category_uids,
                    category_lookup.get(product.artifact_id, {}),
                )
                row_data["image_urls_json"] = image_lookup.get(product.id, [])

                parsed = map_receiver_row_to_raw_product(
                    row_data,
                    default_parser_name=self._default_parser_name,
                )
                if parser_filter and parsed.parser_name.strip().lower() != parser_filter:
                    continue
                out.append(parsed)

            return out

    def _validate_schema(self) -> None:
        inspector = inspect(self._engine)
        if not inspector.has_table("run_artifacts"):
            raise RuntimeError("Receiver schema is missing table run_artifacts")

        columns = {item["name"] for item in inspector.get_columns("run_artifacts")}
        if "parser_name" not in columns:
            raise RuntimeError(
                "Unsupported receiver schema: run_artifacts.parser_name is missing. "
                "Apply receiver manual migrations from 2026-02-26."
            )

    def _load_category_title_lookup(
        self,
        session: Session,
        artifact_ids: list[int],
    ) -> dict[int, dict[str, str]]:
        if not artifact_ids:
            return {}

        stmt = select(
            _RunArtifactCategory.artifact_id,
            _RunArtifactCategory.uid,
            _RunArtifactCategory.title,
        ).where(_RunArtifactCategory.artifact_id.in_(artifact_ids))

        out: dict[int, dict[str, str]] = {}
        for artifact_id, uid, title in session.execute(stmt):
            u = _safe_str(uid)
            t = _safe_str(title)
            if u is None or t is None:
                continue
            out.setdefault(int(artifact_id), {})[u] = t

        return out

    def _load_image_lookup(self, session: Session, product_ids: list[int]) -> dict[int, list[str]]:
        if not product_ids:
            return {}

        stmt = (
            select(_RunArtifactProductImage.product_id, _RunArtifactProductImage.url)
            .where(_RunArtifactProductImage.product_id.in_(product_ids))
            .order_by(_RunArtifactProductImage.product_id.asc(), _RunArtifactProductImage.sort_order.asc())
        )

        out: dict[int, list[str]] = {}
        for product_id, url in session.execute(stmt):
            token = _safe_str(url)
            if token is None:
                continue
            bucket = out.setdefault(int(product_id), [])
            if token not in bucket:
                bucket.append(token)

        return out

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

    @staticmethod
    def _normalize_watermark(value: str | datetime | None) -> str | datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        token = _safe_str(value)
        return token


class ReceiverSQLiteRepository(ReceiverRepository):
    def __init__(self, db_path: str | Path, *, default_parser_name: str = "fixprice") -> None:
        path = Path(db_path)
        if not path.is_file():
            raise FileNotFoundError(f"receiver sqlite db not found: {path}")
        super().__init__(
            f"sqlite:///{path.resolve()}",
            default_parser_name=default_parser_name,
        )
