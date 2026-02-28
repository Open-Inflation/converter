from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

Unit = Literal["PCE", "KGM", "LTR"]
PackageUnit = Literal["KGM", "LTR"]


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(slots=True)
class RawProductRecord:
    parser_name: str
    title: str

    source_id: str | None = None
    plu: str | None = None
    sku: str | None = None
    brand: str | None = None

    unit: Unit | None = None
    available_count: float | None = None
    package_quantity: float | None = None
    package_unit: PackageUnit | None = None

    category: str | None = None
    geo: str | None = None
    composition: str | None = None

    image_urls: list[str] = field(default_factory=list)
    observed_at: datetime = field(default_factory=utcnow)
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TitleNormalizationResult:
    raw_title: str

    name_original: str
    brand: str | None

    name_normalized: str
    original_name_no_stopwords: str
    normalized_name_no_stopwords: str

    unit: Unit
    available_count: float | None
    package_quantity: float | None
    package_unit: PackageUnit | None


@dataclass(slots=True)
class NormalizedProductRecord:
    parser_name: str

    raw_title: str
    title_original: str
    title_normalized: str
    title_original_no_stopwords: str
    title_normalized_no_stopwords: str
    brand: str | None

    unit: Unit
    available_count: float | None
    package_quantity: float | None
    package_unit: PackageUnit | None

    source_id: str | None = None
    plu: str | None = None
    sku: str | None = None
    canonical_product_id: str | None = None

    category_raw: str | None = None
    category_normalized: str | None = None

    geo_raw: str | None = None
    geo_normalized: str | None = None

    composition_raw: str | None = None
    composition_normalized: str | None = None

    image_urls: list[str] = field(default_factory=list)
    duplicate_image_urls: list[str] = field(default_factory=list)
    image_fingerprints: list[str] = field(default_factory=list)

    observed_at: datetime = field(default_factory=utcnow)
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def identity_candidates(self) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for key_name, value in (
            ("plu", self.plu),
            ("sku", self.sku),
            ("source_id", self.source_id),
        ):
            if value and value.strip():
                out.append((key_name, value.strip()))
        return out
