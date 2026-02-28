from __future__ import annotations

import copy
import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import uuid4

from .models import NormalizedProductRecord


class ProductIdentityResolver(Protocol):
    def resolve(self, record: NormalizedProductRecord) -> str:
        raise NotImplementedError


class ImageHasher(Protocol):
    def fingerprint(self, image_url: str) -> str:
        raise NotImplementedError


@dataclass(slots=True)
class ImageDedupResult:
    unique_urls: list[str]
    duplicate_urls: list[str]
    fingerprints: list[str]


class UrlStringHasher:
    """
    Lightweight deterministic fallback hasher by URL.
    Real implementation can hash downloaded image bytes.
    """

    def fingerprint(self, image_url: str) -> str:
        normalized = image_url.strip()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class InMemoryProductIdentityResolver:
    """
    Canonical product identity resolver.
    Priority: parser + plu -> parser + sku -> parser + source_id -> parser + normalized name.
    """

    def __init__(self) -> None:
        self._index: dict[tuple[str, str, str], str] = {}

    def resolve(self, record: NormalizedProductRecord) -> str:
        parser_name = record.parser_name

        for id_type, id_value in record.identity_candidates():
            key = (parser_name, id_type, id_value)
            existing = self._index.get(key)
            if existing:
                return existing

        fallback_key = (
            parser_name,
            "normalized_name",
            record.title_normalized_no_stopwords,
        )
        existing = self._index.get(fallback_key)
        if existing:
            for id_type, id_value in record.identity_candidates():
                self._index[(parser_name, id_type, id_value)] = existing
            return existing

        product_id = str(uuid4())
        self._index[fallback_key] = product_id
        for id_type, id_value in record.identity_candidates():
            self._index[(parser_name, id_type, id_value)] = product_id

        return product_id


class NullBackfillService:
    """
    Fills None/empty values from the temporally nearest non-empty version of the same product.
    """

    DEFAULT_FIELDS = (
        "brand",
        "category_normalized",
        "geo_normalized",
        "composition_original",
        "composition_normalized",
        "package_quantity",
        "package_unit",
    )

    def __init__(self, fields: tuple[str, ...] | None = None) -> None:
        self._fields = fields or self.DEFAULT_FIELDS
        self._history: dict[str, list[NormalizedProductRecord]] = defaultdict(list)

    def apply(self, record: NormalizedProductRecord) -> NormalizedProductRecord:
        if not record.canonical_product_id:
            return record

        history = self._history[record.canonical_product_id]
        for field_name in self._fields:
            current_value = getattr(record, field_name)
            if not self._is_missing(current_value):
                continue

            candidate = self._closest_non_missing(history, field_name, record.observed_at)
            if candidate is not None:
                setattr(record, field_name, candidate)

        history.append(copy.deepcopy(record))
        history.sort(key=lambda item: item.observed_at)
        return record

    @staticmethod
    def _is_missing(value: object) -> bool:
        if value is None:
            return True
        if isinstance(value, str) and not value.strip():
            return True
        return False

    def _closest_non_missing(
        self,
        history: list[NormalizedProductRecord],
        field_name: str,
        target_time: datetime,
    ) -> object | None:
        nearest_delta: float | None = None
        nearest_value: object | None = None

        for item in history:
            candidate = getattr(item, field_name)
            if self._is_missing(candidate):
                continue

            delta = abs((item.observed_at - target_time).total_seconds())
            if nearest_delta is None or delta < nearest_delta:
                nearest_delta = delta
                nearest_value = candidate

        return nearest_value


class PersistentImageDeduplicator:
    """
    Keeps persistent image fingerprints and maps new duplicates to a canonical URL.
    """

    def __init__(self, hasher: ImageHasher | None = None) -> None:
        self._hasher = hasher or UrlStringHasher()
        self._canonical_by_fingerprint: dict[str, str] = {}

    def process(self, image_urls: list[str]) -> ImageDedupResult:
        unique_urls: list[str] = []
        duplicate_urls: list[str] = []
        fingerprints: list[str] = []

        seen_in_record: set[str] = set()
        for raw_url in image_urls:
            url = raw_url.strip()
            if not url:
                continue

            fingerprint = self._hasher.fingerprint(url)
            canonical_url = self._canonical_by_fingerprint.get(fingerprint)

            if canonical_url is None:
                self._canonical_by_fingerprint[fingerprint] = url
                canonical_url = url
            elif canonical_url != url:
                duplicate_urls.append(url)

            if fingerprint in seen_in_record:
                if url != canonical_url:
                    duplicate_urls.append(url)
                continue

            seen_in_record.add(fingerprint)
            unique_urls.append(canonical_url)
            fingerprints.append(fingerprint)

        return ImageDedupResult(
            unique_urls=unique_urls,
            duplicate_urls=duplicate_urls,
            fingerprints=fingerprints,
        )
