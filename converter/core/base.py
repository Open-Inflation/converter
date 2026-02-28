from __future__ import annotations

import re
from abc import ABC, abstractmethod

from .models import NormalizedProductRecord, RawProductRecord, TitleNormalizationResult

_SPACES_RE = re.compile(r"\s+")


class BaseParserHandler(ABC):
    parser_name: str

    def handle(self, raw: RawProductRecord) -> NormalizedProductRecord:
        title = self.normalize_title(raw.title)
        brand = title.brand or raw.brand
        unit = raw.unit or title.unit
        available_count = raw.available_count if raw.available_count is not None else title.available_count

        package_quantity = raw.package_quantity
        package_unit = raw.package_unit
        if package_quantity is None and package_unit is None:
            package_quantity = title.package_quantity
            package_unit = title.package_unit
        elif (package_quantity is None) != (package_unit is None):
            package_quantity = title.package_quantity
            package_unit = title.package_unit

        return NormalizedProductRecord(
            parser_name=self.parser_name,
            title_original=title.name_original,
            title_normalized=title.name_normalized,
            title_original_no_stopwords=title.original_name_no_stopwords,
            title_normalized_no_stopwords=title.normalized_name_no_stopwords,
            brand=brand,
            unit=unit,
            available_count=available_count,
            package_quantity=package_quantity,
            package_unit=package_unit,
            price=raw.price,
            discount_price=raw.discount_price,
            loyal_price=raw.loyal_price,
            price_unit=raw.price_unit,
            source_page_url=raw.source_page_url,
            description=raw.description,
            producer_name=raw.producer_name,
            producer_country=raw.producer_country,
            expiration_date_in_days=raw.expiration_date_in_days,
            rating=raw.rating,
            reviews_count=raw.reviews_count,
            adult=raw.adult,
            is_new=raw.is_new,
            promo=raw.promo,
            season=raw.season,
            hit=raw.hit,
            data_matrix=raw.data_matrix,
            source_id=raw.source_id,
            plu=raw.plu,
            sku=raw.sku,
            category_normalized=self.normalize_category(raw.category),
            geo_normalized=self.normalize_geo(raw.geo),
            composition_original=self._raw_string(raw.composition),
            composition_normalized=self.normalize_composition(raw.composition),
            image_urls=list(raw.image_urls),
            observed_at=raw.observed_at,
            source_payload=dict(raw.payload),
        )

    @abstractmethod
    def normalize_title(self, title: str) -> TitleNormalizationResult:
        raise NotImplementedError

    def normalize_category(self, category: str | None) -> str | None:
        return self._normalize_string(category)

    def normalize_geo(self, geo: str | None) -> str | None:
        return self._normalize_string(geo)

    def normalize_composition(self, composition: str | None) -> str | None:
        return self._normalize_string(composition)

    @staticmethod
    def _raw_string(value: str | None) -> str | None:
        if value is None:
            return None
        token = value.strip()
        return token or None

    @staticmethod
    def _normalize_string(value: str | None) -> str | None:
        if value is None:
            return None

        cleaned = _SPACES_RE.sub(" ", value.strip().replace("ё", "е").lower())
        return cleaned or None
