from __future__ import annotations

import re

from converter.core.base import BaseParserHandler
from converter.core.models import NormalizedProductRecord, RawProductRecord, TitleNormalizationResult
from converter.parsers.category_normalization import normalize_category_text
from converter.parsers.normalizers import RussianTextNormalizer

from .patterns import DIM_CM_RE, DIM_GENERIC_RE, WVL_RE
from .title_parser import FixPriceTitleParser

_COMMA_SPACES_RE = re.compile(r"\s*,\s*")


class FixPriceHandler(BaseParserHandler):
    parser_name = "fixprice"

    def __init__(self, text_normalizer: RussianTextNormalizer | None = None) -> None:
        normalizer = text_normalizer or RussianTextNormalizer()
        self._text_normalizer = normalizer
        self._title_parser = FixPriceTitleParser(text_normalizer=normalizer)

    def normalize_title(self, title: str) -> TitleNormalizationResult:
        return self._title_parser.parse(title)

    def handle(self, raw: RawProductRecord) -> NormalizedProductRecord:
        normalized = super().handle(raw)
        brand = normalized.brand.strip() if isinstance(normalized.brand, str) else ""
        if brand and (DIM_CM_RE.search(brand) or DIM_GENERIC_RE.search(brand) or WVL_RE.search(brand)):
            normalized.brand = None
        return normalized

    def normalize_category(self, category: str | None) -> str | None:
        normalized = super().normalize_category(category)
        if normalized is None:
            return None

        return normalize_category_text(normalized, text_normalizer=self._text_normalizer)

    def normalize_composition(self, composition: str | None) -> str | None:
        normalized = super().normalize_composition(composition)
        if normalized is None:
            return None

        return _COMMA_SPACES_RE.sub(", ", normalized)
