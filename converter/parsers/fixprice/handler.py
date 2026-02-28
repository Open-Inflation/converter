from __future__ import annotations

import re

from converter.core.base import BaseParserHandler
from converter.core.models import TitleNormalizationResult
from converter.parsers.category_normalization import normalize_category_text

from .normalizers import RussianTextNormalizer
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
