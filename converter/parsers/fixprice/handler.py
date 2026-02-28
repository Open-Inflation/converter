from __future__ import annotations

import re

from converter.core.base import BaseParserHandler
from converter.core.models import TitleNormalizationResult

from .normalizers import RussianTextNormalizer
from .title_parser import FixPriceTitleParser

_COMMA_SPACES_RE = re.compile(r"\s*,\s*")


class FixPriceHandler(BaseParserHandler):
    parser_name = "fixprice"

    def __init__(self, text_normalizer: RussianTextNormalizer | None = None) -> None:
        normalizer = text_normalizer or RussianTextNormalizer()
        self._title_parser = FixPriceTitleParser(text_normalizer=normalizer)

    def normalize_title(self, title: str) -> TitleNormalizationResult:
        return self._title_parser.parse(title)

    def normalize_category(self, category: str | None) -> str | None:
        normalized = super().normalize_category(category)
        if normalized is None:
            return None

        category_map = {
            "напитки и соки": "напитки",
            "канцтовары": "канцелярия",
            "бытовая химия и уборка": "бытовая химия",
        }
        return category_map.get(normalized, normalized)

    def normalize_geo(self, geo: str | None) -> str | None:
        normalized = super().normalize_geo(geo)
        if normalized is None:
            return None

        if normalized == "российская федерация":
            return "россия"

        return normalized

    def normalize_composition(self, composition: str | None) -> str | None:
        normalized = super().normalize_composition(composition)
        if normalized is None:
            return None

        return _COMMA_SPACES_RE.sub(", ", normalized)
