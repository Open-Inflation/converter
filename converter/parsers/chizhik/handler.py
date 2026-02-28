from __future__ import annotations

from converter.core.base import BaseParserHandler
from converter.core.models import TitleNormalizationResult
from converter.parsers.fixprice.normalizers import RussianTextNormalizer

from .title_parser import ChizhikTitleParser


class ChizhikHandler(BaseParserHandler):
    parser_name = "chizhik"

    def __init__(self, text_normalizer: RussianTextNormalizer | None = None) -> None:
        normalizer = text_normalizer or RussianTextNormalizer()
        self._title_parser = ChizhikTitleParser(text_normalizer=normalizer)

    def normalize_title(self, title: str) -> TitleNormalizationResult:
        return self._title_parser.parse(title)
