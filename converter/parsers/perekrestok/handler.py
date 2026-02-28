from __future__ import annotations

from converter.core.base import BaseParserHandler
from converter.core.models import TitleNormalizationResult
from converter.parsers.fixprice.normalizers import RussianTextNormalizer

from .title_parser import PerekrestokTitleParser


class PerekrestokHandler(BaseParserHandler):
    parser_name = "perekrestok"

    def __init__(self, text_normalizer: RussianTextNormalizer | None = None) -> None:
        normalizer = text_normalizer or RussianTextNormalizer()
        self._title_parser = PerekrestokTitleParser(text_normalizer=normalizer)

    def normalize_title(self, title: str) -> TitleNormalizationResult:
        return self._title_parser.parse(title)

