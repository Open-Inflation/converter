from __future__ import annotations

import re

from converter.core.base import BaseParserHandler
from converter.core.models import NormalizedProductRecord, RawProductRecord, TitleNormalizationResult
from converter.parsers.category_normalization import normalize_category_text
from converter.parsers.normalizers import RussianTextNormalizer

from .title_parser import ChizhikTitleParser


_STRIP_CHARS = ".,;:()[]{}\"'«»"
_MULTISPACE_RE = re.compile(r"\s+")


def _normalize_token_for_compare(token: str) -> str:
    return token.strip(_STRIP_CHARS).casefold()


def _remove_brand_phrase(title: str, brand: str) -> str:
    title_words = [token for token in str(title).split() if token]
    brand_words = [token for token in str(brand).split() if token]
    if not title_words or not brand_words:
        return str(title).strip()

    normalized_brand = [_normalize_token_for_compare(token) for token in brand_words]
    normalized_brand = [token for token in normalized_brand if token]
    if not normalized_brand:
        return str(title).strip()

    out: list[str] = []
    idx = 0
    width = len(normalized_brand)
    while idx < len(title_words):
        window = title_words[idx : idx + width]
        normalized_window = [_normalize_token_for_compare(token) for token in window]
        if len(normalized_window) == width and normalized_window == normalized_brand:
            idx += width
            continue
        out.append(title_words[idx])
        idx += 1

    cleaned = " ".join(out).strip(" ,.;:-")
    return _MULTISPACE_RE.sub(" ", cleaned).strip() or str(title).strip()


class ChizhikHandler(BaseParserHandler):
    parser_name = "chizhik"

    def __init__(self, text_normalizer: RussianTextNormalizer | None = None) -> None:
        normalizer = text_normalizer or RussianTextNormalizer()
        self._text_normalizer = normalizer
        self._title_parser = ChizhikTitleParser(text_normalizer=normalizer)

    def normalize_title(self, title: str) -> TitleNormalizationResult:
        return self._title_parser.parse(title)

    def handle(self, raw: RawProductRecord) -> NormalizedProductRecord:
        normalized = super().handle(raw)
        brand = normalized.brand.strip() if isinstance(normalized.brand, str) else ""
        if not brand:
            return normalized

        cleaned_original = _remove_brand_phrase(normalized.title_original, brand)
        if cleaned_original == normalized.title_original:
            return normalized

        normalized.title_original = cleaned_original
        normalized.title_normalized = self._text_normalizer.lemmatize(cleaned_original)
        normalized.title_original_no_stopwords = self._text_normalizer.remove_stopwords(cleaned_original)
        normalized.title_normalized_no_stopwords = self._text_normalizer.remove_stopwords(
            normalized.title_normalized
        )
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

        lemmatized = self._text_normalizer.lemmatize(normalized)
        if not lemmatized:
            return None
        without_stopwords = self._text_normalizer.remove_stopwords(lemmatized)
        return without_stopwords or lemmatized
