from __future__ import annotations

import re
from typing import Protocol

_CATEGORY_SEPARATORS_RE = re.compile(r"[/,]+")
_SPACES_RE = re.compile(r"\s+")


class _CategoryTextNormalizer(Protocol):
    def lemmatize(self, text: str) -> str: ...

    def remove_stopwords(self, text: str) -> str: ...


def normalize_category_text(
    value: str, *, text_normalizer: _CategoryTextNormalizer
) -> str | None:
    collapsed = _SPACES_RE.sub(" ", _CATEGORY_SEPARATORS_RE.sub(" ", value)).strip()
    if not collapsed:
        return None

    lemmatized = text_normalizer.lemmatize(collapsed)
    if not lemmatized:
        return None

    without_stopwords = text_normalizer.remove_stopwords(lemmatized)
    return without_stopwords or lemmatized
