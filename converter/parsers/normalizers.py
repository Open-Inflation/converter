from __future__ import annotations

import re
from collections.abc import Iterable

from razdel import tokenize as razdel_tokenize
from stop_words import get_stop_words

ASSORT_RE = re.compile(r"\bв\s+ассортименте\b", re.IGNORECASE)
QUOTE_RE = re.compile(r"[\"“”«»]")
NON_WORD_RE = re.compile(r"[^\w\s.,xх×-]+", re.UNICODE)
MULTISPACE_RE = re.compile(r"\s+")
TOKEN_RE = re.compile(r"[a-zа-я0-9-]+", re.IGNORECASE)
CYRILLIC_RE = re.compile(r"[а-я]", re.IGNORECASE)

_MIXED_CYR_LAT_TOKEN_RE = re.compile(
    r"\b(?=[a-zа-я0-9-]*[а-я])(?=[a-zа-я0-9-]*[a-z])[a-zа-я0-9-]+\b", re.IGNORECASE
)
_LATIN_TO_CYRILLIC = str.maketrans(
    {
        "a": "а",
        "b": "в",
        "c": "с",
        "e": "е",
        "h": "н",
        "k": "к",
        "m": "м",
        "o": "о",
        "p": "р",
        "t": "т",
        "x": "х",
        "y": "у",
    }
)
_NO_LEMMATIZE_TOKENS = {
    "см",
    "мм",
    "м",
    "км",
    "г",
    "кг",
    "мг",
    "л",
    "мл",
    "шт",
    "вт",
    "квт",
}


class RussianTextNormalizer:
    def __init__(self, extra_stopwords: Iterable[str] | None = None) -> None:
        import pymorphy3  # type: ignore

        self._morph = pymorphy3.MorphAnalyzer()
        self._stopwords = {word.lower().replace("ё", "е") for word in get_stop_words("ru")}
        if extra_stopwords is not None:
            self._stopwords.update({word.lower().replace("ё", "е") for word in extra_stopwords})

    def clean_text(self, text: str) -> str:
        cleaned = text.strip().lower().replace("ё", "е")
        cleaned = cleaned.replace("×", "x")
        cleaned = _MIXED_CYR_LAT_TOKEN_RE.sub(lambda match: match.group(0).translate(_LATIN_TO_CYRILLIC), cleaned)
        cleaned = QUOTE_RE.sub("", cleaned)
        cleaned = NON_WORD_RE.sub(" ", cleaned)
        cleaned = MULTISPACE_RE.sub(" ", cleaned).strip()
        return cleaned

    def tokenize(self, text: str) -> list[str]:
        cleaned = self.clean_text(text)
        out: list[str] = []
        for token in razdel_tokenize(cleaned):
            word = token.text.strip().lower().replace("ё", "е")
            if not word:
                continue
            if not TOKEN_RE.fullmatch(word):
                continue
            out.append(word)
        return out

    def lemmatize(self, text: str) -> str:
        tokens = self.tokenize(text)
        if not tokens:
            return ""

        lemmas: list[str] = []
        for token in tokens:
            if token in _NO_LEMMATIZE_TOKENS:
                lemmas.append(token)
                continue
            if CYRILLIC_RE.search(token):
                parsed = self._morph.parse(token)
                lemmas.append(parsed[0].normal_form)
            else:
                lemmas.append(token)
        return " ".join(lemmas)

    def remove_stopwords(self, text: str) -> str:
        cleaned = ASSORT_RE.sub(" ", self.clean_text(text))
        tokens = self.tokenize(cleaned)
        filtered = [token for token in tokens if token not in self._stopwords]
        return " ".join(filtered).strip()
