from __future__ import annotations

from collections.abc import Iterable

from razdel import tokenize as razdel_tokenize
from stop_words import get_stop_words

from .patterns import ASSORT_RE, CYRILLIC_RE, MULTISPACE_RE, NON_WORD_RE, QUOTE_RE, TOKEN_RE


class RussianTextNormalizer:
    def __init__(self, extra_stopwords: Iterable[str] | None = None) -> None:
        import pymorphy3  # type: ignore

        self._morph = pymorphy3.MorphAnalyzer()
        self._stopwords = {word.lower().replace("ё", "е") for word in get_stop_words("ru")}
        if extra_stopwords is not None:
            self._stopwords.update({word.lower().replace("ё", "е") for word in extra_stopwords})

    def clean_text(self, text: str) -> str:
        cleaned = text.strip().lower().replace("ё", "е")
        cleaned = cleaned.replace("×", "x").replace("х", "x")
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
