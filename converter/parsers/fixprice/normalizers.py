from __future__ import annotations

from .patterns import ASSORT_RE, CYRILLIC_RE, MULTISPACE_RE, NON_WORD_RE, QUOTE_RE, STOPWORDS, TOKEN_RE


class RussianTextNormalizer:
    def __init__(self) -> None:
        self._morph = self._build_morph()

    @staticmethod
    def _build_morph() -> object | None:
        try:
            import pymorphy2  # type: ignore
        except Exception:
            return None

        try:
            return pymorphy2.MorphAnalyzer()
        except Exception:
            return None

    def clean_text(self, text: str) -> str:
        cleaned = text.strip().lower().replace("ё", "е")
        cleaned = cleaned.replace("×", "x").replace("х", "x")
        cleaned = QUOTE_RE.sub("", cleaned)
        cleaned = NON_WORD_RE.sub(" ", cleaned)
        cleaned = MULTISPACE_RE.sub(" ", cleaned).strip()
        return cleaned

    def tokenize(self, text: str) -> list[str]:
        cleaned = self.clean_text(text)
        return TOKEN_RE.findall(cleaned)

    def lemmatize(self, text: str) -> str:
        tokens = self.tokenize(text)
        if not tokens:
            return ""
        if self._morph is None:
            return " ".join(tokens)

        lemmas: list[str] = []
        for token in tokens:
            if CYRILLIC_RE.search(token):
                parsed = self._morph.parse(token)
                lemmas.append(parsed[0].normal_form)
            else:
                lemmas.append(token)
        return " ".join(lemmas)

    def remove_stopwords(self, text: str) -> str:
        cleaned = self.clean_text(text)
        cleaned = ASSORT_RE.sub(" ", cleaned)

        tokens = TOKEN_RE.findall(cleaned)
        filtered = [token for token in tokens if token not in STOPWORDS]
        return " ".join(filtered).strip()
