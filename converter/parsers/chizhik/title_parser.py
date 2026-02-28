from __future__ import annotations

from converter.core.models import PackageUnit, TitleNormalizationResult, Unit
from converter.parsers.fixprice.normalizers import RussianTextNormalizer

from .patterns import (
    BY_VOLUME_RE,
    BY_WEIGHT_RE,
    LATIN_RE,
    MULTIPACK_RE,
    MULTISPACE_RE,
    PACKAGE_RE,
    PIECE_COUNT_RE,
)


def _to_float(value: str) -> float:
    return float(value.replace(",", ".").strip())


def _to_package_quantity(quantity_raw: str, unit_raw: str) -> tuple[float | None, PackageUnit | None]:
    quantity = _to_float(quantity_raw)
    unit = unit_raw.lower()

    if unit == "г":
        return quantity / 1000.0, "KGM"
    if unit == "кг":
        return quantity, "KGM"
    if unit == "мл":
        return quantity / 1000.0, "LTR"
    if unit in ("л", "l"):
        return quantity, "LTR"
    return None, None


def _extract_multipack(title: str) -> tuple[float | None, float | None, PackageUnit | None]:
    matches = list(MULTIPACK_RE.finditer(title))
    if not matches:
        return None, None, None

    match = matches[-1]
    available_count = float(int(match.group("count")))
    package_quantity, package_unit = _to_package_quantity(match.group("q"), match.group("u"))
    return available_count, package_quantity, package_unit


def _extract_package(title: str) -> tuple[float | None, PackageUnit | None]:
    matches = list(PACKAGE_RE.finditer(title))
    if not matches:
        return None, None

    match = matches[-1]
    return _to_package_quantity(match.group("q"), match.group("u"))


def _extract_piece_count(title: str) -> float | None:
    matches = list(PIECE_COUNT_RE.finditer(title))
    if not matches:
        return None
    return float(int(matches[-1].group("count")))


def _strip_pack_tokens(title: str) -> str:
    value = MULTIPACK_RE.sub(" ", title)
    value = PACKAGE_RE.sub(" ", value)
    value = PIECE_COUNT_RE.sub(" ", value)
    value = MULTISPACE_RE.sub(" ", value).strip(" ,.;:-")
    return value or title.strip()


def _is_uppercase_word(word: str) -> bool:
    letters = [char for char in word if char.isalpha()]
    if not letters:
        return False
    return all(char == char.upper() for char in letters)


def _is_title_case_word(word: str) -> bool:
    for char in word:
        if char.isalpha():
            return char == char.upper()
    return False


def _extract_brand(name_part: str) -> str | None:
    words = [token.strip(".,;:()[]{}\"'«»") for token in name_part.split()]
    words = [token for token in words if token]
    if len(words) < 2:
        return None

    candidates: list[str] = []
    for token in words[1:]:
        if any(char.isdigit() for char in token):
            break
        if LATIN_RE.search(token):
            candidates.append(token)
            continue
        if _is_uppercase_word(token) or _is_title_case_word(token):
            candidates.append(token)
            continue
        break

    if not candidates:
        return None

    return " ".join(candidates[:3])


class ChizhikTitleParser:
    def __init__(self, text_normalizer: RussianTextNormalizer | None = None) -> None:
        self._normalizer = text_normalizer or RussianTextNormalizer()

    def parse(self, title: str) -> TitleNormalizationResult:
        raw = title.strip()
        name_original = _strip_pack_tokens(raw)
        brand = _extract_brand(name_original)

        available_count, package_quantity, package_unit = _extract_multipack(raw)
        if available_count is None:
            available_count = _extract_piece_count(raw)
        if package_quantity is None and package_unit is None:
            package_quantity, package_unit = _extract_package(raw)

        if BY_WEIGHT_RE.search(raw):
            unit: Unit = "KGM"
            available_count = None
            package_quantity, package_unit = None, None
        elif BY_VOLUME_RE.search(raw):
            unit = "LTR"
            available_count = None
            package_quantity, package_unit = None, None
        else:
            unit = "PCE"

        name_for_normalization = name_original
        if brand and brand.lower() not in name_original.lower():
            name_for_normalization = f"{name_original} {brand}"
        name_normalized = self._normalizer.lemmatize(name_for_normalization)

        original_without_stopwords = self._normalizer.remove_stopwords(name_original)
        normalized_without_stopwords = self._normalizer.remove_stopwords(name_normalized)

        return TitleNormalizationResult(
            raw_title=raw,
            name_original=name_original,
            brand=brand,
            name_normalized=name_normalized,
            original_name_no_stopwords=original_without_stopwords,
            normalized_name_no_stopwords=normalized_without_stopwords,
            unit=unit,
            available_count=available_count,
            package_quantity=package_quantity,
            package_unit=package_unit,
        )
