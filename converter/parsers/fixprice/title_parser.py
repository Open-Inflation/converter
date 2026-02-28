from __future__ import annotations

import re

from converter.core.models import PackageUnit, TitleNormalizationResult, Unit

from .normalizers import RussianTextNormalizer
from .patterns import ASSORT_RE, BY_VOLUME_RE, BY_WEIGHT_RE, DIM_CM_RE, WVL_RE


def _to_float(value: str) -> float:
    return float(value.replace(",", ".").strip())


def _split_by_commas(title: str) -> list[str]:
    no_assort = ASSORT_RE.sub("", title).strip(" ,")
    return [part.strip() for part in no_assort.split(",") if part.strip()]


def _extract_package(title: str) -> tuple[float | None, PackageUnit | None]:
    match = WVL_RE.search(title)
    if not match:
        return None, None

    quantity = _to_float(match.group("q"))
    unit = match.group("u").lower()

    if unit == "г":
        return quantity / 1000.0, "KGM"
    if unit == "кг":
        return quantity, "KGM"
    if unit == "мл":
        return quantity / 1000.0, "LTR"
    if unit in ("л", "l"):
        return quantity, "LTR"

    return None, None


def _extract_count_heuristic(title: str) -> int | None:
    scrubbed = DIM_CM_RE.sub(" ", title)
    scrubbed = WVL_RE.sub(" ", scrubbed)
    scrubbed = ASSORT_RE.sub(" ", scrubbed)

    numbers = [int(token) for token in re.findall(r"\b\d+\b", scrubbed)]
    if not numbers:
        return None

    plausible = [num for num in numbers if 2 <= num <= 200]
    if plausible:
        return plausible[-1]

    if len(numbers) == 1 and 1 <= numbers[0] <= 200:
        return numbers[0]

    return None


def _guess_brand(parts: list[str], normalizer: RussianTextNormalizer) -> str | None:
    if len(parts) < 2:
        return None

    candidate = parts[1]
    if DIM_CM_RE.search(candidate) or WVL_RE.search(candidate) or re.search(r"\b\d+\b", candidate):
        return None

    if len(normalizer.clean_text(candidate)) < 2:
        return None

    return candidate


class FixPriceTitleParser:
    def __init__(self, text_normalizer: RussianTextNormalizer | None = None) -> None:
        self._normalizer = text_normalizer or RussianTextNormalizer()

    def parse(self, title: str) -> TitleNormalizationResult:
        raw = title.strip()
        parts = _split_by_commas(raw)

        name_original = parts[0] if parts else raw
        brand = _guess_brand(parts, self._normalizer)

        title_without_assort = ASSORT_RE.sub("", raw).strip(" ,")

        package_quantity, package_unit = _extract_package(title_without_assort)
        count = _extract_count_heuristic(title_without_assort)

        if BY_WEIGHT_RE.search(title_without_assort):
            unit: Unit = "KGM"
            available_count = None
            package_quantity, package_unit = None, None
        elif BY_VOLUME_RE.search(title_without_assort):
            unit = "LTR"
            available_count = None
            package_quantity, package_unit = None, None
        else:
            unit = "PCE"
            available_count = float(count) if count is not None else None

        name_for_normalization = f"{name_original} {brand}" if brand else name_original
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
