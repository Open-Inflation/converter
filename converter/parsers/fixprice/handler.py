from __future__ import annotations

import re
from typing import Any

from converter.core.base import BaseParserHandler
from converter.core.models import NormalizedProductRecord, RawProductRecord, TitleNormalizationResult
from converter.parsers.category_normalization import normalize_category_text
from converter.parsers.normalizers import RussianTextNormalizer

from .patterns import DIM_CM_RE, DIM_GENERIC_RE, WVL_RE
from .title_parser import FixPriceTitleParser

_DIMENSION_SEQUENCE_RE = re.compile(
    r"(?P<a>\d+(?:[.,]\d+)?)\s*(?P<ua>мм|см|м|mm|cm|m|метр(?:а|ов)?|meter|metre)?\s*[xх×]\s*"
    r"(?P<b>\d+(?:[.,]\d+)?)\s*(?P<ub>мм|см|м|mm|cm|m|метр(?:а|ов)?|meter|metre)?"
    r"(?:\s*[xх×]\s*(?P<c>\d+(?:[.,]\d+)?)\s*(?P<uc>мм|см|м|mm|cm|m|метр(?:а|ов)?|meter|metre)?)?"
    r"\s*(?P<tail>мм|см|м|mm|cm|m|метр(?:а|ов)?|meter|metre)?",
    re.IGNORECASE,
)
_LENGTH_RE = re.compile(
    r"(?P<num>\d+(?:[.,]\d+)?)\s*(?P<unit>мм|см|м|mm|cm|m|метр(?:а|ов)?|meter|metre)?",
    re.IGNORECASE,
)
_DIMENSION_KEY_RE = re.compile(
    r"(габарит|размер|dimension|size|д\s*[xх×]\s*ш|ш\s*[xх×]\s*в|в\s*[xх×]\s*г)",
    re.IGNORECASE,
)
_DIMENSION_ORDER_RE = re.compile(
    r"([швгдlhwd](?:\s*[xх×]\s*[швгдlhwd]){1,2})",
    re.IGNORECASE,
)
_HEIGHT_KEY_RE = re.compile(r"(высот|height|\bh\b)", re.IGNORECASE)
_WIDTH_KEY_RE = re.compile(r"(ширин|width|\bw\b)", re.IGNORECASE)
_DEPTH_KEY_RE = re.compile(r"(глубин|длин|depth|length|\bd\b|\bl\b)", re.IGNORECASE)
_HEIGHT_VALUE_RE = re.compile(
    r"(?:высот[аы]?|height)\s*[:=]?\s*(?P<num>\d+(?:[.,]\d+)?)\s*(?P<unit>мм|см|м|mm|cm|m|метр(?:а|ов)?|meter|metre)?",
    re.IGNORECASE,
)
_WIDTH_VALUE_RE = re.compile(
    r"(?:ширин[аы]?|width)\s*[:=]?\s*(?P<num>\d+(?:[.,]\d+)?)\s*(?P<unit>мм|см|м|mm|cm|m|метр(?:а|ов)?|meter|metre)?",
    re.IGNORECASE,
)
_DEPTH_VALUE_RE = re.compile(
    r"(?:глубин[аы]?|длин[аы]?|depth|length)\s*[:=]?\s*(?P<num>\d+(?:[.,]\d+)?)\s*(?P<unit>мм|см|м|mm|cm|m|метр(?:а|ов)?|meter|metre)?",
    re.IGNORECASE,
)
_MM_UNIT_RE = re.compile(r"\b(?:мм|mm)\b", re.IGNORECASE)
_CM_UNIT_RE = re.compile(r"\b(?:см|cm)\b", re.IGNORECASE)
_M_UNIT_RE = re.compile(r"\b(?:м|m|метр(?:а|ов)?|meter|metre)\b", re.IGNORECASE)
_DIMENSION_SEPARATOR_RE = re.compile(r"[xх×]", re.IGNORECASE)
_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)?")

_UNIT_FACTORS: dict[str, float] = {
    "mm": 0.001,
    "мм": 0.001,
    "cm": 0.01,
    "см": 0.01,
    "m": 1.0,
    "м": 1.0,
    "метр": 1.0,
    "meter": 1.0,
    "metre": 1.0,
    "метра": 1.0,
    "метров": 1.0,
}

_DIMENSION_SYMBOL_TO_AXIS = {
    "в": "height",
    "h": "height",
    "ш": "width",
    "w": "width",
    "г": "depth",
    "д": "depth",
    "d": "depth",
    "l": "depth",
}


def _safe_str(value: object) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _normalize_unit(unit: str | None) -> str | None:
    if unit is None:
        return None
    token = unit.strip().lower()
    return token if token in _UNIT_FACTORS else None


def _to_meters(value_raw: str, unit_raw: str | None, *, default_unit: str | None) -> float | None:
    try:
        number = float(value_raw.replace(",", "."))
    except ValueError:
        return None
    if number <= 0:
        return None

    unit = _normalize_unit(unit_raw) or _normalize_unit(default_unit)
    if unit is None:
        return None
    return number * _UNIT_FACTORS[unit]


def _detect_unit_hint(text: str | None) -> str | None:
    token = _safe_str(text)
    if token is None:
        return None
    if _MM_UNIT_RE.search(token):
        return "mm"
    if _CM_UNIT_RE.search(token):
        return "cm"
    if _M_UNIT_RE.search(token):
        return "m"
    return None


def _parse_single_length_m(text: str, *, default_unit: str | None) -> float | None:
    match = _LENGTH_RE.search(text)
    if match is None:
        return None
    return _to_meters(
        match.group("num"),
        match.group("unit"),
        default_unit=default_unit,
    )


def _parse_dimension_sequence_m(
    text: str,
    *,
    default_unit: str | None,
) -> tuple[float | None, float | None, float | None] | None:
    match = _DIMENSION_SEQUENCE_RE.search(text)
    if match is None:
        return None

    tail_unit = match.group("tail")
    first = _to_meters(
        match.group("a"),
        match.group("ua"),
        default_unit=tail_unit or default_unit,
    )
    second = _to_meters(
        match.group("b"),
        match.group("ub"),
        default_unit=tail_unit or default_unit,
    )
    third_raw = _safe_str(match.group("c"))
    third = (
        _to_meters(
            third_raw,
            match.group("uc"),
            default_unit=tail_unit or default_unit,
        )
        if third_raw is not None
        else None
    )

    if first is None or second is None:
        return None
    return first, second, third


def _parse_labeled_dimension_m(
    text: str,
    *,
    pattern: re.Pattern[str],
    default_unit: str | None,
) -> float | None:
    match = pattern.search(text)
    if match is None:
        return None
    return _to_meters(
        match.group("num"),
        match.group("unit"),
        default_unit=default_unit,
    )


def _parse_labeled_dimensions_m(
    text: str,
    *,
    default_unit: str | None,
) -> tuple[float | None, float | None, float | None] | None:
    height = _parse_labeled_dimension_m(text, pattern=_HEIGHT_VALUE_RE, default_unit=default_unit)
    width = _parse_labeled_dimension_m(text, pattern=_WIDTH_VALUE_RE, default_unit=default_unit)
    depth = _parse_labeled_dimension_m(text, pattern=_DEPTH_VALUE_RE, default_unit=default_unit)

    if all(value is None for value in (height, width, depth)):
        return None
    return height, width, depth


def _dimension_order_from_key(text: str | None) -> tuple[str, ...] | None:
    token = _safe_str(text)
    if token is None:
        return None
    match = _DIMENSION_ORDER_RE.search(token.lower())
    if match is None:
        return None

    raw_order = [part.strip() for part in _DIMENSION_SEPARATOR_RE.split(match.group(1)) if part.strip()]
    out: list[str] = []
    for part in raw_order:
        axis = _DIMENSION_SYMBOL_TO_AXIS.get(part[0])
        if axis is not None:
            out.append(axis)
    return tuple(out) if out else None


def _apply_dimension_order(
    values: tuple[float | None, float | None, float | None],
    order: tuple[str, ...] | None,
) -> tuple[float | None, float | None, float | None]:
    axis_order = ["height", "width", "depth"]
    if order:
        head = [axis for axis in order if axis in axis_order]
        tail = [axis for axis in axis_order if axis not in head]
        axis_order = [*head, *tail]

    out: dict[str, float | None] = {"height": None, "width": None, "depth": None}
    for idx, axis in enumerate(axis_order):
        if idx >= len(values):
            break
        value = values[idx]
        if value is not None:
            out[axis] = value
    return out["height"], out["width"], out["depth"]


def _looks_like_dimension_sequence(text: str | None) -> bool:
    token = _safe_str(text)
    if token is None:
        return False
    if _DIMENSION_SEPARATOR_RE.search(token) is None:
        return False
    return len(_NUMBER_RE.findall(token)) >= 2


def _fill_missing_dimensions(
    base: tuple[float | None, float | None, float | None],
    fallback: tuple[float | None, float | None, float | None] | None,
) -> tuple[float | None, float | None, float | None]:
    if fallback is None:
        return base
    return (
        base[0] if base[0] is not None else fallback[0],
        base[1] if base[1] is not None else fallback[1],
        base[2] if base[2] is not None else fallback[2],
    )


def _extract_dimensions_from_meta(payload: dict[str, Any]) -> tuple[float | None, float | None, float | None] | None:
    meta_rows = payload.get("receiver_product_meta")
    if not isinstance(meta_rows, list):
        return None

    height: float | None = None
    width: float | None = None
    depth: float | None = None
    sequence_candidate: tuple[float | None, float | None, float | None] | None = None

    for item in meta_rows:
        if not isinstance(item, dict):
            continue
        name = _safe_str(item.get("name"))
        alias = _safe_str(item.get("alias"))
        value = _safe_str(item.get("value_text"))
        if value is None:
            continue

        key = " ".join(part for part in (name, alias) if part) or None
        unit_hint = _detect_unit_hint(" ".join(part for part in (key, value) if part))
        default_unit = unit_hint or "cm"

        labeled = _parse_labeled_dimensions_m(value, default_unit=default_unit)
        if labeled is not None:
            height, width, depth = _fill_missing_dimensions((height, width, depth), labeled)

        if key:
            if height is None and _HEIGHT_KEY_RE.search(key):
                height = _parse_single_length_m(value, default_unit=default_unit)
            if width is None and _WIDTH_KEY_RE.search(key):
                width = _parse_single_length_m(value, default_unit=default_unit)
            if depth is None and _DEPTH_KEY_RE.search(key):
                depth = _parse_single_length_m(value, default_unit=default_unit)

        should_parse_sequence = _looks_like_dimension_sequence(value)
        if key and _DIMENSION_KEY_RE.search(key):
            should_parse_sequence = True

        if sequence_candidate is None and should_parse_sequence:
            sequence = _parse_dimension_sequence_m(value, default_unit=default_unit)
            if sequence is not None:
                sequence_candidate = _apply_dimension_order(sequence, _dimension_order_from_key(key))

    merged = _fill_missing_dimensions((height, width, depth), sequence_candidate)
    if all(value is None for value in merged):
        return None
    return merged


def _extract_dimensions_from_text(
    text: str | None,
) -> tuple[float | None, float | None, float | None] | None:
    token = _safe_str(text)
    if token is None:
        return None
    unit_hint = _detect_unit_hint(token) or "cm"

    labeled = _parse_labeled_dimensions_m(token, default_unit=unit_hint)
    sequence = _parse_dimension_sequence_m(token, default_unit=unit_hint)
    ordered_sequence = _apply_dimension_order(sequence, None) if sequence is not None else None

    merged = _fill_missing_dimensions((None, None, None), labeled)
    merged = _fill_missing_dimensions(merged, ordered_sequence)
    if all(value is None for value in merged):
        return None
    return merged


def _extract_dimensions_for_fixprice(raw: RawProductRecord) -> tuple[float | None, float | None, float | None] | None:
    payload = raw.payload if isinstance(raw.payload, dict) else {}
    merged = _fill_missing_dimensions((None, None, None), _extract_dimensions_from_text(raw.title))
    merged = _fill_missing_dimensions(merged, _extract_dimensions_from_meta(payload))
    merged = _fill_missing_dimensions(merged, _extract_dimensions_from_text(raw.brand))
    if all(value is None for value in merged):
        return None
    return merged


class FixPriceHandler(BaseParserHandler):
    parser_name = "fixprice"

    def __init__(self, text_normalizer: RussianTextNormalizer | None = None) -> None:
        normalizer = text_normalizer or RussianTextNormalizer()
        self._text_normalizer = normalizer
        self._title_parser = FixPriceTitleParser(text_normalizer=normalizer)

    def normalize_title(self, title: str) -> TitleNormalizationResult:
        return self._title_parser.parse(title)

    def handle(self, raw: RawProductRecord) -> NormalizedProductRecord:
        normalized = super().handle(raw)
        brand = normalized.brand.strip() if isinstance(normalized.brand, str) else ""
        if brand and (DIM_CM_RE.search(brand) or DIM_GENERIC_RE.search(brand) or WVL_RE.search(brand)):
            normalized.brand = None

        parsed_dimensions = _fill_missing_dimensions(
            (None, None, None),
            _extract_dimensions_from_text(normalized.title_normalized_no_stopwords),
        )
        parsed_dimensions = _fill_missing_dimensions(
            parsed_dimensions,
            _extract_dimensions_from_text(normalized.title_normalized),
        )
        parsed_dimensions = _fill_missing_dimensions(
            parsed_dimensions,
            _extract_dimensions_for_fixprice(raw),
        )
        if parsed_dimensions is not None:
            height_m, width_m, depth_m = parsed_dimensions
            if normalized.dimension_height_m is None and height_m is not None:
                normalized.dimension_height_m = height_m
            if normalized.dimension_width_m is None and width_m is not None:
                normalized.dimension_width_m = width_m
            if normalized.dimension_depth_m is None and depth_m is not None:
                normalized.dimension_depth_m = depth_m
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
