from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Optional, Tuple

from razdel import tokenize
import pymorphy2

Unit = Literal["PCE", "KGM", "LTR"]
PackageUnit = Literal["KGM", "LTR"]

morph = pymorphy2.MorphAnalyzer()

# --- паттерны ---
ASSORT_RE = re.compile(r"\bв\s+ассортименте\b", re.IGNORECASE)

# 10х1,5 см | 10x1.5 см | 10×1,5 см | 10х1,5х2 см
DIM_CM_RE = re.compile(
    r"(?P<a>\d+(?:[.,]\d+)?)\s*[xх×]\s*(?P<b>\d+(?:[.,]\d+)?)(?:\s*[xх×]\s*(?P<c>\d+(?:[.,]\d+)?))?\s*см\b",
    re.IGNORECASE,
)

# 200 г | 0,5 л | 500 мл | 1l
WVL_RE = re.compile(r"(?P<q>\d+(?:[.,]\d+)?)\s*(?P<u>г|кг|мл|л|l)\b", re.IGNORECASE)

BY_WEIGHT_RE = re.compile(r"\b(весов(ой|ая|ые)|на\s+вес)\b", re.IGNORECASE)
BY_VOLUME_RE = re.compile(r"\b(на\s+розлив|розлив|разлив)\b", re.IGNORECASE)

# стоп-слова (можешь расширять). Здесь базово: предлоги/служебные + мусор-маркеры.
STOPWORDS = {
    "в", "на", "для", "и", "с", "со", "по", "из", "к", "от", "при", "под", "над",
    "без", "про", "за", "у", "о", "об", "обо", "это", "эта", "этот", "эти",
    "ассортимент", "ассорти", "уп", "уп.", "упаковка", "упаковки",
}

QUOTE_RE = re.compile(r"[\"“”«»]")
NON_WORD_RE = re.compile(r"[^\w\s.,xх×-]+", re.UNICODE)
MULTISPACE_RE = re.compile(r"\s+")


def _to_float(s: str) -> float:
    return float(s.replace(",", ".").strip())


def _clean_text(s: str) -> str:
    """Лёгкая чистка: нижний регистр, ё->е, унификация x/х/×, убираем кавычки/лишние символы."""
    t = s.strip().lower().replace("ё", "е")
    t = t.replace("×", "x").replace("х", "x")  # кириллица -> латиница
    t = QUOTE_RE.sub("", t)
    t = NON_WORD_RE.sub(" ", t)
    t = MULTISPACE_RE.sub(" ", t).strip()
    return t


def _lemmatize(text: str) -> str:
    """
    Лемматизация (нормализация) по токенам.
    Важно: числа/латиница/бренды не ломаем — лемматизируем только кириллицу.
    """
    text = _clean_text(text)
    out = []
    for tok in tokenize(text):
        w = tok.text
        if re.fullmatch(r"[а-яa-z0-9-]+", w, re.IGNORECASE):
            if re.search(r"[а-я]", w):  # кириллица
                out.append(morph.parse(w)[0].normal_form)
            else:
                out.append(w)
    return " ".join(out).strip()


def _remove_stopwords(text: str) -> str:
    """Удаление стоп-слов по токенам (после чистки)."""
    text = _clean_text(text)
    # вырежем фразу целиком
    text = ASSORT_RE.sub(" ", text)
    toks = [t.text for t in tokenize(text)]
    toks = [t for t in toks if t not in STOPWORDS]
    return " ".join(toks).strip()


def _split_by_commas(title: str) -> list[str]:
    t = ASSORT_RE.sub("", title).strip(" ,")
    return [p.strip() for p in t.split(",") if p.strip()]


def _extract_package(title: str) -> Tuple[Optional[float], Optional[PackageUnit]]:
    """
    Извлекает package_quantity + package_unit из веса/объёма.
    Конвертация:
      г -> KGM (q/1000)
      кг -> KGM
      мл -> LTR (q/1000)
      л/l -> LTR
    Берём первое найденное совпадение (для fixprice обычно достаточно).
    """
    m = WVL_RE.search(title)
    if not m:
        return None, None
    q = _to_float(m.group("q"))
    u = m.group("u").lower()
    if u == "г":
        return q / 1000.0, "KGM"
    if u == "кг":
        return q, "KGM"
    if u == "мл":
        return q / 1000.0, "LTR"
    if u in ("л", "l"):
        return q, "LTR"
    return None, None


def _extract_count_heuristic(title: str) -> Optional[int]:
    """
    "int(сколько штук ...), опционально" — слово может быть любым.
    Поэтому:
      - вырезаем размеры (см) и вес/объём (г/мл/л)
      - вырезаем ассортимент
      - берём кандидатные числа и выбираем "самое правдоподобное" для количества:
        * предпочтение числам 2..200
        * если есть несколько — берём последнее подходящее
    """
    scrub = title
    scrub = DIM_CM_RE.sub(" ", scrub)
    scrub = WVL_RE.sub(" ", scrub)
    scrub = ASSORT_RE.sub(" ", scrub)

    nums = [int(x) for x in re.findall(r"\b\d+\b", scrub)]
    if not nums:
        return None

    # фильтр адекватных количеств
    plausible = [n for n in nums if 2 <= n <= 200]
    if plausible:
        return plausible[-1]
    # если только 1 — может быть и количество (например "1 шт"), но это сомнительно
    if len(nums) == 1 and 1 <= nums[0] <= 200:
        return nums[0]
    return None


def _guess_brand(parts: list[str]) -> Optional[str]:
    """
    Эвристика бренда: второй сегмент после первой запятой,
    если он не содержит чисел/единиц/размеров.
    """
    if len(parts) < 2:
        return None
    cand = parts[1]
    if DIM_CM_RE.search(cand) or WVL_RE.search(cand) or re.search(r"\b\d+\b", cand):
        return None
    # ещё отсечём слишком короткие "и", "в" и т.д.
    if len(_clean_text(cand)) < 2:
        return None
    return cand


@dataclass
class FixPriceParsed:
    raw_title: str

    name_original: str
    brand: Optional[str]

    name_normalized: str
    original_name_no_stopwords: str
    normalized_name_no_stopwords: str

    unit: Unit
    available_count: Optional[float]
    package_quantity: Optional[float]
    package_unit: Optional[PackageUnit]


def parse_fixprice_title(title: str) -> FixPriceParsed:
    raw = title.strip()
    parts = _split_by_commas(raw)

    # базово: имя — это первый сегмент, бренд — эвристика по второму
    name_original = parts[0] if parts else raw
    brand = _guess_brand(parts)

    title_wo_assort = ASSORT_RE.sub("", raw).strip(" ,")

    # package (вес/объем)
    package_qty, package_u = _extract_package(title_wo_assort)

    # count (шт в упаковке)
    count = _extract_count_heuristic(title_wo_assort)

    # unit по гайду
    if BY_WEIGHT_RE.search(title_wo_assort):
        unit: Unit = "KGM"
        available_count = None
        package_qty, package_u = None, None
    elif BY_VOLUME_RE.search(title_wo_assort):
        unit = "LTR"
        available_count = None
        package_qty, package_u = None, None
    else:
        unit = "PCE"
        available_count = float(count) if count is not None else None

    # нормализация имени (для дедупа обычно полезно включить бренд)
    name_for_norm = f"{name_original} {brand}" if brand else name_original
    name_normalized = _lemmatize(name_for_norm)

    original_no_sw = _remove_stopwords(name_original)
    normalized_no_sw = _remove_stopwords(name_normalized)

    return FixPriceParsed(
        raw_title=raw,
        name_original=name_original,
        brand=brand,
        name_normalized=name_normalized,
        original_name_no_stopwords=original_no_sw,
        normalized_name_no_stopwords=normalized_no_sw,
        unit=unit,
        available_count=available_count,
        package_quantity=package_qty,
        package_unit=package_u,
    )


# --- demo ---
if __name__ == "__main__":
    s = 'Ручка гелевая "Помада", With Love, 10х1,5 см, в ассортименте'
    print(parse_fixprice_title(s))

    s2 = "Шоколад молочный, 200 г, 15 шт, в ассортименте"
    print(parse_fixprice_title(s2))

    s3 = "Вода питьевая, 0,5 л, 6 бутылок, в ассортименте"
    print(parse_fixprice_title(s3))