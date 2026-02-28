from __future__ import annotations

import re

ASSORT_RE = re.compile(r"\bв\s+ассортименте\b", re.IGNORECASE)

DIM_CM_RE = re.compile(
    r"(?P<a>\d+(?:[.,]\d+)?)\s*[xх×]\s*(?P<b>\d+(?:[.,]\d+)?)(?:\s*[xх×]\s*(?P<c>\d+(?:[.,]\d+)?))?\s*см\b",
    re.IGNORECASE,
)

WVL_RE = re.compile(r"(?P<q>\d+(?:[.,]\d+)?)\s*(?P<u>г|кг|мл|л|l)\b", re.IGNORECASE)

BY_WEIGHT_RE = re.compile(r"\b(весов(?:ой|ая|ые)?|на\s+вес)\b", re.IGNORECASE)
BY_VOLUME_RE = re.compile(r"\b(на\s+розлив|розлив|разлив)\b", re.IGNORECASE)

STOPWORDS = {
    "в",
    "на",
    "для",
    "и",
    "с",
    "со",
    "по",
    "из",
    "к",
    "от",
    "при",
    "под",
    "над",
    "без",
    "про",
    "за",
    "у",
    "о",
    "об",
    "обо",
    "это",
    "эта",
    "этот",
    "эти",
    "ассортимент",
    "ассорти",
    "уп",
    "уп.",
    "упаковка",
    "упаковки",
}

QUOTE_RE = re.compile(r"[\"“”«»]")
NON_WORD_RE = re.compile(r"[^\w\s.,xх×-]+", re.UNICODE)
MULTISPACE_RE = re.compile(r"\s+")
TOKEN_RE = re.compile(r"[a-zа-я0-9-]+", re.IGNORECASE)
CYRILLIC_RE = re.compile(r"[а-я]", re.IGNORECASE)
