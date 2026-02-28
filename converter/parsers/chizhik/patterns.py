from __future__ import annotations

import re

MULTIPACK_RE = re.compile(
    r"(?P<count>\d+)\s*[xх×]\s*(?P<q>\d+(?:[.,]\d+)?)\s*(?P<u>г|кг|мл|л|l)\b",
    re.IGNORECASE,
)

PACKAGE_RE = re.compile(r"(?P<q>\d+(?:[.,]\d+)?)\s*(?P<u>г|кг|мл|л|l)\b", re.IGNORECASE)
PIECE_COUNT_RE = re.compile(r"(?P<count>\d+)\s*(?:шт|штук)\b", re.IGNORECASE)

BY_WEIGHT_RE = re.compile(r"\b(весов(?:ой|ая|ые)?|на\s+вес)\b", re.IGNORECASE)
BY_VOLUME_RE = re.compile(r"\b(на\s+розлив|розлив|разлив)\b", re.IGNORECASE)

MULTISPACE_RE = re.compile(r"\s+")
LATIN_RE = re.compile(r"[a-z]", re.IGNORECASE)
