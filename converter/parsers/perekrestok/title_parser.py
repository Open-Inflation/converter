from __future__ import annotations

from converter.parsers.chizhik.title_parser import ChizhikTitleParser


class PerekrestokTitleParser(ChizhikTitleParser):
    """
    Perekrestok title format is compatible with Chizhik-like extraction rules:
    - multipack `2х64г`
    - single package `1.5л` / `100г`
    - piece count `3шт`
    """

