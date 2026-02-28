from __future__ import annotations

from converter.core.registry import HandlerRegistry

from . import chizhik
from . import fixprice
from . import perekrestok


def register_builtin_handlers(registry: HandlerRegistry) -> None:
    fixprice.register(registry)
    chizhik.register(registry)
    perekrestok.register(registry)


__all__ = ["register_builtin_handlers"]
