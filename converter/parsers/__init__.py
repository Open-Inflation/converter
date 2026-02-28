from __future__ import annotations

from converter.core.registry import HandlerRegistry

from . import fixprice


def register_builtin_handlers(registry: HandlerRegistry) -> None:
    fixprice.register(registry)


__all__ = ["register_builtin_handlers"]
