from __future__ import annotations

from converter.core.registry import HandlerRegistry

from .handler import FixPriceHandler


def register(registry: HandlerRegistry) -> None:
    registry.register(FixPriceHandler())


__all__ = ["FixPriceHandler", "register"]
