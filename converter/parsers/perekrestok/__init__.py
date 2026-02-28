from __future__ import annotations

from converter.core.registry import HandlerRegistry

from .handler import PerekrestokHandler


def register(registry: HandlerRegistry) -> None:
    registry.register(PerekrestokHandler())


__all__ = ["PerekrestokHandler", "register"]
