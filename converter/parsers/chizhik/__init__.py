from __future__ import annotations

from converter.core.registry import HandlerRegistry

from .handler import ChizhikHandler


def register(registry: HandlerRegistry) -> None:
    registry.register(ChizhikHandler())


__all__ = ["ChizhikHandler", "register"]
