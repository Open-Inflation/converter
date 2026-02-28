from __future__ import annotations

from .base import BaseParserHandler


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, BaseParserHandler] = {}

    def register(self, handler: BaseParserHandler) -> None:
        parser_name = handler.parser_name.strip().lower()
        if not parser_name:
            raise ValueError("Handler parser_name must be non-empty")
        if parser_name in self._handlers:
            raise ValueError(f"Handler for parser '{parser_name}' already exists")

        self._handlers[parser_name] = handler

    def get(self, parser_name: str) -> BaseParserHandler:
        key = parser_name.strip().lower()
        try:
            return self._handlers[key]
        except KeyError as exc:
            known = ", ".join(sorted(self._handlers)) or "<empty>"
            raise KeyError(f"No handler for parser '{parser_name}'. Known: {known}") from exc

    def registered_parsers(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))
