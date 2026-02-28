from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Protocol

from .models import NormalizedProductRecord, RawProductRecord


class ReceiverRepository(Protocol):
    def fetch_batch(self, limit: int = 100) -> Iterable[RawProductRecord]:
        raise NotImplementedError


class CatalogRepository(Protocol):
    def upsert_many(self, records: Sequence[NormalizedProductRecord]) -> None:
        raise NotImplementedError


class StorageRepository(Protocol):
    def delete_images(self, urls: Sequence[str]) -> None:
        raise NotImplementedError
