from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from .models import ChunkApplyResultV2, RawProductRecord, SyncChunkV2


class ReceiverRepository(Protocol):
    def fetch_batch(
        self,
        limit: int = 100,
        *,
        parser_name: str | None = None,
        after_ingested_at: str | None = None,
        after_product_id: int | None = None,
    ) -> Iterable[RawProductRecord]:
        raise NotImplementedError


class CatalogWriteRepositoryV2(Protocol):
    def apply_chunk(self, chunk: SyncChunkV2) -> ChunkApplyResultV2:
        raise NotImplementedError

    def get_receiver_cursor(self, parser_name: str) -> tuple[str | None, int | None]:
        raise NotImplementedError


class StorageRepository(Protocol):
    def delete_images(self, urls: Sequence[str]) -> None:
        raise NotImplementedError
