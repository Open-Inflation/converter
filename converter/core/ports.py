from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol
from collections.abc import Sequence

from .models import AckResult, ChunkApplyResultV2, RawProductRecord, SyncChunkV2


class ReceiverRepository(Protocol):
    def fetch_batch(
        self,
        limit: int = 100,
        *,
        parser_name: str | None = None,
    ) -> Iterable[RawProductRecord]:
        raise NotImplementedError

    def delete_processed_products(
        self,
        product_ids: Sequence[int],
        *,
        chunk_started_at: float,
    ) -> AckResult:
        raise NotImplementedError


class CatalogWriteRepositoryV2(Protocol):
    def apply_chunk(self, chunk: SyncChunkV2) -> ChunkApplyResultV2:
        raise NotImplementedError


class StorageRepository(Protocol):
    def persist_images(self, urls: Sequence[str]) -> list[str]:
        raise NotImplementedError

    def get_image_sizes(self, urls: Sequence[str]) -> list[int | None]:
        raise NotImplementedError

    def delete_images(self, urls: Sequence[str]) -> None:
        raise NotImplementedError
