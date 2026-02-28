from __future__ import annotations

from collections.abc import Iterable

from .core.models import NormalizedProductRecord, RawProductRecord
from .core.registry import HandlerRegistry
from .core.services import (
    InMemoryProductIdentityResolver,
    NullBackfillService,
    PersistentImageDeduplicator,
    ProductIdentityResolver,
)


class ConverterPipeline:
    def __init__(
        self,
        registry: HandlerRegistry,
        identity_resolver: ProductIdentityResolver | None = None,
        backfill_service: NullBackfillService | None = None,
        image_deduplicator: PersistentImageDeduplicator | None = None,
    ) -> None:
        self._registry = registry
        self._identity_resolver = identity_resolver or InMemoryProductIdentityResolver()
        self._backfill_service = backfill_service or NullBackfillService()
        self._image_deduplicator = image_deduplicator or PersistentImageDeduplicator()

    def process_one(self, raw_record: RawProductRecord) -> NormalizedProductRecord:
        handler = self._registry.get(raw_record.parser_name)
        normalized = handler.handle(raw_record)

        normalized.canonical_product_id = self._identity_resolver.resolve(normalized)

        dedup_result = self._image_deduplicator.process(normalized.image_urls)
        normalized.image_urls = dedup_result.unique_urls
        normalized.duplicate_image_urls = dedup_result.duplicate_urls
        normalized.image_fingerprints = dedup_result.fingerprints

        self._backfill_service.apply(normalized)

        return normalized

    def process_many(self, records: Iterable[RawProductRecord]) -> list[NormalizedProductRecord]:
        return [self.process_one(record) for record in records]
