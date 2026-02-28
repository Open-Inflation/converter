from __future__ import annotations

from datetime import datetime, timezone
from pprint import pprint

from converter import build_default_pipeline
from converter.core.models import RawProductRecord


def main() -> None:
    pipeline = build_default_pipeline()

    records = [
        RawProductRecord(
            parser_name="fixprice",
            source_id="fp-001",
            plu="10001",
            title='Ручка гелевая "Помада", With Love, 10х1,5 см, в ассортименте',
            category="Канцтовары",
            geo="Москва",
            composition="Пластик, Чернила",
            image_urls=[
                "https://cdn.fixprice.example/images/pen-1.jpg",
                "https://cdn.fixprice.example/images/pen-1.jpg",
            ],
            observed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
            payload={"price": 99.0},
        ),
        RawProductRecord(
            parser_name="fixprice",
            source_id="fp-002",
            plu="10002",
            title="Шоколад молочный, 200 г, 15 шт, в ассортименте",
            category="Продукты",
            geo="Санкт-Петербург",
            composition="Сахар, какао, молоко",
            image_urls=["https://cdn.fixprice.example/images/choco-1.jpg"],
            observed_at=datetime(2026, 2, 1, 1, tzinfo=timezone.utc),
            payload={"price": 79.0},
        ),
        RawProductRecord(
            parser_name="fixprice",
            source_id="fp-003",
            plu="10002",
            title="Шоколад молочный, 200 г, 15 шт",
            category=None,
            geo=None,
            composition=None,
            image_urls=["https://img-mirror.example/choco-1-copy.jpg"],
            observed_at=datetime(2026, 2, 2, tzinfo=timezone.utc),
            payload={"price": 75.0},
        ),
    ]

    for item in pipeline.process_many(records):
        pprint(item)
        print("-" * 80)


if __name__ == "__main__":
    main()
