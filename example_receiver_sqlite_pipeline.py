from __future__ import annotations

from pathlib import Path
from pprint import pprint

from converter import ReceiverSQLiteRepository, build_default_pipeline


def main() -> None:
    receiver_db = Path("../receiver/data/receiver.db").resolve()

    repository = ReceiverSQLiteRepository(receiver_db)
    pipeline = build_default_pipeline()

    records = repository.fetch_batch(limit=5, parser_name="fixprice")
    print(f"Loaded {len(records)} raw records from {receiver_db}")

    for item in pipeline.process_many(records):
        pprint(item)
        print("-" * 80)


if __name__ == "__main__":
    main()
