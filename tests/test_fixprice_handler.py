from __future__ import annotations

import unittest
from datetime import datetime, timezone

from converter import build_default_pipeline
from converter.core.models import RawProductRecord
from converter.parsers.fixprice.handler import FixPriceHandler


class FixPriceHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.handler = FixPriceHandler()

    def test_title_parser_extracts_brand_and_stopwords(self) -> None:
        result = self.handler.normalize_title(
            'Ручка гелевая "Помада", With Love, 10х1,5 см, в ассортименте'
        )

        self.assertEqual(result.name_original, 'Ручка гелевая "Помада"')
        self.assertEqual(result.brand, "With Love")
        self.assertEqual(result.unit, "PCE")
        self.assertEqual(result.original_name_no_stopwords, "ручка гелевая помада")

    def test_title_parser_extracts_package_and_count(self) -> None:
        result = self.handler.normalize_title("Шоколад молочный, 200 г, 15 шт, в ассортименте")

        self.assertEqual(result.unit, "PCE")
        self.assertEqual(result.available_count, 15.0)
        self.assertEqual(result.package_unit, "KGM")
        self.assertAlmostEqual(result.package_quantity or 0.0, 0.2)


class PipelineBackfillTests(unittest.TestCase):
    def test_pipeline_backfills_missing_fields_from_previous_version(self) -> None:
        pipeline = build_default_pipeline()

        older = RawProductRecord(
            parser_name="fixprice",
            plu="10002",
            title="Шоколад молочный, 200 г, 15 шт",
            category="Продукты",
            geo="Санкт-Петербург",
            composition="Сахар, какао, молоко",
            observed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        )
        newer = RawProductRecord(
            parser_name="fixprice",
            plu="10002",
            title="Шоколад молочный, 200 г, 15 шт",
            category=None,
            geo=None,
            composition=None,
            observed_at=datetime(2026, 2, 2, tzinfo=timezone.utc),
        )

        first = pipeline.process_one(older)
        second = pipeline.process_one(newer)

        self.assertEqual(first.canonical_product_id, second.canonical_product_id)
        self.assertEqual(second.category_normalized, "продукты")
        self.assertEqual(second.geo_normalized, "санкт-петербург")
        self.assertEqual(second.composition_normalized, "сахар, какао, молоко")


if __name__ == "__main__":
    unittest.main()
