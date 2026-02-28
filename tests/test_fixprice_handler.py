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

    def test_title_parser_handles_mixed_latin_cyrillic_prefix_without_split(self) -> None:
        result = self.handler.normalize_title("Cалфетки Kitchen Collection 30x30см")

        self.assertEqual(result.original_name_no_stopwords.split()[0], "салфетки")
        self.assertEqual(result.name_normalized.split()[0], "салфетка")
        self.assertIn("см", result.name_normalized.split())
        self.assertNotIn("смотреть", result.name_normalized.split())

    def test_title_parser_handles_latin_x_in_cyrillic_word_without_split(self) -> None:
        result = self.handler.normalize_title("Xлебцы Magic Grain мультизлаковые")

        self.assertEqual(result.original_name_no_stopwords.split()[0], "хлебцы")
        self.assertEqual(result.name_normalized.split()[0], "хлебец")

    def test_category_normalization_removes_separators_and_lemmatizes(self) -> None:
        result = self.handler.normalize_category("молочные продукты, яйца")

        self.assertEqual(result, "молочный продукт яйцо")

    def test_category_normalization_removes_stopwords(self) -> None:
        result = self.handler.normalize_category("напитки и соки")

        self.assertEqual(result, "напиток сок")

    def test_geo_normalization_has_no_manual_remap(self) -> None:
        result = self.handler.normalize_geo("Российская Федерация")

        self.assertEqual(result, "российская федерация")


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
        self.assertEqual(second.category_normalized, "продукт")
        self.assertEqual(second.geo_normalized, "санкт-петербург")
        self.assertEqual(second.composition_original, "Сахар, какао, молоко")
        self.assertEqual(second.composition_normalized, "сахар, какао, молоко")


if __name__ == "__main__":
    unittest.main()
