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
        self.assertIsNone(result.available_count)
        self.assertEqual(result.package_count, 15.0)
        self.assertEqual(result.package_unit, "KGM")
        self.assertAlmostEqual(result.package_quantity or 0.0, 0.2)

    def test_title_parser_extracts_package_count_from_compact_pattern(self) -> None:
        result = self.handler.normalize_title("Салфетки бумажные, 20шт, в ассортименте")

        self.assertEqual(result.unit, "PCE")
        self.assertEqual(result.package_count, 20.0)
        self.assertIsNone(result.available_count)

    def test_handle_keeps_stock_available_count_separate_from_package_count(self) -> None:
        raw = RawProductRecord(
            parser_name="fixprice",
            title="Шоколад молочный, 200 г, 15 шт, в ассортименте",
            available_count=52.0,
        )
        normalized = self.handler.handle(raw)

        self.assertEqual(normalized.available_count, 52.0)
        self.assertEqual(normalized.package_count, 15.0)

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

    def test_title_parser_splits_latin_prefix_from_cyrillic_word_without_letter_fragmentation(self) -> None:
        result = self.handler.normalize_title("PROактивность")

        self.assertEqual(result.name_normalized, "pro активность")
        self.assertEqual(result.normalized_name_no_stopwords, "pro активность")

    def test_title_parser_does_not_treat_dimensions_as_brand(self) -> None:
        result = self.handler.normalize_title("Пакет подарочный, 12х14, в ассортименте")

        self.assertEqual(result.name_original, "Пакет подарочный")
        self.assertIsNone(result.brand)

    def test_handle_drops_dimension_like_raw_brand(self) -> None:
        raw = RawProductRecord(
            parser_name="fixprice",
            title="Пакет подарочный, в ассортименте",
            brand="12х14",
        )

        normalized = self.handler.handle(raw)
        self.assertIsNone(normalized.brand)

    def test_handle_extracts_dimensions_from_meta_sequence_with_order_hint(self) -> None:
        raw = RawProductRecord(
            parser_name="fixprice",
            title="Органайзер",
            payload={
                "receiver_product_meta": [
                    {"name": "Габариты (ШхВхГ), см", "value_text": "20x30x40"},
                ]
            },
        )

        normalized = self.handler.handle(raw)
        self.assertAlmostEqual(normalized.dimension_height_m or 0.0, 0.3)
        self.assertAlmostEqual(normalized.dimension_width_m or 0.0, 0.2)
        self.assertAlmostEqual(normalized.dimension_depth_m or 0.0, 0.4)

    def test_handle_extracts_dimensions_from_title_without_unit_as_centimeters(self) -> None:
        raw = RawProductRecord(
            parser_name="fixprice",
            title="Пакет подарочный 12х14",
        )

        normalized = self.handler.handle(raw)
        self.assertAlmostEqual(normalized.dimension_height_m or 0.0, 0.12)
        self.assertAlmostEqual(normalized.dimension_width_m or 0.0, 0.14)
        self.assertIsNone(normalized.dimension_depth_m)

    def test_handle_extracts_dimensions_from_labeled_meta_rows(self) -> None:
        raw = RawProductRecord(
            parser_name="fixprice",
            title="Коробка",
            payload={
                "receiver_product_meta": [
                    {"name": "Высота", "value_text": "1,2 м"},
                    {"name": "Ширина", "value_text": "30 см"},
                    {"name": "Глубина", "value_text": "450 мм"},
                ]
            },
        )

        normalized = self.handler.handle(raw)
        self.assertAlmostEqual(normalized.dimension_height_m or 0.0, 1.2)
        self.assertAlmostEqual(normalized.dimension_width_m or 0.0, 0.3)
        self.assertAlmostEqual(normalized.dimension_depth_m or 0.0, 0.45)

    def test_handle_keeps_explicit_raw_dimensions(self) -> None:
        raw = RawProductRecord(
            parser_name="fixprice",
            title="Пакет подарочный 12х14",
            dimension_height_m=2.0,
            dimension_width_m=3.0,
            dimension_depth_m=4.0,
        )

        normalized = self.handler.handle(raw)
        self.assertEqual(normalized.dimension_height_m, 2.0)
        self.assertEqual(normalized.dimension_width_m, 3.0)
        self.assertEqual(normalized.dimension_depth_m, 4.0)

    def test_normalized_title_no_stopwords_does_not_include_brand_tokens(self) -> None:
        result = self.handler.normalize_title("Форма для кулича, O'Kitchen, в ассортименте")

        self.assertEqual(result.brand, "O'Kitchen")
        self.assertNotIn("kitchen", result.normalized_name_no_stopwords)
        self.assertNotIn("o", result.normalized_name_no_stopwords.split())

    def test_normalized_title_no_stopwords_does_not_include_cyrillic_brand(self) -> None:
        result = self.handler.normalize_title("Красители пищевые жидкие, Перцов")

        self.assertEqual(result.brand, "Перцов")
        self.assertNotIn("перцов", result.normalized_name_no_stopwords)
        self.assertNotIn("перцовый", result.normalized_name_no_stopwords)

    def test_category_normalization_removes_separators_and_lemmatizes(self) -> None:
        result = self.handler.normalize_category("молочные продукты, яйца")

        self.assertEqual(result, "молочный продукт яйцо")

    def test_composition_normalization_lemmatizes_and_removes_stopwords(self) -> None:
        result = self.handler.normalize_composition("Сахар, какао и молоко")

        self.assertEqual(result, "сахар какао молоко")

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
        self.assertEqual(second.composition_normalized, "сахар какао молоко")

    def test_pipeline_does_not_merge_different_sku_by_normalized_name(self) -> None:
        pipeline = build_default_pipeline()

        first = RawProductRecord(
            parser_name="fixprice",
            sku="5093200",
            source_id="receiver:run-1:1",
            title="Тарелка десертная O`Kit",
            observed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        )
        second = RawProductRecord(
            parser_name="fixprice",
            sku="5093201",
            source_id="receiver:run-1:2",
            title="Тарелка десертная O`Kit",
            observed_at=datetime(2026, 2, 2, tzinfo=timezone.utc),
        )

        first_norm = pipeline.process_one(first)
        second_norm = pipeline.process_one(second)

        self.assertNotEqual(first_norm.canonical_product_id, second_norm.canonical_product_id)

    def test_pipeline_merges_by_normalized_name_when_plu_and_sku_missing(self) -> None:
        pipeline = build_default_pipeline()

        first = RawProductRecord(
            parser_name="fixprice",
            source_id="receiver:run-1:1",
            title="Тарелка десертная O`Kit",
            observed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        )
        second = RawProductRecord(
            parser_name="fixprice",
            source_id="receiver:run-2:2",
            title="Тарелка десертная O`Kit",
            observed_at=datetime(2026, 2, 2, tzinfo=timezone.utc),
        )

        first_norm = pipeline.process_one(first)
        second_norm = pipeline.process_one(second)

        self.assertEqual(first_norm.canonical_product_id, second_norm.canonical_product_id)


if __name__ == "__main__":
    unittest.main()
