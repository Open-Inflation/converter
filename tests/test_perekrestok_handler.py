from __future__ import annotations

import unittest

from converter import build_default_pipeline
from converter.core.models import RawProductRecord
from converter.parsers.perekrestok.handler import PerekrestokHandler


class PerekrestokHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.handler = PerekrestokHandler()

    def test_title_parser_extracts_single_package_volume(self) -> None:
        # Source: perekrestok_api snapshots
        result = self.handler.normalize_title("Вода питьевая Сенежская негазированная, 1.5л")

        self.assertEqual(result.unit, "PCE")
        self.assertAlmostEqual(result.package_quantity or 0.0, 1.5)
        self.assertEqual(result.package_unit, "LTR")
        self.assertIsNone(result.available_count)

    def test_title_parser_extracts_multipack_weight(self) -> None:
        # Source: perekrestok_api snapshots
        result = self.handler.normalize_title(
            "Пончик Перекрёсток Берлинский с варёной сгущёнкой, 2х64г"
        )

        self.assertEqual(result.unit, "PCE")
        self.assertEqual(result.available_count, 2.0)
        self.assertAlmostEqual(result.package_quantity or 0.0, 0.064)
        self.assertEqual(result.package_unit, "KGM")

    def test_title_parser_extracts_piece_count(self) -> None:
        # Source: perekrestok_api snapshots
        result = self.handler.normalize_title("Чеснок, 3шт")

        self.assertEqual(result.unit, "PCE")
        self.assertEqual(result.available_count, 3.0)
        self.assertIsNone(result.package_quantity)
        self.assertIsNone(result.package_unit)

    def test_title_parser_does_not_treat_dimensions_as_package(self) -> None:
        # Source: perekrestok_api snapshots
        result = self.handler.normalize_title("Сумка подарочная Арт и Дизайн DE 6.3х14.5х11.5см")

        self.assertEqual(result.unit, "PCE")
        self.assertIsNone(result.available_count)
        self.assertIsNone(result.package_quantity)
        self.assertIsNone(result.package_unit)

    def test_category_normalization_removes_separators_and_stopwords(self) -> None:
        result = self.handler.normalize_category("напитки и соки")

        self.assertEqual(result, "напиток сок")


class PerekrestokPipelineIntegrationTests(unittest.TestCase):
    def test_default_pipeline_resolves_perekrestok_handler(self) -> None:
        pipeline = build_default_pipeline()
        normalized = pipeline.process_one(
            RawProductRecord(
                parser_name="perekrestok",
                source_id="receiver:run-perekrestok:1",
                plu="2103416",
                title="Сумка подарочная Арт и Дизайн DE 6.3х14.5х11.5см",
            )
        )

        self.assertEqual(normalized.parser_name, "perekrestok")
        self.assertEqual(normalized.plu, "2103416")
        self.assertEqual(normalized.unit, "PCE")
        self.assertTrue(normalized.canonical_product_id)


if __name__ == "__main__":
    unittest.main()
