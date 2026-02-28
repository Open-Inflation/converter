from __future__ import annotations

import unittest

from converter import build_default_pipeline
from converter.core.models import RawProductRecord
from converter.parsers.chizhik.handler import ChizhikHandler


class ChizhikHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.handler = ChizhikHandler()

    def test_title_parser_extracts_single_package_weight(self) -> None:
        result = self.handler.normalize_title("Шоколад Вдохновение классический 100г")

        self.assertEqual(result.unit, "PCE")
        self.assertEqual(result.brand, "Вдохновение")
        self.assertAlmostEqual(result.package_quantity or 0.0, 0.1)
        self.assertEqual(result.package_unit, "KGM")
        self.assertIsNone(result.available_count)

    def test_title_parser_extracts_multipack(self) -> None:
        result = self.handler.normalize_title("Чай Greenfield Summer Bouquet травяной 25х2г")

        self.assertEqual(result.unit, "PCE")
        self.assertEqual(result.brand, "Greenfield Summer Bouquet")
        self.assertEqual(result.available_count, 25.0)
        self.assertAlmostEqual(result.package_quantity or 0.0, 0.002)
        self.assertEqual(result.package_unit, "KGM")

    def test_title_parser_extracts_piece_count(self) -> None:
        result = self.handler.normalize_title("Презервативы Contex Classic 3шт")

        self.assertEqual(result.unit, "PCE")
        self.assertEqual(result.brand, "Contex Classic")
        self.assertEqual(result.available_count, 3.0)
        self.assertIsNone(result.package_quantity)
        self.assertIsNone(result.package_unit)

    def test_title_parser_extracts_volume(self) -> None:
        result = self.handler.normalize_title("Молоко Простоквашино пастер. 3.4-4.5% 930мл")

        self.assertEqual(result.unit, "PCE")
        self.assertEqual(result.brand, "Простоквашино")
        self.assertAlmostEqual(result.package_quantity or 0.0, 0.93)
        self.assertEqual(result.package_unit, "LTR")

    def test_title_parser_handles_mixed_script_and_does_not_expand_cm(self) -> None:
        result = self.handler.normalize_title("Cалфетки Kitchen Collection 30x30см")

        self.assertEqual(result.name_normalized.split()[0], "салфетка")
        self.assertIn("см", result.name_normalized.split())
        self.assertNotIn("смотреть", result.name_normalized.split())

    def test_category_normalization_removes_separators_and_stopwords(self) -> None:
        result = self.handler.normalize_category("напитки и соки")

        self.assertEqual(result, "напиток сок")


class ChizhikPipelineIntegrationTests(unittest.TestCase):
    def test_default_pipeline_resolves_chizhik_handler(self) -> None:
        pipeline = build_default_pipeline()
        normalized = pipeline.process_one(
            RawProductRecord(
                parser_name="chizhik",
                plu="2070249",
                title="Чай Greenfield Summer Bouquet травяной 25х2г",
            )
        )

        self.assertEqual(normalized.parser_name, "chizhik")
        self.assertEqual(normalized.plu, "2070249")
        self.assertEqual(normalized.unit, "PCE")
        self.assertEqual(normalized.available_count, 25.0)
        self.assertAlmostEqual(normalized.package_quantity or 0.0, 0.002)
        self.assertEqual(normalized.package_unit, "KGM")
        self.assertTrue(normalized.canonical_product_id)


if __name__ == "__main__":
    unittest.main()
