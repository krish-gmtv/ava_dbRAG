"""Tests for Pinecone metadata filter expansion (int vs string indexing)."""

import unittest

import semantic_search_pinecone_final as ss


class MetadataFilterVariantsTests(unittest.TestCase):
    def test_three_int_fields_produce_eight_variants(self) -> None:
        flt = {"buyer_id": 2, "period_year": 2019, "period_quarter": 1}
        v = ss.metadata_filter_variants(flt)
        self.assertEqual(len(v), 8)
        self.assertIn({"buyer_id": 2, "period_year": 2019, "period_quarter": 1}, v)
        self.assertIn({"buyer_id": "2", "period_year": "2019", "period_quarter": "1"}, v)

    def test_single_field(self) -> None:
        v = ss.metadata_filter_variants({"buyer_id": 7})
        self.assertEqual(len(v), 2)

    def test_build_policy_includes_buyer_fallback_by_default(self) -> None:
        pol = ss.build_policy_sequence(
            "buyer_quarter_year", 2, 2019, 1, 5, allow_buyer_only_fallback=True
        )
        self.assertTrue(any(p.get("_fallback") == "buyer_only" for p in pol))

    def test_build_policy_can_disable_buyer_fallback(self) -> None:
        pol = ss.build_policy_sequence(
            "buyer_quarter_year", 2, 2019, 1, 5, allow_buyer_only_fallback=False
        )
        self.assertFalse(any(p.get("_fallback") == "buyer_only" for p in pol))


if __name__ == "__main__":
    unittest.main()
