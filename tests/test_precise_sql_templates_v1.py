"""Tests for NL→slots→CLI bridge (parameterized Postgres templates, not generative SQL)."""

import unittest

import precise_sql_templates_v1 as pst


class RegistryTests(unittest.TestCase):
    def test_registry_covers_known_handlers(self) -> None:
        handlers = {e["handler"] for e in pst.PRECISE_TEMPLATE_REGISTRY}
        self.assertIn("precise_list_buyer_upsheets", handlers)
        self.assertIn("precise_list_buyer_opportunities", handlers)


class ExtractSlotsTests(unittest.TestCase):
    def test_extract_slots_from_plan(self) -> None:
        plan = {
            "entity": {"resolved_id": "119"},
            "timeframe": {
                "granularity": "quarter",
                "start": "2026-01-01",
                "end": "2026-03-31",
                "raw_text": "Q1 2026",
            },
            "retrieval_plan": {
                "query_family": "list_buyer_opportunities",
                "handler": "precise_list_buyer_opportunities",
            },
        }
        slots = pst.extract_slots_from_plan(plan)
        self.assertEqual(slots["buyer_id"], 119)
        self.assertEqual(slots["timeframe"]["granularity"], "quarter")
        self.assertEqual(slots["timeframe"]["start"], "2026-01-01")
        self.assertEqual(slots["query_family"], "list_buyer_opportunities")


class CombineExtraArgsTests(unittest.TestCase):
    def test_combine_matches_planner_then_kpi_fallback(self) -> None:
        plan = {
            "entity": {"resolved_id": 2},
            "timeframe": {
                "granularity": "quarter",
                "start": "2018-01-01",
                "end": "2018-03-31",
            },
        }
        a = pst.combine_precise_extra_args("precise_list_buyer_upsheets", plan)
        self.assertIn("--buyer-id", a)
        self.assertIn("2018-01-01", a)


if __name__ == "__main__":
    unittest.main()
