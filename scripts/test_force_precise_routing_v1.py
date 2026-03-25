"""Unit tests for execute_query_v1.resolve_force_precise_handler (no subprocess)."""

import unittest

import execute_query_v1 as eq


class ResolveForcePreciseTests(unittest.TestCase):
    def test_already_precise_kpis_unchanged(self) -> None:
        plan = {
            "mode": "precise",
            "retrieval_plan": {
                "handler": "precise_get_buyer_quarter_kpis",
                "query_family": "buyer_quarter_kpis",
            },
        }
        h, err = eq.resolve_force_precise_handler(plan)
        self.assertEqual(h, "precise_get_buyer_quarter_kpis")
        self.assertIsNone(err)

    def test_semantic_performance_with_buyer_and_quarter_maps_to_sql(self) -> None:
        plan = {
            "mode": "semantic",
            "entity": {"resolved_id": 9},
            "timeframe": {
                "granularity": "quarter",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "buyer_performance_summary",
            },
        }
        h, err = eq.resolve_force_precise_handler(plan)
        self.assertEqual(h, "precise_get_buyer_quarter_kpis")
        self.assertIsNone(err)

    def test_semantic_opportunities_rejected(self) -> None:
        plan = {
            "mode": "semantic",
            "entity": {"resolved_id": 1},
            "timeframe": {"granularity": "quarter"},
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "list_buyer_opportunities",
            },
        }
        h, err = eq.resolve_force_precise_handler(plan)
        self.assertIsNone(h)
        self.assertIsNotNone(err)
        self.assertIn("precise SQL path", err)

    def test_kpi_range_cli_from_plan_passes_start_end(self) -> None:
        plan = {
            "timeframe": {
                "granularity": "range",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
        }
        extras = eq.kpi_range_cli_from_plan("precise_get_buyer_quarter_kpis", plan)
        self.assertEqual(
            extras,
            ["--start-date", "2026-01-01", "--end-date", "2026-03-31"],
        )

    def test_kpi_range_cli_quarter_granularity_passes_start_end(self) -> None:
        plan = {
            "timeframe": {
                "granularity": "quarter",
                "raw_text": "Q1 2026",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
        }
        extras = eq.kpi_range_cli_from_plan("precise_get_buyer_quarter_kpis", plan)
        self.assertEqual(
            extras,
            ["--start-date", "2026-01-01", "--end-date", "2026-03-31"],
        )

    def test_kpi_range_cli_empty_for_semantic_handler(self) -> None:
        plan = {
            "timeframe": {
                "granularity": "range",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
        }
        self.assertEqual(eq.kpi_range_cli_from_plan("semantic_buyer_performance_summary", plan), [])

    def test_semantic_performance_year_only_rejected(self) -> None:
        plan = {
            "mode": "semantic",
            "entity": {"resolved_id": 3},
            "timeframe": {"granularity": "year"},
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "buyer_performance_summary",
            },
        }
        h, err = eq.resolve_force_precise_handler(plan)
        self.assertIsNone(h)
        self.assertIn("quarter", err.lower())


if __name__ == "__main__":
    unittest.main()
