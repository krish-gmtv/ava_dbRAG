"""Tests for intent routing: KPI/performance -> semantic; listings -> precise."""

import unittest

import intent_router_v1 as ir


class KpiAndPerformanceSemanticTests(unittest.TestCase):
    def test_kpi_family_is_semantic(self) -> None:
        plan = ir.build_execution_plan("What was Buyer 2's close rate in Q1 2018?")
        self.assertEqual(plan["mode"], "semantic")
        self.assertEqual(
            plan["retrieval_plan"]["handler"], "semantic_buyer_performance_summary"
        )
        self.assertEqual(plan["retrieval_plan"]["query_family"], "buyer_quarter_kpis")

    def test_performance_in_range_is_semantic(self) -> None:
        plan = ir.build_execution_plan(
            "How did Buyer 119 perform between 2026-01-01 and 2026-03-31?"
        )
        self.assertEqual(plan["mode"], "semantic")
        self.assertEqual(
            plan["retrieval_plan"]["handler"], "semantic_buyer_performance_summary"
        )

    def test_list_upsheets_is_precise(self) -> None:
        plan = ir.build_execution_plan("List all upsheets for Buyer 2 in Q1 2018")
        self.assertEqual(plan["mode"], "precise")
        self.assertEqual(
            plan["retrieval_plan"]["handler"], "precise_list_buyer_upsheets"
        )


class NormalizeCloseRateTests(unittest.TestCase):
    def test_zero_opportunities_sets_close_rate_zero(self) -> None:
        from precise_get_buyer_quarter_kpis import _normalize_close_rate_when_no_opportunities

        row = {"opportunity_upsheets": 0, "close_rate": None}
        _normalize_close_rate_when_no_opportunities(row)
        self.assertEqual(row["close_rate"], 0.0)


if __name__ == "__main__":
    unittest.main()
