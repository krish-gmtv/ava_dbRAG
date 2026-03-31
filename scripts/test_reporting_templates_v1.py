"""Unit tests for unified reporting headlines in answer_renderer_v1."""

import unittest

import answer_renderer_v1 as ar


class ReportingTemplateTests(unittest.TestCase):
    def test_precise_upsheets_and_close_rate_share_grammar(self) -> None:
        plan = {"mode": "precise", "entity": {"resolved_id": 1}}
        upsheets = {
            "query_type": "list_buyer_upsheets",
            "input_query": "List upsheets for Buyer 1 in Q1 2018",
            "params": {"buyer_id": 1, "period_year": 2018, "period_quarter": 1},
            "result": {"rows": [], "row_count": 0},
        }
        kpi = {
            "query_type": "buyer_quarter_kpis",
            "input_query": "What was Buyer 2 close rate in Q1 2018?",
            "params": {"buyer_id": 2, "period_year": 2018, "period_quarter": 1},
            "result": {"close_rate": None},
        }
        self.assertEqual(
            ar.render_precise(plan, upsheets)["request_summary"],
            "Buyer 1 upsheets for Q1 2018.",
        )
        self.assertEqual(
            ar.render_precise(plan, kpi)["request_summary"],
            "Buyer 2 close rate for Q1 2018.",
        )

    def test_semantic_kpi_headline_matches_precise_kpi_shape(self) -> None:
        plan = {
            "mode": "semantic",
            "entity": {"resolved_id": 1},
            "retrieval_plan": {"query_family": "buyer_quarter_kpis"},
            "intent": "buyer_quarter_kpis",
        }
        ho = {
            "params": {"buyer_id": 1, "period_year": 2018, "period_quarter": 1},
            "input_query": "What was Buyer 1 close rate in Q1 2018?",
            "result": {"matches": [{"buyer_name": "Acme", "summary_snippet": "ok"}]},
        }
        self.assertEqual(
            ar.render_semantic(plan, ho)["request_summary"],
            "Buyer 1 close rate for Q1 2018.",
        )

    def test_semantic_performance_summary_default_subject(self) -> None:
        plan = {
            "mode": "semantic",
            "entity": {"resolved_id": 7},
            "retrieval_plan": {"query_family": "buyer_performance_summary"},
            "intent": "buyer_performance",
        }
        ho = {
            "params": {"buyer_id": 7, "period_year": 2018, "period_quarter": 1},
            "input_query": "How did Buyer 7 perform in Q1 2018?",
            "result": {"matches": [{"buyer_name": "Co", "summary_snippet": "x"}]},
        }
        self.assertEqual(
            ar.render_semantic(plan, ho)["request_summary"],
            "Buyer 7 performance summary for Q1 2018.",
        )


if __name__ == "__main__":
    unittest.main()
