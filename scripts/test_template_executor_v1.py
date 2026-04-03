"""Unit tests for saved-report template executor (mocked retrieval)."""

import unittest
from unittest.mock import patch

import template_executor_v1 as te


def _fake_render(
    *,
    narrative_exec: str = "Narrative summary.",
    kpi_exec: str = "KPI narrative.",
) -> dict:
    def _one(final: dict, handler: str) -> dict:
        return {
            "execution_plan": {
                "entity": {"resolved_id": 2},
                "timeframe": {"raw_text": "Q1 2019"},
                "retrieval_plan": {"query_family": "buyer_performance_summary"},
            },
            "selected_handler": handler,
            "final_response": final,
        }

    return {
        "semantic_quarterly_narrative": _one(
            {
                "mode": "semantic",
                "executive_summary": narrative_exec,
                "trend_narrative": "",
                "highlights": ["n1"],
                "suggested_next_question": "Want listing?",
            },
            "semantic_buyer_performance_summary",
        ),
        "kpi_snapshot_quarter": _one(
            {
                "mode": "semantic",
                "executive_summary": kpi_exec,
                "highlights": ["k1"],
                "suggested_next_question": "Want listing?",
            },
            "semantic_buyer_performance_summary",
        ),
    }


class TemplateExecutorMergeTests(unittest.TestCase):
    def test_execute_merges_narrative_and_kpi(self) -> None:
        plan = {
            "template_id": "buyer_performance_report_v1",
            "display_name": "Buyer performance report",
            "slots": {"buyer_id": 2, "timeframe": {"raw_text": "Q1 2019"}},
            "data_blocks": [
                {"block_id": "semantic_quarterly_narrative", "status": "selected"},
                {"block_id": "kpi_snapshot_quarter", "status": "selected"},
                {"block_id": "row_listing_upsheets", "status": "skipped_not_requested"},
            ],
        }
        fakes = _fake_render()

        def side_effect(_ij: str, q: str, force_precise: bool = False) -> dict:
            if "How did Buyer" in q:
                return fakes["semantic_quarterly_narrative"]
            if "KPI metrics" in q:
                return fakes["kpi_snapshot_quarter"]
            raise AssertionError(f"unexpected query: {q}")

        with patch.object(te, "get_combined_payload", side_effect=side_effect):
            with patch.object(te, "run_phrasing_for_final_response") as ph:
                ph.return_value = {
                    "mode": "deterministic",
                    "text": "x",
                    "validation": {"is_valid": True},
                    "error": None,
                }
                out = te.execute_saved_report_plan(
                    "buyer performance report for Buyer 2 in Q1 2019",
                    plan,
                )

        fr = out["final_response"]
        self.assertEqual(fr["mode"], "saved_report")
        self.assertEqual(fr["template_id"], "buyer_performance_report_v1")
        self.assertIn("Narrative summary", fr["executive_summary"])
        self.assertEqual(fr["kpi_snapshot"].get("kpi_summary"), "KPI narrative.")
        self.assertIn("n1", fr["highlights"])
        self.assertIn("k1", fr["highlights"])
        self.assertEqual(len(out["template_block_runs"]), 2)


if __name__ == "__main__":
    unittest.main()
