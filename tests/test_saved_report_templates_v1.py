"""Tests for saved report template registry and orchestrator (v1)."""

import unittest

import saved_report_templates_v1 as srt
import template_matcher_v1 as m
import template_report_orchestrator_v1 as orch


class TemplateMatcherTests(unittest.TestCase):
    def test_match_longest_trigger_wins(self) -> None:
        # "buyer performance report" should beat shorter "buyer report" in scoring
        tid = m.match_saved_report_template("I need a buyer performance report for B2")
        self.assertEqual(tid, "buyer_performance_report_v1")
    
    def test_match_upsheets_listing_template(self) -> None:
        tid = m.match_saved_report_template("Please list upsheets for Buyer 5 in Q2 2021")
        self.assertEqual(tid, "buyer_upsheets_listing_report_v1")

    def test_no_match_empty_or_irrelevant(self) -> None:
        self.assertIsNone(m.match_saved_report_template(""))
        self.assertIsNone(m.match_saved_report_template("   "))
        self.assertIsNone(m.match_saved_report_template("what is the weather"))

    def test_explicit_listing_phrases(self) -> None:
        self.assertTrue(m.explicit_listing_requested("show list upsheet for buyer"))
        self.assertTrue(m.explicit_listing_requested("include raw rows in the report"))
        self.assertFalse(
            m.explicit_listing_requested("buyer performance report for B1 Q1 2018")
        )

    def test_missing_required_slots_buyer(self) -> None:
        tpl = srt.get_template("buyer_performance_report_v1")
        self.assertIn(
            "buyer",
            m.missing_required_slots(tpl, {"buyer_id": None, "timeframe": {"raw_text": "Q1 2018"}}),
        )

    def test_extract_slots(self) -> None:
        tpl = srt.get_template("buyer_performance_report_v1")
        slots = m.extract_template_slots(
            "buyer report for Buyer 5 in quarter 3 2019", tpl
        )
        self.assertEqual(slots.get("buyer_id"), 5)
        self.assertIsNotNone(slots.get("timeframe"))


class SavedReportTemplateRegistryTests(unittest.TestCase):
    def test_buyer_performance_template_registered(self) -> None:
        t = srt.get_template("buyer_performance_report_v1")
        self.assertIn("request_summary", t.section_order)
        self.assertIn("kpi_snapshot", t.section_order)
        self.assertIn("buyer", t.required_slots)


class TemplateOrchestratorTests(unittest.TestCase):
    def test_no_match_for_generic_question(self) -> None:
        # Routing convergence: buyer+period performance question should map to saved report.
        plan = orch.plan_saved_report("How did Buyer 2 perform in Q1 2019?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan["template_id"], "buyer_performance_report_v1")
    
    def test_match_performance_typo_perfomance(self) -> None:
        plan = orch.plan_saved_report("what was the buyer perfomance of buyer 119 in Q2 2021?")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan["template_id"], "buyer_performance_report_v1")

    def test_match_and_slots(self) -> None:
        q = "I want a buyer performance report on Buyer 2 for Q1 2019"
        plan = orch.plan_saved_report(q)
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan["template_id"], "buyer_performance_report_v1")
        self.assertEqual(plan["slots"]["buyer_id"], 2)
        self.assertEqual(plan["slots"]["timeframe"]["granularity"], "quarter")
        self.assertIn("2019", plan["slots"]["timeframe"]["raw_text"])
        self.assertEqual(plan["missing_required_slots"], [])
        self.assertTrue(plan["ready_to_execute"])

    def test_upsheets_listing_template_requires_timeframe(self) -> None:
        q = "list upsheets for Buyer 2"
        plan = orch.plan_saved_report(q)
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan["template_id"], "buyer_upsheets_listing_report_v1")
        # timeframe is required for this template
        self.assertIn("timeframe", plan["missing_required_slots"])
        self.assertFalse(plan["ready_to_execute"])

    def test_upsheets_listing_template_ready(self) -> None:
        q = "list upsheets for Buyer 2 in Q2 2021"
        plan = orch.plan_saved_report(q)
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan["template_id"], "buyer_upsheets_listing_report_v1")
        self.assertEqual(plan["slots"]["buyer_id"], 2)
        self.assertEqual(plan["slots"]["timeframe"]["granularity"], "quarter")
        self.assertEqual(plan["missing_required_slots"], [])
        self.assertTrue(plan["ready_to_execute"])

    def test_missing_buyer(self) -> None:
        q = "give me a buyer performance report for Q1 2019"
        plan = orch.plan_saved_report(q)
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertIn("buyer", plan["missing_required_slots"])
        self.assertFalse(plan["ready_to_execute"])

    def test_listing_block_skipped_by_default(self) -> None:
        q = "buyer performance report for Buyer 1 in Q1 2018"
        plan = orch.plan_saved_report(q)
        self.assertIsNotNone(plan)
        assert plan is not None
        blocks = {b["block_id"]: b for b in plan["data_blocks"]}
        self.assertEqual(
            blocks["row_listing_upsheets"]["status"], "skipped_not_requested"
        )

    def test_listing_block_selected_when_explicit(self) -> None:
        q = "buyer performance report for Buyer 1 in Q1 2018 also list upsheets"
        plan = orch.plan_saved_report(q)
        self.assertIsNotNone(plan)
        assert plan is not None
        blocks = {b["block_id"]: b for b in plan["data_blocks"]}
        self.assertEqual(blocks["row_listing_upsheets"]["status"], "selected")


if __name__ == "__main__":
    unittest.main()
