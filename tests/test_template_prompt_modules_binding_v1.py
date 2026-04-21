"""Tests for template <-> prompt module binding (Milestone 4)."""

from __future__ import annotations

import unittest

from saved_report_templates_v1 import (
    BUYER_PERFORMANCE_REPORT_V1,
    DataBlockSpec,
    SavedReportTemplate,
)
from template_report_orchestrator_v1 import plan_saved_report


class TemplatePromptModuleBindingTests(unittest.TestCase):
    def test_buyer_performance_template_declares_prompt_modules(self) -> None:
        tpl = BUYER_PERFORMANCE_REPORT_V1
        self.assertEqual(
            tpl.prompt_modules,
            ("executive_summary", "kpi_narrative", "highlights", "notes"),
        )

    def test_plan_carries_template_prompt_modules_through(self) -> None:
        plan = plan_saved_report("I want a buyer performance report on Buyer 2 for Q1 2019")
        self.assertIsNotNone(plan)
        assert plan is not None  # for type checkers
        self.assertEqual(
            plan.get("prompt_modules"),
            ["executive_summary", "kpi_narrative", "highlights", "notes"],
        )

    def test_template_rejects_unknown_prompt_modules_at_construction(self) -> None:
        block = DataBlockSpec(
            block_id="b1",
            description="test",
            block_type="executive_summary",
            retrieval_mode="semantic",
            query_family_hint="x",
            output_key="executive_summary",
        )
        with self.assertRaises(ValueError):
            SavedReportTemplate(
                template_id="bad_template_v1",
                display_name="Bad",
                purpose="-",
                trigger_phrases=("bad",),
                required_slots=frozenset({"buyer"}),
                optional_slots=frozenset(),
                section_order=("executive_summary",),
                data_blocks=(block,),
                disallowed_without_explicit_request=frozenset(),
                phrasing_rules=(),
                prompt_modules=("executive_summary", "does_not_exist"),
            )


if __name__ == "__main__":
    unittest.main()
