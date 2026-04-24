"""Unit tests for saved-report template executor (mocked retrieval)."""

import unittest
from unittest.mock import patch

import scripts.templates.template_executor_v1 as te


def _fake_render(
    *,
    narrative_exec: str = "Narrative summary.",
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
                "semantic_quality": {"render_mode": "ok"},
            },
            "semantic_buyer_performance_summary",
        ),
    }


class TemplateExecutorMergeTests(unittest.TestCase):
    def test_execute_merges_narrative_and_precise_kpi(self) -> None:
        plan = {
            "template_id": "buyer_performance_report_v1",
            "display_name": "Buyer performance report",
            "section_order": ["request_summary", "executive_summary", "kpi_snapshot"],
            "slots": {
                "buyer_id": 2,
                "timeframe": {
                    "raw_text": "Q1 2019",
                    "start": "2019-01-01",
                    "end": "2019-03-31",
                },
            },
            "data_blocks": [
                {
                    "block_id": "semantic_quarterly_narrative",
                    "block_type": "executive_summary",
                    "output_key": "executive_summary",
                    "status": "selected",
                },
                {
                    "block_id": "kpi_snapshot_quarter",
                    "block_type": "kpi_table",
                    "output_key": "kpi_snapshot",
                    "source_mode": "precise",
                    "query_family_hint": "buyer_quarter_kpis",
                    "status": "selected",
                },
                {"block_id": "row_listing_upsheets", "status": "skipped_not_requested"},
            ],
        }
        fakes = _fake_render()

        def side_effect(_ij: str, q: str, force_precise: bool = False) -> dict:
            if "How did Buyer" in q:
                return fakes["semantic_quarterly_narrative"]
            raise AssertionError(f"unexpected query: {q}")

        with patch.object(te, "get_combined_payload", side_effect=side_effect):
            with patch.object(
                te,
                "_run_precise_buyer_quarter_kpis",
                return_value={
                    "result": {"close_rate": "42.1%", "conversion_rate": "18.3%"}
                },
            ):
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
        self.assertEqual(fr["kpi_snapshot"].get("close_rate"), "42.1%")
        self.assertIn("n1", fr["highlights"])
        self.assertEqual(len(out["template_block_runs"]), 2)
        self.assertTrue(isinstance(out.get("template_block_outputs_v2"), list))

        pm = out.get("prompt_modules") or {}
        self.assertEqual(pm.get("module_selection_source"), "default")
        self.assertEqual(
            pm.get("modules_used"),
            ["executive_summary", "kpi_narrative", "highlights", "notes"],
        )
        self.assertEqual(pm.get("modules_referenced_by_template"), [])
        self.assertEqual(pm.get("unknown_modules_in_template"), [])

    def test_execute_honors_template_prompt_modules_selection(self) -> None:
        plan = {
            "template_id": "buyer_performance_report_v1",
            "display_name": "Buyer performance report",
            "section_order": ["request_summary", "executive_summary", "kpi_snapshot"],
            "slots": {
                "buyer_id": 2,
                "timeframe": {
                    "raw_text": "Q1 2019",
                    "start": "2019-01-01",
                    "end": "2019-03-31",
                },
            },
            # Template-level selection: omit 'notes' intentionally.
            "prompt_modules": ["executive_summary", "highlights"],
            "data_blocks": [
                {
                    "block_id": "semantic_quarterly_narrative",
                    "block_type": "executive_summary",
                    "output_key": "executive_summary",
                    "status": "selected",
                },
                {
                    "block_id": "kpi_snapshot_quarter",
                    "block_type": "kpi_table",
                    "output_key": "kpi_snapshot",
                    "source_mode": "precise",
                    "query_family_hint": "buyer_quarter_kpis",
                    "status": "selected",
                },
            ],
        }
        fakes = _fake_render()

        def side_effect(_ij: str, q: str, force_precise: bool = False) -> dict:
            if "How did Buyer" in q:
                return fakes["semantic_quarterly_narrative"]
            raise AssertionError(f"unexpected query: {q}")

        with patch.object(te, "get_combined_payload", side_effect=side_effect):
            with patch.object(
                te,
                "_run_precise_buyer_quarter_kpis",
                return_value={"result": {"close_rate": "42.1%"}},
            ):
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

        pm = out.get("prompt_modules") or {}
        self.assertEqual(pm.get("module_selection_source"), "template")
        self.assertEqual(pm.get("modules_used"), ["executive_summary", "highlights"])
        self.assertEqual(
            pm.get("modules_referenced_by_template"),
            ["executive_summary", "highlights"],
        )
        self.assertEqual(pm.get("unknown_modules_in_template"), [])
        payload_modules = [m["module_id"] for m in pm["assembled_payload"]["modules"]]
        self.assertEqual(payload_modules, ["executive_summary", "highlights"])

    def test_execute_falls_back_when_template_references_unknown_modules(self) -> None:
        plan = {
            "template_id": "buyer_performance_report_v1",
            "display_name": "Buyer performance report",
            "section_order": ["request_summary", "executive_summary", "kpi_snapshot"],
            "slots": {
                "buyer_id": 2,
                "timeframe": {
                    "raw_text": "Q1 2019",
                    "start": "2019-01-01",
                    "end": "2019-03-31",
                },
            },
            "prompt_modules": ["executive_summary", "nonexistent_module"],
            "data_blocks": [
                {
                    "block_id": "semantic_quarterly_narrative",
                    "block_type": "executive_summary",
                    "output_key": "executive_summary",
                    "status": "selected",
                },
                {
                    "block_id": "kpi_snapshot_quarter",
                    "block_type": "kpi_table",
                    "output_key": "kpi_snapshot",
                    "source_mode": "precise",
                    "query_family_hint": "buyer_quarter_kpis",
                    "status": "selected",
                },
            ],
        }
        fakes = _fake_render()

        def side_effect(_ij: str, q: str, force_precise: bool = False) -> dict:
            if "How did Buyer" in q:
                return fakes["semantic_quarterly_narrative"]
            raise AssertionError(f"unexpected query: {q}")

        with patch.object(te, "get_combined_payload", side_effect=side_effect):
            with patch.object(
                te,
                "_run_precise_buyer_quarter_kpis",
                return_value={"result": {"close_rate": "42.1%"}},
            ):
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

        pm = out.get("prompt_modules") or {}
        self.assertEqual(pm.get("module_selection_source"), "default_fallback")
        self.assertEqual(pm.get("unknown_modules_in_template"), ["nonexistent_module"])
        self.assertEqual(
            pm.get("modules_used"),
            ["executive_summary", "kpi_narrative", "highlights", "notes"],
        )

    def test_execute_listing_only_template_does_not_require_narrative(self) -> None:
        plan = {
            "template_id": "buyer_upsheets_listing_report_v1",
            "display_name": "Buyer upsheets listing",
            "section_order": ["request_summary", "kpi_snapshot", "notes", "next_steps"],
            "prompt_modules": ["kpi_narrative", "notes"],
            "slots": {
                "buyer_id": 2,
                "timeframe": {
                    "raw_text": "Q2 2021",
                    "start": "2021-04-01",
                    "end": "2021-06-30",
                },
            },
            "data_blocks": [
                {
                    "block_id": "row_listing_upsheets",
                    "block_type": "row_listing",
                    "output_key": "kpi_snapshot",
                    "status": "selected",
                }
            ],
        }

        def side_effect(_ij: str, q: str, force_precise: bool = False) -> dict:
            # Ensure it tries to call the listing query path.
            if "List all upsheets" in q or "upsheets" in q.lower():
                return {
                    "execution_plan": {
                        "entity": {"resolved_id": 2},
                        "timeframe": {"raw_text": "Q2 2021"},
                        "retrieval_plan": {"query_family": "list_buyer_upsheets"},
                        "mode": "precise",
                    },
                    "selected_handler": "precise_list_buyer_upsheets",
                    "final_response": {
                        "mode": "precise",
                        "kpi_snapshot": {"row_count": 3},
                        "data_coverage_notes": ["Source database: ava_sandboxV2"],
                        "suggested_next_question": "Want performance report?",
                    },
                }
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
                    "list upsheets for Buyer 2 in Q2 2021",
                    plan,
                )

        fr = out["final_response"]
        self.assertEqual(fr["mode"], "saved_report")
        self.assertEqual(fr["template_id"], "buyer_upsheets_listing_report_v1")
        self.assertEqual(fr["kpi_snapshot"].get("row_count"), 3)
        pm = out.get("prompt_modules") or {}
        self.assertEqual(pm.get("modules_used"), ["kpi_narrative", "notes"])
        # listing-only template should not require semantic_quality
        self.assertTrue(fr.get("semantic_quality") is None or isinstance(fr.get("semantic_quality"), dict))


if __name__ == "__main__":
    unittest.main()
