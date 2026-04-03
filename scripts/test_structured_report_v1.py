"""Tests for deterministic structured report mapping (no synthetic language)."""

import unittest

import structured_report_v1 as sr


class StructuredReportMappingTests(unittest.TestCase):
    def test_precise_maps_snapshot_to_rows(self) -> None:
        fr = {
            "mode": "precise",
            "request_summary": "Buyer 1 upsheets for Q1 2018.",
            "kpi_snapshot": {"row_count": 3, "total_opportunities": 0},
            "data_coverage_notes": ["Note one"],
            "suggested_next_question": "Next?",
        }
        rep = sr.build_structured_report(fr)
        self.assertEqual(rep["schema_version"], sr.REPORT_SCHEMA_VERSION)
        self.assertEqual(rep["report_kind"], "precise")
        rows = rep["sections"]["kpi_table"]
        self.assertEqual(len(rows), 2)
        keys = {r["metric_key"] for r in rows}
        self.assertEqual(keys, {"row_count", "total_opportunities"})
        self.assertEqual(rep["sections"]["notes"], ["Note one"])

    def test_semantic_includes_optional_fields(self) -> None:
        fr = {
            "mode": "semantic",
            "request_summary": "Buyer 2 performance for 2019.",
            "executive_summary": "Summary text.",
            "trend_narrative": "Different trend.",
            "highlights": ["H1"],
            "confidence_note": "C1",
            "key_drivers": ["D1"],
            "suggested_next_question": "S1",
            "retrieval_status": "Limited match quality.",
            "semantic_quality": {"confidence_level": "medium", "render_mode": "weak_semantic"},
        }
        rep = sr.build_structured_report(fr)
        self.assertEqual(rep["report_kind"], "semantic")
        self.assertEqual(rep["sections"]["trend_narrative"], "Different trend.")
        self.assertEqual(rep["sections"]["highlights"], ["H1"])
        self.assertEqual(rep["sections"]["confidence_note"], "C1")
        self.assertEqual(rep["sections"]["retrieval_status"], "Limited match quality.")
        self.assertIn("semantic_quality", rep["sections"])

    def test_semantic_drops_redundant_trend_when_same_as_executive(self) -> None:
        fr = {
            "mode": "semantic",
            "request_summary": "R",
            "executive_summary": "Same",
            "trend_narrative": "Same",
            "highlights": [],
            "suggested_next_question": "",
        }
        rep = sr.build_structured_report(fr)
        self.assertIsNone(rep["sections"]["trend_narrative"])

    def test_force_precise_unavailable(self) -> None:
        fr = {
            "mode": "force_precise_unavailable",
            "request_summary": "Unavailable.",
            "executive_summary": "Detail.",
            "suggested_next_question": "Do this instead.",
        }
        rep = sr.build_structured_report(fr)
        self.assertEqual(rep["report_kind"], "force_precise_unavailable")
        self.assertEqual(rep["sections"]["executive_summary"], "Detail.")

    def test_saved_report_kind(self) -> None:
        fr = {
            "mode": "saved_report",
            "template_id": "buyer_performance_report_v1",
            "request_summary": "Buyer performance report: Buyer 1, Q1 2018.",
            "executive_summary": "Exec body.",
            "kpi_snapshot": {"kpi_summary": "KPI text", "listing_row_count": "3"},
            "highlights": ["H1"],
            "notes": ["N1"],
            "suggested_next_question": "Next?",
        }
        rep = sr.build_structured_report(fr)
        self.assertEqual(rep["report_kind"], "saved_report")
        self.assertEqual(rep["sections"]["executive_summary"], "Exec body.")
        keys = {r["metric_key"] for r in rep["sections"]["kpi_table"]}
        self.assertEqual(keys, {"kpi_summary", "listing_row_count"})

    def test_ui_fallback(self) -> None:
        rep = sr.build_structured_report_from_ui_fallback(
            display_text="Hello",
            source_mode="gratitude",
        )
        self.assertEqual(rep["report_kind"], "ui_message")
        self.assertEqual(rep["sections"]["message"], "Hello")


if __name__ == "__main__":
    unittest.main()
