"""Tests for structured report including saved_report kpi_narrative."""

from __future__ import annotations

import unittest

from structured_report_v1 import build_structured_report


class StructuredReportSavedReportKpiNarrativeTests(unittest.TestCase):
    def test_saved_report_includes_kpi_narrative_section(self) -> None:
        final_response = {
            "mode": "saved_report",
            "request_summary": "x",
            "executive_summary": "",
            "kpi_narrative": "Row count is 1 for the requested window.",
            "kpi_snapshot": {"row_count": 1},
            "highlights": [],
            "notes": [],
            "suggested_next_question": "",
            "template_id": "buyer_upsheets_listing_report_v1",
        }
        sr = build_structured_report(final_response)
        self.assertEqual(sr["report_kind"], "saved_report")
        self.assertEqual(
            sr["sections"].get("kpi_narrative"),
            "Row count is 1 for the requested window.",
        )


if __name__ == "__main__":
    unittest.main()

