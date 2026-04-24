"""Tests for deterministic saved_report KPI narrative line."""

from __future__ import annotations

import unittest

from ava_phraser_v1 import deterministic_phrase


class AvaPhraserSavedReportKpiNarrativeTests(unittest.TestCase):
    def test_saved_report_includes_kpi_narrative_for_row_count(self) -> None:
        fr = {
            "mode": "saved_report",
            "request_summary": "Buyer upsheets listing: Buyer 119, Q2 2021.",
            "executive_summary": "",
            "kpi_snapshot": {"row_count": 1},
            "highlights": [],
            "notes": [],
            "suggested_next_question": "",
        }
        txt = deterministic_phrase(fr)
        self.assertIn("KPI snapshot", txt)
        self.assertIn("KPI narrative: Row count is 1", txt)


if __name__ == "__main__":
    unittest.main()

