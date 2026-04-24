"""Tests for KPI narrative enforcement in saved_report phrasing validation."""

from __future__ import annotations

import unittest

from validate_ava_output_v1 import validate_ava_output


class ValidateAvaOutputSavedReportKpiNarrativeTests(unittest.TestCase):
    def test_saved_report_warns_without_kpi_narrative_when_kpis_present(self) -> None:
        fr = {
            "mode": "saved_report",
            "request_summary": "x",
            "executive_summary": "y",
            "kpi_snapshot": {"row_count": 1},
            "highlights": [],
            "notes": [],
            "suggested_next_question": "z",
        }
        out = validate_ava_output(
            final_response=fr,
            phrased_text="Request summary\n\nExecutive summary\ny\n\nKPI snapshot\n- Row Count: 1\n\nNext: z",
            strict_headings=False,
        )
        self.assertTrue(out["is_valid"])
        self.assertTrue(any("kpi narrative" in w.lower() for w in out["warnings"]))

    def test_saved_report_strict_requires_kpi_narrative_when_kpis_present(self) -> None:
        fr = {
            "mode": "saved_report",
            "request_summary": "x",
            "executive_summary": "y",
            "kpi_snapshot": {"row_count": 1},
            "highlights": [],
            "notes": [],
            "suggested_next_question": "z",
        }
        out = validate_ava_output(
            final_response=fr,
            phrased_text="Request summary\n\nExecutive summary\ny\n\nKPI snapshot\n- Row Count: 1\n\nNext: z",
            strict_headings=True,
        )
        self.assertFalse(out["is_valid"])
        self.assertTrue(any("kpi narrative" in e.lower() for e in out["errors"]))


if __name__ == "__main__":
    unittest.main()

