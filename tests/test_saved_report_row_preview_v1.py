"""Tests: row preview carried into structured_report for saved_report."""

from __future__ import annotations

import unittest

from structured_report_v1 import build_structured_report


class SavedReportRowPreviewTests(unittest.TestCase):
    def test_saved_report_row_preview_table_shape(self) -> None:
        fr = {
            "mode": "saved_report",
            "template_id": "buyer_upsheets_listing_report_v1",
            "request_summary": "x",
            "executive_summary": "",
            "kpi_narrative": "",
            "kpi_snapshot": {"row_count": 1},
            "row_preview_tables": [
                {
                    "block_id": "row_listing_upsheets",
                    "title": "Upsheets",
                    "rows": [
                        {"upsheet_id": 1, "vin": "ABC", "sale_price": "1000"},
                        {"upsheet_id": 2, "vin": "DEF", "sale_price": "2000"},
                    ],
                }
            ],
            "highlights": [],
            "notes": [],
            "suggested_next_question": "",
        }
        sr = build_structured_report(fr)
        tables = sr["sections"]["row_preview_tables"]
        self.assertEqual(len(tables), 1)
        t = tables[0]["table"]
        self.assertEqual(t["columns"][:2], ["upsheet_id", "vin"])
        self.assertEqual(len(t["rows"]), 2)
        self.assertEqual(t["rows"][0][0], "1")


if __name__ == "__main__":
    unittest.main()

