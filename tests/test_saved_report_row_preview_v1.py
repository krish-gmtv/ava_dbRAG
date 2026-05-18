"""Tests: row preview carried into structured_report for saved_report."""

from __future__ import annotations

import unittest

from report_normalizer_v2 import BlockOutput, normalize_saved_report_v2
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

    def test_empty_opportunities_listing_still_has_columns(self) -> None:
        fr = normalize_saved_report_v2(
            user_query="x",
            template_id="t",
            display_name="T",
            buyer_id=119,
            period_label="Q2 2021",
            section_order=["opportunities_listing", "kpi_snapshot"],
            block_outputs=[
                BlockOutput(
                    block_id="row_listing_opportunities",
                    block_type="row_listing",
                    output_key="row_preview_tables",
                    source="precise",
                    payload={"rows_preview": [], "kpi_snapshot": {"total_opportunities": 0}},
                ),
            ],
        )
        sr = build_structured_report(fr)
        tables = sr["sections"]["row_preview_tables"]
        self.assertEqual(len(tables), 1)
        self.assertIn("created_at", tables[0]["table"]["columns"])
        self.assertEqual(tables[0]["table"]["rows"], [])


if __name__ == "__main__":
    unittest.main()

