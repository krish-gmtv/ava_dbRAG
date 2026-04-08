"""
Integration-style test: range timeframe causes listing subprocess to receive
--start-date/--end-date from the plan. Mocks subprocess only; no database.
"""

import io
import json
import sys
import unittest
from unittest.mock import MagicMock, patch

import execute_query_v1 as eq


class ExecuteQueryListingRangeCliTests(unittest.TestCase):
    def test_upsheets_subprocess_gets_date_range_from_plan(self) -> None:
        router_out = {
            "mode": "precise",
            "entity": {"resolved_id": 119},
            "timeframe": {
                "granularity": "range",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
            "retrieval_plan": {
                "handler": "precise_list_buyer_upsheets",
                "query_family": "list_buyer_upsheets",
            },
            "reason_codes": [],
            "report_template_id": "buyer_detail_v1_precise",
        }
        listing_out = {
            "query_type": "list_buyer_upsheets",
            "params": {"buyer_id": 119},
            "result": {"row_count": 0, "rows": []},
            "input_query": "List upsheets for Buyer 119 between 2026-01-01 and 2026-03-31",
        }
        calls: list[list[str]] = []

        def fake_run(cmd: list, capture_output=True, text=True, check=False) -> MagicMock:
            calls.append([str(x) for x in cmd])
            m = MagicMock()
            m.returncode = 0
            m.stderr = ""
            script = calls[-1][1].replace("\\", "/")
            if script.endswith("intent_router_v1.py"):
                m.stdout = json.dumps(router_out)
            else:
                m.stdout = json.dumps(listing_out)
            return m

        argv = [
            "execute_query_v1.py",
            "--query",
            "List upsheets for Buyer 119 between 2026-01-01 and 2026-03-31",
        ]
        buf = io.StringIO()
        with patch.object(sys, "argv", argv):
            with patch("execute_query_v1.subprocess.run", side_effect=fake_run):
                with patch.object(sys, "stdout", buf):
                    eq.main()

        ups_cmds = [
            c for c in calls if any("precise_list_buyer_upsheets" in p for p in c)
        ]
        self.assertEqual(len(ups_cmds), 1)
        ups_cmd = ups_cmds[0]
        self.assertIn("--query", ups_cmd)
        self.assertIn("--start-date", ups_cmd)
        self.assertIn("2026-01-01", ups_cmd)
        self.assertIn("--end-date", ups_cmd)
        self.assertIn("2026-03-31", ups_cmd)


if __name__ == "__main__":
    unittest.main()
