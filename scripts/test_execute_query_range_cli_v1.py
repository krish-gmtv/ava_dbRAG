"""
Integration-style test: range timeframe causes KPI subprocess to receive --start-date/--end-date.
Mocks subprocess only; no database or Pinecone.
"""

import io
import json
import sys
import unittest
from unittest.mock import MagicMock, patch

import execute_query_v1 as eq


class ExecuteQueryKpiRangeCliTests(unittest.TestCase):
    def test_kpi_subprocess_gets_date_range_from_plan(self) -> None:
        router_out = {
            "mode": "precise",
            "entity": {"resolved_id": 119},
            "timeframe": {
                "granularity": "range",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
            "retrieval_plan": {
                "handler": "precise_get_buyer_quarter_kpis",
                "query_family": "buyer_quarter_kpis",
            },
            "reason_codes": [],
            "report_template_id": "buyer_detail_v1_precise",
        }
        kpi_out = {
            "query_type": "buyer_quarter_kpis",
            "params": {"buyer_id": 119},
            "result": {},
            "input_query": "Buyer 119 between 2026-01-01 and 2026-03-31",
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
                m.stdout = json.dumps(kpi_out)
            return m

        argv = [
            "execute_query_v1.py",
            "--query",
            "Buyer 119 between 2026-01-01 and 2026-03-31",
        ]
        buf = io.StringIO()
        with patch.object(sys, "argv", argv):
            with patch("execute_query_v1.subprocess.run", side_effect=fake_run):
                with patch.object(sys, "stdout", buf):
                    eq.main()

        kpi_cmds = [
            c for c in calls if any("precise_get_buyer_quarter_kpis" in p for p in c)
        ]
        self.assertEqual(len(kpi_cmds), 1)
        kpi_cmd = kpi_cmds[0]
        self.assertIn("--query", kpi_cmd)
        self.assertIn("--start-date", kpi_cmd)
        self.assertIn("2026-01-01", kpi_cmd)
        self.assertIn("--end-date", kpi_cmd)
        self.assertIn("2026-03-31", kpi_cmd)


if __name__ == "__main__":
    unittest.main()
