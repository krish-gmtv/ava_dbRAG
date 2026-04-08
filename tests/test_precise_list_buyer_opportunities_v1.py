"""
Direct unit tests for precise_list_buyer_opportunities.py.
No database is required; SQL execution is mocked.
Tests: date parsing, quarter parsing, CLI overrides, payload shape, period-semantics note.
"""

import io
import json
import sys
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import precise_list_buyer_opportunities as handler


class ParseBuyerIdTests(unittest.TestCase):
    def test_standard_format(self) -> None:
        self.assertEqual(handler.parse_buyer_id("List opportunities for Buyer 119 in Q1 2026"), 119)

    def test_case_insensitive(self) -> None:
        self.assertEqual(handler.parse_buyer_id("buyer 2 Q1 2018"), 2)

    def test_missing_returns_none(self) -> None:
        self.assertIsNone(handler.parse_buyer_id("What happened last quarter?"))


class ParseQuarterTests(unittest.TestCase):
    def test_standard(self) -> None:
        self.assertEqual(handler.parse_quarter("Q1 2026"), (2026, 1))

    def test_case_insensitive(self) -> None:
        self.assertEqual(handler.parse_quarter("buyer 2 q4 2018"), (2018, 4))

    def test_no_match(self) -> None:
        self.assertEqual(handler.parse_quarter("no dates here"), (None, None))


class ParseBetweenDatesTests(unittest.TestCase):
    def test_between_iso(self) -> None:
        start, end = handler.parse_between_dates("between 2026-01-01 and 2026-03-31")
        self.assertEqual(start, date(2026, 1, 1))
        self.assertEqual(end, date(2026, 3, 31))

    def test_from_to(self) -> None:
        start, end = handler.parse_between_dates("from 2018-01-01 to 2018-03-31")
        self.assertEqual(start, date(2018, 1, 1))
        self.assertEqual(end, date(2018, 3, 31))

    def test_no_match(self) -> None:
        self.assertEqual(handler.parse_between_dates("Q1 2026"), (None, None))


class QuarterDateRangeTests(unittest.TestCase):
    def test_q1(self) -> None:
        self.assertEqual(handler.quarter_date_range(2026, 1), (date(2026, 1, 1), date(2026, 3, 31)))

    def test_q4(self) -> None:
        self.assertEqual(handler.quarter_date_range(2025, 4), (date(2025, 10, 1), date(2025, 12, 31)))


class BuildPayloadTests(unittest.TestCase):
    def _sample_rows(self):
        return [
            {
                "opportunity_id": 1,
                "upsheet_id": 10,
                "created_at": "2026-01-15",
                "expected_amount": 25000,
                "buyer_id": 119,
                "buyer_name": "Buyer119 User119",
                "upsheet_status": "Delivered",
            }
        ]

    def test_payload_shape(self) -> None:
        rows = self._sample_rows()
        payload = handler.build_payload(
            "List opportunities for Buyer 119 in Q1 2026",
            119,
            date(2026, 1, 1),
            date(2026, 3, 31),
            rows,
        )
        self.assertEqual(payload["query_type"], "list_buyer_opportunities")
        self.assertEqual(payload["source_mode"], "precise")
        self.assertEqual(payload["params"]["buyer_id"], 119)
        self.assertEqual(payload["params"]["start_date"], "2026-01-01")
        self.assertEqual(payload["params"]["end_date"], "2026-03-31")
        self.assertEqual(payload["result"]["row_count"], 1)
        self.assertEqual(payload["result"]["rows"], rows)
        self.assertIn("filter_basis", payload["notes"])
        self.assertIn("created_at", payload["notes"]["filter_basis"])

    def test_empty_rows(self) -> None:
        payload = handler.build_payload("q", 5, date(2018, 1, 1), date(2018, 3, 31), [])
        self.assertEqual(payload["result"]["row_count"], 0)
        self.assertEqual(payload["result"]["rows"], [])


class ListOpportunitiesSqlContractTests(unittest.TestCase):
    """
    Verify that list_opportunities() passes the right parameters to the SQL cursor
    and respects the created_at date-window filter semantics.
    """

    def _make_conn(self, rows=None):
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = list(rows or [])
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=fake_cursor)
        ctx.__exit__ = MagicMock(return_value=False)
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = ctx
        return fake_conn, fake_cursor

    def test_execute_called_with_correct_params(self) -> None:
        conn, cur = self._make_conn()
        handler.list_opportunities(conn, 119, date(2026, 1, 1), date(2026, 3, 31))
        args, _ = cur.execute.call_args
        # Second positional arg is the parameter tuple
        params = args[1]
        self.assertEqual(params[0], 119, "buyer_id should be forwarded as first SQL param")
        self.assertEqual(params[1], "2026-01-01", "start_date should be ISO-formatted")
        self.assertEqual(params[2], "2026-03-31", "end_date should be ISO-formatted")

    def test_execute_called_once(self) -> None:
        conn, cur = self._make_conn()
        handler.list_opportunities(conn, 2, date(2018, 1, 1), date(2018, 3, 31))
        self.assertEqual(cur.execute.call_count, 1)

    def test_returns_fetchall_result(self) -> None:
        sample = [{"opportunity_id": 7, "buyer_id": 119}]
        conn, _ = self._make_conn(rows=sample)
        rows = handler.list_opportunities(conn, 119, date(2026, 1, 1), date(2026, 3, 31))
        self.assertEqual(rows, sample)

    def test_sql_contains_created_at_filter(self) -> None:
        conn, cur = self._make_conn()
        handler.list_opportunities(conn, 1, date(2026, 1, 1), date(2026, 3, 31))
        sql_called = cur.execute.call_args[0][0]
        self.assertIn("o.created_at", sql_called, "SQL must filter on opportunities.created_at")
        self.assertIn("assigned_user_id", sql_called, "SQL must filter by buyer via assigned_user_id")

    def test_sql_date_window_uses_inclusive_lower_and_day_offset_upper(self) -> None:
        """
        The date window must use >= for the lower bound and add INTERVAL '1 day' for the
        upper bound (inclusive end-date semantics). Changing either operator would silently
        drop or double-count boundary-day rows.
        """
        conn, cur = self._make_conn()
        handler.list_opportunities(conn, 1, date(2026, 1, 1), date(2026, 3, 31))
        sql_called = cur.execute.call_args[0][0]
        self.assertIn(">=", sql_called, "Lower date bound must be >= (inclusive)")
        self.assertIn("INTERVAL", sql_called, "Upper date bound must use INTERVAL day-offset for inclusive end-date")

    def test_different_buyers_produce_different_param_tuples(self) -> None:
        conn_a, cur_a = self._make_conn()
        conn_b, cur_b = self._make_conn()
        handler.list_opportunities(conn_a, 1, date(2026, 1, 1), date(2026, 3, 31))
        handler.list_opportunities(conn_b, 999, date(2026, 1, 1), date(2026, 3, 31))
        self.assertNotEqual(
            cur_a.execute.call_args[0][1][0],
            cur_b.execute.call_args[0][1][0],
        )


class CliMainTests(unittest.TestCase):
    def _run_main(self, argv, mock_rows):
        """Run main() with mocked argv, DB, and return parsed stdout JSON."""
        out = io.StringIO()
        fake_conn = MagicMock()
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = [dict(r) for r in mock_rows]
        fake_conn.cursor.return_value.__enter__ = lambda s: fake_cursor
        fake_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(sys, "argv", argv):
            with patch("precise_list_buyer_opportunities.connect_pg", return_value=fake_conn):
                with patch.object(sys, "stdout", out):
                    handler.main()
        return json.loads(out.getvalue())

    def test_cli_quarter_natural_language(self) -> None:
        payload = self._run_main(
            ["handler.py", "--query", "List opportunities for Buyer 2 in Q1 2018"],
            mock_rows=[],
        )
        self.assertEqual(payload["params"]["buyer_id"], 2)
        self.assertEqual(payload["params"]["start_date"], "2018-01-01")
        self.assertEqual(payload["params"]["end_date"], "2018-03-31")

    def test_cli_explicit_buyer_and_dates(self) -> None:
        payload = self._run_main(
            [
                "handler.py",
                "--query", "opportunities",
                "--buyer-id", "119",
                "--start-date", "2026-01-01",
                "--end-date", "2026-03-31",
            ],
            mock_rows=[],
        )
        self.assertEqual(payload["params"]["buyer_id"], 119)
        self.assertEqual(payload["params"]["start_date"], "2026-01-01")
        self.assertEqual(payload["params"]["end_date"], "2026-03-31")

    def test_cli_explicit_year_and_quarter(self) -> None:
        payload = self._run_main(
            [
                "handler.py",
                "--query", "opportunities Buyer 5",
                "--year", "2018",
                "--quarter", "2",
            ],
            mock_rows=[],
        )
        self.assertEqual(payload["params"]["start_date"], "2018-04-01")
        self.assertEqual(payload["params"]["end_date"], "2018-06-30")

    def test_cli_date_range_in_query(self) -> None:
        payload = self._run_main(
            [
                "handler.py",
                "--query",
                "List opportunities for Buyer 3 between 2026-01-01 and 2026-03-31",
            ],
            mock_rows=[],
        )
        self.assertEqual(payload["params"]["buyer_id"], 3)
        self.assertEqual(payload["params"]["start_date"], "2026-01-01")
        self.assertEqual(payload["params"]["end_date"], "2026-03-31")


if __name__ == "__main__":
    unittest.main()
