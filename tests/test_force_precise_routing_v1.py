"""Unit tests for execute_query_v1 routing + advisor shadow behavior."""

import io
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import execute_query_v1 as eq


class ResolveForcePreciseTests(unittest.TestCase):
    def test_precise_kpi_handler_blocked_when_force_precise(self) -> None:
        plan = {
            "mode": "precise",
            "retrieval_plan": {
                "handler": "precise_get_buyer_quarter_kpis",
                "query_family": "buyer_quarter_kpis",
            },
        }
        h, err = eq.resolve_force_precise_handler(plan)
        self.assertIsNone(h)
        self.assertIsNotNone(err)
        self.assertIn("listings", err.lower())
        self.assertIn("upsheets", err.lower())

    def test_semantic_performance_does_not_map_to_kpi_sql(self) -> None:
        plan = {
            "mode": "semantic",
            "entity": {"resolved_id": 9},
            "timeframe": {
                "granularity": "quarter",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "buyer_performance_summary",
            },
        }
        h, err = eq.resolve_force_precise_handler(plan)
        self.assertIsNone(h)
        self.assertIsNotNone(err)
        self.assertIn("semantic", err.lower())

    def test_semantic_opportunities_rejected(self) -> None:
        plan = {
            "mode": "semantic",
            "entity": {"resolved_id": 1},
            "timeframe": {"granularity": "quarter"},
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "list_buyer_opportunities",
            },
        }
        h, err = eq.resolve_force_precise_handler(plan, planner_v2_enabled=False)
        self.assertIsNone(h)
        self.assertIsNotNone(err)
        self.assertIn("opportunities", err.lower())

    def test_semantic_opportunities_maps_when_planner_v2_on(self) -> None:
        plan = {
            "mode": "semantic",
            "entity": {"resolved_id": 1},
            "timeframe": {"granularity": "quarter", "start": "2026-01-01", "end": "2026-03-31"},
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "list_buyer_opportunities",
            },
        }
        h, err = eq.resolve_force_precise_handler(plan, planner_v2_enabled=True)
        self.assertEqual(h, "precise_list_buyer_opportunities")
        self.assertIsNone(err)

    def test_kpi_range_cli_from_plan_passes_start_end(self) -> None:
        plan = {
            "timeframe": {
                "granularity": "range",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
        }
        extras = eq.kpi_range_cli_from_plan("precise_get_buyer_quarter_kpis", plan)
        self.assertEqual(
            extras,
            ["--start-date", "2026-01-01", "--end-date", "2026-03-31"],
        )

    def test_kpi_range_cli_quarter_granularity_passes_start_end(self) -> None:
        plan = {
            "timeframe": {
                "granularity": "quarter",
                "raw_text": "Q1 2026",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
        }
        extras = eq.kpi_range_cli_from_plan("precise_get_buyer_quarter_kpis", plan)
        self.assertEqual(
            extras,
            ["--start-date", "2026-01-01", "--end-date", "2026-03-31"],
        )

    def test_kpi_range_cli_empty_for_semantic_handler(self) -> None:
        plan = {
            "timeframe": {
                "granularity": "range",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
        }
        self.assertEqual(eq.kpi_range_cli_from_plan("semantic_buyer_performance_summary", plan), [])

    def test_precise_cli_args_from_plan_buyer_and_dates(self) -> None:
        plan = {
            "entity": {"resolved_id": 119},
            "timeframe": {
                "granularity": "quarter",
                "start": "2026-01-01",
                "end": "2026-03-31",
            },
        }
        args = eq.precise_cli_args_from_plan("precise_list_buyer_opportunities", plan)
        self.assertEqual(
            args,
            ["--buyer-id", "119", "--start-date", "2026-01-01", "--end-date", "2026-03-31"],
        )

    def test_semantic_performance_year_only_rejected(self) -> None:
        plan = {
            "mode": "semantic",
            "entity": {"resolved_id": 3},
            "timeframe": {"granularity": "year"},
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "buyer_performance_summary",
            },
        }
        h, err = eq.resolve_force_precise_handler(plan)
        self.assertIsNone(h)
        self.assertIn("list upsheets", err.lower())

    def test_parse_advisor_json_from_plain_object(self) -> None:
        parsed = eq.parse_advisor_json(
            '{"recommended_mode":"precise","query_family":"buyer_quarter_kpis","confidence":0.87,"explanation":"exact metric"}'
        )
        self.assertEqual(parsed["recommended_mode"], "precise")
        self.assertEqual(parsed["query_family"], "buyer_quarter_kpis")
        self.assertAlmostEqual(parsed["confidence"], 0.87, places=6)

    def test_parse_advisor_json_from_wrapped_text(self) -> None:
        parsed = eq.parse_advisor_json(
            '{"recommended_mode":"semantic","query_family":"buyer_performance_summary","confidence":"0.42","explanation":"summary query"}'
        )
        self.assertEqual(parsed["recommended_mode"], "semantic")
        self.assertEqual(parsed["query_family"], "buyer_performance_summary")
        self.assertAlmostEqual(parsed["confidence"], 0.42, places=6)

    def test_parse_advisor_json_rejects_wrapped_text(self) -> None:
        with self.assertRaises(Exception):
            eq.parse_advisor_json(
                "Here is the result:\n"
                '{"recommended_mode":"semantic","query_family":"buyer_performance_summary","confidence":"0.42","explanation":"summary query"}'
            )

    def test_call_ava_route_advisor_http_uses_session_manager(self) -> None:
        prism_payload = [
            "not json",
            '{"recommended_mode":"precise","query_family":"buyer_quarter_kpis","confidence":0.8,"explanation":"metric"}',
        ]
        with patch("execute_query_v1.get_session_id", return_value="sess-123") as m_get_session:
            with patch("execute_query_v1.requests.post") as m_post:
                resp = MagicMock()
                resp.raise_for_status.return_value = None
                resp.json.return_value = prism_payload
                m_post.return_value = resp

                out = eq.call_ava_route_advisor_http(
                    query="What was Buyer 2 close rate in Q1 2018?",
                    user_id="advisor-u",
                    token="tok",
                )

        self.assertEqual(out["session_id"], "sess-123")
        self.assertEqual(out["recommended_mode"], "precise")
        m_get_session.assert_called_once_with(user_id="advisor-u", token="tok")
        _, kwargs = m_post.call_args
        self.assertEqual(kwargs["headers"]["Authorization"], "tok")
        self.assertEqual(kwargs["json"]["session_id"], "sess-123")

    def test_call_ava_route_advisor_http_accepts_dict_payload(self) -> None:
        prism_payload = {
            "recommended_mode": "semantic",
            "query_family": "buyer_performance_summary",
            "confidence": 0.61,
            "explanation": "summary intent",
        }
        with patch("execute_query_v1.get_session_id", return_value="sess-456"):
            with patch("execute_query_v1.requests.post") as m_post:
                resp = MagicMock()
                resp.raise_for_status.return_value = None
                resp.json.return_value = prism_payload
                m_post.return_value = resp

                out = eq.call_ava_route_advisor_http(
                    query="How is Buyer 2 doing overall?",
                    user_id="advisor-u",
                    token="tok",
                )
        self.assertEqual(out["session_id"], "sess-456")
        self.assertEqual(out["recommended_mode"], "semantic")
        self.assertEqual(out["query_family"], "buyer_performance_summary")

    def test_main_shadow_disabled_does_not_call_advisor(self) -> None:
        plan = {
            "mode": "semantic",
            "reason_codes": [],
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "buyer_performance_summary",
            },
        }
        handler_output = {"result": {"matches": []}}
        calls = {"n": 0}

        def fake_run(script_path, query, extra_args=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return plan
            return handler_output

        argv = ["execute_query_v1.py", "--query", "How is Buyer 1 doing?"]
        with patch.object(sys, "argv", argv):
            with patch.dict(os.environ, {"AVA_ROUTE_ADVISOR_SHADOW": "0"}, clear=False):
                with patch("execute_query_v1.run_python_json", side_effect=fake_run):
                    with patch("execute_query_v1.call_ava_route_advisor_http") as m_adv:
                        with patch.object(sys, "stdout", io.StringIO()):
                            eq.main()
        m_adv.assert_not_called()

    def test_main_shadow_enabled_logs_error_and_keeps_deterministic(self) -> None:
        plan = {
            "mode": "semantic",
            "reason_codes": [],
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "buyer_performance_summary",
            },
        }
        handler_output = {"result": {"matches": []}}
        calls = {"n": 0}

        def fake_run(script_path, query, extra_args=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return plan
            return handler_output

        events = []
        argv = ["execute_query_v1.py", "--query", "How is Buyer 1 doing?"]
        out = io.StringIO()
        with patch.object(sys, "argv", argv):
            with patch.dict(
                os.environ,
                {"AVA_ROUTE_ADVISOR_SHADOW": "1", "AVA_TOKEN": "tok", "AVA_ROUTE_ADVISOR_USER_ID": "u1"},
                clear=False,
            ):
                with patch("execute_query_v1.run_python_json", side_effect=fake_run):
                    with patch("execute_query_v1.call_ava_route_advisor_http", side_effect=RuntimeError("boom")):
                        with patch("execute_query_v1.log_shadow_event", side_effect=lambda e: events.append(e)):
                            with patch.object(sys, "stdout", out):
                                eq.main()

        payload = json.loads(out.getvalue())
        self.assertEqual(payload["selected_handler"], "semantic_buyer_performance_summary")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["status"], "error")
        self.assertIn("boom", events[0]["error"])

    def test_main_shadow_enabled_logs_success_with_executed_route(self) -> None:
        plan = {
            "mode": "semantic",
            "reason_codes": [],
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "buyer_performance_summary",
            },
        }
        handler_output = {"result": {"matches": []}}
        calls = {"n": 0}

        def fake_run(script_path, query, extra_args=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return plan
            return handler_output

        events = []
        argv = ["execute_query_v1.py", "--query", "How is Buyer 1 doing?"]
        out = io.StringIO()
        with patch.object(sys, "argv", argv):
            with patch.dict(
                os.environ,
                {"AVA_ROUTE_ADVISOR_SHADOW": "1", "AVA_TOKEN": "tok", "AVA_ROUTE_ADVISOR_USER_ID": "u1"},
                clear=False,
            ):
                with patch("execute_query_v1.run_python_json", side_effect=fake_run):
                    with patch(
                        "execute_query_v1.call_ava_route_advisor_http",
                        return_value={
                            "session_id": "sess-1",
                            "recommended_mode": "semantic",
                            "query_family": "buyer_performance_summary",
                            "confidence": 0.9,
                            "explanation": "summary question",
                        },
                    ):
                        with patch("execute_query_v1.log_shadow_event", side_effect=lambda e: events.append(e)):
                            with patch.object(sys, "stdout", out):
                                eq.main()

        payload = json.loads(out.getvalue())
        self.assertEqual(payload["selected_handler"], "semantic_buyer_performance_summary")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["status"], "ok")
        self.assertEqual(events[0]["executed"]["handler"], "semantic_buyer_performance_summary")
        self.assertIn("router_pre_override", events[0])

    def test_main_shadow_force_precise_blocked_logs_executed_and_disagreement(self) -> None:
        plan = {
            "mode": "semantic",
            "reason_codes": [],
            "retrieval_plan": {
                "handler": "semantic_buyer_performance_summary",
                "query_family": "list_buyer_opportunities",
            },
        }
        calls = {"n": 0}

        def fake_run(script_path, query, extra_args=None):
            calls["n"] += 1
            return plan

        events = []
        argv = ["execute_query_v1.py", "--query", "List opportunities for Buyer 1", "--force-precise"]
        out = io.StringIO()
        with patch.object(sys, "argv", argv):
            with patch.dict(
                os.environ,
                {
                    "AVA_ROUTE_ADVISOR_SHADOW": "1",
                    "AVA_TOKEN": "tok",
                    "AVA_ROUTE_ADVISOR_USER_ID": "u1",
                    # Default PRECISE_PLANNER_V2 is now true; this test asserts blocked mapping.
                    "PRECISE_PLANNER_V2": "false",
                },
                clear=False,
            ):
                with patch("execute_query_v1.run_python_json", side_effect=fake_run):
                    with patch(
                        "execute_query_v1.call_ava_route_advisor_http",
                        return_value={
                            "session_id": "sess-1",
                            "recommended_mode": "semantic",
                            "query_family": "list_buyer_opportunities",
                            "confidence": 0.88,
                            "explanation": "opportunities list",
                        },
                    ):
                        with patch("execute_query_v1.log_shadow_event", side_effect=lambda e: events.append(e)):
                            with patch.object(sys, "stdout", out):
                                eq.main()

        payload = json.loads(out.getvalue())
        self.assertIsNone(payload["selected_handler"])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["status"], "ok")
        self.assertEqual(events[0]["executed"]["mode"], "force_precise_blocked")
        self.assertIn("disagreement", events[0])


if __name__ == "__main__":
    unittest.main()
