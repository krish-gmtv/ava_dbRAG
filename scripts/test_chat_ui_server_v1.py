import unittest

import chat_ui_server_v1 as server


class ChatUiServerFollowupTests(unittest.TestCase):
    def setUp(self) -> None:
        server.THREAD_CONTEXT.clear()

    def test_update_thread_ctx_sets_exact_followup_from_semantic_response(self) -> None:
        payload = {
            "execution_plan": {
                "entity": {"resolved_id": 7},
                "timeframe": {
                    "raw_text": "Q1 2018",
                    "start": "2018-01-01",
                    "end": "2018-03-31",
                    "granularity": "quarter",
                },
            },
            "final_response": {
                "request_summary": "Semantic performance summary for Buyer 7 in Q1 2018.",
                "suggested_next_question": "Would you like exact KPI values from direct SQL for this same period?",
            },
        }

        server.update_thread_ctx("thread-a", payload)
        ctx = server.get_thread_ctx("thread-a")

        self.assertEqual(ctx.get("buyer_id"), 7)
        self.assertEqual(ctx.get("period_year"), 2018)
        self.assertEqual(ctx.get("period_quarter"), 1)
        self.assertTrue(ctx.get("pending_exact_kpi_followup"))
        self.assertFalse(ctx.get("pending_trend_followup"))

    def test_update_thread_ctx_derives_quarter_from_start_date_when_raw_text_missing(self) -> None:
        payload = {
            "execution_plan": {
                "entity": {"resolved_id": 3},
                "timeframe": {
                    "raw_text": None,
                    "start": "2018-04-01",
                    "end": "2018-06-30",
                    "granularity": "quarter",
                },
            },
            "final_response": {
                "request_summary": "Semantic performance summary for Buyer 3 in the requested period.",
                "suggested_next_question": "Would you like exact KPI values from direct SQL for this same period?",
            },
        }

        server.update_thread_ctx("thread-b", payload)
        ctx = server.get_thread_ctx("thread-b")

        self.assertEqual(ctx.get("period_year"), 2018)
        self.assertEqual(ctx.get("period_quarter"), 2)

    def test_affirmative_without_candidate_keeps_pending_state(self) -> None:
        ctx = server.get_thread_ctx("thread-c")
        ctx["buyer_id"] = 5
        ctx["pending_exact_kpi_followup"] = True
        ctx["pending_trend_followup"] = False

        rewritten_query = "yes"
        if server.is_affirmative("yes"):
            if bool(ctx.get("pending_exact_kpi_followup")):
                candidate = server.build_precise_followup_query(ctx)
                if candidate:
                    rewritten_query = candidate
                    ctx["pending_exact_kpi_followup"] = False
                    ctx["pending_trend_followup"] = False
            elif bool(ctx.get("pending_trend_followup")):
                candidate = server.build_trend_followup_query(ctx)
                if candidate:
                    rewritten_query = candidate
                    ctx["pending_exact_kpi_followup"] = False
                    ctx["pending_trend_followup"] = False

        self.assertEqual(rewritten_query, "yes")
        self.assertTrue(ctx.get("pending_exact_kpi_followup"))

    def test_prev_quarter_helper(self) -> None:
        self.assertEqual(server._prev_quarter(2026, 1), (2025, 4))
        self.assertEqual(server._prev_quarter(2026, 3), (2026, 2))

    def test_trend_followup_targets_previous_quarter_sql(self) -> None:
        q = server.build_trend_followup_query(
            {"buyer_id": 119, "period_year": 2026, "period_quarter": 1}
        )
        self.assertIn("Q4 2025", q)
        self.assertIn("Buyer 119", q)
        self.assertTrue(q.endswith("?"))

    def test_precise_suggested_next_sets_pending_trend(self) -> None:
        pending = server.detect_pending_followups(
            "Would you like the previous quarter's SQL close rate so you can compare it side by side "
            "with this period?"
        )
        self.assertTrue(pending["pending_trend_followup"])


if __name__ == "__main__":
    unittest.main()
