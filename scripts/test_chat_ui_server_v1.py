import unittest

import chat_ui_server_v1 as server

SEMANTIC_LISTING_NEXT = (
    "Would you like a Postgres row listing for the same buyer and period? "
    "Reply yes to list upsheets, or ask to list opportunities for the same quarter."
)


class ChatUiServerFollowupTests(unittest.TestCase):
    def setUp(self) -> None:
        server.THREAD_CONTEXT.clear()

    def test_update_thread_ctx_sets_listing_followup_from_semantic_response(self) -> None:
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
                "request_summary": "Buyer 7 performance summary for Q1 2018.",
                "suggested_next_question": SEMANTIC_LISTING_NEXT,
            },
        }

        server.update_thread_ctx("thread-a", payload)
        ctx = server.get_thread_ctx("thread-a")

        self.assertEqual(ctx.get("buyer_id"), 7)
        self.assertEqual(ctx.get("period_year"), 2018)
        self.assertEqual(ctx.get("period_quarter"), 1)
        self.assertTrue(ctx.get("pending_listing_followup"))
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
                "suggested_next_question": SEMANTIC_LISTING_NEXT,
            },
        }

        server.update_thread_ctx("thread-b", payload)
        ctx = server.get_thread_ctx("thread-b")

        self.assertEqual(ctx.get("period_year"), 2018)
        self.assertEqual(ctx.get("period_quarter"), 2)

    def test_range_timeframe_derives_quarter_for_yes_followup(self) -> None:
        payload = {
            "execution_plan": {
                "entity": {"resolved_id": 119},
                "timeframe": {
                    "raw_text": None,
                    "start": "2026-01-01",
                    "end": "2026-03-31",
                    "granularity": "range",
                },
            },
            "final_response": {
                "request_summary": "Buyer 119 performance summary for the requested period.",
                "suggested_next_question": SEMANTIC_LISTING_NEXT,
            },
        }
        server.update_thread_ctx("thread-range", payload)
        ctx = server.get_thread_ctx("thread-range")
        self.assertEqual(ctx.get("period_quarter"), 1)
        self.assertEqual(ctx.get("period_year"), 2026)
        self.assertTrue(ctx.get("pending_listing_followup"))

    def test_affirmative_without_candidate_keeps_pending_state(self) -> None:
        ctx = server.get_thread_ctx("thread-c")
        ctx["buyer_id"] = 5
        ctx["pending_listing_followup"] = True
        ctx["pending_trend_followup"] = False
        # Missing period — cannot build listing query
        ctx.pop("period_year", None)
        ctx.pop("period_quarter", None)

        rewritten_query = "yes"
        if server.is_affirmative("yes"):
            if bool(ctx.get("pending_listing_followup")):
                candidate = server.build_list_upsheets_followup_query(ctx)
                if candidate:
                    rewritten_query = candidate
                    ctx["pending_listing_followup"] = False
                    ctx["pending_trend_followup"] = False

        self.assertEqual(rewritten_query, "yes")
        self.assertTrue(ctx.get("pending_listing_followup"))

    def test_yes_rewrites_to_list_upsheets_when_context_complete(self) -> None:
        ctx = {
            "buyer_id": 119,
            "period_year": 2026,
            "period_quarter": 1,
            "pending_listing_followup": True,
        }
        rewritten = "yes"
        if server.is_affirmative("yes"):
            c = server.build_list_upsheets_followup_query(ctx)
            if c:
                rewritten = c
        self.assertIn("List all upsheets", rewritten)
        self.assertIn("119", rewritten)
        self.assertIn("Q1 2026", rewritten)

    def test_prev_quarter_helper(self) -> None:
        self.assertEqual(server._prev_quarter(2026, 1), (2025, 4))
        self.assertEqual(server._prev_quarter(2026, 3), (2026, 2))

    def test_trend_followup_targets_previous_quarter_semantic(self) -> None:
        q = server.build_trend_followup_query(
            {"buyer_id": 119, "period_year": 2026, "period_quarter": 1}
        )
        self.assertIn("Q4 2025", q)
        self.assertIn("Buyer 119", q)
        self.assertIn("perform", q.lower())
        self.assertTrue(q.endswith("?"))

    def test_detect_listing_pending_from_suggested_copy(self) -> None:
        pending = server.detect_pending_followups(SEMANTIC_LISTING_NEXT)
        self.assertTrue(pending["pending_listing_followup"])
        self.assertFalse(pending["pending_trend_followup"])

    def test_detect_trend_pending_from_previous_quarter_wording(self) -> None:
        pending = server.detect_pending_followups(
            "Would you like the previous quarter so you can compare side by side with this period?"
        )
        self.assertTrue(pending["pending_trend_followup"])

    def test_is_gratitude(self) -> None:
        self.assertTrue(server.is_gratitude("Ok thanks"))
        self.assertTrue(server.is_gratitude("thank you"))
        self.assertTrue(server.is_gratitude("Thanks!"))


if __name__ == "__main__":
    unittest.main()
