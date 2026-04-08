"""Tests for semantic retrieval quality gating."""

import unittest

import semantic_quality_v1 as sq


def _plan() -> dict:
    return {
        "entity": {"resolved_id": 2},
        "retrieval_plan": {"query_family": "buyer_performance_summary"},
    }


class SemanticQualityTests(unittest.TestCase):
    def test_no_matches_is_low(self) -> None:
        ho = {"params": {"buyer_id": 2, "period_year": 2019, "period_quarter": 1}, "result": {"matches": []}}
        q = sq.evaluate_semantic_quality(_plan(), ho)
        self.assertEqual(q.render_mode, "no_semantic_summary")
        self.assertEqual(q.confidence_level, "low")

    def test_high_score_aligned_snippet_is_full(self) -> None:
        ho = {
            "params": {"buyer_id": 2, "period_year": 2019, "period_quarter": 1},
            "result": {
                "matches": [
                    {
                        "buyer_id": 2,
                        "period_label": "Q1 2019",
                        "summary_snippet": "Solid execution in the quarter.",
                        "score": 0.78,
                    }
                ]
            },
        }
        q = sq.evaluate_semantic_quality(_plan(), ho)
        self.assertEqual(q.render_mode, "full_semantic")
        self.assertEqual(q.confidence_level, "high")

    def test_mismatch_period_downgrades(self) -> None:
        ho = {
            "params": {"buyer_id": 2, "period_year": 2019, "period_quarter": 1},
            "result": {
                "matches": [
                    {
                        "buyer_id": 2,
                        "period_label": "Q1 2026",
                        "summary_snippet": "Different quarter in index.",
                        "score": 0.9,
                    }
                ]
            },
        }
        q = sq.evaluate_semantic_quality(_plan(), ho)
        self.assertEqual(q.render_mode, "no_semantic_summary")
        self.assertFalse(q.metadata_aligned)


if __name__ == "__main__":
    unittest.main()
