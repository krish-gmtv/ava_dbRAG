from __future__ import annotations

import json
import unittest

from scripts.ava.ava_session_manager import (
    extract_last_model_reply,
    parse_session_events_payload,
)


class AvaSessionEventsTests(unittest.TestCase):
    def test_parse_events_as_json_string(self) -> None:
        raw = [
            {"role": "user", "text": "Hello!"},
            {"role": "model", "text": "Hi! How can I help?"},
        ]
        payload = {"events": json.dumps(raw, ensure_ascii=False)}
        out = parse_session_events_payload(payload)
        self.assertEqual(len(out), 2)
        self.assertEqual(extract_last_model_reply(out), "Hi! How can I help?")

    def test_parse_events_as_list(self) -> None:
        payload = {
            "events": [
                {"role": "user", "text": "x"},
                {"role": "model", "text": "y"},
            ]
        }
        out = parse_session_events_payload(payload)
        self.assertEqual(extract_last_model_reply(out), "y")

    def test_assistant_role_alternate(self) -> None:
        out = [
            {"role": "user", "text": "a"},
            {"role": "assistant", "text": "b"},
        ]
        self.assertEqual(extract_last_model_reply(out), "b")

    def test_empty_payload(self) -> None:
        self.assertEqual(parse_session_events_payload({}), [])
        self.assertEqual(parse_session_events_payload({"events": ""}), [])


if __name__ == "__main__":
    unittest.main()
