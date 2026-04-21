"""Tests for template-as-data JSON docs (v1)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from template_docs_v1 import (
    TEMPLATE_DOC_VERSION_V1,
    TEMPLATES_DIR_ENV,
    load_template_docs_from_dir,
    template_from_doc_v1,
    validate_template_doc_v1,
)


class TemplateDocsV1Tests(unittest.TestCase):
    def test_validate_rejects_unknown_prompt_modules(self) -> None:
        doc = {
            "template_doc_version": TEMPLATE_DOC_VERSION_V1,
            "template_id": "t1",
            "display_name": "T1",
            "purpose": "p",
            "trigger_phrases": ["x"],
            "required_slots": ["buyer"],
            "optional_slots": [],
            "section_order": ["executive_summary"],
            "phrasing_rules": ["r1"],
            "prompt_modules": ["does_not_exist"],
            "disallowed_without_explicit_request": [],
            "data_blocks": [
                {
                    "block_id": "b1",
                    "description": "d",
                    "block_type": "executive_summary",
                    "retrieval_mode": "semantic",
                    "query_family_hint": "buyer_performance_summary",
                    "output_key": "executive_summary",
                }
            ],
        }
        with self.assertRaises(ValueError):
            validate_template_doc_v1(doc)

    def test_template_from_doc_constructs_template(self) -> None:
        doc = {
            "template_doc_version": TEMPLATE_DOC_VERSION_V1,
            "template_id": "t2",
            "display_name": "T2",
            "purpose": "p",
            "trigger_phrases": ["buyer performance report"],
            "required_slots": ["buyer"],
            "optional_slots": ["timeframe"],
            "section_order": ["request_summary", "executive_summary"],
            "phrasing_rules": ["r1"],
            "prompt_modules": ["executive_summary", "highlights", "notes"],
            "disallowed_without_explicit_request": [],
            "data_blocks": [
                {
                    "block_id": "b1",
                    "description": "d",
                    "block_type": "executive_summary",
                    "retrieval_mode": "semantic",
                    "query_family_hint": "buyer_performance_summary",
                    "output_key": "executive_summary",
                }
            ],
        }
        tpl = template_from_doc_v1(doc)
        self.assertEqual(tpl.template_id, "t2")
        self.assertIn("buyer performance report", tpl.trigger_phrases)
        self.assertEqual(tpl.prompt_modules, ("executive_summary", "highlights", "notes"))

    def test_load_template_docs_from_dir_reads_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "t3.json"
            p.write_text(
                json.dumps(
                    {
                        "template_doc_version": TEMPLATE_DOC_VERSION_V1,
                        "template_id": "t3",
                        "display_name": "T3",
                        "purpose": "p",
                        "trigger_phrases": ["buyer report"],
                        "required_slots": ["buyer"],
                        "optional_slots": ["timeframe"],
                        "section_order": ["request_summary", "executive_summary"],
                        "phrasing_rules": ["r1"],
                        "prompt_modules": ["executive_summary", "highlights", "notes"],
                        "disallowed_without_explicit_request": [],
                        "data_blocks": [
                            {
                                "block_id": "b1",
                                "description": "d",
                                "block_type": "executive_summary",
                                "retrieval_mode": "semantic",
                                "query_family_hint": "buyer_performance_summary",
                                "output_key": "executive_summary",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            loaded = load_template_docs_from_dir(Path(td))
            self.assertIn("t3", loaded)

    def test_env_dir_override_smoke(self) -> None:
        # Ensure the env var can be set without errors by the loader (smoke only).
        old = os.environ.get(TEMPLATES_DIR_ENV)
        try:
            with tempfile.TemporaryDirectory() as td:
                os.environ[TEMPLATES_DIR_ENV] = td
                loaded = load_template_docs_from_dir(Path(td))
                self.assertEqual(loaded, {})
        finally:
            if old is None:
                os.environ.pop(TEMPLATES_DIR_ENV, None)
            else:
                os.environ[TEMPLATES_DIR_ENV] = old


if __name__ == "__main__":
    unittest.main()

