"""Tests for the governed prompt module registry + assembler (v1)."""

from __future__ import annotations

import json

from prompt_assembler_v1 import (
    ASSEMBLER_VERSION,
    SAVED_REPORT_MODULE_SELECTION,
    build_saved_report_prompt_payload,
    build_saved_report_ws_message,
    build_typed_payload,
)
from prompt_modules_v1 import PROMPT_MODULES, get_module, list_modules, module_summary


def _sample_final_response():
    return {
        "mode": "saved_report",
        "template_id": "buyer_performance_report_v1",
        "request_summary": "Buyer performance report: Buyer 119, Q2 2021.",
        "executive_summary": "",
        "kpi_snapshot": {},
        "highlights": [],
        "notes": [],
        "suggested_next_question": "",
        "semantic_quality": {"confidence_level": "high"},
    }


def _sample_block_outputs():
    return [
        {
            "block_id": "semantic_quarterly_narrative",
            "block_type": "executive_summary",
            "output_key": "executive_summary",
            "source": "semantic",
            "payload": {"text": "Buyer119 recorded strong conversion in 2021 Q2."},
        },
        {
            "block_id": "semantic_quarterly_narrative__highlights",
            "block_type": "highlights",
            "output_key": "highlights",
            "source": "semantic",
            "payload": {
                "items": [
                    "Top matched document: Buyer119 User119 - Q2 2021.",
                    "Top similarity score: 0.8346.",
                ]
            },
        },
        {
            "block_id": "semantic_quarterly_narrative__notes",
            "block_type": "notes",
            "output_key": "notes",
            "source": "semantic",
            "payload": {"items": ["Source: precomputed quarterly summaries."]},
        },
        {
            "block_id": "kpi_snapshot_quarter",
            "block_type": "kpi_table",
            "output_key": "kpi_snapshot",
            "source": "precise",
            "payload": {"snapshot": {"close_rate": "0.0", "total_sale_value": "16195.00"}},
        },
    ]


def test_registry_has_four_v1_modules():
    assert set(list_modules()) == {
        "executive_summary",
        "kpi_narrative",
        "highlights",
        "notes",
    }
    for mid in list_modules():
        mod = get_module(mid)
        assert mod.version == "v1"
        # kpi_narrative writes into the KPI snapshot section
        if mid == "kpi_narrative":
            assert mod.section_key == "kpi_snapshot"
        else:
            assert mod.section_key == mid
        assert mod.output_contract
        assert mod.instructions
        assert mod.guardrails


def test_module_summary_is_versioned():
    summary = module_summary()
    assert summary == {
        "executive_summary": "v1",
        "kpi_narrative": "v1",
        "highlights": "v1",
        "notes": "v1",
    }


def test_saved_report_module_selection_matches_registry():
    assert set(SAVED_REPORT_MODULE_SELECTION) <= set(PROMPT_MODULES.keys())


def test_build_typed_payload_extracts_only_typed_fields():
    fr = _sample_final_response()
    bos = _sample_block_outputs()
    payload = build_typed_payload(
        final_response=fr,
        block_outputs=bos,
        buyer_label="Buyer 119",
        period_label="Q2 2021",
    )
    assert payload["buyer_label"] == "Buyer 119"
    assert payload["period_label"] == "Q2 2021"
    assert payload["template_id"] == "buyer_performance_report_v1"
    assert payload["executive_summary_snippet"].startswith("Buyer119")
    assert payload["highlights_items"] == [
        "Top matched document: Buyer119 User119 - Q2 2021.",
        "Top similarity score: 0.8346.",
    ]
    assert payload["notes_items"] == ["Source: precomputed quarterly summaries."]
    assert payload["kpi_snapshot"] == {"close_rate": "0.0", "total_sale_value": "16195.00"}


def test_build_prompt_payload_is_deterministic():
    fr = _sample_final_response()
    bos = _sample_block_outputs()
    a = build_saved_report_prompt_payload(
        final_response=fr, block_outputs=bos, buyer_label="Buyer 119", period_label="Q2 2021"
    )
    b = build_saved_report_prompt_payload(
        final_response=fr, block_outputs=bos, buyer_label="Buyer 119", period_label="Q2 2021"
    )
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    assert a["assembler_version"] == ASSEMBLER_VERSION
    assert a["modules_used"] == {
        "executive_summary": "v1",
        "kpi_narrative": "v1",
        "highlights": "v1",
        "notes": "v1",
    }
    module_ids = [m["module_id"] for m in a["modules"]]
    assert module_ids == list(SAVED_REPORT_MODULE_SELECTION)


def test_ws_message_includes_payload_and_contract():
    fr = _sample_final_response()
    bos = _sample_block_outputs()
    msg = build_saved_report_ws_message(
        final_response=fr, block_outputs=bos, buyer_label="Buyer 119", period_label="Q2 2021"
    )
    assert "Hard constraints:" in msg
    assert "saved_report" in msg
    assert "\"assembler_version\":" in msg
    assert "\"modules_used\":" in msg
    assert "buyer_performance_report_v1" in msg


def test_empty_block_outputs_still_produce_valid_payload():
    fr = _sample_final_response()
    payload = build_saved_report_prompt_payload(
        final_response=fr, block_outputs=[], buyer_label="Buyer 2", period_label="Q1 2019"
    )
    data = payload["data"]
    assert data["executive_summary_snippet"] == ""
    assert data["highlights_items"] == []
    assert data["notes_items"] == []
    assert data["kpi_snapshot"] == {}
    assert payload["modules_used"] == {
        "executive_summary": "v1",
        "kpi_narrative": "v1",
        "highlights": "v1",
        "notes": "v1",
    }
