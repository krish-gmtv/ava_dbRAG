from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.templates.template_docs_v1 import load_template_docs_from_dir, validate_template_doc_v1
from scripts.templates.template_versions_v1 import (
    activate_template_revision,
    has_legacy_flat,
    has_versioned_package,
    list_template_versions,
    load_active_template_doc,
    publish_template_versioned,
    unpublish_template,
)


def _minimal_doc(template_id: str, *, display_name: str = "Test") -> dict:
    return {
        "template_doc_version": "saved_report_template_doc_v1",
        "template_id": template_id,
        "display_name": display_name,
        "purpose": "Test purpose",
        "trigger_phrases": ["test trigger"],
        "required_slots": ["buyer", "timeframe"],
        "optional_slots": [],
        "section_order": ["request_summary", "executive_summary", "next_steps"],
        "phrasing_rules": ["rule"],
        "prompt_modules": ["executive_summary"],
        "disallowed_without_explicit_request": [],
        "data_blocks": [
            {
                "block_id": "semantic_quarterly_narrative",
                "description": "d",
                "block_type": "executive_summary",
                "retrieval_mode": "semantic",
                "query_family_hint": "buyer_performance_summary",
                "output_key": "executive_summary",
            }
        ],
    }


def test_first_publish_creates_v1_package(tmp_path: Path) -> None:
    doc = _minimal_doc("demo_tpl_v1")
    validate_template_doc_v1(doc)
    pub = publish_template_versioned(tmp_path, doc, version_policy="new_revision")
    assert pub["revision"] == 1
    assert pub["active_revision"] == 1
    assert has_versioned_package(tmp_path, "demo_tpl_v1")
    assert (tmp_path / "demo_tpl_v1" / "v1.json").exists()
    active = load_active_template_doc(tmp_path, "demo_tpl_v1")
    assert active is not None
    assert active["display_name"] == "Test"


def test_second_publish_appends_v2(tmp_path: Path) -> None:
    doc = _minimal_doc("demo_tpl_v1")
    publish_template_versioned(tmp_path, doc, version_policy="new_revision")
    doc2 = _minimal_doc("demo_tpl_v1", display_name="Test v2")
    pub = publish_template_versioned(tmp_path, doc2, version_policy="new_revision")
    assert pub["revision"] == 2
    assert pub["created_new_revision"] is True
    info = list_template_versions(tmp_path, "demo_tpl_v1")
    assert info["active_revision"] == 2
    assert len(info["revisions"]) == 2
    active = load_active_template_doc(tmp_path, "demo_tpl_v1")
    assert active is not None
    assert active["display_name"] == "Test v2"
    assert (tmp_path / "demo_tpl_v1" / "v1.json").exists()


def test_replace_active_keeps_revision_count(tmp_path: Path) -> None:
    doc = _minimal_doc("demo_tpl_v1")
    publish_template_versioned(tmp_path, doc, version_policy="new_revision")
    publish_template_versioned(tmp_path, doc, version_policy="new_revision")
    doc3 = _minimal_doc("demo_tpl_v1", display_name="Replaced")
    pub = publish_template_versioned(tmp_path, doc3, version_policy="replace_active")
    assert pub["revision"] == 2
    assert pub["created_new_revision"] is False
    info = list_template_versions(tmp_path, "demo_tpl_v1")
    assert len(info["revisions"]) == 2
    active = load_active_template_doc(tmp_path, "demo_tpl_v1")
    assert active is not None
    assert active["display_name"] == "Replaced"


def test_migrate_legacy_flat_on_publish(tmp_path: Path) -> None:
    doc = _minimal_doc("legacy_tpl")
    (tmp_path / "legacy_tpl.json").write_text(json.dumps(doc), encoding="utf-8")
    assert has_legacy_flat(tmp_path, "legacy_tpl")
    doc2 = _minimal_doc("legacy_tpl", display_name="After migrate")
    pub = publish_template_versioned(tmp_path, doc2, version_policy="new_revision")
    assert pub["migrated_from_legacy"] is True
    assert not has_legacy_flat(tmp_path, "legacy_tpl")
    assert has_versioned_package(tmp_path, "legacy_tpl")
    info = list_template_versions(tmp_path, "legacy_tpl")
    assert info["active_revision"] == 2
    assert (tmp_path / "legacy_tpl" / "v1.json").exists()
    assert (tmp_path / "legacy_tpl" / "v2.json").exists()


def test_load_template_docs_from_dir_reads_active_revision(tmp_path: Path) -> None:
    doc = _minimal_doc("load_me")
    publish_template_versioned(tmp_path, doc, version_policy="new_revision")
    publish_template_versioned(
        tmp_path, _minimal_doc("load_me", display_name="Active"), version_policy="new_revision"
    )
    loaded = load_template_docs_from_dir(tmp_path)
    assert "load_me" in loaded
    assert loaded["load_me"].display_name == "Active"


def test_unpublish_removes_package(tmp_path: Path) -> None:
    doc = _minimal_doc("gone_tpl")
    publish_template_versioned(tmp_path, doc, version_policy="new_revision")
    removed = unpublish_template(tmp_path, "gone_tpl")
    assert removed["removed"] == "revision_package"
    assert not (tmp_path / "gone_tpl").exists()


def test_activate_template_revision(tmp_path: Path) -> None:
    doc = _minimal_doc("demo_tpl_v1")
    publish_template_versioned(tmp_path, doc, version_policy="new_revision")
    publish_template_versioned(
        tmp_path, _minimal_doc("demo_tpl_v1", display_name="v2"), version_policy="new_revision"
    )
    act = activate_template_revision(tmp_path, "demo_tpl_v1", 1)
    assert act["active_revision"] == 1
    info = list_template_versions(tmp_path, "demo_tpl_v1")
    assert info["active_revision"] == 1
    active = load_active_template_doc(tmp_path, "demo_tpl_v1")
    assert active is not None
    assert active["display_name"] == "Test"


def test_invalid_template_id() -> None:
    with pytest.raises(ValueError):
        publish_template_versioned(Path("."), _minimal_doc("bad id"))
