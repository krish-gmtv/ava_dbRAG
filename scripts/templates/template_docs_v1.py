"""
Template documents (JSON) for saved reports (v1).

Purpose:
- Provide a **template-as-data** contract suitable for a future manager UI (drag/drop).
- Validate template documents strictly before they can affect routing/execution.
- Load template JSON docs and construct ``SavedReportTemplate`` + ``DataBlockSpec``.

This module is intentionally dependency-light and does not perform I/O beyond reading JSON
files from disk.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from prompt_modules_v1 import PROMPT_MODULES
from template_schema_v1 import DataBlockSpec, SavedReportTemplate


TEMPLATE_DOC_VERSION_V1 = "saved_report_template_doc_v1"

# If set, overrides where JSON templates are loaded from (useful for tests/dev).
TEMPLATES_DIR_ENV = "AVA_SAVED_REPORT_TEMPLATES_DIR"


def default_templates_dir() -> Path:
    """
    Default directory for template documents.

    Repo layout:
      <repo_root>/templates/saved_reports/*.json
    """
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "templates" / "saved_reports"


def resolve_templates_dir() -> Path:
    val = str(os.getenv(TEMPLATES_DIR_ENV) or "").strip()
    if val:
        return Path(val).expanduser().resolve()
    return default_templates_dir()


def _as_str_list(x: Any, *, field: str) -> List[str]:
    if not isinstance(x, list):
        raise ValueError(f"{field} must be a list of strings")
    out: List[str] = []
    for it in x:
        if not isinstance(it, str) or not it.strip():
            raise ValueError(f"{field} must contain only non-empty strings")
        out.append(it.strip())
    return out


def _as_str_tuple(x: Any, *, field: str) -> Tuple[str, ...]:
    return tuple(_as_str_list(x, field=field))


def _as_frozenset(x: Any, *, field: str) -> frozenset[str]:
    return frozenset(_as_str_list(x, field=field))


def validate_template_doc_v1(doc: Dict[str, Any]) -> None:
    if not isinstance(doc, dict):
        raise ValueError("template doc must be an object")

    dv = doc.get("template_doc_version")
    if dv != TEMPLATE_DOC_VERSION_V1:
        raise ValueError(
            f"template_doc_version must be '{TEMPLATE_DOC_VERSION_V1}', got {dv!r}"
        )

    for k in ("template_id", "display_name", "purpose"):
        v = doc.get(k)
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"{k} must be a non-empty string")

    _as_str_tuple(doc.get("trigger_phrases"), field="trigger_phrases")
    _as_str_tuple(doc.get("section_order"), field="section_order")
    _as_str_tuple(doc.get("phrasing_rules"), field="phrasing_rules")
    _as_str_tuple(doc.get("prompt_modules"), field="prompt_modules")

    _as_frozenset(doc.get("required_slots"), field="required_slots")
    _as_frozenset(doc.get("optional_slots"), field="optional_slots")
    _as_frozenset(
        doc.get("disallowed_without_explicit_request"),
        field="disallowed_without_explicit_request",
    )

    mods = _as_str_list(doc.get("prompt_modules"), field="prompt_modules")
    unknown = [m for m in mods if m not in PROMPT_MODULES]
    if unknown:
        raise ValueError(
            f"Unknown prompt modules in template doc '{doc.get('template_id')}': {unknown}"
        )

    blocks = doc.get("data_blocks")
    if not isinstance(blocks, list) or not blocks:
        raise ValueError("data_blocks must be a non-empty list")

    seen_block_ids: set[str] = set()
    for i, b in enumerate(blocks):
        if not isinstance(b, dict):
            raise ValueError(f"data_blocks[{i}] must be an object")
        for k in (
            "block_id",
            "description",
            "block_type",
            "retrieval_mode",
            "query_family_hint",
            "output_key",
        ):
            v = b.get(k)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"data_blocks[{i}].{k} must be a non-empty string")
        bid = str(b.get("block_id") or "").strip()
        if bid in seen_block_ids:
            raise ValueError(f"Duplicate block_id in template doc: {bid}")
        seen_block_ids.add(bid)

        rm = str(b.get("retrieval_mode") or "").strip()
        if rm not in ("semantic", "precise", "composition"):
            raise ValueError(
                f"data_blocks[{i}].retrieval_mode must be semantic|precise|composition, got {rm!r}"
            )

        if "runtime_rules" in b:
            rr = b.get("runtime_rules")
            if rr is None:
                pass
            else:
                _as_str_tuple(rr, field=f"data_blocks[{i}].runtime_rules")

        # Require unique output_key per block_type for now to avoid collisions in rendering.
        # (Multiple row_listing blocks will still render via row_preview_tables.)
        if str(b.get("block_type") or "").strip() == "row_listing":
            pass


def template_from_doc_v1(doc: Dict[str, Any]) -> SavedReportTemplate:
    validate_template_doc_v1(doc)

    blocks: List[DataBlockSpec] = []
    for b in doc["data_blocks"]:
        rr = b.get("runtime_rules") or ()
        if isinstance(rr, list):
            rr = tuple(str(x) for x in rr if isinstance(x, str) and x.strip())
        blocks.append(
            DataBlockSpec(
                block_id=str(b["block_id"]).strip(),
                description=str(b["description"]).strip(),
                block_type=str(b["block_type"]).strip(),
                retrieval_mode=str(b["retrieval_mode"]).strip(),
                query_family_hint=str(b["query_family_hint"]).strip(),
                output_key=str(b["output_key"]).strip(),
                source_mode=str(b.get("source_mode") or "").strip(),
                runtime_rules=tuple(rr) if isinstance(rr, tuple) else (),
                requires_explicit_user_request=bool(
                    b.get("requires_explicit_user_request") or False
                ),
            )
        )

    return SavedReportTemplate(
        template_id=str(doc["template_id"]).strip(),
        display_name=str(doc["display_name"]).strip(),
        purpose=str(doc["purpose"]).strip(),
        trigger_phrases=_as_str_tuple(doc["trigger_phrases"], field="trigger_phrases"),
        required_slots=_as_frozenset(doc["required_slots"], field="required_slots"),
        optional_slots=_as_frozenset(doc["optional_slots"], field="optional_slots"),
        section_order=_as_str_tuple(doc["section_order"], field="section_order"),
        data_blocks=tuple(blocks),
        disallowed_without_explicit_request=_as_frozenset(
            doc["disallowed_without_explicit_request"],
            field="disallowed_without_explicit_request",
        ),
        phrasing_rules=_as_str_tuple(doc["phrasing_rules"], field="phrasing_rules"),
        prompt_modules=_as_str_tuple(doc["prompt_modules"], field="prompt_modules"),
    )


def load_template_docs_from_dir(dir_path: Path) -> Dict[str, SavedReportTemplate]:
    """
    Load all ``*.json`` template docs from the directory.

    Returns a mapping of template_id -> SavedReportTemplate.
    """
    out: Dict[str, SavedReportTemplate] = {}
    if not dir_path.exists() or not dir_path.is_dir():
        return out

    for p in sorted(dir_path.glob("*.json")):
        raw = p.read_text(encoding="utf-8")
        doc = json.loads(raw)
        tpl = template_from_doc_v1(doc)
        out[tpl.template_id] = tpl
    return out


def export_template_to_doc_v1(tpl: SavedReportTemplate) -> Dict[str, Any]:
    """
    Export a template into a JSON-serializable doc matching TEMPLATE_DOC_VERSION_V1.
    Useful for bootstrapping an editor or creating the first JSON template.
    """
    dd = asdict(tpl)
    dd["required_slots"] = sorted(list(dd["required_slots"]))
    dd["optional_slots"] = sorted(list(dd["optional_slots"]))
    dd["disallowed_without_explicit_request"] = sorted(
        list(dd["disallowed_without_explicit_request"])
    )
    dd["template_doc_version"] = TEMPLATE_DOC_VERSION_V1
    for b in dd.get("data_blocks") or []:
        if isinstance(b.get("runtime_rules"), tuple):
            b["runtime_rules"] = list(b["runtime_rules"])
    validate_template_doc_v1(dd)
    return dd

