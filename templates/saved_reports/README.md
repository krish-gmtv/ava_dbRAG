# Saved report templates (JSON)

This folder contains **manager-defined report templates** as JSON documents.

These files are part of the product surface: the long-term goal is that a manager UI
(drag/drop) will write these JSON docs, and the backend will execute them deterministically.

---

## What’s in a template doc?

Each `*.json` file defines:
- **Identity**: `template_id`, `display_name`, `purpose`
- **Matching**: `trigger_phrases` (simple substring matcher in v1)
- **Slots**: `required_slots`, `optional_slots` (e.g. buyer, timeframe)
- **Layout**: `section_order` (fixed report section order)
- **Blocks**: `data_blocks[]` (what data is fetched / assembled)
- **Phrasing**: `prompt_modules[]` (governed prompt modules that may phrase/explain sections)
- **Rules**: `phrasing_rules[]` (non-negotiable layout constraints)

The current schema version marker is:
- `template_doc_version = "saved_report_template_doc_v1"`

Validation + loading lives in:
- `scripts/templates/template_docs_v1.py`

---

## How to test a template (planner only)

From repo root:

```powershell
python scripts/template_report_orchestrator_v1.py --query "I want a buyer performance report on Buyer 119 for Q2 2021"
python scripts/template_report_orchestrator_v1.py --query "list upsheets for Buyer 119 in Q2 2021"
```

You should see `kind: saved_report_plan_v1`, filled slots, and selected blocks.

---

## Notes

- Templates may run **semantic** blocks for narrative, but KPI/row listing blocks can be
  declared as **precise** (SQL) via `source_mode` / `retrieval_mode`.
- Keep previews small (top N) and deterministic.

