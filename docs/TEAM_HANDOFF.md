# Team handoff — Ava saved-report milestone

Short guide for reviewing this repo. For setup commands see `README.md`; for milestone history see `MILESTONES.md`.

## What this delivers

A **template-driven reporting path**: manager-defined JSON templates → slot extraction → execution plan → typed data blocks (semantic narrative, precise KPI SQL, row listings) → normalized `saved_report` payload → structured UI + optional Ava phrasing.

Legacy **query-only** routing (semantic/precise without a template) still exists for ad-hoc questions.

## Demo script (5 minutes)

1. Start server: `python scripts/server/chat_ui_server_v1.py`
2. Open http://127.0.0.1:8787/
3. Enable **Developer mode** in the UI.
4. Main input: `Buyer 119 Q2 2021`
5. **Template builder:** add KPI snapshot + Opportunities listing → **Run preview**
6. Confirm in diagnostics:
   - `final_response_mode`: `saved_report`
   - `saved_report_runtime_version`: `v2_typed_blocks_2026-04-09`
   - `row_listing_opportunities` block with `precise_list_buyer_opportunities`
7. **Opportunities listing** may show **0 rows** — that is valid SQL for this buyer/quarter (`total_opportunities: 0`). Add **Upsheets listing** to export upsheet rows.

## KPI snapshot vs row listing

| UI section | Data |
|------------|------|
| KPI snapshot | Aggregated metrics (Field/Value) from `precise_get_buyer_quarter_kpis` |
| Upsheets / Opportunities listing | Raw Postgres rows; Week/Month/Year split export on date columns |

## Key files

| Area | File |
|------|------|
| HTTP + UI | `scripts/server/chat_ui_server_v1.py` |
| Template execution | `scripts/templates/template_executor_v1.py` |
| Merge blocks → report | `scripts/reporting/report_normalizer_v2.py` |
| UI contract | `scripts/reporting/structured_report_v1.py` |
| Publish / revisions | `scripts/templates/template_versions_v1.py` |
| Template JSON | `templates/saved_reports/*.json` |

## Tests

```powershell
python -m pytest tests -q
```

Covers template docs, executor merge, prompt assembler, structured report, precise routing, publish/versioning. Most tests mock subprocess retrieval.

## Known limitations (v1)

- Template builder UI is a single HTML file (`ui/chat_ui_v1.html`).
- Row preview capped (~25 rows) for UI/export; full-table export is a future improvement.
- Ava phrasing may fall back to deterministic text if validation fails (see `phrasing_mode` in diagnostics).
- Repo root `*_v1.py` shims exist for import compatibility; canonical code lives under `scripts/`.

## Shipped example templates

- `buyer_performance_report_v1.json`
- `buyer_upsheets_listing_report_v1.json`
- `buyer_opportunities_listing_report_v1.json`

Personal `test_*` / draft folders are gitignored; do not rely on them in review.
