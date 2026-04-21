# Ava DB RAG — Reporting product milestones

Living checklist: **what is done**, **what is next**, and **how to verify** you are on the saved-report / contract path vs legacy chat routing.

---

## End goal (definition of “done”)

The product is a **deterministic, manager-defined report execution platform**, not a free-form chat bot.

| Principle | Meaning |
|-----------|---------|
| **Templates are source of truth** | Section order, which blocks run, and which prompt modules apply come from the template (and eventually from saved template JSON / UI). |
| **Blocks produce data** | Retrieval and SQL steps output typed payloads; they do not “write the report.” |
| **KPI trust** | KPI-style numbers come from **declared precise sources** (e.g. SQL aggregates), not from semantic narrative or Ava synthesis. |
| **Ava is narrow** | Ava **phrases and explains** within fixed sections using **governed, versioned prompt modules** assembled deterministically. |
| **Two UI modes** | **User mode**: clean report. **Developer mode**: plan, block outputs, semantic quality, prompt assembly audit trail. |

---

## How to tell which path ran (quick sanity)

Use **Developer diagnostics**:

| Signal | Path |
|--------|------|
| `final_response_mode` is **`saved_report`** | Saved-report template pipeline (typed blocks, KPI snapshot from precise where configured, prompt modules). |
| `saved_report_runtime_version` is **non-null** (e.g. `v2_typed_blocks_2026-04-09`) | Typed block runtime v2 is active. |
| `prompt_modules` present with `assembled_payload` | Governed prompt assembly ran for that saved-report execution. |
| `final_response_mode` is **`semantic`** / **`precise`** | Legacy query-driven routing (intent router). **Not** the full saved-report assembly unless you explicitly chose that flow. |

**Tip:** Natural “performance” questions without template phrasing may still hit **semantic** unless routing convergence (Milestone 5) is implemented.

---

## Completed milestones

### M1 — Template-driven reporting skeleton

- [x] Frozen template: `buyer_performance_report_v1` (slots, section order, allowed blocks).
- [x] Template registry + matcher (trigger phrases → template).
- [x] Slot extraction (buyer, timeframe).
- [x] Planner / orchestrator: deterministic **`saved_report_plan_v1`** JSON (`plan_saved_report`).
- [x] Executor runs selected blocks and merges into **`mode: saved_report`** `final_response`.
- [x] UI: structured report + developer diagnostics (baseline).

**Primary files:** `scripts/templates/saved_report_templates_v1.py`, `scripts/templates/template_report_orchestrator_v1.py`, `scripts/templates/template_executor_v1.py`, `scripts/reporting/structured_report_v1.py`.

---

### M2 — Saved report runtime v2 (typed blocks + KPI source rule)

- [x] `DataBlockSpec` extended with `block_type`, `output_key`, `source_mode`, `runtime_rules`.
- [x] Executor produces typed **`BlockOutput`** list (not ad-hoc merge by `block_id` only).
- [x] **`report_normalizer_v2`**: single place that builds user-facing `final_response` from block outputs.
- [x] KPI block: **`kpi_table`** + **`source_mode: precise`** → SQL script path; snapshot not taken from semantic KPI narrative.
- [x] Semantic quality gate fail-closed where appropriate; catalog hints for indexed quarters.
- [x] Developer diagnostics: `template_block_outputs_v2`, `saved_report_runtime_version`.
- [x] Tests around executor + KPI sourcing.

**Primary files:** `scripts/templates/template_executor_v1.py`, `scripts/reporting/report_normalizer_v2.py`, `scripts/reporting/semantic_quality_v1.py`, `scripts/reporting/semantic_catalog_v1.py`.

---

### M3 — Governed prompt modules + prompt assembler (v1)

- [x] Prompt **module registry** (versioned): `executive_summary`, `highlights`, `notes` (`scripts/ava/prompt_modules_v1.py`).
- [x] **Prompt assembler**: deterministic `assembled_payload` + WS message body (`scripts/ava/prompt_assembler_v1.py`).
- [x] Saved-report phrasing uses assembler when assembly inputs are attached (see M2 executor wiring).
- [x] Developer diagnostics include **`prompt_modules`** / `assembled_payload` for audit.
- [x] Shims for imports: `scripts/prompt_*_v1.py`, repo-root `prompt_*_v1.py`.
- [x] Tests: `tests/test_prompt_assembler_v1.py`.

**Why it matters:** You can prove **which module versions** ran and **exactly what structured data** was passed into phrasing.

---

### M4 — Templates explicitly reference prompt modules

- [x] `SavedReportTemplate.prompt_modules`: ordered list of module ids; **validated** against registry at template construction.
- [x] Plan JSON includes **`prompt_modules`** (from template).
- [x] Executor honors `plan["prompt_modules"]` with safe fallback + diagnostics (`module_selection_source`, `unknown_modules_in_template`).
- [x] Tests: `tests/test_template_prompt_modules_binding_v1.py`, executor tests in `tests/test_template_executor_v1.py`.

**Why it matters:** This is the backend bridge to **manager drag-and-drop blocks** → each block maps to a **module id** on the template.

---

## Forward roadmap (checklist)

### M5 — Routing convergence (recommended next)

**Goal:** Buyer + quarter “performance” style questions **prefer** `buyer_performance_report_v1` so users do not accidentally land in legacy **`semantic`** without the saved-report KPI table.

- [ ] Define clear match rules (e.g. buyer id resolved + quarter + performance intent) vs explicit “report” wording only.
- [ ] Wire in `chat_pipeline_v1` / matcher without breaking listing-only or off-topic queries.
- [ ] Tests: same buyer/quarter via “report” vs “how did X perform” → both `saved_report` when intended.
- [ ] Document the behavior in this file after implementation.

---

### M6 — Additional prompt modules (product-visible phrasing)

- [ ] `kpi_narrative@v1`: short prose **only** from `kpi_snapshot` + allowed labels (no new numbers).
- [ ] Optional: `next_steps@v1` governed follow-up copy.
- [ ] Extend assembler + template `prompt_modules` for `buyer_performance_report_v1` as needed.
- [ ] Tests: assembler includes new module; Ava message contract stable.

---

### M7 — Second saved template (prove generalization)

- [ ] New `template_id` with different `data_blocks` and/or `prompt_modules`.
- [ ] Matcher phrases + tests (no collision with existing template).
- [ ] Executor remains spec-driven (no template-specific `if block_id` hacks).

---

### M8 — Template-as-data (editor / persistence contract)

- [ ] Serialize template definition to **JSON** (or DB row) with schema validation.
- [ ] Loader: JSON → same in-memory objects used today.
- [ ] Version field on template document for migrations.
- [ ] UI can later read/write this JSON without code changes.

---

### M9 — Multi-source blocks + permissions seam

- [ ] Blocks declare **data sources** (tables / query families) explicitly in template spec.
- [ ] Central “can run block?” hook with **RBAC** stub (real auth when available).
- [ ] Diagnostics: which sources were accessed.

---

### M10 — UI productization

- [ ] Template picker / “run saved report” affordance.
- [ ] Clear distinction or full merge: **chat** vs **report** (product decision).
- [ ] Developer mode: export / copy full audit payload (plan + blocks + modules).

---

## Verification commands (local)

```powershell
cd "C:\Users\suhru\Desktop\Ava_DB RAG"
python -m pytest -q
python scripts/template_report_orchestrator_v1.py --query "I want a buyer performance report on Buyer 119 for Q2 2021"
```

Expect plan JSON to include **`prompt_modules`** and **`ready_to_execute: true`** when buyer + period are present.

---

## Glossary

| Term | Meaning |
|------|---------|
| **Saved report template** | Fixed blueprint: slots, section order, data blocks, phrasing rules, `prompt_modules`. |
| **Plan** | `saved_report_plan_v1` JSON: what would run; may be incomplete if slots missing. |
| **Block** | One data-producing step (semantic narrative, precise KPI table, row listing, …). |
| **BlockOutput** | Typed result: `block_type`, `output_key`, `source`, `payload`. |
| **Prompt module** | Versioned instructions + contract for one phrasing slice (e.g. `executive_summary@v1`). |
| **Prompt assembler** | Deterministic builder of the final Ava message from modules + typed data. |
| **Legacy semantic/precise** | Intent-router path for ad-hoc questions; different from **`saved_report`** assembly. |

---

## Changelog (manual)

| Date | Note |
|------|------|
| 2026-04-20 | Initial checklist: M1–M4 complete; M5–M10 forward roadmap. |

Update this section when you ship a new milestone.
