# Repo navigation (where the real logic lives)

This repo uses **compatibility entrypoints / shims** to keep older import paths working
after the codebase was organized into subfolders.

When reviewing or debugging, prefer the **“Real implementation”** path.

---

## Key areas (real implementations)

- **Saved report templates / planning / execution**
  - `scripts/templates/saved_report_templates_v1.py`
  - `scripts/templates/template_report_orchestrator_v1.py`
  - `scripts/templates/template_executor_v1.py`

- **Template-as-data (JSON docs)**
  - `templates/saved_reports/*.json`
  - `scripts/templates/template_docs_v1.py` (validator + loader)
  - `scripts/templates/template_schema_v1.py` (dataclasses only)

- **Report normalization + UI contract**
  - `scripts/reporting/report_normalizer_v2.py`
  - `scripts/reporting/structured_report_v1.py`

- **Prompt modules + prompt assembler (governed phrasing)**
  - `scripts/ava/prompt_modules_v1.py`
  - `scripts/ava/prompt_assembler_v1.py`

- **Chat orchestration (server + pipeline)**
  - `scripts/server/chat_ui_server_v1.py`
  - `scripts/pipeline/chat_pipeline_v1.py`
  - `scripts/pipeline/saved_report_flow_v1.py` (saved report branch extracted)

- **UI**
  - `ui/chat_ui_v1.html`

---

## Common shims → real implementations

| Shim / entrypoint | Real implementation |
|---|---|
| `chat_ui_server_v1.py` | `scripts/server/chat_ui_server_v1.py` |
| `scripts/chat_ui_server_v1.py` | `scripts/server/chat_ui_server_v1.py` |
| `chat_pipeline_v1.py` | `scripts/pipeline/chat_pipeline_v1.py` |
| `scripts/chat_pipeline_v1.py` | `scripts/pipeline/chat_pipeline_v1.py` |
| `saved_report_templates_v1.py` | `scripts/templates/saved_report_templates_v1.py` |
| `template_report_orchestrator_v1.py` | `scripts/templates/template_report_orchestrator_v1.py` |
| `template_executor_v1.py` | `scripts/templates/template_executor_v1.py` |
| `structured_report_v1.py` | `scripts/reporting/structured_report_v1.py` |
| `report_normalizer_v2.py` | `scripts/reporting/report_normalizer_v2.py` |
| `prompt_modules_v1.py` | `scripts/ava/prompt_modules_v1.py` |
| `prompt_assembler_v1.py` | `scripts/ava/prompt_assembler_v1.py` |
| `template_docs_v1.py` | `scripts/templates/template_docs_v1.py` |
| `template_schema_v1.py` | `scripts/templates/template_schema_v1.py` |
| `saved_report_flow_v1.py` | `scripts/pipeline/saved_report_flow_v1.py` |

---

## Quick “what runs when?”

- **HTTP surface (local UI server)**
  - `GET /chat` (or `/`): serves `ui/chat_ui_v1.html`
  - `POST /api/chat`: main entrypoint (template-aware)
  - `GET /api/templates`: list available saved-report templates for the UI picker
  - `POST /api/export/xlsx`: generate an `.xlsx` workbook from table rows (optional split by week/month/year)

- **Saved report** path runs when `plan_saved_report(query)` matches a template:
  - plan → execute blocks → normalize → structured report → UI render

- **Legacy routing** (semantic/precise) runs when no saved template matches:
  - intent router chooses handler → renderer builds `final_response` → structured report

---

## Platform map (where to change what)

- **UI (rendering + downloads)**: `ui/chat_ui_v1.html`
  - Renders `structured_report.sections.*`
  - Triggers CSV/XLSX downloads (XLSX via `/api/export/xlsx`)

- **Server (HTTP endpoints + wiring)**: `scripts/server/chat_ui_server_v1.py`
  - Owns `/api/chat`, `/api/templates`, `/api/export/xlsx`

- **Export implementation**: `scripts/server/xlsx_export_v1.py`
  - Owns workbook creation, sheet splitting, filename handling

- **Saved-report pipeline boundary**: `scripts/pipeline/saved_report_flow_v1.py`
  - Decides: template flow vs legacy flow

- **Template planning (match + slots)**: `scripts/templates/template_report_orchestrator_v1.py`
  - `plan_saved_report(...)` (auto match)
  - `plan_saved_report_for_template(...)` (forced template picker)

- **Template execution (run blocks → merge)**: `scripts/templates/template_executor_v1.py`
  - Calls existing retrieval scripts via `answer_renderer_v1.py`
  - Produces `final_response.mode = "saved_report"`

- **Legacy routing (semantic vs precise)**: `scripts/intent_router_v1.py`
  - Produces execution plans for `execute_query_v1.py` / `answer_renderer_v1.py`

- **Stable UI contract**: `scripts/reporting/structured_report_v1.py`
  - Converts `final_response` into a stable `structured_report`

