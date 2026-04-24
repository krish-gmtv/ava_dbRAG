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

- **Saved report** path runs when `plan_saved_report(query)` matches a template:
  - plan → execute blocks → normalize → structured report → UI render

- **Legacy routing** (semantic/precise) runs when no saved template matches:
  - intent router chooses handler → renderer builds `final_response` → structured report

