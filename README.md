# Ava DB RAG — Buyer reporting (local)

Python backend and HTML UI for buyer/quarter questions: **semantic summaries** (Pinecone), **precise SQL** (Postgres KPIs and row listings), and **saved report templates** with a fixed section order.

## Quick start

**Prerequisites:** Python 3.10+, Postgres sandbox (`ava_sandboxV2` by default), optional Pinecone + Ava credentials for full semantic/phrasing paths.

```powershell
cd "path\to\Ava_DB RAG"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
# Edit .env with Postgres password and optional Ava/Pinecone keys
python scripts\server\chat_ui_server_v1.py
```

Open **http://127.0.0.1:8787/** (UI is served from `ui/chat_ui_v1.html`).

## Tests

Unit tests mock retrieval and do not require Postgres for most cases:

```powershell
python -m pytest tests -q
```

Optional batch contrast tool (not part of pytest): `python scripts/tools/ui_contrast_matrix_v1.py --help`

## Repository layout

| Path | Role |
|------|------|
| `scripts/server/chat_ui_server_v1.py` | HTTP API + static UI |
| `scripts/pipeline/` | Chat orchestration |
| `scripts/templates/` | Template registry, planner, executor, publish/versions |
| `scripts/reporting/` | `structured_report`, normalizer, semantic quality |
| `scripts/ava/` | Ava phrasing, prompt modules, assembler |
| `templates/saved_reports/` | Published template JSON (product surface) |
| `ui/chat_ui_v1.html` | Chat + template builder (v1 monolith) |
| `tests/` | Pytest suite |
| `MILESTONES.md` | Milestone checklist and “which path ran” signals |
| `docs/TEAM_HANDOFF.md` | Short handoff for reviewers |

**Import shims:** Files like `intent_router_v1.py` at the repo root re-export `scripts/` modules so older imports and subprocesses keep working. Prefer `scripts.*` in new code.

## Saved reports vs legacy chat

- **Saved report:** `final_response_mode: saved_report`, template blocks, KPI from precise SQL where configured. See developer diagnostics in the UI.
- **Legacy:** Router-only semantic/precise answers without a template plan.

Details: `MILESTONES.md` and `docs/TEAM_HANDOFF.md`.

## Environment

Copy `.env.example` → `.env`. Minimum for precise KPI/listings: `PRECISE_PG_*`. For Ava phrasing and semantic blocks: `AVA_TOKEN` (or `AVA_USERNAME` / `AVA_PASSWORD`). For Pinecone: `PINECONE_API_KEY`, `PINECONE_INDEX_NAME`.

## Database and offline data

- `DB_creation/` — Postgres init/seed scripts for the sandbox.
- `KPIs/` — SQL definitions and seed CSVs used to build quarterly chunks and KPI validation.

Not required to run the chat UI if your sandbox DB is already loaded.

## Template docs

See `templates/saved_reports/README.md` for the JSON template schema and planner smoke commands.
