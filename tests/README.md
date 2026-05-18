# Tests

Run from repo root:

```powershell
python -m pytest tests -q
```

`conftest.py` adds `scripts/` to `sys.path` so tests can import modules like `execute_query_v1` and `template_executor_v1`.

Integration-style batch tools live under `scripts/tools/` (not collected by pytest).
