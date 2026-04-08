"""Compatibility entrypoint.

The implementation lives in ``scripts/server/chat_ui_server_v1.py`` to make the
top-level ``scripts/`` folder easier to scan. This file dynamically loads that
implementation and re-exports its public symbols so existing imports keep working.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Dict


def _load_impl() -> ModuleType:
    impl_path = Path(__file__).resolve().parent / "server" / "chat_ui_server_v1.py"
    spec = importlib.util.spec_from_file_location("_chat_ui_server_v1_impl", impl_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load chat_ui_server_v1 implementation at {impl_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_impl = _load_impl()

# Re-export: everything except private/internal names.
_exports: Dict[str, object] = {k: getattr(_impl, k) for k in dir(_impl) if not k.startswith("__")}
globals().update(_exports)

if __name__ == "__main__":
    # Preserve old CLI behavior.
    if hasattr(_impl, "main"):
        _impl.main()
    else:
        raise SystemExit("chat_ui_server_v1 implementation missing main()")


