"""Compatibility entrypoint for saved-report planning (v1).

Implementation moved to ``scripts/templates/template_report_orchestrator_v1.py``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_impl_path = Path(__file__).resolve().parent / "templates" / "template_report_orchestrator_v1.py"
_spec = importlib.util.spec_from_file_location("_template_report_orchestrator_v1_impl", _impl_path)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load template_report_orchestrator_v1 implementation at {_impl_path}")
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)
globals().update({k: getattr(_mod, k) for k in dir(_mod) if not k.startswith('__')})


if __name__ == "__main__":
    # Preserve old CLI behavior: delegate to the real module's main().
    if hasattr(_mod, "main"):
        _mod.main()
    else:
        raise SystemExit("template_report_orchestrator_v1 implementation missing main()")

