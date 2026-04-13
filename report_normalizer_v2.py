"""Compatibility entrypoint for saved-report normalizer (v2).

Implementation lives at ``scripts/reporting/report_normalizer_v2.py``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_impl_path = Path(__file__).resolve().parent / "scripts" / "reporting" / "report_normalizer_v2.py"
_spec = importlib.util.spec_from_file_location("_report_normalizer_v2_impl", _impl_path)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load report_normalizer_v2 implementation at {_impl_path}")

_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)

globals().update({k: getattr(_mod, k) for k in dir(_mod) if not k.startswith("__")})

