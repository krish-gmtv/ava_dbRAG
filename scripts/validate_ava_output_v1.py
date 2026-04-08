"""Compatibility entrypoint for Ava output validation (v1).

Implementation moved to ``scripts/reporting/validate_ava_output_v1.py``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_impl_path = Path(__file__).resolve().parent / "reporting" / "validate_ava_output_v1.py"
_spec = importlib.util.spec_from_file_location("_validate_ava_output_v1_impl", _impl_path)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load validate_ava_output_v1 implementation at {_impl_path}")
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
globals().update({k: getattr(_mod, k) for k in dir(_mod) if not k.startswith('__')})

