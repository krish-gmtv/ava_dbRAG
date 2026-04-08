"""Compatibility entrypoint for safe Ava phrasing wrapper."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_impl_path = Path(__file__).resolve().parent / "ava" / "ava_safe_phraser.py"
_spec = importlib.util.spec_from_file_location("_ava_safe_phraser_impl", _impl_path)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load ava_safe_phraser implementation at {_impl_path}")
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
globals().update({k: getattr(_mod, k) for k in dir(_mod) if not k.startswith('__')})

