"""Compatibility entrypoint for Ava phrasing layer (v1).

Implementation moved to ``scripts/ava/ava_phraser_v1.py``.
This shim exists so modules executed from within the ``scripts/`` directory can still
`import ava_phraser_v1` successfully.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_impl_path = Path(__file__).resolve().parent / "ava" / "ava_phraser_v1.py"
_spec = importlib.util.spec_from_file_location("_ava_phraser_v1_impl", _impl_path)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load ava_phraser_v1 implementation at {_impl_path}")
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)
globals().update({k: getattr(_mod, k) for k in dir(_mod) if not k.startswith("__")})


if __name__ == "__main__":
    # Preserve old CLI behavior if invoked directly.
    if hasattr(_mod, "main"):
        _mod.main()
    else:
        raise SystemExit("ava_phraser_v1 implementation missing main()")

