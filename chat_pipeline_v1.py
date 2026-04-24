"""Repo-root import shim for ``chat_pipeline_v1``.

The real implementation lives at ``scripts/pipeline/chat_pipeline_v1.py``.
This shim dynamically loads it so ``import chat_pipeline_v1`` keeps working
even as internal code is reorganized.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_impl_path = Path(__file__).resolve().parent / "scripts" / "pipeline" / "chat_pipeline_v1.py"
_spec = importlib.util.spec_from_file_location("_chat_pipeline_v1_impl", _impl_path)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load chat_pipeline_v1 implementation at {_impl_path}")

_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)

globals().update({k: getattr(_mod, k) for k in dir(_mod) if not k.startswith("__")})

