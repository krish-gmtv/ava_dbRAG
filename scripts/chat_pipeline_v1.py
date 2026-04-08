"""Compatibility entrypoint for chat pipeline.

Implementation lives in ``scripts/pipeline/chat_pipeline_v1.py`` and is dynamically loaded
so existing ``import chat_pipeline_v1`` / ``import scripts.chat_pipeline_v1`` usage works.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Dict


def _load_impl() -> ModuleType:
    impl_path = Path(__file__).resolve().parent / "pipeline" / "chat_pipeline_v1.py"
    spec = importlib.util.spec_from_file_location("_chat_pipeline_v1_impl", impl_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load chat_pipeline_v1 implementation at {impl_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_impl = _load_impl()
_exports: Dict[str, object] = {
    k: getattr(_impl, k) for k in dir(_impl) if not k.startswith("_")
}
globals().update(_exports)



