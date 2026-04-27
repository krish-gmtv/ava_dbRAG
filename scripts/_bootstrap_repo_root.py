"""
Runtime import bootstrap for direct script execution.

This repo has modules organized under ``scripts/``. When a file is executed as a script
(e.g. ``python scripts/server/chat_ui_server_v1.py``), Python changes ``sys.path[0]``
to that script's directory which may not include the repo root. That breaks imports
like ``import scripts.reporting...``.

Call ``ensure_repo_root_on_syspath()`` near the top of scripts that may be executed
directly.
"""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_repo_root_on_syspath() -> Path:
    """
    Ensure the repo root directory is on ``sys.path``.

    Returns the resolved repo root path.
    """
    scripts_dir = Path(__file__).resolve().parent
    repo_root = scripts_dir.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root

