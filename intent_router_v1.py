"""Repo-root import shim for ``intent_router_v1``.

Some modules are imported as top-level scripts (e.g. from within ``scripts/``). When
running tests from ``tests/``, the repo root is on sys.path, not ``scripts/``.
This shim keeps imports stable.
"""

from scripts.intent_router_v1 import *  # noqa: F403

