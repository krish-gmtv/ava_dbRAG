import sys
from pathlib import Path


def pytest_configure() -> None:
    """
    Ensure modules in ``scripts/`` are importable as top-level modules in tests.

    Many tests import modules like ``execute_query_v1`` which live at
    ``scripts/execute_query_v1.py``.
    """
    repo_root = Path(__file__).resolve().parent.parent
    scripts_dir = repo_root / "scripts"

    # Ensure the repo root is first so ``import scripts.<module>`` resolves correctly.
    if str(repo_root) in sys.path:
        sys.path.remove(str(repo_root))
    sys.path.insert(0, str(repo_root))

    # Also allow top-level imports like ``import execute_query_v1`` which map to
    # ``scripts/execute_query_v1.py``.
    if str(scripts_dir) not in sys.path:
        sys.path.insert(1, str(scripts_dir))

