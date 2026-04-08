"""Repo-root import shim for ``chat_ui_server_v1``."""

import importlib

_m = importlib.import_module("scripts.chat_ui_server_v1")
globals().update({k: getattr(_m, k) for k in dir(_m) if not k.startswith("__")})

