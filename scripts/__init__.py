"""scripts package.

Pytest runs from repo root and imports many modules that live under ``scripts/``.
Having this package marker lets shims import ``scripts.<module>`` safely.
"""

