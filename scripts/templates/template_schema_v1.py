"""
Saved report template schema (v1).

This module contains **only** dataclasses / schema and is intentionally dependency-light.
It exists to avoid circular imports between:
- template registry (built-in Python templates + JSON overrides)
- JSON template docs loader/validator
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, FrozenSet

from prompt_modules_v1 import PROMPT_MODULES


@dataclass(frozen=True)
class DataBlockSpec:
    """One allowed retrieval / assembly step the template executor may run."""

    block_id: str
    description: str
    block_type: str
    # How this block is usually satisfied given current product routing.
    retrieval_mode: str  # semantic | precise | composition
    query_family_hint: str
    # Which UI section key this block intends to fill.
    output_key: str
    # Canonical name: source mode for the block's truth. Keep retrieval_mode for backward-compat.
    source_mode: str = ""
    # Optional enforcement hints owned by the template spec.
    runtime_rules: Tuple[str, ...] = ()
    requires_explicit_user_request: bool = False

    def __post_init__(self) -> None:  # type: ignore[override]
        # Ensure source_mode defaults to retrieval_mode if not explicitly set.
        if not self.source_mode:
            object.__setattr__(self, "source_mode", self.retrieval_mode)


@dataclass(frozen=True)
class SavedReportTemplate:
    template_id: str
    display_name: str
    purpose: str
    # Phrases in user text (lowercased match). Longest phrase wins at match time.
    trigger_phrases: Tuple[str, ...]
    required_slots: FrozenSet[str]
    optional_slots: FrozenSet[str]
    # Fixed UI / report section keys, in order. Executor fills each from data blocks.
    section_order: Tuple[str, ...]
    data_blocks: Tuple[DataBlockSpec, ...]
    # Blocks that must never run unless the user clearly asks (e.g. raw listings).
    disallowed_without_explicit_request: FrozenSet[str]
    phrasing_rules: Tuple[str, ...]
    # Ordered list of prompt module ids (from the governed prompt module registry)
    # that the phrasing layer will run for this template. Empty means "use the
    # executor's default saved-report module selection".
    prompt_modules: Tuple[str, ...] = ()

    def __post_init__(self) -> None:  # type: ignore[override]
        if self.prompt_modules:
            unknown = [mid for mid in self.prompt_modules if mid not in PROMPT_MODULES]
            if unknown:
                raise ValueError(
                    f"Template '{self.template_id}' references unknown prompt modules: "
                    f"{unknown}. Known modules: {sorted(PROMPT_MODULES.keys())}"
                )

