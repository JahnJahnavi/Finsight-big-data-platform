#!/usr/bin/env python3
"""
FinSight - Phase 10: customer segment rules (pure Python, unit-tested).

The five NovaCrest customer segments (spec section 10). `validation.js` carries
the same list in JS; keep the two in sync.
"""
from __future__ import annotations

# Canonical set - spec section 10. Order is display order, not priority.
EXPECTED_SEGMENTS: tuple[str, ...] = (
    "Premium",
    "Standard",
    "Basic",
    "Private Banking",
    "Student",
)


def validate_segment_counts(counts_by_segment: dict[str, int], total_docs: int) -> list[str]:
    """Return a list of human-readable problems (empty list == valid).

    Checks, against the imported `customers` collection:
      * every one of the five expected segments is present with >= 1 customer
      * no customer carries a segment outside the expected set
      * the per-segment counts sum to the collection total (no NULL / missing)
    """
    problems: list[str] = []
    counts = {k: int(v) for k, v in counts_by_segment.items()}

    missing = [s for s in EXPECTED_SEGMENTS if counts.get(s, 0) <= 0]
    if missing:
        problems.append(f"missing / empty segment(s): {missing}")

    unexpected = sorted(set(counts) - set(EXPECTED_SEGMENTS))
    if unexpected:
        problems.append(f"unexpected segment value(s): {unexpected}")

    summed = sum(counts.values())
    if summed != int(total_docs):
        problems.append(
            f"segment counts sum to {summed} but the collection holds "
            f"{total_docs} document(s) - some customers have a NULL / missing segment"
        )

    return problems
