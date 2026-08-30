"""
Phase 1 infrastructure validation (pytest wrapper around scripts/healthcheck.py).

Run after `scripts/start.sh` (full stack):

    pytest tests/integration/test_phase1_infra.py -v

Services whose Compose profile is not active are reported SKIPPED, not failed.
Set FINSIGHT_REQUIRE_ALL=1 to turn skips into failures (CI / full-stack gate).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import healthcheck  # noqa: E402

REQUIRE_ALL = os.environ.get("FINSIGHT_REQUIRE_ALL") == "1"

_results = {r.name: r for r in healthcheck.build_checks()}


@pytest.mark.parametrize("service", sorted(_results))
def test_service_healthy(service: str) -> None:
    r = _results[service]
    if r.ok is None:
        if REQUIRE_ALL:
            pytest.fail(f"{service}: {r.detail} (FINSIGHT_REQUIRE_ALL=1)")
        pytest.skip(f"{service}: {r.detail}")
    failed = [f"{label}: {detail}" for label, ok, detail in r.checks if not ok]
    assert r.ok, f"{service} unhealthy -> " + "; ".join(failed or [r.detail])


def test_core_services_present() -> None:
    """The always-on core must never be skipped."""
    for name in ("kafka", "namenode", "datanode", "mongodb", "neo4j"):
        assert _results[name].ok is not None, f"core service {name} not running"
