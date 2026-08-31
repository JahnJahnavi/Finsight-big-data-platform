"""
FinSight - Phase 14: end-to-end validation as a pytest gate.

Runs scripts/validate_e2e.py against the live stack and asserts there are no
FAIL rows (BLOCKED / WARN are allowed - they mean "run the job" / "known
synthetic-data quirk", see docs/testing/known-issues.md).

Skipped automatically when Docker or the FinSight stack is not up, so it is
safe in a unit-only CI run.

    pytest tests/integration/test_e2e_validation.py -v -s
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
VALIDATOR = REPO / "scripts" / "validate_e2e.py"


def _stack_up() -> bool:
    if not shutil.which("docker"):
        return False
    r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}",
                        "finsight-kafka"], capture_output=True, text=True)
    return r.stdout.strip() == "true"


pytestmark = pytest.mark.skipif(not _stack_up(),
                                reason="FinSight docker stack is not running")


def _run_validator(extra: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(VALIDATOR), *extra],
                          capture_output=True, text=True, cwd=REPO, timeout=900)


def test_validator_script_is_importable():
    r = subprocess.run([sys.executable, "-m", "py_compile", str(VALIDATOR)],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_no_failures_in_full_run():
    """The gate: a full e2e sweep must contain zero FAIL rows."""
    r = _run_validator(["--no-report"])
    tail = "\n".join((r.stdout + r.stderr).splitlines()[-40:])
    # exit 1 == at least one FAIL; exit 0 == only PASS/BLOCKED/WARN
    assert "FAIL" not in _fail_line(r.stdout), (
        f"validate_e2e.py reported FAIL row(s):\n{tail}")
    assert r.returncode in (0, 1)
    if r.returncode == 1:
        pytest.fail(f"validate_e2e.py exited 1 (real defect):\n{tail}")


def _fail_line(out: str) -> str:
    for ln in out.splitlines():
        if ln.strip().startswith("TOTAL "):
            # e.g. "TOTAL 75  |  PASS 63  FAIL 0  BLOCKED 11  WARN 1  SKIP 0"
            seg = ln.split("FAIL", 1)[1].strip().split()[0]
            return "FAIL" if seg != "0" else "ok"
    return "ok"


@pytest.mark.parametrize("section", [1, 3, 4, 5, 18, 19])
def test_core_infra_checkpoints_pass(section: int):
    """Checkpoints that must always pass once the stack is up (no pipeline run needed)."""
    r = _run_validator(["--section", str(section)])
    assert "[FAIL]" not in r.stdout, r.stdout
