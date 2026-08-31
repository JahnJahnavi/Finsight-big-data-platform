#!/usr/bin/env python3
"""
FinSight - Phase 10 validation: MongoDB customer data (spec section 10).

Checks the `finsight.customers` collection after import:
  * collection populated; customerId present on every doc, string, unique
  * the required compound index { customerId: 1, segment: 1 } exists
    (+ the unique index on customerId, the join key)
  * customer counts by segment cover exactly the five expected segments and
    sum to the collection total
  * mongodb/validation.js exits 0

Runs everything through `docker exec finsight-mongodb mongosh` - no host
mongo tooling or Python driver required.

    python scripts/validate_phase10.py           # validate current state
    python scripts/validate_phase10.py --import  # (re-)run the import first

Exit 0 only if every check passes.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
MONGO = "finsight-mongodb"
EXPECTED_SEGMENTS = ["Premium", "Standard", "Basic", "Private Banking", "Student"]


def _env(keys: set[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    envf = REPO / ".env"
    if envf.exists():
        for line in envf.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k in keys:
                out[k] = v
    return out


_E = _env({"MONGO_DB", "MONGO_COLLECTION", "MONGO_INITDB_ROOT_USERNAME",
           "MONGO_INITDB_ROOT_PASSWORD"})
DB = _E.get("MONGO_DB", "finsight")
COLL = _E.get("MONGO_COLLECTION", "customers")
USER = _E.get("MONGO_INITDB_ROOT_USERNAME", "finsight_admin")
PW = _E.get("MONGO_INITDB_ROOT_PASSWORD", "")
URI = f"mongodb://{USER}:{PW}@localhost:27017/{DB}?authSource=admin"


def sh(*cmd, timeout=300, stdin=None):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                          cwd=REPO, input=stdin)


def mongo_eval(js: str):
    """Run a JS expression and JSON-parse the last non-empty output line."""
    r = sh("docker", "exec", MONGO, "mongosh", URI, "--quiet", "--eval",
           f"print(JSON.stringify(({js})))")
    lines = [l for l in (r.stdout + r.stderr).splitlines() if l.strip()]
    for l in reversed(lines):
        try:
            return json.loads(l)
        except json.JSONDecodeError:
            continue
    return None


class Check:
    def __init__(self): self.rows = []
    def add(self, name, ok, detail=""):
        self.rows.append(ok)
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  - {detail}" if detail else ""))
        return ok
    def report(self):
        ok = sum(self.rows)
        print("\n  " + "=" * 62)
        print(f"  Phase 10 validation: {ok}/{len(self.rows)} checks passed")
        print("  " + "=" * 62)
        return 0 if ok == len(self.rows) else 1


def main() -> int:
    c = Check()
    print("\n  FinSight Phase 10 - MongoDB customer data\n  " + "-" * 58)

    if "--import" in sys.argv:
        print("  running mongodb/import_customers.sh ...")
        r = sh("bash", str(REPO / "mongodb" / "import_customers.sh"), timeout=600)
        print("   ", (r.stdout + r.stderr).strip().splitlines()[-1] if (r.stdout + r.stderr).strip() else "")

    total = mongo_eval(f'db.getSiblingDB("{DB}").{COLL}.countDocuments({{}})')
    c.add(f"1. {DB}.{COLL} populated", isinstance(total, int) and total > 0,
          f"{total} documents")
    if not total:
        print("      -> run:  python scripts/validate_phase10.py --import")
        return c.report()

    # --- customerId: present, string, unique (the join key, preserved exactly) ---
    missing_id = mongo_eval(
        f'db.getSiblingDB("{DB}").{COLL}.countDocuments('
        f'{{$or:[{{customerId:{{$exists:false}}}},{{customerId:null}}]}})')
    non_string = mongo_eval(
        f'db.getSiblingDB("{DB}").{COLL}.countDocuments('
        f'{{customerId:{{$exists:true,$not:{{$type:"string"}}}}}})')
    distinct_id = mongo_eval(
        f'db.getSiblingDB("{DB}").{COLL}.distinct("customerId").length')
    c.add("2. customerId present on every document", missing_id == 0,
          f"{missing_id} missing")
    c.add("3. customerId is a string (exact join key)", non_string == 0,
          f"{non_string} non-string")
    c.add("4. customerId unique", distinct_id == total,
          f"{distinct_id} distinct / {total}")

    # --- a known id from the source survives verbatim ---
    sample = mongo_eval(
        f'db.getSiblingDB("{DB}").{COLL}.findOne({{}}, {{customerId:1,_id:0}}).customerId')
    round_trip = mongo_eval(
        f'db.getSiblingDB("{DB}").{COLL}.countDocuments({{customerId:{json.dumps(sample)}}})')
    c.add("5. customerId value preserved exactly (round-trip lookup)",
          isinstance(sample, str) and round_trip == 1, f"{sample!r}")

    # --- required compound index { customerId: 1, segment: 1 } ---
    idx = mongo_eval(f'db.getSiblingDB("{DB}").{COLL}.getIndexes()') or []
    keys = [list(ix.get("key", {}).keys()) for ix in idx]
    has_compound = ["customerId", "segment"] in keys
    has_unique_id = any(ix.get("unique") and list(ix.get("key", {})) == ["customerId"]
                        for ix in idx)
    c.add("6. compound index { customerId: 1, segment: 1 } exists", has_compound,
          f"indexes: {[ix.get('name') for ix in idx]}")
    c.add("7. unique index on customerId exists", has_unique_id)

    # --- customer counts by segment ---
    agg = mongo_eval(
        f'db.getSiblingDB("{DB}").{COLL}.aggregate('
        f'[{{$group:{{_id:"$segment",n:{{$sum:1}}}}}}]).toArray()') or []
    by_seg = {r["_id"]: r["n"] for r in agg}
    present = all(by_seg.get(s, 0) > 0 for s in EXPECTED_SEGMENTS)
    unexpected = sorted(set(by_seg) - set(EXPECTED_SEGMENTS))
    summed = sum(by_seg.values())
    c.add("8. all five expected segments present", present,
          "; ".join(f"{s}={by_seg.get(s, 0)}" for s in EXPECTED_SEGMENTS))
    c.add("9. no unexpected segment values", not unexpected,
          f"unexpected: {unexpected}" if unexpected else "clean")
    c.add("10. segment counts sum to the collection total", summed == total,
          f"{summed} / {total}")

    # --- mongodb/validation.js gates green ---
    r = sh("docker", "exec", MONGO, "mongosh", URI, "--quiet", "--file",
           "/opt/finsight/mongodb/validation.js")
    c.add("11. mongodb/validation.js exits 0", r.returncode == 0,
          (r.stdout + r.stderr).strip().splitlines()[-1] if r.stdout or r.stderr else "")

    return c.report()


if __name__ == "__main__":
    sys.exit(main())
