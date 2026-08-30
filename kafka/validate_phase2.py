#!/usr/bin/env python3
"""
FinSight - Phase 2 end-to-end validation.

Runs the full acceptance checklist for the Kafka ingestion phase:

  1. Kafka is reachable
  2. the required topics exist          (txn-raw, txn-flagged, txn-churn)
  3. partition counts are correct       (3, 1, 1)
  4. produce N records into txn-raw     (default 100, from a fresh sample CSV)
  5. consume the records back
  6. validate JSON structure / fields
  7. confirm txn-raw actually received the records

Usage:
    python kafka/validate_phase2.py                 # 100 records, generates sample
    python kafka/validate_phase2.py --records 100 --file data/sample/transactions_sample.csv

Exit code 0 only if every step passes.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from config import CONFIG, setup_logging

log = setup_logging("validate_phase2")
KAFKA_DIR = Path(__file__).resolve().parent
PY = sys.executable

_PASS = "PASS"
_FAIL = "FAIL"


class Checklist:
    def __init__(self) -> None:
        self.results: list[tuple[str, bool, str]] = []

    def record(self, name: str, ok: bool, detail: str = "") -> bool:
        self.results.append((name, ok, detail))
        log.info("[%s] %s %s", _PASS if ok else _FAIL, name,
                 f"- {detail}" if detail else "")
        return ok

    def report(self) -> int:
        print()
        print("  FinSight Phase 2 - validation")
        print("  " + "=" * 58)
        for name, ok, detail in self.results:
            print(f"  [{_PASS if ok else _FAIL}] {name}")
            if detail:
                print(f"         {detail}")
        passed = sum(1 for _, ok, _ in self.results if ok)
        print("  " + "=" * 58)
        print(f"  {passed}/{len(self.results)} checks passed")
        print()
        return 0 if passed == len(self.results) else 1


def _run(script: str, *args: str) -> subprocess.CompletedProcess:
    cmd = [PY, str(KAFKA_DIR / script), *args]
    log.info("$ %s", " ".join(cmd))
    return subprocess.run(cmd, capture_output=True, text=True, cwd=KAFKA_DIR.parent)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--bootstrap-servers", default=CONFIG.bootstrap_servers)
    ap.add_argument("--records", type=int, default=100)
    ap.add_argument("--file", type=Path,
                    default=Path("data/sample/transactions_sample.csv"))
    ap.add_argument("--keep-sample", action="store_true",
                    help="do not regenerate the sample CSV if it already exists")
    args = ap.parse_args(argv)

    chk = Checklist()
    bs = args.bootstrap_servers
    admin = AdminClient({"bootstrap.servers": bs})

    # --- 1. Kafka reachable ---------------------------------------------------
    try:
        md = admin.list_topics(timeout=10)
        chk.record("1. Kafka is running", True,
                   f"{bs} - {len(md.brokers)} broker(s)")
    except Exception as exc:  # noqa: BLE001
        chk.record("1. Kafka is running", False, str(exc))
        return chk.report()

    # --- create topics (idempotent) ----------------------------------------
    cp = _run("create_topics.py", "--bootstrap-servers", bs)
    if cp.returncode != 0:
        log.error(cp.stdout + cp.stderr)

    # --- 2. required topics exist -----------------------------------------
    md = admin.list_topics(timeout=10)
    specs = CONFIG.topic_specs()
    present = {s.name: (s.name in md.topics) for s in specs}
    chk.record("2. Required topics exist", all(present.values()),
               ", ".join(f"{k}={'yes' if v else 'NO'}" for k, v in present.items()))

    # --- 3. partition counts correct -----------------------------------
    part_ok = True
    part_detail = []
    for s in specs:
        got = len(md.topics[s.name].partitions) if s.name in md.topics else 0
        part_detail.append(f"{s.name}={got}/{s.partitions}")
        part_ok &= got == s.partitions
    chk.record("3. Partition counts correct", part_ok, ", ".join(part_detail))

    # --- generate sample data --------------------------------------------
    if not args.file.exists() or not args.keep_sample:
        gp = _run("generate_sample_data.py", "--rows", str(max(args.records, 200)),
                  "--out", str(args.file))
        if gp.returncode != 0:
            chk.record("4. Produce N records", False,
                       "sample generation failed: " + gp.stderr.strip())
            return chk.report()
        log.info(gp.stdout.strip())

    # --- baseline offsets on txn-raw (for step 7) -----------------------
    before = _topic_message_count(bs, CONFIG.topic_raw)

    # --- 4. produce N records ------------------------------------------
    pp = _run("producer.py", "--file", str(args.file),
              "--limit", str(args.records), "--rate", "0",
              "--bootstrap-servers", bs)
    produced_ok = pp.returncode == 0
    log.info(pp.stdout.strip().splitlines()[-1] if pp.stdout.strip() else "")
    if not produced_ok:
        log.error((pp.stdout + pp.stderr).strip())
    chk.record("4. Produce N records", produced_ok,
               f"producer exit={pp.returncode}, target={args.records}")

    time.sleep(1.0)
    after = _topic_message_count(bs, CONFIG.topic_raw)
    delta = after - before

    # --- 5 + 6. consume and validate JSON structure -----------------------
    group = f"finsight-validate-{uuid.uuid4().hex[:8]}"
    cp2 = _run("consumer_test.py", "--expect", str(args.records),
               "--timeout", "40", "--group", group,
               "--bootstrap-servers", bs, "--no-sample")
    consume_ok = cp2.returncode == 0
    tail = cp2.stdout.strip().splitlines()
    summary = next((l for l in tail if "messages received" in l), "")
    result_line = next((l for l in tail if "RESULT" in l), "")
    chk.record("5. Consume the records", consume_ok, summary.strip())
    chk.record("6. Validate JSON structure", consume_ok, result_line.strip())

    # --- 7. confirm txn-raw received the records ------------------------
    chk.record("7. txn-raw received the records", delta >= args.records,
               f"topic message count {before} -> {after} (+{delta})")

    return chk.report()


def _topic_message_count(bootstrap_servers: str, topic: str) -> int:
    """Sum of (high watermark - low watermark) across all partitions."""
    c = Consumer({"bootstrap.servers": bootstrap_servers,
                  "group.id": f"finsight-count-{uuid.uuid4().hex[:6]}",
                  "enable.auto.commit": False})
    try:
        md = c.list_topics(topic, timeout=10)
        if topic not in md.topics or md.topics[topic].error is not None:
            return 0
        total = 0
        from confluent_kafka import TopicPartition

        for pid in md.topics[topic].partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, pid), timeout=10)
            total += max(hi - lo, 0)
        return total
    finally:
        c.close()


if __name__ == "__main__":
    sys.exit(main())
