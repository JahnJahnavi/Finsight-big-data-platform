#!/usr/bin/env python3
"""
FinSight - Phase 4 end-to-end validation: streaming fraud detection.

    txn-raw  ──►  spark/streaming/fraud_detection.py (--once)  ──►  txn-flagged
                                                              └──►  HDFS streaming_metrics

Publishes the five spec test transactions, runs the streaming job once, and
asserts that ONLY the two qualifying transactions are flagged and that the
per-micro-batch fraud-rate metric is written to HDFS.

    python scripts/validate_phase4.py

Exit 0 only if every check passes.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "kafka"))
from transaction_schema import to_envelope  # noqa: E402

from confluent_kafka import Consumer, Producer  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

BOOTSTRAP = "localhost:9092"
NN = "finsight-namenode"
CONNECTOR = "finsight-hdfs-sink-txn-raw"
CONNECT_URL = "http://localhost:8083"
INPUT_TOPIC = "txn-raw"
OUTPUT_TOPIC = "txn-flagged"
CHECKPOINT = "/finsight/checkpoints/fraud"
METRICS = "/finsight/processed/streaming_metrics"

# --- the five spec test cases -------------------------------------------- #
#  (txnId, type, amount, newbalanceDest, expected_flagged)
TEST_TXNS = [
    ("TEST-1-TRANSFER-QUALIFY", "TRANSFER", 250_000.0, 0.0,      True),
    ("TEST-2-CASHOUT-QUALIFY",  "CASH_OUT", 500_000.0, 0.0,      True),
    ("TEST-3-TRANSFER-LOW",     "TRANSFER", 150_000.0, 0.0,      False),
    ("TEST-4-PAYMENT-HIGH",     "PAYMENT",  300_000.0, 0.0,      False),
    ("TEST-5-CASHOUT-NONZERO",  "CASH_OUT", 400_000.0, 1_234.56, False),
]
EXPECTED_FLAGGED = {t[0] for t in TEST_TXNS if t[4]}


def sh(*cmd, timeout=300, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                          cwd=REPO, **kw)


def hdfs(*args, timeout=60):
    return sh("docker", "exec", NN, "hdfs", "dfs", *args, timeout=timeout)


def connect(method: str, path: str):
    req = urllib.request.Request(f"{CONNECT_URL}{path}", method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status
    except Exception:  # noqa: BLE001
        return 0


class Check:
    def __init__(self):
        self.rows = []

    def add(self, name, ok, detail=""):
        self.rows.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  - {detail}" if detail else ""))
        return ok

    def report(self):
        ok = sum(1 for _, o, _ in self.rows if o)
        print("\n  " + "=" * 60)
        print(f"  Phase 4 validation: {ok}/{len(self.rows)} checks passed")
        print("  " + "=" * 60)
        return 0 if ok == len(self.rows) else 1


def _record(txn_id, ttype, amount, newbal_dest):
    return {
        "step": 1, "type": ttype, "amount": amount,
        "nameOrig": f"C{abs(hash(txn_id)) % 10**10:010d}",
        "oldbalanceOrg": amount, "newbalanceOrig": 0.0,
        "nameDest": f"C{abs(hash(txn_id + 'd')) % 10**10:010d}",
        "oldbalanceDest": 0.0, "newbalanceDest": newbal_dest,
        "isFraud": 0, "isFlaggedFraud": 0,
        "txnId": txn_id, "ingest_ts": "2023-01-01T00:00:00+00:00",
    }


def reset_state(admin: AdminClient) -> None:
    print("      resetting: pause connector, recreate topics, clear checkpoint + metrics...")
    connect("PUT", f"/connectors/{CONNECTOR}/pause")
    for t in (INPUT_TOPIC, OUTPUT_TOPIC):
        try:
            fs = admin.delete_topics([t])
            for f in fs.values():
                f.result()
        except Exception:  # noqa: BLE001
            pass
    time.sleep(4)
    fs = admin.create_topics([
        NewTopic(INPUT_TOPIC, 3, 1),
        NewTopic(OUTPUT_TOPIC, 1, 1),
    ])
    for f in fs.values():
        try:
            f.result()
        except Exception:  # noqa: BLE001
            pass
    hdfs("-rm", "-r", "-f", "-skipTrash", CHECKPOINT, METRICS)


def produce_test_txns() -> None:
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for txn_id, ttype, amount, newbal, _ in TEST_TXNS:
        rec = _record(txn_id, ttype, amount, newbal)
        p.produce(INPUT_TOPIC, key=rec["nameOrig"].encode(),
                  value=json.dumps(to_envelope(rec)).encode())
    p.flush(20)


SPARK = "finsight-spark-master"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def run_job(master: str = "local[2]") -> subprocess.CompletedProcess:
    """spark-submit fraud_detection.py --once, directly via docker exec.

    (validate_phase4 does not go through run_fraud_detection.sh so it has no
    dependency on a POSIX shell being on PATH.)
    """
    return sh(
        "docker", "exec", SPARK, "/opt/spark/bin/spark-submit",
        "--master", master,
        "--packages", KAFKA_PKG,
        "--conf", "spark.jars.ivy=/opt/spark/.ivy2",
        "--conf", "spark.driver.memory=640m",
        "--conf", "spark.executor.memory=640m",
        "--conf", "spark.cores.max=2",
        "/opt/finsight/spark/streaming/fraud_detection.py",
        "--master", "",
        "--once", "--starting-offsets", "earliest", "--reset-checkpoint",
        timeout=600,
    )


def consume_flagged(expect_min: int, timeout_s: int = 40) -> list[dict]:
    c = Consumer({"bootstrap.servers": BOOTSTRAP,
                  "group.id": f"finsight-p4-validate-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([OUTPUT_TOPIC])
    out, deadline = [], time.time() + timeout_s
    while time.time() < deadline:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        try:
            out.append(json.loads(m.value()))
        except Exception:  # noqa: BLE001
            pass
        if len(out) >= expect_min:
            break
    c.close()
    return out


def read_metrics() -> list[dict]:
    r = hdfs("-cat", f"{METRICS}/*.json")
    if r.returncode != 0:
        return []
    rows = []
    for ln in r.stdout.splitlines():
        ln = ln.strip()
        if ln:
            try:
                rows.append(json.loads(ln))
            except json.JSONDecodeError:
                pass
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--keep-state", action="store_true",
                    help="do not pause the connector / recreate topics first")
    args = ap.parse_args()

    c = Check()
    print("\n  FinSight Phase 4 - streaming fraud detection\n  " + "-" * 58)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    try:
        admin.list_topics(timeout=10)
        c.add("1. Kafka reachable", True, BOOTSTRAP)
    except Exception as exc:  # noqa: BLE001
        return c.add("1. Kafka reachable", False, str(exc)) or c.report()

    running = sh("docker", "inspect", "-f", "{{.State.Running}}",
                 "finsight-spark-master").stdout.strip() == "true"
    c.add("2. Spark master container running", running)

    if not args.keep_state:
        reset_state(admin)

    produce_test_txns()
    c.add("3. Published 5 test transactions to txn-raw", True,
          "2 qualifying, 3 non-qualifying")

    print("      running fraud_detection.py --once (first run downloads the "
          "Kafka package, ~1-2 min)...")
    job = run_job()
    tail = "\n".join((job.stdout + job.stderr).strip().splitlines()[-4:])
    c.add("4. Streaming job completed", job.returncode == 0,
          f"exit={job.returncode}")
    if job.returncode != 0:
        print("      ---- job output tail ----")
        print("      " + tail.replace("\n", "\n      "))

    flagged = consume_flagged(expect_min=len(EXPECTED_FLAGGED))
    ids = {f.get("txnId") for f in flagged}
    c.add("5. Exactly the qualifying transactions were flagged",
          ids == EXPECTED_FLAGGED,
          f"flagged={sorted(ids)}  expected={sorted(EXPECTED_FLAGGED)}")

    c.add("6. Flagged records carry rule + detection time",
          all("fraud_rule" in f and "detected_at" in f for f in flagged) and bool(flagged),
          f"{len(flagged)} record(s)")

    metrics = read_metrics()
    agg_total = sum(m.get("total_count", 0) for m in metrics)
    agg_flagged = sum(m.get("flagged_count", 0) for m in metrics)
    c.add("7. Per-micro-batch metrics written to HDFS", len(metrics) >= 1,
          f"{len(metrics)} metric row(s) at {METRICS}")
    ok_counts = agg_total == len(TEST_TXNS) and agg_flagged == len(EXPECTED_FLAGGED)
    rate = (agg_flagged / agg_total * 100.0) if agg_total else -1
    c.add("8. Metrics: total=5, flagged=2, fraud_rate=40.0%", ok_counts and abs(rate - 40.0) < 1e-6,
          f"total={agg_total} flagged={agg_flagged} rate={rate:.4f}%")

    # checkpoint exists
    cp = hdfs("-test", "-d", f"{CHECKPOINT}/offsets")
    c.add("9. Checkpoint created at /finsight/checkpoints/fraud", cp.returncode == 0)

    if not args.keep_state:
        connect("PUT", f"/connectors/{CONNECTOR}/resume")

    return c.report()


if __name__ == "__main__":
    sys.exit(main())
