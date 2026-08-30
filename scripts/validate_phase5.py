#!/usr/bin/env python3
"""
FinSight - Phase 5 end-to-end validation: streaming churn detection.

    history CSV -> bootstrap_customer_history.py -> customer_baseline (HDFS)
    test txns   -> txn-raw -> churn_detection.py (--once) -> txn-churn + HDFS churn_alerts

Builds a baseline for six test customers, streams transactions crafted to trigger
each churn signal and combinations, runs the job once, and asserts that exactly
the multi-signal customers are flagged with the expected signal sets.

    python scripts/validate_phase5.py

Exit 0 only if every check passes.
"""
from __future__ import annotations

import argparse
import csv
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
SPARK = "finsight-spark-master"
CONNECTOR = "finsight-hdfs-sink-txn-raw"
CONNECT_URL = "http://localhost:8083"
INPUT_TOPIC = "txn-raw"
OUTPUT_TOPIC = "txn-churn"
CHECKPOINT = "/finsight/checkpoints/churn"
ALERTS = "/finsight/processed/churn_alerts"
BASELINE = "/finsight/processed/customer_baseline"
HISTORY_CSV = REPO / "data" / "sample" / "churn_history.csv"
HISTORY_IN_CONTAINER = "/opt/finsight/data/sample/churn_history.csv"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

CSV_COLS = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"]

# --- test customers -----------------------------------------------------------
# history:  (customerId, n_txns over steps 1..60, avg_amount)  -> baseline
HISTORY = {
    "CHURN-S12":   (30, 40_000.0),   # hist_freq 6/12
    "CHURN-S34":   (6,  10_000.0),   # hist_freq 1.2/12
    "CHURN-S123":  (40, 100_000.0),  # hist_freq 8/12
    "NOFLAG-S1":   (25, 10_000.0),   # hist_freq 5/12
    "NOFLAG-S3":   (6,  10_000.0),   # hist_freq 1.2/12
    "NOFLAG-NONE": (10, 10_000.0),   # hist_freq 2/12
}

# streaming test txns:  (customerId, [(step, type, amount, newbalanceOrig), ...])
STREAM = {
    "CHURN-S12":   [(10, "PAYMENT", 2_000.0, 9_000.0)],                       # S1 + S2
    "CHURN-S34":   [(8, "CASH_OUT", 9_500.0, 400.0),
                    (9, "CASH_OUT", 9_400.0, 100.0),
                    (10, "CASH_OUT", 9_600.0, 50.0)],                          # S3 + S4
    "CHURN-S123":  [(12, "CASH_OUT", 500.0, 9_000.0)],                         # S1 + S2 + S3
    "NOFLAG-S1":   [(10, "TRANSFER", 10_000.0, 9_000.0)],                      # S1 only
    "NOFLAG-S3":   [(8, "CASH_OUT", 10_000.0, 6_000.0),
                    (9, "CASH_OUT", 10_000.0, 7_000.0)],                       # S3 only
    "NOFLAG-NONE": [(5, "PAYMENT", 10_000.0, 9_000.0),
                    (6, "PAYMENT", 10_000.0, 9_100.0),
                    (7, "PAYMENT", 10_000.0, 9_200.0)],                        # none
}
EXPECTED = {
    "CHURN-S12":  {"S1_LOW_FREQUENCY", "S2_AMOUNT_DROP"},
    "CHURN-S34":  {"S3_EXCLUSIVE_CASHOUT", "S4_CONSECUTIVE_LOW_BALANCE"},
    "CHURN-S123": {"S1_LOW_FREQUENCY", "S2_AMOUNT_DROP", "S3_EXCLUSIVE_CASHOUT"},
}


def sh(*cmd, timeout=600):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=REPO)


def hdfs(*a, timeout=60):
    return sh("docker", "exec", NN, "hdfs", "dfs", *a, timeout=timeout)


def connect(method, path):
    try:
        with urllib.request.urlopen(
                urllib.request.Request(f"{CONNECT_URL}{path}", method=method), timeout=15) as r:
            return r.status
    except Exception:  # noqa: BLE001
        return 0


class Check:
    def __init__(self): self.rows = []
    def add(self, name, ok, detail=""):
        self.rows.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  - {detail}" if detail else ""))
        return ok
    def report(self):
        ok = sum(1 for _, o, _ in self.rows if o)
        print("\n  " + "=" * 62)
        print(f"  Phase 5 validation: {ok}/{len(self.rows)} checks passed")
        print("  " + "=" * 62)
        return 0 if ok == len(self.rows) else 1


def write_history_csv() -> None:
    HISTORY_CSV.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLS)
        w.writeheader()
        for cid, (n, avg) in HISTORY.items():
            for i in range(n):
                step = 1 + round(i * 59 / max(n - 1, 1))
                w.writerow({"step": step, "type": "PAYMENT", "amount": f"{avg:.2f}",
                            "nameOrig": cid, "oldbalanceOrg": "100000.0",
                            "newbalanceOrig": "90000.0", "nameDest": "M0000000001",
                            "oldbalanceDest": "0.0", "newbalanceDest": "0.0",
                            "isFraud": "0", "isFlaggedFraud": "0"})


def produce_stream() -> None:
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for cid, txns in STREAM.items():
        for i, (step, ttype, amount, nbo) in enumerate(txns):
            rec = {"step": step, "type": ttype, "amount": amount, "nameOrig": cid,
                   "oldbalanceOrg": amount + nbo, "newbalanceOrig": nbo,
                   "nameDest": f"C{i:010d}", "oldbalanceDest": 0.0,
                   "newbalanceDest": 0.0, "isFraud": 0, "isFlaggedFraud": 0,
                   "txnId": f"{cid}-{i}", "ingest_ts": "2023-01-01T00:00:00+00:00"}
            p.produce(INPUT_TOPIC, key=cid.encode(),
                      value=json.dumps(to_envelope(rec)).encode())
    p.flush(20)


def reset(admin: AdminClient) -> None:
    print("      resetting: pause connector, recreate topics, clear churn HDFS state...")
    connect("PUT", f"/connectors/{CONNECTOR}/pause")
    for t in (INPUT_TOPIC, OUTPUT_TOPIC):
        try:
            for f in admin.delete_topics([t]).values():
                f.result()
        except Exception:  # noqa: BLE001
            pass
    time.sleep(4)
    for f in admin.create_topics([NewTopic(INPUT_TOPIC, 3, 1),
                                  NewTopic(OUTPUT_TOPIC, 1, 1)]).values():
        try:
            f.result()
        except Exception:  # noqa: BLE001
            pass
    hdfs("-rm", "-r", "-f", "-skipTrash", CHECKPOINT, ALERTS, BASELINE)


def run_bootstrap() -> subprocess.CompletedProcess:
    return sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit", "--master", "local[2]",
             "--conf", "spark.jars.ivy=/opt/spark/.ivy2",
             "/opt/finsight/spark/streaming/bootstrap_customer_history.py",
             "--master", "", "--from", "csv", "--csv", HISTORY_IN_CONTAINER,
             timeout=300)


def run_job() -> subprocess.CompletedProcess:
    return sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit", "--master", "local[2]",
             "--packages", KAFKA_PKG,
             "--py-files", "/opt/finsight/spark/streaming/churn_rule.py",
             "--conf", "spark.jars.ivy=/opt/spark/.ivy2",
             "--conf", "spark.sql.execution.arrow.pyspark.enabled=true",
             "--conf", "spark.driver.memory=700m", "--conf", "spark.executor.memory=700m",
             "--conf", "spark.cores.max=2",
             "/opt/finsight/spark/streaming/churn_detection.py",
             "--master", "", "--once", "--starting-offsets", "earliest",
             "--reset-checkpoint", timeout=600)


def consume_churn(timeout_s=45) -> dict:
    c = Consumer({"bootstrap.servers": BOOTSTRAP,
                  "group.id": f"finsight-p5-validate-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([OUTPUT_TOPIC])
    latest: dict = {}
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        try:
            a = json.loads(m.value())
            latest.setdefault(a["customerId"], set()).update(a.get("signals", []))
        except Exception:  # noqa: BLE001
            pass
    c.close()
    return latest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--keep-state", action="store_true")
    args = ap.parse_args()

    c = Check()
    print("\n  FinSight Phase 5 - streaming churn detection\n  " + "-" * 60)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        admin.list_topics(timeout=10)
        c.add("1. Kafka reachable", True, BOOTSTRAP)
    except Exception as exc:  # noqa: BLE001
        return c.add("1. Kafka reachable", False, str(exc)) or c.report()

    if not args.keep_state:
        reset(admin)

    write_history_csv()
    bs = run_bootstrap()
    bs_ok = bs.returncode == 0 and hdfs("-test", "-d", BASELINE).returncode == 0
    c.add("2. Customer baseline built", bs_ok,
          (bs.stdout + bs.stderr).strip().splitlines()[-1][:110] if not bs_ok else
          f"{len(HISTORY)} customers -> {BASELINE}")

    produce_stream()
    n_txn = sum(len(v) for v in STREAM.values())
    c.add("3. Streamed churn test transactions", True,
          f"{n_txn} txns for {len(STREAM)} customers")

    print("      running churn_detection.py --once...")
    job = run_job()
    tail = "\n".join((job.stdout + job.stderr).strip().splitlines()[-5:])
    if not c.add("4. Streaming job completed", job.returncode == 0, f"exit={job.returncode}"):
        print("      ---- job tail ----\n      " + tail.replace("\n", "\n      "))

    got = consume_churn()
    flagged = set(got)
    c.add("5. Exactly the multi-signal customers flagged", flagged == set(EXPECTED),
          f"flagged={sorted(flagged)}  expected={sorted(EXPECTED)}")

    signals_ok = all(got.get(cid, set()) >= sig for cid, sig in EXPECTED.items())
    c.add("6. Each flagged customer has the expected signal(s)", signals_ok,
          "; ".join(f"{cid}:{sorted(got.get(cid, []))}" for cid in EXPECTED))

    c.add("7. Non-qualifying customers NOT flagged",
          all(cid not in flagged for cid in ("NOFLAG-S1", "NOFLAG-S3", "NOFLAG-NONE")),
          f"noflag seen: {[x for x in flagged if x.startswith('NOFLAG')]}")

    # HDFS Parquet alerts
    parquet_ok = _spark_count_alerts()
    c.add("8. churn_alerts Parquet written to HDFS", parquet_ok >= len(EXPECTED),
          f"{parquet_ok} alert row(s) at {ALERTS}")

    cp = hdfs("-test", "-d", f"{CHECKPOINT}/offsets")
    c.add("9. Checkpoint created at /finsight/checkpoints/churn", cp.returncode == 0)

    if not args.keep_state:
        connect("PUT", f"/connectors/{CONNECTOR}/resume")
    return c.report()


def _spark_count_alerts() -> int:
    script = (
        "from pyspark.sql import SparkSession\n"
        "s=SparkSession.builder.getOrCreate()\n"
        "try:\n"
        f"    df=s.read.parquet('hdfs://namenode:8020{ALERTS}')\n"
        "    print('ALERT_ROWS', df.count())\n"
        "    df.select('customerId','signals','window_start_step','window_end_step').show(20, False)\n"
        "except Exception as e:\n"
        "    print('ALERT_ROWS 0', e)\n"
    )
    (REPO / "scripts" / "_p5_read.py").write_text(script)
    sh("docker", "cp", str(REPO / "scripts" / "_p5_read.py"), f"{SPARK}:/tmp/_p5_read.py")
    r = sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit", "--master", "local[1]",
           "/tmp/_p5_read.py", timeout=180)
    (REPO / "scripts" / "_p5_read.py").unlink(missing_ok=True)
    for ln in (r.stdout + r.stderr).splitlines():
        if ln.startswith("ALERT_ROWS"):
            try:
                return int(ln.split()[1])
            except (IndexError, ValueError):
                return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
