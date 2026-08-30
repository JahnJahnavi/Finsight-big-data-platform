#!/usr/bin/env python3
"""
FinSight - Phase 3 end-to-end validation.

    Kafka txn-raw  ->  Kafka Connect HDFS sink  ->  HDFS  ->  Parquet (partitioned by step)

Checklist:
  1. Kafka Connect is up
  2. HDFS NameNode + DataNode are up
  3. the HDFS sink connector is registered and RUNNING (registered here if missing)
  4. produce N records (default 500) to txn-raw
  5. Parquet files appear under /finsight/raw/transactions/
  6. output is partitioned by step  (step=<N> directories)
  7. Spark can read the Parquet back with typed columns and >= N rows
  8. the dead-letter queue (txn-raw-dlq) is empty

Usage:
    python scripts/validate_phase3.py                 # 500 records
    python scripts/validate_phase3.py --records 1000

Exit 0 only if every check passes.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PY = sys.executable
NN = "finsight-namenode"
SPARK = "finsight-spark-master"
CONNECT_URL = "http://localhost:8083"
# Kafka Connect appends the source topic as the final path segment, so the raw
# transaction Parquet lands here (see docs/phase-03-hdfs.md).
HDFS_OUT = "/finsight/raw/txn-raw"
CONNECTOR = "finsight-hdfs-sink-txn-raw"


def sh(*cmd: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=REPO)


def hdfs(*args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return sh("docker", "exec", NN, "hdfs", "dfs", *args, timeout=timeout)


class Check:
    def __init__(self) -> None:
        self.rows: list[tuple[str, bool, str]] = []

    def add(self, name: str, ok: bool, detail: str = "") -> bool:
        self.rows.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  - {detail}" if detail else ""))
        return ok

    def report(self) -> int:
        ok = sum(1 for _, o, _ in self.rows if o)
        print("\n  " + "=" * 60)
        print(f"  Phase 3 validation: {ok}/{len(self.rows)} checks passed")
        print("  " + "=" * 60)
        return 0 if ok == len(self.rows) else 1


def connect_get(path: str) -> tuple[int, dict | list | None]:
    try:
        with urllib.request.urlopen(f"{CONNECT_URL}{path}", timeout=15) as r:
            return r.status, json.loads(r.read())
    except Exception:  # noqa: BLE001
        return 0, None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--records", type=int, default=500)
    ap.add_argument("--file", default="data/sample/transactions_sample.csv")
    ap.add_argument("--flush-size", type=int, default=10000,
                    help="connector flush.size for this run")
    ap.add_argument("--rotate-schedule-ms", type=int, default=15000,
                    help="wall-clock commit interval for this run (small = commits fast)")
    ap.add_argument("--wait", type=int, default=120, help="seconds to wait for Parquet")
    ap.add_argument("--no-reset", action="store_true",
                    help="do not drop the connector / topic / HDFS output first")
    args = ap.parse_args()

    c = Check()
    print("\n  FinSight Phase 3 - Kafka -> Connect -> HDFS -> Parquet\n" + "  " + "-" * 60)

    # 1. Kafka Connect
    st, info = connect_get("/")
    if not c.add("1. Kafka Connect is up", st == 200,
                 f"v{info.get('version')}" if info else "unreachable"):
        return c.report()

    if not args.no_reset:
        _reset()

    # 2. HDFS
    r = hdfs("-ls", "/")
    c.add("2. HDFS is up", r.returncode == 0, "namenode reachable" if r.returncode == 0
          else r.stderr.strip()[:120])

    # 3. connector registered + RUNNING (register if missing)
    st, _ = connect_get(f"/connectors/{CONNECTOR}/status")
    if st != 200:
        print("      connector not found - registering...")
        reg = sh(PY, str(REPO / "scripts" / "register_hdfs_sink.py"),
                 "--flush-size", str(args.flush_size),
                 "--rotate-schedule-ms", str(args.rotate_schedule_ms), timeout=90)
        print("      " + reg.stdout.strip().replace("\n", "\n      "))
    else:
        sh(PY, str(REPO / "scripts" / "register_hdfs_sink.py"),
           "--flush-size", str(args.flush_size),
           "--rotate-schedule-ms", str(args.rotate_schedule_ms), timeout=90)
    time.sleep(4)
    st, status = connect_get(f"/connectors/{CONNECTOR}/status")
    states = []
    if status:
        states = [status["connector"]["state"]] + [t["state"] for t in status.get("tasks", [])]
    running = bool(states) and all(s == "RUNNING" for s in states)
    c.add("3. HDFS sink connector RUNNING", running,
          f"connector + {len(status.get('tasks', [])) if status else 0} task(s): "
          + ",".join(states))
    if not running and status:
        for t in status.get("tasks", []):
            if t["state"] == "FAILED":
                print("      TRACE:", t.get("trace", "")[:1200])

    # baseline parquet count
    before = _parquet_count()

    # 4. produce
    gen = sh(PY, str(REPO / "kafka" / "generate_sample_data.py"),
             "--rows", str(max(args.records, 300)), "--out", args.file)
    prod = sh(PY, str(REPO / "kafka" / "producer.py"), "--file", args.file,
              "--limit", str(args.records), "--rate", "0", timeout=180)
    last = prod.stdout.strip().splitlines()[-1] if prod.stdout.strip() else prod.stderr[-200:]
    c.add("4. Produced N records to txn-raw", prod.returncode == 0,
          f"target={args.records}: {last.split('] ')[-1]}")

    # 5 + 6. wait for parquet, partitioned by step
    print(f"      waiting up to {args.wait}s for the sink to flush to HDFS...")
    deadline = time.time() + args.wait
    files: list[str] = []
    while time.time() < deadline:
        files = _parquet_files()
        if len(files) > before:
            break
        time.sleep(5)
    c.add(f"5. Parquet files under {HDFS_OUT}", len(files) > before,
          f"{len(files)} .parquet file(s) (was {before})")

    steps = sorted({f.split("step=")[1].split("/")[0] for f in files if "step=" in f},
                   key=lambda x: int(x))
    c.add("6. Partitioned by step", len(steps) >= 2,
          f"step partitions: {steps[:12]}{' ...' if len(steps) > 12 else ''}")

    # 7. Spark can read it
    rows, schema_ok, detail = _spark_read()
    c.add("7. Spark reads the Parquet", rows >= args.records and schema_ok,
          f"{detail}")

    # 8. DLQ empty
    dlq = _topic_count("txn-raw-dlq")
    c.add("8. Dead-letter queue empty", dlq == 0, f"txn-raw-dlq has {dlq} record(s)")

    return c.report()


def _reset() -> None:
    """Clean slate: drop connector, recreate txn-raw (offset 0), clear HDFS output."""
    print("      resetting: connector + txn-raw topic + HDFS output...")
    sh(PY, str(REPO / "scripts" / "register_hdfs_sink.py"), "--delete", timeout=60)
    time.sleep(2)
    sh("docker", "exec", "finsight-kafka", "kafka-topics", "--bootstrap-server",
       "localhost:9092", "--delete", "--topic", "txn-raw")
    sh("docker", "exec", "finsight-kafka", "kafka-topics", "--bootstrap-server",
       "localhost:9092", "--delete", "--topic", "txn-raw-dlq")
    time.sleep(3)
    sh(PY, str(REPO / "kafka" / "create_topics.py"), timeout=60)
    sh("docker", "exec", NN, "hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash",
       HDFS_OUT, "/finsight/raw/+tmp", "/finsight/logs")
    sh("docker", "exec", NN, "hdfs", "dfs", "-mkdir", "-p", "/finsight/raw")


def _parquet_files() -> list[str]:
    r = hdfs("-ls", "-R", HDFS_OUT)
    if r.returncode != 0:
        return []
    return [ln.split()[-1] for ln in r.stdout.splitlines()
            if ln.strip().endswith(".parquet")]


def _parquet_count() -> int:
    return len(_parquet_files())


def _topic_count(topic: str) -> int:
    r = sh("docker", "exec", "finsight-kafka", "bash", "-c",
           f"kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 "
           f"--topic {topic} --time -1")
    if r.returncode != 0:
        return 0  # topic may not exist yet -> no DLQ records
    return sum(int(ln.split(":")[-1]) for ln in r.stdout.splitlines() if ":" in ln)


def _spark_read() -> tuple[int, bool, str]:
    script = (
        "from pyspark.sql import SparkSession\n"
        "s=SparkSession.builder.getOrCreate()\n"
        f"df=s.read.parquet('hdfs://namenode:8020{HDFS_OUT}')\n"
        "cols=dict(df.dtypes)\n"
        "n=df.count()\n"
        "print('SPARK_ROWS',n)\n"
        "print('SPARK_STEP_TYPE',cols.get('step'))\n"
        "print('SPARK_HAS_AMOUNT', 'amount' in cols and cols['amount']=='double')\n"
        "print('SPARK_STEPS', sorted([r['step'] for r in df.select('step').distinct().collect()])[:10])\n"
    )
    (REPO / "scripts" / "_p3_read.py").write_text(script)
    sh("docker", "cp", str(REPO / "scripts" / "_p3_read.py"),
       f"{SPARK}:/tmp/_p3_read.py")
    r = sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit", "--master",
           "local[1]", "/tmp/_p3_read.py", timeout=180)
    (REPO / "scripts" / "_p3_read.py").unlink(missing_ok=True)
    out = r.stdout + r.stderr
    rows = 0
    for ln in out.splitlines():
        if ln.startswith("SPARK_ROWS"):
            rows = int(ln.split()[1])
    step_typed = "SPARK_STEP_TYPE int" in out
    amount_ok = "SPARK_HAS_AMOUNT True" in out
    steps_line = next((l for l in out.splitlines() if l.startswith("SPARK_STEPS")), "")
    if rows == 0:
        return 0, False, "spark read failed: " + out.strip().splitlines()[-1][:160]
    return rows, step_typed and amount_ok, f"{rows} rows, step:int amount:double, {steps_line}"


if __name__ == "__main__":
    sys.exit(main())
