#!/usr/bin/env python3
"""
FinSight - Phase 3: register / manage the Kafka Connect HDFS sink connector.

Reads docker/kafka-connect/connectors/hdfs-sink-txn-raw.json and PUTs it to the
Kafka Connect REST API. A few values can be overridden from the CLI / .env so
the same config works for a 100-record test and a full replay.

Usage:
    python scripts/register_hdfs_sink.py                    # create / update
    python scripts/register_hdfs_sink.py --flush-size 100   # smaller files (testing)
    python scripts/register_hdfs_sink.py --status
    python scripts/register_hdfs_sink.py --restart
    python scripts/register_hdfs_sink.py --delete

Env (optional): CONNECT_URL (default http://localhost:8083),
                CONNECT_FLUSH_SIZE, CONNECT_ROTATE_INTERVAL_MS, CONNECT_TASKS_MAX
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_FILE = REPO_ROOT / "docker" / "kafka-connect" / "connectors" / "hdfs-sink-txn-raw.json"

try:
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env", override=False)
except ModuleNotFoundError:
    pass

CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083").rstrip("/")


def _req(method: str, path: str, body: dict | None = None) -> tuple[int, str]:
    url = f"{CONNECT_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()
    except urllib.error.URLError as exc:
        print(f"ERROR: cannot reach Kafka Connect at {CONNECT_URL}: {exc}", file=sys.stderr)
        sys.exit(2)


def load_config() -> tuple[str, dict]:
    spec = json.loads(CONFIG_FILE.read_text())
    name, cfg = spec["name"], spec["config"]

    env_map = {
        "CONNECT_FLUSH_SIZE": "flush.size",
        "CONNECT_ROTATE_INTERVAL_MS": "rotate.interval.ms",
        "CONNECT_ROTATE_SCHEDULE_MS": "rotate.schedule.interval.ms",
        "CONNECT_TASKS_MAX": "tasks.max",
    }
    for env_key, cfg_key in env_map.items():
        val = os.environ.get(env_key)
        if val:
            cfg[cfg_key] = str(val)
    return name, cfg


def register(overrides: dict) -> int:
    name, cfg = load_config()
    cfg.update(overrides)
    status, body = _req("PUT", f"/connectors/{name}/config", cfg)
    if status not in (200, 201):
        print(f"FAILED ({status}):\n{body}")
        return 1
    print(f"registered {name!r} (HTTP {status})")
    print(f"  output path    : hdfs://namenode:8020/{cfg['topics.dir']}/{cfg['topics']}/step=<N>/*.parquet")
    print(f"  partition.field: {cfg['partition.field.name']}")
    print(f"  format         : {cfg['format.class'].split('.')[-1]} ({cfg.get('parquet.codec','')})")
    print(f"  flush.size={cfg['flush.size']}  rotate.interval.ms={cfg['rotate.interval.ms']}  "
          f"rotate.schedule.interval.ms={cfg.get('rotate.schedule.interval.ms')}")
    _wait_running(name)
    return 0


def _wait_running(name: str, timeout: int = 40) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, body = _req("GET", f"/connectors/{name}/status")
        if status == 200:
            st = json.loads(body)
            states = [st["connector"]["state"]] + [t["state"] for t in st.get("tasks", [])]
            if st.get("tasks") and all(s == "RUNNING" for s in states):
                print(f"  state          : connector + {len(st['tasks'])} task(s) RUNNING")
                return
            failed = [t for t in st.get("tasks", []) if t["state"] == "FAILED"]
            if failed:
                print("  TASK FAILED:")
                print("   " + failed[0].get("trace", "")[:1500])
                return
        time.sleep(3)
    print("  (still starting - check --status)")


def show_status() -> int:
    name = json.loads(CONFIG_FILE.read_text())["name"]
    status, body = _req("GET", f"/connectors/{name}/status")
    if status == 404:
        print(f"{name!r} is not registered")
        return 1
    print(json.dumps(json.loads(body), indent=2))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--flush-size", type=int, help="override flush.size")
    ap.add_argument("--rotate-ms", type=int, help="override rotate.interval.ms")
    ap.add_argument("--rotate-schedule-ms", type=int,
                    help="override rotate.schedule.interval.ms (wall-clock commit)")
    ap.add_argument("--tasks", type=int, help="override tasks.max")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--restart", action="store_true")
    ap.add_argument("--delete", action="store_true")
    args = ap.parse_args()

    name = json.loads(CONFIG_FILE.read_text())["name"]

    if args.status:
        return show_status()
    if args.delete:
        status, _ = _req("DELETE", f"/connectors/{name}")
        print(f"delete {name!r}: HTTP {status}")
        return 0 if status in (204, 404) else 1
    if args.restart:
        _req("POST", f"/connectors/{name}/restart?includeTasks=true&onlyFailed=false")
        print(f"restart requested for {name!r}")
        return 0

    overrides: dict = {}
    if args.flush_size:
        overrides["flush.size"] = str(args.flush_size)
    if args.rotate_ms is not None:
        overrides["rotate.interval.ms"] = str(args.rotate_ms)
    if args.rotate_schedule_ms is not None:
        overrides["rotate.schedule.interval.ms"] = str(args.rotate_schedule_ms)
    if args.tasks:
        overrides["tasks.max"] = str(args.tasks)
    return register(overrides)


if __name__ == "__main__":
    sys.exit(main())
