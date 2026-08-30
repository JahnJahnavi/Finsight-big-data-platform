#!/usr/bin/env python3
"""
FinSight - infrastructure health check.

Verifies that every service in the docker-compose stack is running AND
functionally reachable (not just "container up"). Pure standard library so it
runs without installing requirements.

Usage:
    python scripts/healthcheck.py               # one pass
    python scripts/healthcheck.py --wait 180    # retry for up to 180s
    python scripts/healthcheck.py --json        # machine-readable output

Exit code 0 = every non-skipped service healthy, 1 = at least one failure.
A service whose container is not running (e.g. its Compose profile is not
active) is reported as SKIPPED and does not fail the run.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
COMPOSE_PROJECT = "finsight"

GREEN, RED, YELLOW, DIM, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


@dataclass
class Result:
    name: str
    ok: bool | None                       # True ok, False fail, None skipped
    detail: str = ""
    checks: list[tuple[str, bool, str]] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Low-level probes
# --------------------------------------------------------------------------- #
def _docker() -> str:
    exe = shutil.which("docker")
    if not exe:
        print(f"{RED}docker CLI not found on PATH{RESET}")
        sys.exit(2)
    return exe


def container_state(name: str) -> tuple[bool, str]:
    """(running, health/status string). running=False if container missing."""
    try:
        out = subprocess.run(
            [_docker(), "inspect", "-f",
             "{{.State.Running}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}",
             name],
            capture_output=True, text=True, timeout=15,
        )
    except subprocess.TimeoutExpired:
        return False, "docker-inspect-timeout"
    if out.returncode != 0:
        return False, "absent"
    running, _, health = out.stdout.strip().partition("|")
    return running == "true", health or "unknown"


def docker_exec(container: str, cmd: list[str], timeout: int = 25,
                maxlen: int = 200) -> tuple[bool, str]:
    try:
        out = subprocess.run(
            [_docker(), "exec", container, *cmd],
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, "timeout"
    txt = (out.stdout + out.stderr).strip().replace("\n", " ")
    return out.returncode == 0, txt[:maxlen]


def tcp_ok(host: str, port: int, timeout: float = 4.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, f"tcp {host}:{port} open"
    except OSError as exc:
        return False, f"tcp {host}:{port} - {exc}"


def http_ok(url: str, timeout: float = 6.0, accept=(200, 301, 302, 401, 403)) -> tuple[bool, str]:
    req = urllib.request.Request(url, headers={"User-Agent": "finsight-healthcheck"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = resp.status
            body = resp.read(400).decode("utf-8", "replace")
            return code in accept, f"HTTP {code} {url} {DIM}{body[:80].strip()}{RESET}"
    except urllib.error.HTTPError as exc:
        return exc.code in accept, f"HTTP {exc.code} {url}"
    except Exception as exc:  # noqa: BLE001
        return False, f"{url} - {exc}"


def http_json(url: str, timeout: float = 6.0) -> tuple[bool, dict, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", "replace"))
            return True, data, f"HTTP {resp.status} {url}"
    except Exception as exc:  # noqa: BLE001
        return False, {}, f"{url} - {exc}"


# --------------------------------------------------------------------------- #
# Per-service checks
# --------------------------------------------------------------------------- #
def check(name: str, container: str, probes) -> Result:
    running, health = container_state(container)
    if not running:
        return Result(name, None, f"container '{container}' not running ({health})")

    res = Result(name, True, f"container up (health={health})")
    if health == "unhealthy":
        res.ok = False
    for label, fn in probes:
        try:
            ok, detail = fn()
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, f"probe error: {exc}"
        res.checks.append((label, ok, detail))
        if not ok:
            res.ok = False
    return res


def build_checks() -> list[Result]:
    results: list[Result] = []

    # --- Kafka ---
    results.append(check("kafka", "finsight-kafka", [
        ("broker-api-versions", lambda: docker_exec(
            "finsight-kafka",
            ["kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"])),
        ("host listener :9092", lambda: tcp_ok("localhost", 9092)),
    ]))

    # --- Kafka UI ---
    results.append(check("kafka-ui", "finsight-kafka-ui", [
        ("web :8085", lambda: http_ok("http://localhost:8085/")),
    ]))

    # --- Kafka Connect ---
    def connect_probe():
        ok, data, msg = http_json("http://localhost:8083/")
        if ok:
            return True, f"Connect v{data.get('version', '?')} kafka={data.get('kafka_cluster_id', '?')}"
        return False, msg

    def connect_plugins():
        ok, data, msg = http_json("http://localhost:8083/connector-plugins")
        if not ok:
            return False, msg
        has_hdfs = any("hdfs" in p.get("class", "").lower() for p in data)
        return has_hdfs, ("hdfs3 sink plugin present" if has_hdfs
                          else f"{len(data)} plugins, HDFS sink MISSING")

    results.append(check("kafka-connect", "finsight-kafka-connect", [
        ("REST :8083", connect_probe),
        ("hdfs3 sink plugin", connect_plugins),
    ]))

    # --- HDFS NameNode (+ functional cluster report) ---
    def hdfs_report():
        ok, txt = docker_exec("finsight-namenode",
                              ["hdfs", "dfsadmin", "-report"], timeout=40, maxlen=4000)
        if not ok:
            return False, txt
        m = re.search(r"Live datanodes\s*\((\d+)\)", txt)
        live = int(m.group(1)) if m else 0
        return live >= 1, f"live datanodes = {live}"

    results.append(check("namenode", "finsight-namenode", [
        ("web UI :9870", lambda: http_ok("http://localhost:9870/")),
        ("cluster report", hdfs_report),
    ]))

    # --- HDFS DataNode ---
    results.append(check("datanode", "finsight-datanode", [
        ("web UI :9864", lambda: http_ok("http://localhost:9864/")),
    ]))

    # --- Hive metastore backend (Postgres) ---
    results.append(check("hive-postgres", "finsight-hive-postgres", [
        ("pg_isready", lambda: docker_exec("finsight-hive-postgres",
                                           ["pg_isready", "-U", "hive", "-d", "metastore"])),
    ]))

    # --- Hive Metastore ---
    results.append(check("hive-metastore", "finsight-hive-metastore", [
        ("thrift :9083", lambda: tcp_ok("localhost", 9083)),
    ]))

    # --- HiveServer2 ---
    results.append(check("hiveserver2", "finsight-hiveserver2", [
        ("JDBC :10000", lambda: tcp_ok("localhost", 10000)),
        ("web UI :10002", lambda: http_ok("http://localhost:10002/")),
    ]))

    # --- Spark master (+ worker registration) ---
    def spark_master_probe():
        ok, data, msg = http_json("http://localhost:8080/json/")
        if not ok:
            return False, msg
        workers = data.get("aliveworkers", data.get("workers", "?"))
        return True, f"status={data.get('status', '?')} aliveWorkers={workers}"

    def spark_worker_registered():
        ok, data, _ = http_json("http://localhost:8080/json/")
        if not ok:
            return False, "master API unreachable"
        alive = data.get("aliveworkers", 0)
        return alive >= 1, f"master reports {alive} alive worker(s)"

    results.append(check("spark-master", "finsight-spark-master", [
        ("master REST :8080", spark_master_probe),
        ("worker registered", spark_worker_registered),
    ]))

    # --- Spark worker ---
    results.append(check("spark-worker", "finsight-spark-worker", [
        ("worker UI :8081", lambda: http_ok("http://localhost:8081/")),
    ]))

    # --- MongoDB ---
    results.append(check("mongodb", "finsight-mongodb", [
        ("ping", lambda: docker_exec("finsight-mongodb",
                                     ["mongosh", "--quiet", "--eval",
                                      "db.adminCommand('ping').ok"])),
    ]))

    # --- Neo4j ---
    results.append(check("neo4j", "finsight-neo4j", [
        ("HTTP :7474", lambda: http_ok("http://localhost:7474/")),
        ("Bolt :7687", lambda: tcp_ok("localhost", 7687)),
    ]))

    return results


# --------------------------------------------------------------------------- #
# Runner / reporting
# --------------------------------------------------------------------------- #
def render(results: list[Result]) -> None:
    print()
    print("  FinSight infrastructure health")
    print("  " + "-" * 60)
    for r in results:
        if r.ok is None:
            tag = f"{YELLOW}SKIP{RESET}"
        elif r.ok:
            tag = f"{GREEN} OK {RESET}"
        else:
            tag = f"{RED}FAIL{RESET}"
        print(f"  [{tag}] {r.name:<16} {DIM}{r.detail}{RESET}")
        for label, ok, detail in r.checks:
            mark = f"{GREEN}v{RESET}" if ok else f"{RED}x{RESET}"
            print(f"         {mark} {label:<22} {detail}")
    print("  " + "-" * 60)


def main() -> int:
    ap = argparse.ArgumentParser(description="FinSight infra health check")
    ap.add_argument("--wait", type=int, default=0,
                    help="retry until healthy for up to N seconds")
    ap.add_argument("--interval", type=int, default=10, help="seconds between retries")
    ap.add_argument("--json", action="store_true", help="emit JSON summary")
    args = ap.parse_args()

    deadline = time.time() + args.wait
    while True:
        results = build_checks()
        failed = [r for r in results if r.ok is False]
        if not failed or time.time() >= deadline:
            break
        print(f"{YELLOW}[wait] {len(failed)} service(s) not ready, retrying in {args.interval}s...{RESET}")
        time.sleep(args.interval)

    if args.json:
        print(json.dumps(
            {r.name: {"ok": r.ok, "detail": r.detail,
                      "checks": [{"label": c[0], "ok": c[1], "detail": c[2]} for c in r.checks]}
             for r in results},
            indent=2))
    else:
        render(results)
        ok = sum(1 for r in results if r.ok is True)
        skip = sum(1 for r in results if r.ok is None)
        fail = sum(1 for r in results if r.ok is False)
        print(f"  {GREEN}{ok} healthy{RESET}, {YELLOW}{skip} skipped{RESET}, "
              f"{RED}{fail} failed{RESET}\n")

    return 1 if any(r.ok is False for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
