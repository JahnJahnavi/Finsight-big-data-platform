#!/usr/bin/env python3
"""
FinSight - Phase 13: txn-flagged -> Power BI bridge  (resolves ASSUMPTIONS G14).

Power BI cannot consume Kafka directly. This consumer tails the `txn-flagged`
topic (Phase 4 output) and maintains a **rolling CSV** that Power BI imports /
scheduled-refreshes:

    powerbi/exports/flagged_transactions.csv

One row per flagged transaction (deduplicated on txnId), newest-last, capped at
--max-rows (oldest dropped). Adds:
  * event_ts       - SIM_EPOCH + (step-1)h   (ASSUMPTIONS I11)
  * false_positive - 1 when the rule flagged it but isFraud = 0  (Page 1 FP rate)
  * bridged_at     - wall-clock the bridge wrote the row

    python powerbi/kafka_bridge/txn_flagged_bridge.py                 # tail forever
    python powerbi/kafka_bridge/txn_flagged_bridge.py --from-beginning --once
    python powerbi/kafka_bridge/txn_flagged_bridge.py --idle-timeout 10 --once

Not a Power BI artifact; it only prepares an import file. Ctrl-C = clean stop.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import signal
import sys
from pathlib import Path

try:
    from confluent_kafka import Consumer, KafkaException
except ModuleNotFoundError:
    sys.exit("confluent-kafka missing - `pip install -r requirements.txt`")

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "powerbi"))
from model_helpers import step_to_timestamp  # noqa: E402

try:
    from dotenv import load_dotenv

    load_dotenv(REPO / ".env", override=False)
except ModuleNotFoundError:
    pass

OUT = REPO / "powerbi" / "exports" / "flagged_transactions.csv"
COLUMNS = [
    "txnId", "step", "event_ts", "type", "amount",
    "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest",
    "isFraud", "isFlaggedFraud", "false_positive",
    "fraud_rule", "detected_at", "bridged_at",
]
_running = True


def _stop(*_):
    global _running
    _running = False


def unwrap(raw: bytes) -> dict | None:
    try:
        obj = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if isinstance(obj, dict) and "payload" in obj and "schema" in obj:
        obj = obj["payload"]
    return obj if isinstance(obj, dict) else None


def to_row(msg: dict) -> dict | None:
    txn_id = msg.get("txnId") or msg.get("txnID") or msg.get("id")
    if not txn_id or msg.get("step") is None:
        return None
    step = int(msg["step"])
    is_fraud = int(msg.get("isFraud") or 0)
    row = {c: msg.get(c) for c in COLUMNS if c in msg}
    row.update({
        "txnId": txn_id,
        "step": step,
        "event_ts": step_to_timestamp(step).isoformat(),
        "isFraud": is_fraud,
        "false_positive": 1 if is_fraud == 0 else 0,
        "bridged_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    })
    return row


def load_existing() -> "dict[str, dict]":
    if not OUT.exists():
        return {}
    with OUT.open(newline="", encoding="utf-8") as fh:
        return {r["txnId"]: r for r in csv.DictReader(fh)}


def write_all(rows: "dict[str, dict]", max_rows: int) -> None:
    ordered = list(rows.values())[-max_rows:]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT.with_suffix(".csv.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(ordered)
    tmp.replace(OUT)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap-servers",
                    default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:9092"))
    ap.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC_FLAGGED", "txn-flagged"))
    ap.add_argument("--group-id", default="finsight-powerbi-bridge")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--once", action="store_true",
                    help="stop after --idle-timeout seconds with no new message")
    ap.add_argument("--idle-timeout", type=float, default=15.0)
    ap.add_argument("--max-rows", type=int, default=50000)
    ap.add_argument("--flush-every", type=int, default=200)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([args.topic])

    rows = load_existing()
    print(f"[bridge] {args.topic} @ {args.bootstrap_servers} -> {OUT}  "
          f"({len(rows)} existing rows)")

    seen, added, idle = 0, 0, 0.0
    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1.0
                if args.once and idle >= args.idle_timeout:
                    break
                continue
            if msg.error():
                raise KafkaException(msg.error())
            idle = 0.0
            seen += 1
            payload = unwrap(msg.value())
            row = to_row(payload) if payload else None
            if row:
                is_new = row["txnId"] not in rows
                rows[row["txnId"]] = row
                added += is_new
            if seen % args.flush_every == 0:
                write_all(rows, args.max_rows)
                print(f"[bridge] consumed={seen} rows={len(rows)} (+{added} new)")
    finally:
        write_all(rows, args.max_rows)
        consumer.close()

    print(f"[bridge] done - consumed {seen}, wrote {len(rows)} rows "
          f"({added} new) to {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
