#!/usr/bin/env python3
"""
FinSight - Phase 2: consumer test for the ``txn-raw`` topic.

Reads messages back off Kafka and verifies that:
  1. messages are actually received
  2. every value is valid JSON
  3. every record contains the expected fields with the expected types
     (see ``transaction_schema.REQUIRED_FIELDS``)

Examples
--------
    python kafka/consumer_test.py --expect 100
    python kafka/consumer_test.py --topic txn-raw --timeout 30 --from-beginning

Exit codes: 0 = received >= --expect and all messages valid, 1 = otherwise.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer, KafkaError

from config import CONFIG, setup_logging
from transaction_schema import REQUIRED_FIELDS, unwrap, validate_record

log = setup_logging("consumer_test")


def build_consumer(bootstrap_servers: str, group_id: str, from_beginning: bool) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )


def run(
    topic: str,
    bootstrap_servers: str,
    expect: int,
    timeout_s: float,
    group_id: str,
    from_beginning: bool,
    show_sample: bool,
) -> int:
    consumer = build_consumer(bootstrap_servers, group_id, from_beginning)
    consumer.subscribe([topic])
    log.info(
        "consuming topic %r @ %s | group=%s | expect>=%d | timeout=%ss",
        topic, bootstrap_servers, group_id, expect, timeout_s,
    )

    received = 0
    invalid_json = 0
    invalid_schema = 0
    partitions_seen: Counter[int] = Counter()
    first_problem: list[str] = []
    sample_record: dict | None = None

    hard_deadline = time.monotonic() + timeout_s
    empty_polls = 0
    max_empty_polls = 10  # ~10s of silence with nothing received -> give up early
    try:
        while time.monotonic() < hard_deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                empty_polls += 1
                if received == 0 and empty_polls >= max_empty_polls:
                    log.warning("no messages after %ds - stopping", max_empty_polls)
                    break
                continue
            empty_polls = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("consumer error: %s", msg.error())
                continue

            received += 1
            partitions_seen[msg.partition()] += 1

            try:
                record = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError) as exc:
                invalid_json += 1
                if not first_problem:
                    first_problem.append(f"invalid JSON: {exc}")
                continue

            problems = validate_record(record)
            if problems:
                invalid_schema += 1
                if not first_problem:
                    first_problem.append(f"schema: {problems}")
            elif sample_record is None:
                sample_record = unwrap(record)

            if received >= expect:
                break
    finally:
        consumer.close()

    valid = received - invalid_json - invalid_schema
    ok = received >= expect and invalid_json == 0 and invalid_schema == 0

    print()
    print("  txn-raw consumer test")
    print("  " + "-" * 50)
    print(f"  topic .................. {topic}")
    print(f"  messages received ...... {received}   (expected >= {expect})")
    print(f"  valid JSON ............. {received - invalid_json}")
    print(f"  valid schema .......... {valid}")
    print(f"  invalid JSON .......... {invalid_json}")
    print(f"  invalid schema ........ {invalid_schema}")
    print(f"  partitions seen ....... {dict(sorted(partitions_seen.items()))}")
    print(f"  required fields ....... {', '.join(REQUIRED_FIELDS)}")
    if first_problem:
        print(f"  first problem ......... {first_problem[0]}")
    if show_sample and sample_record is not None:
        print("  sample record:")
        print("    " + json.dumps(sample_record, indent=2).replace("\n", "\n    "))
    print("  " + "-" * 50)
    print(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    print()

    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--topic", default=CONFIG.topic_raw)
    ap.add_argument("--bootstrap-servers", default=CONFIG.bootstrap_servers)
    ap.add_argument("--expect", type=int, default=100,
                    help="minimum number of messages to consider the test a pass")
    ap.add_argument("--timeout", type=float, default=30.0,
                    help="overall seconds to wait for messages")
    ap.add_argument("--group", default=None,
                    help="consumer group id (default: a fresh random group)")
    ap.add_argument("--from-beginning", dest="from_beginning", action="store_true",
                    default=True, help="read from the start of the topic (default)")
    ap.add_argument("--from-latest", dest="from_beginning", action="store_false",
                    help="only read messages produced after the consumer starts")
    ap.add_argument("--no-sample", dest="show_sample", action="store_false",
                    default=True, help="do not print a sample record")
    args = ap.parse_args(argv)

    group_id = args.group or f"finsight-consumer-test-{uuid.uuid4().hex[:8]}"
    return run(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        expect=args.expect,
        timeout_s=args.timeout,
        group_id=group_id,
        from_beginning=args.from_beginning,
        show_sample=args.show_sample,
    )


if __name__ == "__main__":
    sys.exit(main())
