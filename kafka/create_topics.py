#!/usr/bin/env python3
"""
FinSight - Phase 2: create the three Kafka topics required by the platform.

    txn-raw      3 partitions   (spec 6.1 - parallel Spark + HDFS-sink consumers)
    txn-flagged  1 partition    (spec 6.1 - low fraud-output volume)
    txn-churn    1 partition    (spec 7.2 R2)

Idempotent: topics that already exist are left untouched (and a warning is
logged if their partition count differs from the spec).

Usage:
    python kafka/create_topics.py                 # create the 3 topics
    python kafka/create_topics.py --describe      # only print current state
    python kafka/create_topics.py --bootstrap-servers localhost:9092
"""
from __future__ import annotations

import argparse
import sys

from confluent_kafka.admin import AdminClient, NewTopic

from config import CONFIG, setup_logging

log = setup_logging("create_topics")


def _admin(bootstrap_servers: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def describe(admin: AdminClient) -> dict[str, int]:
    """Return {topic_name: partition_count} for topics that exist."""
    md = admin.list_topics(timeout=10)
    return {
        name: len(t.partitions)
        for name, t in md.topics.items()
        if not name.startswith("_")
    }


def create_topics(bootstrap_servers: str, dry_run: bool = False) -> int:
    admin = _admin(bootstrap_servers)
    existing = describe(admin)
    log.info("connected to %s", bootstrap_servers)
    log.info("existing non-internal topics: %s", existing or "(none)")

    to_create: list[NewTopic] = []
    for spec in CONFIG.topic_specs():
        if spec.name in existing:
            if existing[spec.name] != spec.partitions:
                log.warning(
                    "topic %r exists with %d partitions, spec wants %d "
                    "(partition count cannot be reduced; leaving as-is)",
                    spec.name,
                    existing[spec.name],
                    spec.partitions,
                )
            else:
                log.info("topic %r already exists (%d partitions) - ok",
                         spec.name, spec.partitions)
            continue
        to_create.append(
            NewTopic(
                spec.name,
                num_partitions=spec.partitions,
                replication_factor=spec.replication,
            )
        )

    if not to_create:
        log.info("nothing to create - all topics present")
        return 0

    if dry_run:
        for nt in to_create:
            log.info("[dry-run] would create %r partitions=%d rf=%d",
                     nt.topic, nt.num_partitions, nt.replication_factor)
        return 0

    futures = admin.create_topics(to_create, request_timeout=30)
    failures = 0
    for name, fut in futures.items():
        try:
            fut.result()
            log.info("created topic %r", name)
        except Exception as exc:  # noqa: BLE001 - report every topic
            if "already exists" in str(exc).lower():
                log.info("topic %r already exists (race) - ok", name)
            else:
                log.error("failed to create topic %r: %s", name, exc)
                failures += 1
    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap-servers", default=CONFIG.bootstrap_servers)
    ap.add_argument("--describe", action="store_true",
                    help="only print current topic/partition state")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.describe:
        state = describe(_admin(args.bootstrap_servers))
        wanted = {s.name for s in CONFIG.topic_specs()}
        print(f"{'topic':<16} {'partitions':>10}   status")
        print("-" * 42)
        for spec in CONFIG.topic_specs():
            got = state.get(spec.name)
            status = (
                "MISSING" if got is None
                else "OK" if got == spec.partitions
                else f"PARTITIONS={got} (want {spec.partitions})"
            )
            print(f"{spec.name:<16} {str(got or '-'):>10}   {status}")
        for name, parts in sorted(state.items()):
            if name not in wanted:
                print(f"{name:<16} {parts:>10}   (other)")
        return 0

    try:
        return create_topics(args.bootstrap_servers, dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001
        log.error("topic creation failed: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
