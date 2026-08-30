#!/usr/bin/env python3
"""
FinSight - Phase 2: Kafka transaction producer.

Replays ``NovaCrest_Transactions.csv`` row by row into the ``txn-raw`` Kafka
topic as JSON messages, at a configurable rate (spec 6.2: target ~1,000 msg/s).

Examples
--------
    # 100 rows from a small test file
    python kafka/producer.py --file data/sample/transactions_sample.csv --limit 100

    # full replay at ~1000 msg/s
    python kafka/producer.py --file data/raw/NovaCrest_Transactions.csv

    # slow replay for a demo
    python kafka/producer.py --file data/raw/NovaCrest_Transactions.csv --rate 50

Message key = ``nameOrig`` so every transaction from one account lands on the
same partition - required by the per-customer stateful churn job in Phase 3.

Exit codes: 0 = all delivered, 1 = completed with delivery/parse failures,
2 = configuration / input error.
"""
from __future__ import annotations

import argparse
import csv
import json
import signal
import sys
import time
from pathlib import Path

from confluent_kafka import KafkaException, Producer

from config import CONFIG, setup_logging
from transaction_schema import SchemaError, row_to_record

log = setup_logging("producer")


class _Stats:
    def __init__(self) -> None:
        self.read = 0
        self.produced = 0
        self.delivered = 0
        self.failed = 0
        self.skipped = 0
        self.started = time.monotonic()

    def elapsed(self) -> float:
        return max(time.monotonic() - self.started, 1e-9)

    def rate(self) -> float:
        return self.delivered / self.elapsed()


class RateLimiter:
    """Simple pacing: keep the running average send rate near ``target``.

    Checked every ``check_every`` messages rather than per-message, which keeps
    overhead negligible at ~1000 msg/s while still smoothing bursts.
    """

    def __init__(self, target_per_sec: float, check_every: int = 100) -> None:
        self.target = max(target_per_sec, 0.0)
        self.check_every = max(check_every, 1)
        self._start = time.monotonic()
        self._count = 0

    def tick(self) -> None:
        if self.target <= 0:
            return
        self._count += 1
        if self._count % self.check_every:
            return
        expected = self._count / self.target
        actual = time.monotonic() - self._start
        if expected > actual:
            time.sleep(expected - actual)


class GracefulShutdown:
    """Flip a flag on SIGINT / SIGTERM so the main loop can stop cleanly."""

    def __init__(self) -> None:
        self.stop = False
        for signame in ("SIGINT", "SIGTERM", "SIGBREAK"):
            sig = getattr(signal, signame, None)
            if sig is None:
                continue
            try:
                signal.signal(sig, self._handler)
            except (ValueError, OSError, AttributeError):
                # e.g. not in main thread, or signal unsupported on this platform
                pass

    def _handler(self, signum, _frame) -> None:  # noqa: ANN001
        log.warning("received signal %s - finishing current batch and flushing...",
                    signal.Signals(signum).name)
        self.stop = True


def _error_cb(err) -> None:  # noqa: ANN001
    """Route librdkafka's background errors through our logger, not stderr."""
    log.warning("kafka client: %s", err)


def check_broker(bootstrap_servers: str, timeout: float = 10.0) -> bool:
    """Fail fast with a clear message if the broker is unreachable."""
    from confluent_kafka.admin import AdminClient

    try:
        md = AdminClient(
            {"bootstrap.servers": bootstrap_servers, "error_cb": _error_cb}
        ).list_topics(timeout=timeout)
        return len(md.brokers) > 0
    except KafkaException as exc:
        log.error("cannot reach Kafka at %s: %s", bootstrap_servers, exc)
        return False


def build_producer(bootstrap_servers: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "finsight-txn-producer",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 20,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 200000,
        "queue.buffering.max.kbytes": 262144,
        "message.send.max.retries": 5,
        "retry.backoff.ms": 200,
        "delivery.timeout.ms": 120000,
        "error_cb": _error_cb,
    }
    return Producer(conf)


def _delivery_report(stats: _Stats):
    def cb(err, msg):  # noqa: ANN001
        if err is not None:
            stats.failed += 1
            if stats.failed <= 10:
                log.error("delivery failed (partition=%s): %s", msg.partition(), err)
        else:
            stats.delivered += 1
    return cb


def produce_file(
    file_path: Path,
    topic: str,
    bootstrap_servers: str,
    rate: float,
    limit: int | None,
    report_every: int,
) -> int:
    if not file_path.is_file():
        log.error("input file not found: %s", file_path)
        return 2

    if not check_broker(bootstrap_servers):
        return 2

    stats = _Stats()
    shutdown = GracefulShutdown()
    limiter = RateLimiter(rate)
    producer = build_producer(bootstrap_servers)
    on_delivery = _delivery_report(stats)

    log.info(
        "producing %s -> topic %r @ %s | rate=%s msg/s | limit=%s",
        file_path, topic, bootstrap_servers,
        rate or "unlimited", limit if limit is not None else "all",
    )

    try:
        with file_path.open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            try:
                _check_header(reader.fieldnames)
            except SchemaError as exc:
                log.error("invalid input CSV: %s", exc)
                producer.flush(1)
                return 2

            for row in reader:
                if shutdown.stop:
                    break
                if limit is not None and stats.read >= limit:
                    break
                stats.read += 1

                try:
                    record = row_to_record(row, sequence=stats.read)
                except SchemaError as exc:
                    stats.skipped += 1
                    if stats.skipped <= 10:
                        log.warning("row %d skipped: %s", stats.read, exc)
                    continue

                _produce_one(producer, topic, record, on_delivery, stats)
                producer.poll(0)
                limiter.tick()

                if stats.read % report_every == 0:
                    log.info(
                        "progress: read=%d produced=%d delivered=%d failed=%d "
                        "skipped=%d | %.0f msg/s",
                        stats.read, stats.produced, stats.delivered,
                        stats.failed, stats.skipped, stats.rate(),
                    )

    except KafkaException as exc:
        log.error("kafka error - aborting: %s", exc)
        _flush(producer, stats)
        return 2
    except OSError as exc:
        log.error("error reading %s: %s", file_path, exc)
        _flush(producer, stats)
        return 2

    _flush(producer, stats)

    log.info(
        "DONE in %.1fs | read=%d produced=%d delivered=%d failed=%d skipped=%d "
        "| avg %.0f msg/s",
        stats.elapsed(), stats.read, stats.produced, stats.delivered,
        stats.failed, stats.skipped, stats.rate(),
    )
    if shutdown.stop:
        log.warning("stopped early on shutdown signal")

    return 0 if (stats.failed == 0 and stats.skipped == 0) else 1


def _check_header(fieldnames) -> None:  # noqa: ANN001
    from transaction_schema import CSV_COLUMNS

    if not fieldnames:
        raise SchemaError("CSV has no header row")
    missing = [c for c in CSV_COLUMNS if c not in fieldnames]
    if missing:
        raise SchemaError(
            f"CSV header missing required column(s): {', '.join(missing)}"
        )


def _produce_one(producer, topic, record, on_delivery, stats) -> None:  # noqa: ANN001
    payload = json.dumps(record, separators=(",", ":")).encode("utf-8")
    key = record["nameOrig"].encode("utf-8")
    while True:
        try:
            producer.produce(topic, value=payload, key=key, on_delivery=on_delivery)
            stats.produced += 1
            return
        except BufferError:
            # local queue full - let librdkafka drain, then retry this record
            producer.poll(0.5)


def _flush(producer, stats) -> None:  # noqa: ANN001
    log.info("flushing %d in-flight message(s)...", len(producer))
    remaining = producer.flush(timeout=30)
    if remaining:
        log.error("%d message(s) still not delivered after flush timeout", remaining)
        stats.failed += remaining


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--file", required=True, type=Path,
                    help="path to the transactions CSV")
    ap.add_argument("--topic", default=CONFIG.topic_raw,
                    help=f"target topic (default: {CONFIG.topic_raw})")
    ap.add_argument("--bootstrap-servers", default=CONFIG.bootstrap_servers,
                    help=f"Kafka bootstrap servers (default: {CONFIG.bootstrap_servers})")
    ap.add_argument("--rate", type=float, default=float(CONFIG.producer_target_rate),
                    help="target messages per second; 0 = as fast as possible "
                         f"(default: {CONFIG.producer_target_rate})")
    ap.add_argument("--limit", type=int, default=None,
                    help="stop after N rows (for testing)")
    ap.add_argument("--report-every", type=int, default=1000,
                    help="log a progress line every N rows (default: 1000)")
    ap.add_argument("--log-level", default=None,
                    help="override LOG_LEVEL (DEBUG/INFO/WARNING/ERROR)")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.log_level:
        setup_logging("producer", args.log_level)
    if args.limit is not None and args.limit <= 0:
        log.error("--limit must be a positive integer")
        return 2
    return produce_file(
        file_path=args.file,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        rate=args.rate,
        limit=args.limit,
        report_every=args.report_every,
    )


if __name__ == "__main__":
    sys.exit(main())
