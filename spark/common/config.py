"""
FinSight - configuration for the PySpark jobs.

Values come from environment variables (loaded from the repo-root ``.env`` when
present - see ``.env.example``). Defaults match ``.env.example`` so the jobs run
with no ``.env`` inside the Spark containers too.

Nothing secret lives here.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Optional .env loading (python-dotenv is not installed in the Spark image;
# that's fine - the container gets its config from `docker exec -e` / defaults).
try:  # pragma: no cover
    from dotenv import load_dotenv

    for _p in (Path(__file__).resolve().parents[2] / ".env",
               Path("/opt/finsight/.env")):
        if _p.is_file():
            load_dotenv(_p, override=False)
            break
except ModuleNotFoundError:  # pragma: no cover
    pass


def _get(name: str, default: str) -> str:
    val = os.environ.get(name)
    return default if val is None or val == "" else val


def _get_float(name: str, default: float) -> float:
    try:
        return float(_get(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class KafkaSettings:
    # Spark jobs run inside the Compose network -> use the internal listener.
    bootstrap_servers: str = field(
        default_factory=lambda: _get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    )
    topic_raw: str = field(default_factory=lambda: _get("KAFKA_TOPIC_RAW", "txn-raw"))
    topic_flagged: str = field(
        default_factory=lambda: _get("KAFKA_TOPIC_FLAGGED", "txn-flagged")
    )
    topic_churn: str = field(
        default_factory=lambda: _get("KAFKA_TOPIC_CHURN", "txn-churn")
    )
    starting_offsets: str = field(
        default_factory=lambda: _get("SPARK_KAFKA_STARTING_OFFSETS", "latest")
    )
    max_offsets_per_trigger: str = field(
        default_factory=lambda: _get("SPARK_KAFKA_MAX_OFFSETS_PER_TRIGGER", "100000")
    )


@dataclass(frozen=True)
class Paths:
    checkpoint_fraud: str = field(
        default_factory=lambda: _get("HDFS_CHECKPOINT_FRAUD", "/finsight/checkpoints/fraud")
    )
    checkpoint_churn: str = field(
        default_factory=lambda: _get("HDFS_CHECKPOINT_CHURN", "/finsight/checkpoints/churn")
    )
    streaming_metrics: str = field(
        default_factory=lambda: _get("HDFS_STREAMING_METRICS",
                                     "/finsight/processed/streaming_metrics")
    )
    hdfs_namenode: str = field(
        default_factory=lambda: _get("HDFS_NAMENODE_INTERNAL", "hdfs://namenode:8020")
    )


@dataclass(frozen=True)
class FraudRule:
    """Frozen from FinSight_Full_Specification_Complete.pdf section 7.1.

    A transaction is flagged when ALL are true:
      1. type in {TRANSFER, CASH_OUT}
      2. amount > 200000
      3. newbalanceDest == 0
    Do NOT change these values (see docs/ASSUMPTIONS.md).
    """
    amount_threshold: float = field(
        default_factory=lambda: _get_float("FRAUD_AMOUNT_THRESHOLD", 200000.0)
    )
    types: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            t.strip() for t in _get("FRAUD_TYPES", "TRANSFER,CASH_OUT").split(",") if t.strip()
        )
    )
    dest_balance: float = 0.0


@dataclass(frozen=True)
class StreamSettings:
    app_name_fraud: str = field(
        default_factory=lambda: _get("SPARK_APPNAME_FRAUD", "finsight-streaming-fraud")
    )
    master: str = field(default_factory=lambda: _get("SPARK_MASTER_URL_INTERNAL",
                                                     "spark://spark-master:7077"))
    trigger_interval: str = field(
        default_factory=lambda: _get("SPARK_STREAM_TRIGGER", "10 seconds")
    )
    log_level: str = field(default_factory=lambda: _get("LOG_LEVEL", "INFO"))
    kafka_pkg_version: str = field(
        default_factory=lambda: _get("SPARK_KAFKA_PKG_VERSION", "3.5.3")
    )


KAFKA = KafkaSettings()
PATHS = Paths()
FRAUD = FraudRule()
STREAM = StreamSettings()
