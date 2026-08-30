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


def _get_int(name: str, default: int) -> int:
    try:
        return int(float(_get(name, str(default))))
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
    churn_alerts: str = field(
        default_factory=lambda: _get("HDFS_CHURN_ALERTS", "/finsight/processed/churn_alerts")
    )
    customer_baseline: str = field(
        default_factory=lambda: _get("HDFS_CUSTOMER_BASELINE",
                                     "/finsight/processed/customer_baseline")
    )
    raw_txn: str = field(
        default_factory=lambda: _get("HDFS_RAW_TXN", "/finsight/raw/txn-raw")
    )
    risk_scores: str = field(
        default_factory=lambda: _get("HDFS_RISK_SCORES", "/finsight/processed/risk_scores")
    )
    daily_summary: str = field(
        default_factory=lambda: _get("HDFS_DAILY_SUMMARY", "/finsight/processed/daily_summary")
    )
    exports: str = field(
        default_factory=lambda: _get("HDFS_EXPORTS", "/finsight/exports")
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
class ChurnRule:
    """Frozen from FinSight_Full_Specification_Complete.pdf section 7.2.

    A customer is flagged when >= 2 of these are observed within a 24-step
    (24-hour) sliding window. Do NOT change these values - see
    docs/ASSUMPTIONS.md and spark/streaming/churn_rule.py.
    """
    # step 1 == this instant; event_ts = SIM_EPOCH + (step-1) hours (ASSUMPTIONS I11)
    sim_epoch: str = field(default_factory=lambda: _get("SIM_EPOCH", "2023-01-01T00:00:00Z"))
    window_steps: int = field(default_factory=lambda: _get_int("CHURN_WINDOW_STEPS", 24))
    slide_steps: int = field(default_factory=lambda: _get_int("CHURN_SLIDE_STEPS", 12))
    min_signals: int = field(default_factory=lambda: _get_int("CHURN_MIN_SIGNALS", 2))

    # signal 1: freq < 1 per 12 steps AND historical avg > 3 per 12 steps
    freq_low_per_12: float = field(
        default_factory=lambda: _get_float("CHURN_FREQ_LOW_PER_12", 1.0))
    freq_hist_per_12: float = field(
        default_factory=lambda: _get_float("CHURN_FREQ_HIST_PER_12", 3.0))
    # signal 2: window avg amount < this fraction of all-time avg
    amount_drop_fraction: float = field(
        default_factory=lambda: _get_float("CHURN_AMOUNT_DROP_FRACTION", 0.20))
    # signal 4: newbalanceOrig below this for >= N consecutive txns
    balance_low_threshold: float = field(
        default_factory=lambda: _get_float("CHURN_BALANCE_LOW_THRESHOLD", 500.0))
    balance_low_consecutive: int = field(
        default_factory=lambda: _get_int("CHURN_BALANCE_LOW_CONSECUTIVE", 2))


@dataclass(frozen=True)
class RiskScoring:
    """Spec section 7.3 - Spark Core batch risk scoring.

    Composite risk score from four factors (spec names them but does NOT give
    weights - see docs/ASSUMPTIONS.md G6). Each raw factor is min-max normalised
    to 0-1 across all customers, then combined as a weighted sum.
    """
    sim_epoch: str = field(default_factory=lambda: _get("SIM_EPOCH", "2023-01-01T00:00:00Z"))
    app_name: str = field(default_factory=lambda: _get("SPARK_APPNAME_RISK",
                                                       "finsight-batch-risk"))
    # weights (default: equal 0.25 - ASSUMPTIONS G6, needs sign-off)
    w_frequency: float = field(default_factory=lambda: _get_float("RISK_W_FREQUENCY", 0.25))
    w_avg_transfer: float = field(
        default_factory=lambda: _get_float("RISK_W_AVG_TRANSFER", 0.25))
    w_cashout_prop: float = field(
        default_factory=lambda: _get_float("RISK_W_CASHOUT_PROP", 0.25))
    w_unique_dest: float = field(
        default_factory=lambda: _get_float("RISK_W_UNIQUE_DEST", 0.25))
    # tiers (spec 7.3 R1)
    tier_low_max: float = field(
        default_factory=lambda: _get_float("RISK_TIER_LOW_MAX", 0.25))
    tier_medium_max: float = field(
        default_factory=lambda: _get_float("RISK_TIER_MEDIUM_MAX", 0.60))

    def weights(self) -> tuple[float, float, float, float]:
        return (self.w_frequency, self.w_avg_transfer,
                self.w_cashout_prop, self.w_unique_dest)


@dataclass(frozen=True)
class StreamSettings:
    app_name_fraud: str = field(
        default_factory=lambda: _get("SPARK_APPNAME_FRAUD", "finsight-streaming-fraud")
    )
    app_name_churn: str = field(
        default_factory=lambda: _get("SPARK_APPNAME_CHURN", "finsight-streaming-churn")
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
CHURN = ChurnRule()
RISK = RiskScoring()
STREAM = StreamSettings()
