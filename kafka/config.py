"""
FinSight - Phase 2 shared configuration and logging.

Every Phase 2 script (create_topics, producer, consumer_test, validate_phase2)
imports its settings from here so that Kafka endpoints, topic names and
partition counts have a single source of truth: environment variables, loaded
from the repo-root ``.env`` file (see ``.env.example``).

Nothing secret lives in this module - only defaults that match ``.env.example``.
"""
from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - dependency guard
    print(
        "ERROR: python-dotenv is not installed.\n"
        "       pip install -r kafka/requirements.txt",
        file=sys.stderr,
    )
    raise

# --------------------------------------------------------------------------- #
# .env loading
# --------------------------------------------------------------------------- #
REPO_ROOT = Path(__file__).resolve().parents[1]
_ENV_FILE = REPO_ROOT / ".env"

# Do not override variables already set in the real environment (CI / Docker).
load_dotenv(dotenv_path=_ENV_FILE, override=False)


def _get(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _get_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"Environment variable {name}={raw!r} is not an integer")


# --------------------------------------------------------------------------- #
# Kafka settings
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication: int


@dataclass(frozen=True)
class KafkaConfig:
    # From the host, clients reach the broker on localhost:9092 (EXTERNAL
    # listener). Inside the Compose network they would use kafka:29092.
    bootstrap_servers: str = field(
        default_factory=lambda: _get("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:9092")
    )
    topic_raw: str = field(default_factory=lambda: _get("KAFKA_TOPIC_RAW", "txn-raw"))
    topic_flagged: str = field(
        default_factory=lambda: _get("KAFKA_TOPIC_FLAGGED", "txn-flagged")
    )
    topic_churn: str = field(
        default_factory=lambda: _get("KAFKA_TOPIC_CHURN", "txn-churn")
    )
    replication: int = field(
        default_factory=lambda: _get_int("KAFKA_TOPIC_REPLICATION", 1)
    )
    partitions_raw: int = field(
        default_factory=lambda: _get_int("KAFKA_TOPIC_RAW_PARTITIONS", 3)
    )
    partitions_flagged: int = field(
        default_factory=lambda: _get_int("KAFKA_TOPIC_FLAGGED_PARTITIONS", 1)
    )
    partitions_churn: int = field(
        default_factory=lambda: _get_int("KAFKA_TOPIC_CHURN_PARTITIONS", 1)
    )
    producer_target_rate: int = field(
        default_factory=lambda: _get_int("PRODUCER_TARGET_RATE", 1000)
    )

    def topic_specs(self) -> list[TopicSpec]:
        """The three topics FinSight requires (spec section 6.1 / 7.2)."""
        return [
            TopicSpec(self.topic_raw, self.partitions_raw, self.replication),
            TopicSpec(self.topic_flagged, self.partitions_flagged, self.replication),
            TopicSpec(self.topic_churn, self.partitions_churn, self.replication),
        ]


CONFIG = KafkaConfig()


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
def setup_logging(name: str, level: str | None = None) -> logging.Logger:
    """Configure root logging once and return a named logger."""
    resolved = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=getattr(logging, resolved, logging.INFO),
        format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    return logging.getLogger(name)
