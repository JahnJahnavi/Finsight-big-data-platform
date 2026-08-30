"""FinSight - SparkSession builder shared by the streaming / batch jobs."""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession


def build_spark(app_name: str, master: str | None = None,
                extra: dict[str, str] | None = None) -> SparkSession:
    """Create (or get) a SparkSession.

    ``master`` is only applied when given - normally spark-submit sets it.
    A distinct ``app_name`` per job keeps them separable in the Spark UI
    (spec 7.4 R2).
    """
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    defaults = {
        "spark.sql.shuffle.partitions": "4",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.streaming.stopGracefullyOnShutdown": "true",
        # keep small metrics/output files from exploding the namespace
        "spark.sql.streaming.minBatchesToRetain": "20",
    }
    defaults.update(extra or {})
    for k, v in defaults.items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    return spark


def set_log_level(spark: SparkSession, level: str) -> logging.Logger:
    spark.sparkContext.setLogLevel(level.upper())
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    return logging.getLogger("finsight")
