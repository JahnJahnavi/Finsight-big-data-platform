"""
FinSight - explicit Kafka message schema for the ``txn-raw`` topic.

The Phase 2/3 producer publishes each transaction as a Kafka Connect JSON
*schema envelope* by default:

    {"schema": {...}, "payload": {<the 13 transaction fields>}}

(or the bare payload when run with ``--raw``). This module defines the strict
Spark ``StructType`` for the payload and a helper that parses either form off a
Kafka ``value`` column.

Field list / types are the source of truth from
``Bigdata Data set file/NOVACR_1.TXT`` (11 CSV columns) plus the producer's
derived ``txnId`` and ``ingest_ts``.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# --- explicit payload schema (no inference) ---
TXN_SCHEMA = StructType(
    [
        StructField("step", IntegerType(), False),
        StructField("type", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("nameOrig", StringType(), False),
        StructField("oldbalanceOrg", DoubleType(), False),
        StructField("newbalanceOrig", DoubleType(), False),
        StructField("nameDest", StringType(), False),
        StructField("oldbalanceDest", DoubleType(), False),
        StructField("newbalanceDest", DoubleType(), False),
        StructField("isFraud", IntegerType(), False),
        StructField("isFlaggedFraud", IntegerType(), False),
        StructField("txnId", StringType(), True),
        StructField("ingest_ts", StringType(), True),
    ]
)

# Envelope = {"schema": <ignored>, "payload": <TXN_SCHEMA>}
_ENVELOPE_SCHEMA = StructType([StructField("payload", TXN_SCHEMA, True)])

TXN_COLUMNS = [f.name for f in TXN_SCHEMA.fields]


def parse_txn_value(value_col: Column) -> Column:
    """Parse a Kafka ``value`` (string) into a struct with the TXN_SCHEMA fields.

    Accepts both the Connect envelope and the bare payload. Returns a single
    struct column; rows that fail to parse yield a struct of nulls (the caller
    filters those out).
    """
    as_envelope = F.from_json(value_col, _ENVELOPE_SCHEMA).getField("payload")
    as_bare = F.from_json(value_col, TXN_SCHEMA)
    # prefer the envelope's payload; fall back to bare when payload is absent
    return F.when(as_envelope.isNotNull(), as_envelope).otherwise(as_bare)
