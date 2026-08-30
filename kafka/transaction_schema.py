"""
FinSight - transaction record schema for the Kafka ``txn-raw`` topic.

Source of truth for the field list: ``Bigdata Data set file/NOVACR_1.TXT``
(DATASET 1 OF 6 - NovaCrest_Transactions.csv, 11 columns).

The producer publishes each CSV row as a JSON object containing:

  * the 11 original columns, correctly typed
  * ``txnId``    - derived stable id (the metadata lists txnId as a derived
                   primary key); format ``TXN`` + 9-digit zero-padded sequence
  * ``ingest_ts``- UTC ISO-8601 timestamp set when the producer read the row

By default the payload is wrapped in a Kafka Connect JSON *schema envelope*
(``{"schema": ..., "payload": ...}``). This is required by the Phase 3 Kafka
Connect HDFS sink: its ``ParquetFormat`` and ``FieldPartitioner`` both need a
typed Connect ``Struct``, and there is no Schema Registry in the stack. Pass
``--raw`` to the producer to emit the bare payload instead.

No business logic (fraud / churn / enrichment) happens here - that belongs to
later phases. This module only parses, types and validates.
"""
from __future__ import annotations

from datetime import datetime, timezone

# The 11 columns exactly as they appear in NovaCrest_Transactions.csv
CSV_COLUMNS: tuple[str, ...] = (
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
)

_INT_FIELDS = ("step", "isFraud", "isFlaggedFraud")
_FLOAT_FIELDS = (
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
)
_STR_FIELDS = ("type", "nameOrig", "nameDest")

ALLOWED_TYPES = {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}

# Fields a consumer must find in every message for it to be considered valid.
REQUIRED_FIELDS: tuple[str, ...] = CSV_COLUMNS + ("txnId", "ingest_ts")

# --------------------------------------------------------------------------- #
# Kafka Connect schema envelope
# --------------------------------------------------------------------------- #
# Connect JSON schema types: int32, int64, float, double, boolean, string, bytes
_CONNECT_TYPE = {
    "step": "int32",
    "type": "string",
    "amount": "double",
    "nameOrig": "string",
    "oldbalanceOrg": "double",
    "newbalanceOrig": "double",
    "nameDest": "string",
    "oldbalanceDest": "double",
    "newbalanceDest": "double",
    "isFraud": "int32",
    "isFlaggedFraud": "int32",
    "txnId": "string",
    "ingest_ts": "string",
}

# Field order is fixed and stable so the connector produces a consistent Parquet
# schema across restarts.
_FIELD_ORDER: tuple[str, ...] = CSV_COLUMNS + ("txnId", "ingest_ts")

CONNECT_VALUE_SCHEMA: dict = {
    "type": "struct",
    "name": "finsight.transaction",
    "version": 1,
    "optional": False,
    "fields": [
        {"field": name, "type": _CONNECT_TYPE[name], "optional": False}
        for name in _FIELD_ORDER
    ],
}


def to_envelope(record: dict) -> dict:
    """Wrap a payload dict in the Connect ``{"schema": ..., "payload": ...}`` form."""
    return {"schema": CONNECT_VALUE_SCHEMA, "payload": record}


def unwrap(value: dict) -> dict:
    """Return the payload whether ``value`` is a bare record or an envelope."""
    if isinstance(value, dict) and "schema" in value and "payload" in value:
        return value["payload"]
    return value


class SchemaError(ValueError):
    """Raised when a CSV row cannot be coerced into a valid transaction."""


def _to_int(field: str, value: str) -> int:
    try:
        # tolerate values written as floats, e.g. "1.0"
        return int(float(value))
    except (TypeError, ValueError):
        raise SchemaError(f"{field}: {value!r} is not an integer")


def _to_float(field: str, value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise SchemaError(f"{field}: {value!r} is not a float")


def row_to_record(row: dict[str, str], sequence: int) -> dict:
    """Convert one ``csv.DictReader`` row into a typed, validated JSON-ready dict.

    ``sequence`` is a monotonically increasing counter used to derive ``txnId``.
    Raises :class:`SchemaError` if a required column is missing or mistyped.
    """
    missing = [c for c in CSV_COLUMNS if c not in row or row[c] is None]
    if missing:
        raise SchemaError(f"missing column(s): {', '.join(missing)}")

    record: dict = {}
    for field in _STR_FIELDS:
        record[field] = str(row[field]).strip()
    for field in _INT_FIELDS:
        record[field] = _to_int(field, row[field])
    for field in _FLOAT_FIELDS:
        record[field] = _to_float(field, row[field])

    # --- semantic checks straight from the metadata spec ---
    if record["type"] not in ALLOWED_TYPES:
        raise SchemaError(
            f"type: {record['type']!r} not in {sorted(ALLOWED_TYPES)}"
        )
    if record["amount"] < 0:
        raise SchemaError(f"amount: {record['amount']} is negative (spec: >= 0)")
    if record["isFraud"] not in (0, 1):
        raise SchemaError(f"isFraud: {record['isFraud']} not in (0, 1)")
    if record["isFlaggedFraud"] not in (0, 1):
        raise SchemaError(
            f"isFlaggedFraud: {record['isFlaggedFraud']} not in (0, 1)"
        )

    # --- derived fields ---
    record["txnId"] = f"TXN{sequence:09d}"
    record["ingest_ts"] = datetime.now(timezone.utc).isoformat()
    return record


def validate_record(record: dict) -> list[str]:
    """Return a list of problems with a decoded message. Empty list == valid.

    Used by ``consumer_test.py`` to check messages coming back off the topic.
    Accepts either a bare record or a Connect schema envelope.
    """
    record = unwrap(record)
    problems: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in record:
            problems.append(f"missing field: {field}")
    if problems:
        return problems

    for field in _INT_FIELDS:
        if not isinstance(record[field], int) or isinstance(record[field], bool):
            problems.append(f"{field} is not an int: {record[field]!r}")
    for field in _FLOAT_FIELDS:
        if not isinstance(record[field], (int, float)) or isinstance(
            record[field], bool
        ):
            problems.append(f"{field} is not numeric: {record[field]!r}")
    for field in _STR_FIELDS:
        if not isinstance(record[field], str) or record[field] == "":
            problems.append(f"{field} is not a non-empty string: {record[field]!r}")

    if record.get("type") not in ALLOWED_TYPES:
        problems.append(f"type not allowed: {record.get('type')!r}")
    if str(record.get("txnId", "")).startswith("TXN") is False:
        problems.append(f"txnId malformed: {record.get('txnId')!r}")

    return problems
