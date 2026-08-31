"""
FinSight - Phase 13: Power BI model-helper unit tests (pure Python).

    pytest tests/unit/test_powerbi_model.py -v
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "powerbi"))
from model_helpers import (  # noqa: E402
    TXN_TYPES,
    build_date_dim,
    sim_epoch_dt,
    step_to_timestamp,
)


def test_sim_epoch_is_2023_01_01_utc():
    assert sim_epoch_dt() == datetime(2023, 1, 1, tzinfo=timezone.utc)


def test_step_to_timestamp_anchors_and_offsets():
    assert step_to_timestamp(1) == datetime(2023, 1, 1, 0, tzinfo=timezone.utc)
    assert step_to_timestamp(25) == datetime(2023, 1, 2, 0, tzinfo=timezone.utc)
    assert step_to_timestamp(168) == datetime(2023, 1, 7, 23, tzinfo=timezone.utc)


def test_transaction_types_are_the_five():
    assert set(TXN_TYPES) == {"PAYMENT", "TRANSFER", "CASH_IN", "CASH_OUT", "DEBIT"}


def test_date_dim_covers_every_step_once():
    dim = build_date_dim(168)
    assert len(dim) == 168
    assert [r["step"] for r in dim] == list(range(1, 169))


def test_date_dim_spans_exactly_seven_days():
    dim = build_date_dim(168)
    assert {r["date"] for r in dim} == {
        f"2023-01-0{d}" for d in range(1, 8)}
    assert dim[0]["day_name"] == "Sunday"       # 2023-01-01
    assert dim[0]["hour_of_day"] == 0
    assert dim[-1]["hour_of_day"] == 23


def test_date_dim_iso_week():
    dim = build_date_dim(168)
    # 2023-01-01 is ISO week 52 of 2022; 2023-01-02 starts 2023-W01
    assert dim[0]["iso_week"] == "2022-W52"
    assert dim[24]["iso_week"] == "2023-W01"
