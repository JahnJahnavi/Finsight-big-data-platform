#!/usr/bin/env python3
"""
FinSight - Phase 13: Power BI data-model helpers (pure Python, unit-tested).

The simulation only carries a relative `step` (1 = one hour). Power BI's date
axes need real timestamps, so every fact is dated by mapping
`step -> SIM_EPOCH + (step - 1) hours` (ASSUMPTIONS I11). `build_date_dim()`
produces the `DimDate` table; `TXN_TYPES` is the `DimTransactionType` list.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

SIM_EPOCH = os.environ.get("SIM_EPOCH", "2023-01-01T00:00:00Z")

# spec: the five NovaCrest transaction types (DimTransactionType)
TXN_TYPES = ("PAYMENT", "TRANSFER", "CASH_IN", "CASH_OUT", "DEBIT")


def sim_epoch_dt(value: str | None = None) -> datetime:
    v = (value or SIM_EPOCH).replace("Z", "+00:00")
    dt = datetime.fromisoformat(v)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def step_to_timestamp(step: int, epoch: datetime | None = None) -> datetime:
    """step 1 -> SIM_EPOCH, step N -> SIM_EPOCH + (N-1) hours."""
    return (epoch or sim_epoch_dt()) + timedelta(hours=int(step) - 1)


def build_date_dim(max_step: int, epoch: datetime | None = None) -> list[dict]:
    """One row per step 1..max_step - the DimDate table (grain = step / hour)."""
    ep = epoch or sim_epoch_dt()
    rows = []
    for step in range(1, int(max_step) + 1):
        ts = step_to_timestamp(step, ep)
        iso_year, iso_week, _ = ts.isocalendar()
        week_start = (ts - timedelta(days=ts.weekday())).date()
        rows.append({
            "step": step,
            "event_ts": ts.isoformat(),
            "date": ts.date().isoformat(),
            "hour_of_day": ts.hour,
            "day_name": ts.strftime("%A"),
            "iso_week": f"{iso_year}-W{iso_week:02d}",
            "week_start": week_start.isoformat(),
        })
    return rows
