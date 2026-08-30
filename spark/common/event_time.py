"""
FinSight - derive an event-time timestamp from the relative ``step`` column.

The dataset has no real timestamp, only ``step`` (1 step = 1 hour). Per
docs/ASSUMPTIONS.md I11:  ``event_ts = SIM_EPOCH + (step - 1) hours``.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def event_ts_expr(sim_epoch_iso: str, step_col: str = "step") -> Column:
    """A timestamp Column: SIM_EPOCH + (step - 1) hours.

    ``step_col`` is the name of the integer step column in the DataFrame.
    """
    epoch = sim_epoch_iso.replace("Z", "+00:00")
    return F.expr(
        f"timestampadd(HOUR, cast({step_col} as int) - 1, to_timestamp('{epoch}'))"
    )
