#!/usr/bin/env python3
"""
FinSight - Phase 2: synthetic sample transaction generator.

The real ``NovaCrest_Transactions.csv`` (6.3M rows) is not committed to the
repo (it is git-ignored, see ``.gitignore``). This script produces a small,
schema-accurate CSV so the Kafka pipeline can be developed and tested against a
few hundred rows before running against the full dataset.

Schema matches ``Bigdata Data set file/NOVACR_1.TXT`` (DATASET 1 OF 6) exactly:
11 columns, PaySim-style semantics. The data is synthetic and only
*approximately* realistic - it is for pipeline testing, NOT for analytics.

Usage:
    python kafka/generate_sample_data.py                       # 200 rows
    python kafka/generate_sample_data.py --rows 1000 --seed 7
    python kafka/generate_sample_data.py --out data/sample/transactions_sample.csv
"""
from __future__ import annotations

import argparse
import csv
import random
import sys
from pathlib import Path

from transaction_schema import CSV_COLUMNS

TYPES = ["PAYMENT", "CASH_OUT", "CASH_IN", "TRANSFER", "DEBIT"]
TYPE_WEIGHTS = [0.34, 0.35, 0.22, 0.06, 0.03]  # roughly PaySim-like


def _account(prefix: str, rng: random.Random) -> str:
    return f"{prefix}{rng.randint(10**9, 10**10 - 1)}"


def _row(step: int, rng: random.Random) -> dict:
    ttype = rng.choices(TYPES, weights=TYPE_WEIGHTS, k=1)[0]
    amount = round(rng.expovariate(1 / 12000) + rng.random() * 500, 2)

    name_orig = _account("C", rng)
    old_org = round(rng.random() * 200000, 2)

    is_fraud = 0
    is_flagged = 0

    if ttype in ("TRANSFER", "CASH_OUT"):
        name_dest = _account("C", rng)
        old_dest = round(rng.random() * 50000, 2)
        # ~0.4% account-emptying fraud pattern on TRANSFER/CASH_OUT
        if rng.random() < 0.004:
            is_fraud = 1
            amount = round(rng.uniform(200_001, 900_000), 2)
            old_org = amount
            new_org = 0.0
            new_dest = 0.0
        else:
            new_org = max(round(old_org - amount, 2), 0.0)
            new_dest = round(old_dest + amount, 2)
        # legacy system flag: amount > 200k on TRANSFER (spec metadata col 11)
        if ttype == "TRANSFER" and amount > 200_000:
            is_flagged = 1
    elif ttype == "CASH_IN":
        name_dest = _account("C", rng)
        old_dest = round(rng.random() * 100000, 2)
        new_org = round(old_org + amount, 2)
        new_dest = max(round(old_dest - amount, 2), 0.0)
    else:  # PAYMENT / DEBIT -> merchant
        name_dest = _account("M", rng)
        old_dest = 0.0
        new_org = max(round(old_org - amount, 2), 0.0)
        new_dest = 0.0

    return {
        "step": step,
        "type": ttype,
        "amount": amount,
        "nameOrig": name_orig,
        "oldbalanceOrg": old_org,
        "newbalanceOrig": new_org,
        "nameDest": name_dest,
        "oldbalanceDest": old_dest,
        "newbalanceDest": new_dest,
        "isFraud": is_fraud,
        "isFlaggedFraud": is_flagged,
    }


def generate(rows: int, out: Path, seed: int) -> None:
    rng = random.Random(seed)
    out.parent.mkdir(parents=True, exist_ok=True)

    fraud = 0
    with out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CSV_COLUMNS))
        writer.writeheader()
        for i in range(rows):
            step = 1 + (i * 168) // max(rows, 1)  # spread across the 168-step week
            row = _row(step, rng)
            fraud += row["isFraud"]
            writer.writerow(row)

    print(f"wrote {rows} rows to {out}  ({fraud} synthetic fraud rows)")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--rows", type=int, default=200)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("data/sample/transactions_sample.csv"),
        help="output CSV path (default: data/sample/transactions_sample.csv)",
    )
    args = ap.parse_args(argv)
    if args.rows <= 0:
        print("--rows must be positive", file=sys.stderr)
        return 2
    generate(args.rows, args.out, args.seed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
