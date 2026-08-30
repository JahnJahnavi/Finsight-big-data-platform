"""
FinSight - the fraud detection rule (spec section 7.1).

    A transaction is flagged when ALL THREE are true:
      1. type is TRANSFER or CASH_OUT
      2. amount > 200000
      3. newbalanceDest == 0            (the classic account-emptying pattern)

DO NOT change these rules (see docs/ASSUMPTIONS.md).

Two equivalent expressions of the same rule are defined here and MUST stay in
sync:
  * ``is_fraud(txn: dict) -> bool``      - pure Python, used by the unit tests
  * ``fraud_condition_sql() -> str``     - a Spark SQL predicate, used by the job
"""
from __future__ import annotations

# Frozen constants (also overridable via .env: FRAUD_TYPES, FRAUD_AMOUNT_THRESHOLD)
FRAUD_TYPES: tuple[str, ...] = ("TRANSFER", "CASH_OUT")
AMOUNT_THRESHOLD: float = 200_000.0
DEST_BALANCE_ZERO: float = 0.0

FRAUD_RULE_ID = "SPEC-7.1: type in (TRANSFER,CASH_OUT) & amount>200000 & newbalanceDest==0"


def is_fraud(
    txn: dict,
    *,
    types: tuple[str, ...] = FRAUD_TYPES,
    amount_threshold: float = AMOUNT_THRESHOLD,
    dest_balance: float = DEST_BALANCE_ZERO,
) -> bool:
    """Return True iff the transaction dict satisfies all three fraud conditions."""
    try:
        return (
            str(txn["type"]).upper() in types
            and float(txn["amount"]) > amount_threshold
            and float(txn["newbalanceDest"]) == dest_balance
        )
    except (KeyError, TypeError, ValueError):
        return False


def fraud_condition_sql(
    *,
    types: tuple[str, ...] = FRAUD_TYPES,
    amount_threshold: float = AMOUNT_THRESHOLD,
    dest_balance: float = DEST_BALANCE_ZERO,
) -> str:
    """The same rule as a Spark SQL predicate string (for DataFrame.filter/expr)."""
    type_list = ", ".join(f"'{t}'" for t in types)
    return (
        f"type IN ({type_list}) "
        f"AND amount > {amount_threshold} "
        f"AND newbalanceDest = {dest_balance}"
    )
