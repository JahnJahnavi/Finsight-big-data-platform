# Power BI — DAX Measures & Calculated Columns

Copy-paste source: [`powerbi/measures/measures.dax`](../../powerbi/measures/measures.dax).
Put all measures on a dedicated `_Measures` table (Enter Data → one dummy
column, then hide it). Calculated columns live on their own tables — see
[data-model.md § Calculated columns](data-model.md#calculated-columns).

## Measures by folder

### Fraud  → Page 1

| Measure | DAX | Format |
|---|---|---|
| `Flagged Transactions` | `DISTINCTCOUNT(FlaggedTransactions[txnId])` | whole # |
| `Flagged Transaction Value` | `SUM(FlaggedTransactions[amount])` | currency |
| `Confirmed Fraud (flagged)` | `CALCULATE([Flagged Transactions], FlaggedTransactions[isFraud]=1)` | whole # |
| `False Positives` | `CALCULATE([Flagged Transactions], FlaggedTransactions[false_positive]=1)` | whole # |
| `False Positive Rate %` | `DIVIDE([False Positives],[Flagged Transactions])*100` | 0.0 "%" |
| `Transactions Processed` | `SUM(StreamingMetrics[total_count])` | whole # |
| `Flagged (from metrics)` | `SUM(StreamingMetrics[flagged_count])` | whole # |
| `Fraud Rate %` | `DIVIDE([Flagged (from metrics)],[Transactions Processed])*100` | 0.00 "%" |

> **`Fraud Rate %` fallback** when `StreamingMetrics` is not loaded:
> `DIVIDE([Flagged Transactions], SUM(TransactionSummary[transaction_count]))*100`.

### Transactions  → Page 3

| Measure | DAX |
|---|---|
| `Total Transaction Volume` | `SUM(TransactionSummary[total_volume])` |
| `Transaction Count` | `SUM(TransactionSummary[transaction_count])` |
| `Summary Fraud Count` | `SUM(TransactionSummary[fraud_count])` |
| `Summary Fraud Rate %` | `DIVIDE([Summary Fraud Count],[Transaction Count])*100` |
| `Avg Transaction Amount` | `DIVIDE([Total Transaction Volume],[Transaction Count])` |

*Weekly fraud-rate trend* = `[Summary Fraud Rate %]` with `DimDate[week_start]`
on the axis.

### Customer  → Page 2

| Measure | DAX |
|---|---|
| `Customers` | `DISTINCTCOUNT(DimCustomer[customerId])` |
| `Avg Risk Score` | `AVERAGE(DimCustomer[risk_score])` |
| `Avg Churn Probability` | `AVERAGE(DimCustomer[churn_probability])` |
| `High Churn Customers` | `CALCULATE([Customers], DimCustomer[churn_band]="High")` |
| `Churn Rate %` | `DIVIDE([High Churn Customers],[Customers])*100` |
| `Avg CLV Score` | `AVERAGE(DimCustomer[clv_score])` |
| `High Value Customers` | `CALCULATE([Customers], DimCustomer[clv_classification]="High Value")` |
| `Customers Holding Product` | `DISTINCTCOUNT(CustomerProducts[customerId])` |
| `Product Holdings` | `COUNTROWS(CustomerProducts)` |
| `Avg Products per Customer` | `DIVIDE(COUNTROWS(CustomerProducts),[Customers])` |

### Compliance / Dormancy  → Page 3

| Measure | DAX |
|---|---|
| `High Risk Types` | `CALCULATE(DISTINCTCOUNT(ComplianceSummary[transaction_type]), ComplianceSummary[risk_classification]="High")` |
| `Dormant Accounts` | `CALCULATE(COUNTROWS(DormancyReport), DormancyReport[dormancy_severity]="Dormant")` |
| `Severely Dormant Accounts` | `CALCULATE(COUNTROWS(DormancyReport), DormancyReport[dormancy_severity]="Severely Dormant")` |
| `Total Dormant Accounts` | `COUNTROWS(DormancyReport)` |

### Accounts  → Page 3 (Top 20 flagged)

| Measure | DAX |
|---|---|
| `Account Flagged Count` | `CALCULATE([Flagged Transactions], ALLEXCEPT(DimCustomer, DimCustomer[customerId]))` |
| `Account Flagged Value` | `CALCULATE([Flagged Transaction Value], ALLEXCEPT(DimCustomer, DimCustomer[customerId]))` |

Top-20 = table visual (`DimCustomer[customerId]`, `[Account Flagged Count]`,
`[Account Flagged Value]`) + visual-level **Top N = 20 by `[Account Flagged Value]`**.

## Calculated columns

Repeated here for convenience — full list in
[data-model.md](data-model.md#calculated-columns).

```DAX
DimCustomer[churn_band] =
    SWITCH(TRUE(), DimCustomer[churn_probability]>=0.66,"High",
                   DimCustomer[churn_probability]>=0.33,"Medium","Low")

DimCustomer[risk_band] =
    SWITCH(TRUE(), DimCustomer[risk_score]>=0.60,"High",
                   DimCustomer[risk_score]>=0.25,"Medium","Low")

DimCustomer[clv_tier] =
    IF(ISBLANK(DimCustomer[clv_classification]),"Unscored",DimCustomer[clv_classification])

DimCustomer[income_band] =
    SWITCH(TRUE(), DimCustomer[annualIncome]>=250000,"250k+",
                   DimCustomer[annualIncome]>=100000,"100-250k",
                   DimCustomer[annualIncome]>=50000,"50-100k","<50k")

FlaggedTransactions[amount_band] =
    SWITCH(TRUE(), FlaggedTransactions[amount]>=1000000,"1M+",
                   FlaggedTransactions[amount]>=500000,"500k-1M",
                   FlaggedTransactions[amount]>=200000,"200-500k","<200k")

DimDate[Date] = DATE(YEAR(DimDate[event_ts]),MONTH(DimDate[event_ts]),DAY(DimDate[event_ts]))
```
