# Page 2 — Customer 360

Portfolio view of the customer base: risk vs churn, segments, channel, lifetime
value and product depth.

**Primary sources:** `DimCustomer` (Mongo ⋈ CLV ⋈ risk), `CustomerProducts`.

## Layout (16 : 9)

```
┌──────────────────────────────────────────────────────────────────────┐
│  FinSight — Customer 360     [slicer: segment][channel][is_active]    │
├───────────────────────────────────┬──────────────────────────────────┤
│  Risk score vs churn probability   │  Customer segment distribution   │
│  (scatter)                         │  (donut)                         │
│  x: Avg Risk Score  y: Avg Churn   │  legend: segment                 │
│  detail: customerId  size: CLV     │  value: Customers                │
├───────────────────────────────────┼──────────────────────────────────┤
│  Churn by segment / channel        │  CLV tiers        │ Product      │
│  (matrix: seg × channel,           │  (bar)            │ holdings     │
│   value = Churn Rate %)            │  clv_tier ×       │ (bar)        │
│                                    │  Customers        │ product ×    │
│                                    │                   │ Customers    │
└───────────────────────────────────┴───────────────────┴──────────────┘
```

## Visuals

| # | Visual | Type | Fields / measures |
|---|---|---|---|
| 1 | **Risk score vs churn probability** | Scatter | X `[Avg Risk Score]`; Y `[Avg Churn Probability]`; Details `DimCustomer[customerId]`; Size `[Avg CLV Score]`; Legend `DimCustomer[segment]`. Add analytics-pane ratio lines at x=0.25/0.60 and y=0.33/0.66. |
| 2 | **Customer segment distribution** | Donut | Legend `DimCustomer[segment]`; Values `[Customers]`; data labels = % of total |
| 3 | **Churn by segment / channel** | Matrix | Rows `DimCustomer[segment]`; Columns `DimCustomer[preferred_channel]`; Values `[Churn Rate %]` (and `[Customers]` as a second value); conditional-format `Churn Rate %` background red-scale |
| 4 | **CLV tiers** | Clustered bar | Axis `DimCustomer[clv_tier]` (High Value / Growth Potential / At Risk / Unscored); Value `[Customers]`; sort by tier order |
| 5 | **Product holdings** | Clustered bar | Axis `CustomerProducts[product]`; Value `[Customers Holding Product]`; sort desc; data labels on |

Optional 6th tile (fits the right column): **Card row** — `[Customers]`,
`[Churn Rate %]`, `[High Value Customers]`, `[Avg Products per Customer]`.

## Slicers

| Slicer | Field | Style |
|---|---|---|
| Segment | `DimCustomer[segment]` | dropdown (multi-select) |
| Channel | `DimCustomer[preferred_channel]` | tile |
| Active | `DimCustomer[is_active]` | toggle (True/False) |
| KYC | `DimCustomer[kyc_status]` | dropdown (optional) |

## Filters

- **Page-level:** none by default. Add `DimCustomer[is_active] = True` if the
  page should show only active customers.
- **Visual 1:** to plot *individual* customers rather than segment averages,
  switch X/Y to the raw columns `risk_score` / `churn_probability` (Don't
  summarize) with `customerId` in Details — but that renders 10 000 points; the
  averaged-by-segment version is the default.
- **Visual 5:** `product` slicer (from `CustomerProducts`) drives customer
  filtering via the bi-directional relationship (data-model.md).

## Interactions

- Donut (2) segment → cross-filters all other visuals.
- Matrix (3) cell → highlights the scatter (1) and CLV bar (4).
- CLV bar (4) → cross-filters product holdings (5).

## Validation

| Check | Expected |
|---|---|
| `[Customers]` | = rows in `dim_customer.csv` (synthetic: **10 000**) |
| Donut (2) total | = `[Customers]`; segments ⊆ {Premium, Standard, Basic, Private Banking, Student} |
| Segment counts | match `mongodb/validation.js` (Standard 4497, Basic 2464, Premium 1528, Student 983, Private Banking 528) |
| `[Product Holdings]` | = rows in `customer_products.csv` (synthetic: **26 095**) |
| `[Avg Churn Probability]` | 0–1 |
| `[Avg Risk Score]` | 0–1 |
| Scatter (1) points | one per segment (averaged) or per `customerId` (raw mode) |
| `clv_tier` values | High Value / Growth Potential / At Risk / Unscored; "Unscored" = customers absent from `customer_clv` (the transaction-set vs profile-set overlap, ~287 scored on synthetic data — see Phase 12 notes) |
