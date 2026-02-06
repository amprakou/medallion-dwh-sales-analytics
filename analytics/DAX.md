# DAX Measures

All measures used across the Sales Dashboard, built on top of the Gold layer star schema (`fact_sales`, `dim_products`, `dim_customers`).

---

## Date Table

Dynamically generated using DAX — the date range is derived automatically from `fact_sales[order_date]`.

```dax
Date Table = 
VAR MinDate = MIN(fact_sales[order_date])
VAR MaxDate = MAX(fact_sales[order_date])
RETURN
ADDCOLUMNS(
    CALENDAR(MinDate, MaxDate),
    "Year",              YEAR([Date]),
    "Quarter",           "Q" & FORMAT([Date], "Q"),
    "Month",             MONTH([Date]),
    "Month Name",        FORMAT([Date], "MMMM"),
    "Month Short Name",  FORMAT([Date], "MMM"),
    "Day of Month",      DAY([Date]),
    "Day Name",          FORMAT([Date], "DDDD"),
    "Day Short Name",    FORMAT([Date], "DDD")
)
```

---

## Core Measures

```dax
Sales = SUM(fact_sales[sales_amount])
```

```dax
Cost = 
SUMX(
    fact_sales,
    fact_sales[quantity] * RELATED(dim_products[cost])
)
```
Cost is not stored directly in `fact_sales` — calculated row-by-row using `SUMX` and `RELATED` to pull the unit cost from `dim_products`.

```dax
Total Profit = 
SUMX(
    fact_sales,
    fact_sales[sales_amount] - (fact_sales[quantity] * RELATED(dim_products[cost]))
)
```
Profit is calculated at the row level (Sales - Cost per transaction) before aggregation, ensuring accurate results across any filter context.

```dax
Profit Margin % = DIVIDE([Total Profit], [Sales], 0)
```
Uses `DIVIDE` with a third argument of `0` to handle division by zero safely.

---

## Month-over-Month (MoM)

All MoM measures follow the same pattern — shown here using Sales as the example. The same structure applies to Cost, Total Profit, and Profit Margin %.

```dax
% MoM Sales = 
VAR _lastmonth = CALCULATE([Sales], DATEADD('Date Table'[Date], -1, MONTH))
VAR pct_MOM = DIVIDE([Sales], _lastmonth, 0) - 1
RETURN
FORMAT(pct_MOM, "0.00%") & 
IF(pct_MOM > 0, "⬆", IF(pct_MOM < 0, "⬇", ""))
```

- `DATEADD` shifts the filter context back by one month to retrieve the previous month's value
- The percentage change is calculated as `(current / previous) - 1`
- The arrow indicator (⬆ / ⬇) is appended for visual reference on KPI cards

### Available MoM Measures

| Measure | Base Measure |
|---------|-------------|
| % MoM Sales | Sales |
| % MoM Cost | Cost |
| % MoM Profit | Total Profit |
| % MoM Margin | Profit Margin % |
