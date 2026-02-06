# Power BI Sales Dashboard

Interactive sales analytics dashboard built on top of the Gold layer star schema.

---

## How to Use

1. Open Power BI Desktop
2. Connect to your data source — either directly to SQL Server (Gold layer views) or import the final CSV files after all transformations
3. Verify relationships in the **Model view** — adjust manually if needed:
   - `fact_sales[product_key]` → `dim_products[product_key]`
   - `fact_sales[customer_key]` → `dim_customers[customer_key]`
   - `fact_sales[order_date]` → `Date Table[Date]`
4. The Date Table is generated dynamically via DAX based on the min/max dates in `fact_sales`. Ensure the relationship is correctly mapped
