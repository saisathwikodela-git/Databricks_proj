# Databricks_proj
This Project includes the Bulk data loading with metadata reading through control Table and Ingesting with Classes and calling different methods in classes/ vaidationfilechecks, and SCDtype1 and SCDtype2.

Design:

dim_customer ─┐
dim_product  ─┤
dim_seller   ─┼──< fact_order_items >── dim_date
dim_geo      ─┘          │
                         ├── order_id ──< fact_payments
                         └── order_id ──< fact_reviews

<img width="1526" height="1451" alt="image" src="https://github.com/user-attachments/assets/7c197108-ffd6-40c6-8349-6fd3765121fb" />




