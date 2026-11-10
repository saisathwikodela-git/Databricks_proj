# Databricks_proj
This Project includes the Bulk data loading with metadata reading through control Table and Ingesting with Classes and calling different methods in classes/ vaidationfilechecks, and SCDtype1 and SCDtype2.

Design:

dim_customer ─┐
dim_product  ─┤
dim_seller   ─┼──< fact_order_items >── dim_date
dim_geo      ─┘          │
                         ├── order_id ──< fact_payments
                         └── order_id ──< fact_reviews
<img width="3748" height="3402" alt="mermaid-diagram" src="https://github.com/user-attachments/assets/53a0c66a-52ee-4ae9-aa1f-51b767eef65d" />


