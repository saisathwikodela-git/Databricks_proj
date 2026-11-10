%sql

create catalog if not exists commondb;
create schema if not exists commondb.audit;

CREATE TABLE IF NOT EXISTS commondb.audit.file_load_control
(   ID              INT,
    layer           STRING,
    source_path     STRING,
    source_database STRING,
    source_schema   STRING,
    source_table    STRING,
    target_database STRING,
    target_schema   STRING,
    target_table    STRING,
    file_type       STRING,
    load_date       DATE,
    is_active       BOOLEAN
)
USING DELTA;



INSERT INTO commondb.audit.control_table
(   ID,
    layer,
    source_path,
    source_database,
    source_schema,
    source_table,
    target_database,
    target_schema,
    target_table,
    file_type,
    load_date,
    is_active
)
VALUES
(1,'RAW', 'commondb.default.rawdata', NULL, NULL, 'customers',       'commondb', 'bronze', 'customers',       'csv', current_date(), TRUE),
(2,'RAW', 'commondb.default.rawdata', NULL, NULL, 'geolocation',     'commondb', 'raw', 'geolocation',     'csv', current_date(), TRUE),
(3,'RAW', 'commondb.default.rawdata', NULL, NULL, 'order_items',     'commondb', 'raw', 'order_items',     'csv', current_date(), TRUE),
(4,'RAW', 'commondb.default.rawdata', NULL, NULL, 'order_payments', 'commondb', 'raw', 'order_payments', 'csv', current_date(), TRUE),
(5,'RAW', 'commondb.default.rawdata', NULL, NULL, 'order_reviews',  'commondb', 'raw', 'order_reviews',  'csv', current_date(), TRUE),
(6,'RAW', 'commondb.default.rawdata', NULL, NULL, 'orders',         'commondb', 'raw', 'orders',         'csv', current_date(), TRUE),
(7,'RAW', 'commondb.default.rawdata', NULL, NULL, 'product_category','commondb', 'raw', 'product_category','csv', current_date(), TRUE),
(8,'RAW', 'commondb.default.rawdata', NULL, NULL, 'products',        'commondb', 'raw', 'products',        'csv', current_date(), TRUE),
(9,'RAW', 'commondb.default.rawdata', NULL, NULL, 'sellers',         'commondb', 'raw', 'sellers',         'csv', current_date(), TRUE);

CREATE TABLE IF NOT EXISTS commondb.audit.audit_table
(
    run_id              STRING,
    processing_layer    STRING,
    source_layer        STRING,
    table_name          STRING,
    execution_date      TIMESTAMP,
    row_count           BIGINT,
    status              STRING,
    error_message       STRING,
    load_type           STRING
)
USING DELTA;
---------------

--Silver Layer ddls:


%sql

CREATE TABLE IF NOT EXISTS commondb.silver.customers (
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY
        (START WITH 1 INCREMENT BY 1),

    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix INT,
    customer_city STRING,
    customer_state STRING,

    CreatedTimestamp TIMESTAMP,
    UpdatedTimestamp TIMESTAMP,
    UpdatedBy STRING,
    CreatedBy STRING,
    FilePath STRING,

    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING DELTA
CLUSTER BY AUTO
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);

CREATE TABLE IF NOT EXISTS commondb.silver.geolocation (
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY
        (START WITH 1 INCREMENT BY 1),

    geolocation_zip_code_prefix INT,
    geolocation_lat DOUBLE,
    geolocation_lng DOUBLE,
    geolocation_city STRING,
    geolocation_state STRING,

    CreatedTimestamp TIMESTAMP,
    UpdatedTimestamp TIMESTAMP,
    UpdatedBy STRING,
    CreatedBy STRING,
    FilePath STRING,

    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING DELTA
CLUSTER BY AUTO
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);

CREATE TABLE IF NOT EXISTS commondb.silver.order_items (
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY
        (START WITH 1 INCREMENT BY 1),

    order_id STRING,
    order_item_id INT,
    product_id STRING,
    seller_id STRING,
    shipping_limit_date TIMESTAMP,
    price DOUBLE,
    freight_value DOUBLE,

    CreatedTimestamp TIMESTAMP,
    UpdatedTimestamp TIMESTAMP,
    UpdatedBy STRING,
    CreatedBy STRING,
    FilePath STRING,

    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING DELTA
CLUSTER BY AUTO
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);

CREATE TABLE IF NOT EXISTS commondb.silver.order_payments (
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY
        (START WITH 1 INCREMENT BY 1),

    order_id STRING,
    payment_sequential INT,
    payment_type STRING,
    payment_installments INT,
    payment_value DOUBLE,

    CreatedTimestamp TIMESTAMP,
    UpdatedTimestamp TIMESTAMP,
    UpdatedBy STRING,
    CreatedBy STRING,
    FilePath STRING,

    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING DELTA
CLUSTER BY AUTO
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);

CREATE TABLE IF NOT EXISTS commondb.silver.order_reviews (
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY
        (START WITH 1 INCREMENT BY 1),

    review_id STRING,
    order_id STRING,
    review_score STRING,
    review_comment_title STRING,
    review_comment_message STRING,
    review_creation_date STRING,
    review_answer_timestamp STRING,

    CreatedTimestamp TIMESTAMP,
    UpdatedTimestamp TIMESTAMP,
    UpdatedBy STRING,
    CreatedBy STRING,
    FilePath STRING,

    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING DELTA
CLUSTER BY AUTO
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);

CREATE TABLE IF NOT EXISTS commondb.silver.orders (
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY
        (START WITH 1 INCREMENT BY 1),

    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,

    CreatedTimestamp TIMESTAMP,
    UpdatedTimestamp TIMESTAMP,
    UpdatedBy STRING,
    CreatedBy STRING,
    FilePath STRING,

    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING DELTA
CLUSTER BY AUTO
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);

CREATE TABLE IF NOT EXISTS commondb.silver.product_category (
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY
        (START WITH 1 INCREMENT BY 1),

    product_category_name STRING,
    product_category_name_english STRING,

    CreatedTimestamp TIMESTAMP,
    UpdatedTimestamp TIMESTAMP,
    UpdatedBy STRING,
    CreatedBy STRING,
    FilePath STRING,

    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING DELTA
CLUSTER BY AUTO
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);

CREATE TABLE IF NOT EXISTS commondb.silver.products (
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY
        (START WITH 1 INCREMENT BY 1),

    product_id STRING,
    product_category_name STRING,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,

    CreatedTimestamp TIMESTAMP,
    UpdatedTimestamp TIMESTAMP,
    UpdatedBy STRING,
    CreatedBy STRING,
    FilePath STRING,

    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING DELTA
CLUSTER BY AUTO
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);

CREATE TABLE IF NOT EXISTS commondb.silver.sellers (
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY
        (START WITH 1 INCREMENT BY 1),

    seller_id STRING,
    seller_zip_code_prefix INT,
    seller_city STRING,
    seller_state STRING,

    CreatedTimestamp TIMESTAMP,
    UpdatedTimestamp TIMESTAMP,
    UpdatedBy STRING,
    CreatedBy STRING,
    FilePath STRING,

    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING DELTA
CLUSTER BY AUTO
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);


CREATE TABLE IF NOT EXISTS commondb.gold.dim_date
(
    date_key INT,
    full_date DATE,

    day INT,
    day_name STRING,
    day_of_week INT,

    week_of_year INT,
    month INT,
    month_name STRING,
    quarter INT,
    year INT,

    is_weekend BOOLEAN,
    is_holiday BOOLEAN
)
USING DELTA
CLUSTER BY AUTO;


INSERT INTO commondb.gold.dim_date

SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
    d AS full_date,

    DAY(d) AS day,
    date_format(d, 'EEEE') AS day_name,
    DAYOFWEEK(d) AS day_of_week,

    WEEKOFYEAR(d) AS week_of_year,
    MONTH(d) AS month,
    date_format(d, 'MMMM') AS month_name,
    QUARTER(d) AS quarter,
    YEAR(d) AS year,

    CASE
        WHEN DAYOFWEEK(d) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    FALSE AS is_holiday

FROM (
    SELECT EXPLODE(
        SEQUENCE(
            DATE('2015-01-01'),
            DATE('2036-12-31'),
            INTERVAL 1 DAY
        )
    ) AS d
);

%sql

ALTER TABLE commondb.audit.control_table
ADD COLUMNS (
    key_column STRING,
    scd_type STRING
);
%sql

UPDATE commondb.audit.control_table
SET
    key_column = CASE LOWER(table_name)

        WHEN 'customers' THEN 'customer_id'
        WHEN 'geolocation' THEN 'geolocation_zip_code_prefix,geolocation_lat,geolocation_lng'
        WHEN 'order_items' THEN 'order_id,order_item_id'
        WHEN 'order_payments' THEN 'order_id,payment_sequential'
        WHEN 'order_reviews' THEN 'review_id'
        WHEN 'orders' THEN 'order_id'
        WHEN 'product_category' THEN 'product_category_name'
        WHEN 'products' THEN 'product_id'
        WHEN 'sellers' THEN 'seller_id'

    END,

    scd_type = CASE LOWER(table_name)
        WHEN 'customers' THEN 'SCD2'
        ELSE 'SCD1'
    END

WHERE LOWER(layer) = 'bronze'
AND LOWER(table_name) IN (
    'customers',
    'geolocation',
    'order_items',
    'order_payments',
    'order_reviews',
    'orders',
    'product_category',
    'products',
    'sellers'
);
ALTER TABLE commondb.audit.control_table
ADD COLUMNS (execution_order INT);
UPDATE commondb.audit.control_table
SET
    key_column = CASE LOWER(source_table)
        WHEN 'customers' THEN 'customer_id'
        WHEN 'geolocation' THEN 'geolocation_zip_code_prefix,geolocation_lat,geolocation_lng'
        WHEN 'product_category' THEN 'product_category_name'
        WHEN 'products' THEN 'product_id'
        WHEN 'sellers' THEN 'seller_id'
        WHEN 'orders' THEN 'order_id'
        WHEN 'order_items' THEN 'order_id,order_item_id'
        WHEN 'order_payments' THEN 'order_id,payment_sequential'
        WHEN 'order_reviews' THEN 'review_id'
    END,

    scd_type = CASE LOWER(source_table)
        WHEN 'customers' THEN 'SCD2'
        WHEN 'geolocation' THEN 'SCD1'
        WHEN 'product_category' THEN 'SCD1'
        WHEN 'products' THEN 'SCD1'
        WHEN 'sellers' THEN 'SCD1'
        ELSE NULL
    END,

    execution_order = CASE LOWER(source_table)
        WHEN 'customers' THEN 1
        WHEN 'geolocation' THEN 2
        WHEN 'product_category' THEN 3
        WHEN 'products' THEN 4
        WHEN 'sellers' THEN 5
        WHEN 'orders' THEN 6
        WHEN 'order_items' THEN 7
        WHEN 'order_payments' THEN 8
        WHEN 'order_reviews' THEN 9
    END

WHERE LOWER(layer) = 'raw'
  AND LOWER(source_table) IN (
      'customers',
      'geolocation',
      'product_category',
      'products',
      'sellers',
      'orders',
      'order_items',
      'order_payments',
      'order_reviews'
  );

-------------------------

Gold


CREATE TABLE IF NOT EXISTS commondb.silver.fact_orders
(
    -- Fact surrogate key
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY,

    -- Fact business keys
    order_id STRING NOT NULL,
    order_item_id INT NOT NULL,

    -- Dimension surrogate keys
    customer_key BIGINT,
    product_key BIGINT,
    seller_key BIGINT,
    category_key BIGINT,
    date_key INT,

    -- Order details
    order_status STRING,
    customer_id STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,

    -- Product and seller details
    product_id STRING,
    seller_id STRING,
    product_category_name_english STRING,
    price DOUBLE,
    freight_value DOUBLE,
    shipping_limit_date TIMESTAMP,
    product_weight_g DOUBLE,

    -- Payment details
    total_payment_value DOUBLE,
    payment_count BIGINT,
    max_installments INT,
    payment_types ARRAY<STRING>,

    -- Review details
    review_id STRING,
    review_score INT,
    review_comment_title STRING,
    review_creation_date TIMESTAMP,

    -- Geolocation details
    customer_latitude DOUBLE,
    customer_longitude DOUBLE,
    seller_latitude DOUBLE,
    seller_longitude DOUBLE,

    -- Audit columns
    CreatedTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UpdatedTimestamp TIMESTAMP,
    CreatedBy STRING,
    UpdatedBy STRING,
    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING DELTA
TBLPROPERTIES
(
    'delta.feature.allowColumnDefaults' = 'supported'
);
--------------

CREATE TABLE IF NOT EXISTS commondb.audit.control_table
(
    ID INT GENERATED ALWAYS AS IDENTITY,
    layer STRING,
    source_path STRING,
    source_database STRING,
    source_schema STRING,
    source_table STRING,
    target_database STRING,
    target_schema STRING,
    target_table STRING,
    file_type STRING,
    load_date DATE,
    is_active BOOLEAN,
    load_type STRING,
    key_column STRING,
    scd_type STRING,
    execution_order INT,
    load_strategy STRING
)
USING DELTA;
-----------------

INSERT INTO commondb.audit.control_table
(
    layer, source_path, source_database, source_schema, source_table,
    target_database, target_schema, target_table, file_type, load_date,
    is_active, load_type, key_column, scd_type, execution_order, load_strategy
)
VALUES

-- RAW to BRONZE
('RAW', '/Volumes/commondb/default/rawdata', NULL, NULL, 'customers',
 'commondb', NULL, 'customers', 'csv', DATE '2026-11-10',
 TRUE, 'append', NULL, NULL, 1, 'dimension'),

('RAW', '/Volumes/commondb/default/rawdata', NULL, NULL, 'geolocation',
 'commondb', NULL, 'geolocation', 'csv', DATE '2026-11-10',
 TRUE, 'append', NULL, NULL, 2, 'dimension'),

('RAW', '/Volumes/commondb/default/rawdata', NULL, NULL, 'product_category',
 'commondb', NULL, 'product_category', 'csv', DATE '2026-11-10',
 TRUE, 'append', NULL, NULL, 3, 'dimension'),

('RAW', '/Volumes/commondb/default/rawdata', NULL, NULL, 'products',
 'commondb', NULL, 'products', 'csv', DATE '2026-11-10',
 TRUE, 'append', NULL, NULL, 4, 'dimension'),

('RAW', '/Volumes/commondb/default/rawdata', NULL, NULL, 'sellers',
 'commondb', NULL, 'sellers', 'csv', DATE '2026-11-10',
 TRUE, 'append', NULL, NULL, 5, 'dimension'),

('RAW', '/Volumes/commondb/default/rawdata', NULL, NULL, 'orders',
 'commondb', NULL, 'orders', 'csv', DATE '2026-11-10',
 TRUE, 'append', NULL, NULL, 6, NULL),

('RAW', '/Volumes/commondb/default/rawdata', NULL, NULL, 'order_items',
 'commondb', NULL, 'order_items', 'csv', DATE '2026-11-10',
 TRUE, 'append', NULL, NULL, 7, NULL),

('RAW', '/Volumes/commondb/default/rawdata', NULL, NULL, 'order_payments',
 'commondb', NULL, 'order_payments', 'csv', DATE '2026-11-10',
 TRUE, 'append', NULL, NULL, 8, NULL),

('RAW', '/Volumes/commondb/default/rawdata', NULL, NULL, 'order_reviews',
 'commondb', NULL, 'order_reviews', 'csv', DATE '2026-11-10',
 TRUE, 'append', NULL, NULL, 9, NULL),

-- BRONZE to SILVER
('SILVER', NULL, 'commondb', 'bronze', 'customers',
 'commondb', 'silver', 'customers', NULL, DATE '2026-11-10',
 TRUE, NULL, 'customer_id', 'SCD2', 1, 'dimension'),

('SILVER', NULL, 'commondb', 'bronze', 'geolocation',
 'commondb', 'silver', 'geolocation', NULL, DATE '2026-11-10',
 TRUE, NULL, 'geolocation_zip_code_prefix,geolocation_lat,geolocation_lng',
 'SCD1', 2, 'dimension'),

('SILVER', NULL, 'commondb', 'bronze', 'product_category',
 'commondb', 'silver', 'product_category', NULL, DATE '2026-11-10',
 TRUE, NULL, 'product_category_name', 'SCD1', 3, 'dimension'),

('SILVER', NULL, 'commondb', 'bronze', 'products',
 'commondb', 'silver', 'products', NULL, DATE '2026-11-10',
 TRUE, NULL, 'product_id', 'SCD1', 4, 'dimension'),

('SILVER', NULL, 'commondb', 'bronze', 'sellers',
 'commondb', 'silver', 'sellers', NULL, DATE '2026-11-10',
 TRUE, NULL, 'seller_id', 'SCD1', 5, 'dimension'),

('SILVER', NULL, 'commondb', 'bronze', 'orders',
 'commondb', 'silver', 'orders', NULL, DATE '2026-11-10',
 TRUE, NULL, 'order_id', NULL, 6, 'fact'),

('SILVER', NULL, 'commondb', 'bronze', 'order_items',
 'commondb', 'silver', 'order_items', NULL, DATE '2026-11-10',
 TRUE, NULL, 'order_id,order_item_id', NULL, 7, 'fact'),

('SILVER', NULL, 'commondb', 'bronze', 'order_payments',
 'commondb', 'silver', 'order_payments', NULL, DATE '2026-11-10',
 TRUE, NULL, 'order_id,payment_sequential', NULL, 8, 'fact'),

('SILVER', NULL, 'commondb', 'bronze', 'order_reviews',
 'commondb', 'silver', 'order_reviews', NULL, DATE '2026-11-10',
 TRUE, NULL, 'review_id', NULL, 9, 'fact');