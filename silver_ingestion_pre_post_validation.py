# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import uuid
from pyspark.sql import functions as F



# COMMAND ----------

class SchemaValidator:
    """Reusable PRE-MERGE and POST-MERGE schema/data quality checks."""

    SYSTEM_COLUMNS = {
        "surrogate_key", "createdtimestamp", "updatedtimestamp",
        "createdby", "updatedby", "filepath", "is_active", "is_deleted"
    }

    @staticmethod
    def _schema_map(schema, ignored_columns=None):
        ignored = {c.lower() for c in (ignored_columns or set())}
        return {
            field.name.lower(): field.dataType.simpleString()
            for field in schema.fields
            if field.name.lower() not in ignored
        }

    @classmethod
    def validate_schema(cls, source_df, target_table, ignored_columns=None):
        ignored = cls.SYSTEM_COLUMNS | {
            c.lower() for c in (ignored_columns or set())
        }
        source = cls._schema_map(source_df.schema, ignored)
        target = cls._schema_map(spark.table(target_table).schema, ignored)

        missing = sorted(set(target) - set(source))
        extra = sorted(set(source) - set(target))
        mismatches = [
            f"{c}: source={source[c]}, target={target[c]}"
            for c in sorted(set(source) & set(target))
            if source[c] != target[c]
        ]

        errors = []
        if missing:
            errors.append(f"Missing source columns: {missing}")
        if extra:
            errors.append(f"Unexpected source columns: {extra}")
        if mismatches:
            errors.append(f"Datatype mismatches: {mismatches}")

        if errors:
            raise ValueError(
                f"Schema validation failed for {target_table}: "
                + " | ".join(errors)
            )

        print(f"Pre-MERGE schema validation passed: {target_table}")

    @classmethod
    def validate_dimension_source(cls, source_df, target_table, keys):
        """PRE-MERGE: validate dimension schema and incoming source data."""
        cls.validate_schema(source_df, target_table)
        cls.validate_required_columns(
            source_df, ["UpdatedTimestamp"], "dimension source"
        )
        cls.validate_keys(source_df, keys, "dimension source")
        print(f"Pre-MERGE dimension data validation passed: {target_table}")

    @classmethod
    def validate_fact_source(cls, source_df, target_table, keys):
        """PRE-MERGE: validate final joined fact schema and source data."""
        cls.validate_schema(source_df, target_table)
        cls.validate_keys(source_df, keys, "fact source")

        duplicate_keys = (
            source_df.groupBy(*keys).count()
            .filter(F.col("count") > 1)
            .count()
        )
        if duplicate_keys:
            raise ValueError(
                f"Fact source has {duplicate_keys} duplicate key groups: {keys}"
            )

        print(f"Pre-MERGE fact data validation passed: {target_table}")

    @staticmethod
    def validate_required_columns(df, columns, label="source"):
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(f"{label} is missing required columns: {missing}")

    @staticmethod
    def validate_keys(df, keys, label="source"):
        missing = [k for k in keys if k not in df.columns]
        if missing:
            raise ValueError(f"{label} is missing key columns: {missing}")

        null_condition = None
        for key in keys:
            condition = F.col(key).isNull()
            null_condition = condition if null_condition is None else (
                null_condition | condition
            )

        null_count = df.filter(null_condition).count()
        if null_count:
            raise ValueError(
                f"{label} contains {null_count} rows with NULL business keys: {keys}"
            )

    @classmethod
    def validate_target_schema_unchanged(cls, target_table, expected_schema):
        actual_schema = spark.table(target_table).schema
        expected = {
            f.name.lower(): f.dataType.simpleString()
            for f in expected_schema.fields
        }
        actual = {
            f.name.lower(): f.dataType.simpleString()
            for f in actual_schema.fields
        }

        if expected != actual:
            raise ValueError(
                f"Target schema changed after write for {target_table}. "
                f"Expected: {expected}; Actual: {actual}"
            )

    @classmethod
    def validate_dimension_data(cls, source_df, target_table, keys, expected_schema):
        target = spark.table(target_table)
        cls.validate_target_schema_unchanged(target_table, expected_schema)

        # Validate business keys in the incoming source.
        cls.validate_keys(source_df, keys, "dimension source")

        active = target.filter(
            (F.col("is_active") == True) &
            (F.col("is_deleted") == False)
        )

        checks = {
            "NULL surrogate keys": target.filter(
                F.col("surrogate_key").isNull()
            ).count(),
            "duplicate surrogate keys": (
                target.groupBy("surrogate_key").count()
                .filter(F.col("count") > 1).count()
            ),
            "NULL CreatedTimestamp": target.filter(
                F.col("CreatedTimestamp").isNull()
            ).count(),
            "NULL CreatedBy": target.filter(
                F.col("CreatedBy").isNull()
            ).count(),
            "NULL active business keys": active.filter(
                F.expr(" OR ".join([f"`{k}` IS NULL" for k in keys]))
            ).count(),
            "duplicate active business keys": (
                active.groupBy(*keys).count()
                .filter(F.col("count") > 1).count()
            )
        }

        # Every distinct incoming key must exist as an active Silver record.
        missing_source_keys = (
            source_df.select(*keys).distinct()
            .join(active.select(*keys).distinct(), keys, "left_anti")
            .count()
        )
        checks["source keys missing from active Silver"] = missing_source_keys

        failures = {name: count for name, count in checks.items() if count > 0}
        if failures:
            raise ValueError(
                f"Post-MERGE data validation failed for {target_table}: {failures}"
            )

        print(
            f"Post-MERGE dimension data validation passed: {target_table}; "
            f"active rows={active.count()}"
        )

    @classmethod
    def validate_fact_data(cls, source_df, target_table, keys, expected_schema):
        target = spark.table(target_table)
        cls.validate_target_schema_unchanged(target_table, expected_schema)
        cls.validate_keys(source_df, keys, "fact source")

        duplicate_source_keys = (
            source_df.groupBy(*keys).count()
            .filter(F.col("count") > 1).count()
        )
        if duplicate_source_keys:
            raise ValueError(
                f"Fact source contains {duplicate_source_keys} duplicate key groups: {keys}"
            )

        active = target.filter(
            (F.col("is_active") == True) &
            (F.col("is_deleted") == False)
        )

        checks = {
            "NULL active business keys": active.filter(
                F.expr(" OR ".join([f"`{k}` IS NULL" for k in keys]))
            ).count(),
            "duplicate active business keys": (
                active.groupBy(*keys).count()
                .filter(F.col("count") > 1).count()
            ),
            "NULL CreatedTimestamp": target.filter(
                F.col("CreatedTimestamp").isNull()
            ).count(),
            "NULL CreatedBy": target.filter(
                F.col("CreatedBy").isNull()
            ).count()
        }

        missing_source_keys = (
            source_df.select(*keys).distinct()
            .join(active.select(*keys).distinct(), keys, "left_anti")
            .count()
        )
        checks["source keys missing from active Silver"] = missing_source_keys

        failures = {name: count for name, count in checks.items() if count > 0}
        if failures:
            raise ValueError(
                f"Post-MERGE fact data validation failed for {target_table}: {failures}"
            )

        print(
            f"Post-MERGE fact data validation passed: {target_table}; "
            f"active rows={active.count()}"
        )


# COMMAND ----------

class Dimensiontypes:

    def __init__(self):
        pass


    def scdtype1(self, df_source, target_table, key_column):

        keys = [k.strip() for k in key_column.split(",")]
        target = spark.table(target_table)
        expected_schema = target.schema
        SchemaValidator.validate_dimension_source(
            df_source, target_table, keys
        )

        df_source = (
            df_source
            .withColumn(
                "Row_number",
                row_number().over(
                    Window.partitionBy(*keys)
                    .orderBy(col("UpdatedTimestamp").desc())
                )
            )
            .filter(col("Row_number") == 1)
            .drop("Row_number")
            .withColumn("is_active", lit(True))
            .withColumn("is_deleted", lit(False))
            .withColumn("UpdatedTimestamp", current_timestamp())
        )

        merge_condition = " AND ".join(
            [f"target.`{key}` = source.`{key}`" for key in keys]
        )

        update_values = {}
        insert_values = {}

        for c in target.columns:
            name = c.lower()
            if name == "surrogate_key":
                continue
            elif name == "is_active":
                update_values[c] = F.lit(True)
                insert_values[c] = F.lit(True)
            elif name == "is_deleted":
                update_values[c] = F.lit(False)
                insert_values[c] = F.lit(False)
            elif name == "updatedtimestamp":
                update_values[c] = F.current_timestamp()
                insert_values[c] = F.current_timestamp()
            elif name == "createdtimestamp":
                insert_values[c] = F.current_timestamp()
            elif name in ("createdby", "updatedby"):
                update_values[c] = F.expr("current_user()")
                insert_values[c] = F.expr("current_user()")
            elif name == "filepath" and c not in df_source.columns:
                insert_values[c] = F.lit(None).cast(target.schema[c].dataType)
            elif c not in df_source.columns:
                raise ValueError(f"Source is missing target column: {c}")
            else:
                update_values[c] = F.col(f"source.`{c}`")
                insert_values[c] = F.col(f"source.`{c}`")

        delta_target = DeltaTable.forName(spark, target_table)

        (
            delta_target.alias("target")
            .merge(df_source.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_values)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )

        SchemaValidator.validate_dimension_data(
            df_source, target_table, keys, expected_schema
        )
        print(f"SCD Type 1 completed successfully for {target_table}")

    #scd type2
    
    def scdtype2(self, df_source, target_table, key_column):
        keys = [k.strip() for k in key_column.split(",")]
        source = df_source
        target = spark.table(target_table)
        expected_schema = target.schema
        metadata = SchemaValidator.SYSTEM_COLUMNS
        SchemaValidator.validate_dimension_source(
            source, target_table, keys
        )

    # Business columns used for change detection
        business_cols = [c for c in target.columns if c.lower() not in metadata and c not in keys]

        missing = set(keys + business_cols) - set(source.columns)
        if missing:
            raise ValueError(f"Missing source columns: {missing}")

    # Keep the latest source record for each business key
        source = (
             source.withColumn("_rn",F.row_number().over(
                Window.partitionBy(*keys)
                .orderBy(F.col("UpdatedTimestamp").desc_nulls_last())))
                .filter("_rn = 1")
                .drop("_rn"))

    # Generate hash
        hash_value = F.sha2(F.to_json(F.struct(*[F.col(c) for c in business_cols]),
            options={"ignoreNullFields": "false"}), 256)

        source = source.withColumn("_source_hash", hash_value)

        active_target = (target.filter("is_active = true AND is_deleted = false").withColumn("_target_hash", hash_value).select(*keys, "_target_hash"))

    # Identify new and changed records
        compare = source.alias("s").join(active_target, keys, "left")

        changed = compare.filter(F.col("_target_hash").isNotNull() &(F.col("_source_hash") != F.col("_target_hash"))).select("s.*")

        new_records = compare.filter(F.col("_target_hash").isNull()).select("s.*")

    # Prepare MERGE actions
        expire = changed
        insert = changed.unionByName(new_records)

        for i, key in enumerate(keys):
            expire = expire.withColumn(f"_merge_key_{i}", F.col(key))
            insert = insert.withColumn(f"_merge_key_{i}",F.lit(None).cast(source.schema[key].dataType))

        stage = (
        expire.withColumn("_action", F.lit("EXPIRE"))
        .unionByName(insert.withColumn("_action", F.lit("INSERT"))))

        merge_condition = " AND ".join(
        f"t.`{key}` = s.`_merge_key_{i}`"
        for i, key in enumerate(keys))

        merge_condition += " AND t.is_active = true AND t.is_deleted = false"

    # Values for new versions
        insert_values = {}

        for col in target.columns:
            name = col.lower()
            if name == "surrogate_key":
             continue
            elif name == "is_active":
             insert_values[col] = F.lit(True)
            elif name == "is_deleted":
             insert_values[col] = F.lit(False)
            elif name in ("createdtimestamp", "updatedtimestamp"):
             insert_values[col] = F.current_timestamp()
            elif name in ("createdby", "updatedby"):
             insert_values[col] = F.expr("current_user()")
            elif name == "filepath" and col not in source.columns:
             insert_values[col] = F.lit(None).cast(target.schema[col].dataType)
            elif col not in source.columns:
             raise ValueError(f"Source is missing target column: {col}")
            else:
             insert_values[col] = F.col(f"s.`{col}`")

        # Values for expired versions
        expire_values = {"is_active": F.lit(False),"is_deleted": F.lit(False),"UpdatedTimestamp": F.current_timestamp(),"UpdatedBy": F.expr("current_user()")}

        # Execute SCD Type 2
        (DeltaTable.forName(spark, target_table)
         .alias("t")
         .merge(stage.alias("s"), merge_condition)
         .whenMatchedUpdate(
             condition="s._action = 'EXPIRE'",
             set=expire_values)
         .whenNotMatchedInsert(
             condition="s._action = 'INSERT'",
             values=insert_values)
         .execute())

        SchemaValidator.validate_dimension_data(
            source, target_table, keys, expected_schema
        )
        print(f"SCD Type 2 completed: {target_table}")



# COMMAND ----------

# DBTITLE 1,Execution cell

import uuid
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
 
run_id = str(uuid.uuid4())
audit_table = "commondb.audit.audit_table"

# Read active dimension tables in execution order
df_table = (
    spark.table("commondb.audit.control_table")
    .filter(
        (F.lower(F.trim(F.col("layer"))) == "silver") &
        (F.lower(F.trim(F.col("is_active").cast("string"))) == "true") &
        (F.lower(F.trim(F.col("load_strategy"))) == "dimension")
    )
    .orderBy("execution_order")
)

table_list = df_table.collect()

print(f"Total dimension tables identified: {len(table_list)}")

pipeline = Dimensiontypes()

audit_records = []
failed_tables = []

# Process dimensions sequentially
for row in table_list:

    table_name = row["source_table"]
    key_column = row["key_column"]
    scd_type = row["scd_type"].strip().upper()

    source_table = f"commondb.bronze.{table_name}"
    target_table = f"commondb.silver.{table_name}"

    start_time = datetime.now()

    try:
        if scd_type == "SCD1":
            pipeline.scdtype1(
                spark.table(source_table),
                target_table,
                key_column
            )

        elif scd_type == "SCD2":
            pipeline.scdtype2(
                spark.table(source_table),
                target_table,
                key_column
            )

        else:
            raise ValueError(f"Invalid SCD type: {scd_type}")

        row_count = spark.table(target_table).count()

        audit_records.append({
            "run_id": run_id,
            "processing_layer": "silver",
            "source_layer": "raw",
            "table_name": table_name,
            "execution_date": start_time,
            "row_count": row_count,
            "status": "SUCCESS",
            "error_message": None,
            "load_type": row["load_type"]
        })

        print(f"SUCCESS: {table_name}")

    except Exception as e:

        failed_tables.append(table_name)

        audit_records.append({
            "run_id": run_id,
            "processing_layer": "silver",
            "source_layer": "raw",
            "table_name": table_name,
            "execution_date": start_time,
            "row_count": None,
            "status": "FAILURE",
            "error_message": str(e)[:4000],
            "load_type": row["load_type"]
        })

        print(f"FAILED: {table_name} - {str(e)}")


# Audit schema
audit_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("processing_layer", StringType(), True),
    StructField("source_layer", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("execution_date", TimestampType(), True),
    StructField("row_count", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("load_type", StringType(), True)
])

# Write audit records
if audit_records:
    audit_df = spark.createDataFrame(audit_records, schema=audit_schema)

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(audit_table)

    print("Audit records inserted successfully.")


# Execution summary
success_count = len([r for r in audit_records if r["status"] == "SUCCESS"])
failure_count = len([r for r in audit_records if r["status"] == "FAILURE"])

print("========================================")
print("       DIMENSION EXECUTION SUMMARY")
print("========================================")
print(f"Run ID          : {run_id}")
print(f"Total Dimensions: {len(table_list)}")
print(f"Successful      : {success_count}")
print(f"Failed          : {failure_count}")
print("========================================")

if failed_tables:
    raise RuntimeError(
        f"Dimension execution failed for {failure_count} tables. "
        f"Failed tables: {failed_tables}. "
        f"Check audit table: {audit_table}. Run ID: {run_id}"
    )

print("All dimension tables processed successfully!")


# COMMAND ----------

# MAGIC %md
# MAGIC ##*Fact creation*

# COMMAND ----------

# DBTITLE 1,Fact creation
import traceback

# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------

target_table = "commondb.silver.fact_orders"
audit_table = "commondb.audit.audit_table"

run_id = str(uuid.uuid4())
source_count = 0
inserted_count = 0
updated_count = 0

status = "FAILED"
error_message = None

# --------------------------------------------------
# 2. AUDIT TABLE
# --------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {audit_table} (
    run_id STRING,
    processing_layer STRING,
    source_layer STRING,
    table_name STRING,
    execution_date TIMESTAMP,
    row_count BIGINT,
    status STRING,
    error_message STRING,
    load_type STRING
)
USING DELTA
""")

audit_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("processing_layer", StringType(), True),
    StructField("source_layer", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("execution_date", TimestampType(), True),
    StructField("row_count", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("load_type", StringType(), True)
])


def write_audit(status, row_count, error_message=None):

    audit_data = [(
        run_id,
        "SILVER",
        "SILVER",
        "fact_orders",
        datetime.now(),
        row_count,
        status,
        error_message,
        "MERGE"
    )]

    audit_df = spark.createDataFrame(
        audit_data,
        schema=audit_schema
    )

    audit_df.write.format("delta").mode("append").saveAsTable(
        audit_table
    )


# --------------------------------------------------
# 3. FACT LOAD
# --------------------------------------------------

try:

    # Read tables
    orders = (
        spark.table("commondb.bronze.orders")
        .dropDuplicates(["order_id"])
        .alias("o")
    )
    items = spark.table("commondb.bronze.order_items").alias("i")
    customers = spark.table("commondb.silver.customers").alias("c")
    products = spark.table("commondb.silver.products").alias("p")
    sellers = spark.table("commondb.silver.sellers").alias("s")
    categories = spark.table("commondb.silver.product_category").alias("pc")
    payments = spark.table("commondb.bronze.order_payments")
    reviews = spark.table("commondb.bronze.order_reviews")
    geo = spark.table("commondb.silver.geolocation")
    dates = spark.table("commondb.silver.dim_date").alias("d")

    # Aggregate payments
    payment_df = payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("total_payment_value"),
        F.count("*").alias("payment_count"),
        F.max("payment_installments").alias("max_installments"),
        F.sort_array(
            F.collect_set("payment_type")
        ).alias("payment_types")
    ).alias("pay")

    # Latest review per order
    review_window = Window.partitionBy("order_id").orderBy(
        F.col("review_creation_date").desc_nulls_last(),
        F.col("review_id").desc_nulls_last()
    )

    review_df = (
        reviews
        .filter(
            "(review_creation_date IS NULL OR "
            "try_cast(review_creation_date AS TIMESTAMP) IS NOT NULL) "
            "AND (review_score IS NULL OR "
            "try_cast(review_score AS INT) IS NOT NULL)"
        )
        .withColumn("rn", F.row_number().over(review_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .alias("r")
    )

    # Aggregate geolocation
    geo_df = geo.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("latitude"),
        F.avg("geolocation_lng").alias("longitude"),
        F.first("geolocation_city", ignorenulls=True).alias("city"),
        F.first("geolocation_state", ignorenulls=True).alias("state")
    )

    customer_geo = geo_df.alias("cg")
    seller_geo = geo_df.alias("sg")

    # Join all 10 tables
    fact_source = (
        items
        .join(
            orders,
            F.col("i.order_id") == F.col("o.order_id"),
            "inner"
        )
        .join(
            customers,
            (F.col("o.customer_id") == F.col("c.customer_id")) &
            (F.col("c.is_active") == True) &
            (F.col("c.is_deleted") == False),
            "left"
        )
        .join(
            products,
            F.col("i.product_id") == F.col("p.product_id"),
            "left"
        )
        .join(
            sellers,
            F.col("i.seller_id") == F.col("s.seller_id"),
            "left"
        )
        .join(
            categories,
            F.col("p.product_category_name") ==
            F.col("pc.product_category_name"),
            "left"
        )
        .join(
            payment_df,
            F.col("o.order_id") == F.col("pay.order_id"),
            "left"
        )
        .join(
            review_df,
            F.col("o.order_id") == F.col("r.order_id"),
            "left"
        )
        .join(
            customer_geo,
            F.col("c.customer_zip_code_prefix") ==
            F.col("cg.geolocation_zip_code_prefix"),
            "left"
        )
        .join(
            seller_geo,
            F.col("s.seller_zip_code_prefix") ==
            F.col("sg.geolocation_zip_code_prefix"),
            "left"
        )
        .join(
            dates,
            F.to_date(F.col("o.order_purchase_timestamp")) ==
            F.col("d.full_date"),
            "left"
        )
        .select(
            F.col("i.order_id"),
            F.col("i.order_item_id"),

            F.col("c.surrogate_key").alias("customer_key"),
            F.col("p.surrogate_key").alias("product_key"),
            F.col("s.surrogate_key").alias("seller_key"),
            F.col("pc.surrogate_key").alias("category_key"),
            F.col("d.date_key"),

            F.col("o.order_status"),
            F.col("o.customer_id"),
            F.col("o.order_purchase_timestamp"),
            F.col("o.order_approved_at"),
            F.col("o.order_delivered_carrier_date"),
            F.col("o.order_delivered_customer_date"),
            F.col("o.order_estimated_delivery_date"),

            F.col("i.product_id"),
            F.col("i.seller_id"),
            F.col("pc.product_category_name_english"),
            F.col("i.price"),
            F.col("i.freight_value"),
            F.col("i.shipping_limit_date"),
            F.col("p.product_weight_g"),

            F.col("pay.total_payment_value"),
            F.col("pay.payment_count"),
            F.col("pay.max_installments"),
            F.col("pay.payment_types"),

            F.col("r.review_id"),
            F.col("r.review_score"),
            F.col("r.review_comment_title"),
            F.col("r.review_creation_date"),

            F.col("cg.latitude").alias("customer_latitude"),
            F.col("cg.longitude").alias("customer_longitude"),
            F.col("sg.latitude").alias("seller_latitude"),
            F.col("sg.longitude").alias("seller_longitude")
        )
    )

    # --------------------------------------------------
    # 4. VALIDATE FACT DATA
    # --------------------------------------------------

    keys = ["order_id", "order_item_id"]

    source_count = fact_source.count()

    # Check target table
    if not spark.catalog.tableExists(target_table):
        raise ValueError(
            f"Target table {target_table} does not exist. Create it first."
        )

    # --------------------------------------------------
    # 5. ADD AUDIT COLUMNS
    # --------------------------------------------------

    expected_fact_schema = spark.table(target_table).schema

    fact_source = (
        fact_source
        .withColumn("CreatedTimestamp", F.current_timestamp())
        .withColumn("UpdatedTimestamp", F.current_timestamp())
        .withColumn("CreatedBy", F.expr("current_user()"))
        .withColumn("UpdatedBy", F.expr("current_user()"))
        .withColumn("is_active", F.lit(True))
        .withColumn("is_deleted", F.lit(False))
    )

    # --------------------------------------------------
    # 6. VALIDATE FACT SCHEMA BEFORE MERGE
    # --------------------------------------------------

    SchemaValidator.validate_fact_source(
        fact_source, target_table, ["order_id", "order_item_id"]
    )

    # --------------------------------------------------
    # 7. PREPARE MERGE
    # --------------------------------------------------

    audit_columns = [
        "CreatedTimestamp",
        "CreatedBy",
        "UpdatedTimestamp",
        "UpdatedBy",
        "is_active",
        "is_deleted"
    ]

    compare_columns = [
        c for c in fact_source.columns
        if c not in keys + audit_columns
    ]

    merge_condition = """
    target.order_id = source.order_id
    AND target.order_item_id = source.order_item_id
    """

    change_condition = " OR ".join(
        [
            f"NOT (target.`{c}` <=> source.`{c}`)"
            for c in compare_columns
        ]
    )

    change_condition += """
    OR NOT target.is_active
    OR target.is_deleted
    """

    update_values = {
        c: f"source.`{c}`"
        for c in fact_source.columns
        if c not in keys + ["CreatedTimestamp", "CreatedBy"]
    }

    update_values.update({
        "UpdatedTimestamp": "current_timestamp()",
        "UpdatedBy": "current_user()",
        "is_active": "true",
        "is_deleted": "false"
    })

    insert_values = {
        c: f"source.`{c}`"
        for c in fact_source.columns
    }

    # --------------------------------------------------
    # 7. EXECUTE DELTA MERGE
    # --------------------------------------------------

    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(
            fact_source.alias("source"),
            merge_condition
        )
        .whenMatchedUpdate(
            condition=change_condition,
            set=update_values
        )
        .whenNotMatchedInsert(
            values=insert_values
        )
        .execute()
    )

    SchemaValidator.validate_fact_data(
        fact_source,
        target_table,
        ["order_id", "order_item_id"],
        expected_fact_schema
    )

    # --------------------------------------------------
    # 8. GET MERGE METRICS
    # --------------------------------------------------

    metrics = (
        DeltaTable.forName(spark, target_table)
        .history(1)
        .select("operationMetrics")
        .first()["operationMetrics"]
    )

    inserted_count = int(
        metrics.get("numTargetRowsInserted", 0)
    )

    updated_count = int(
        metrics.get("numTargetRowsUpdated", 0)
    )

    status = "SUCCESS"

    print(f"Run ID       : {run_id}")
    print(f"Source Rows  : {source_count}")
    print(f"Inserted     : {inserted_count}")
    print(f"Updated      : {updated_count}")
    print(f"Status       : {status}")

except Exception as e:

    status = "FAILED"
    error_message = str(e)[:4000]

    print(f"Fact load failed: {error_message}")
    print(traceback.format_exc())

finally:

    # --------------------------------------------------
    # 9. WRITE AUDIT RECORD
    # --------------------------------------------------

    try:
        write_audit(
            status,
            source_count,
            error_message
        )

        print(f"Audit status : {status}")
        print(f"Run ID       : {run_id}")

    except Exception as audit_error:
        print(f"Audit write failed: {str(audit_error)}")

# --------------------------------------------------
# 10. FAIL PIPELINE IF LOAD FAILED
# --------------------------------------------------

if status == "FAILED":
    raise RuntimeError(
        f"Silver fact_orders load failed. Run ID: {run_id}. "
        f"Error: {error_message}"
    )
