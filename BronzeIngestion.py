# Databricks notebook source
# MAGIC %md
# MAGIC ##*Imports and custom exception*
# MAGIC

# COMMAND ----------


from pyspark.sql.functions import col, lower, trim
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import uuid


class FileNotFoundException(Exception):
    pass


# COMMAND ----------

# MAGIC %md
# MAGIC ##*File validation class*

# COMMAND ----------


class FileValidation:

    def File_validation(
        self,
        source_path,
        schema_folder,
        Tablefoldername
    ):

        Location_path = (
            f"{source_path}/{schema_folder}/{Tablefoldername}/"
        )

        try:
            files = dbutils.fs.ls(Location_path)

            if len(files) == 0:
                raise FileNotFoundException(
                    f"No files found at location: {Location_path}"
                )

            print(f"Files found at: {Location_path}")

            return "Files Found"

        except Exception as e:
            print(f"FILE VALIDATION FAILED: {e}")
            raise


# COMMAND ----------

# MAGIC %md
# MAGIC ##*Read class*

# COMMAND ----------


from pyspark.sql.functions import (
    expr,
    current_timestamp,
    input_file_name
)


class Read:

    def Read_dataframe(self, source_updated_path, file_type):

        file_type = file_type.strip().lower()

        if file_type == "json":

            df = spark.read.format("json") \
                .option("multiline", "true")\
                .option("mode", "PERMISSIVE")\
                .load(source_updated_path)

        elif file_type == "csv":

            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("mode", "PERMISSIVE")\
                .load(source_updated_path)

        elif file_type == "parquet":

            df = spark.read.format("parquet") \
                .option("mode", "PERMISSIVE")\
                .load(source_updated_path)

        else:
            raise ValueError(
                f"Invalid file type: {file_type}"
            )
        # column building  metadata

        df = df.withColumn("CreatedTimestamp", current_timestamp()) \
             .withColumn("UpdatedTimestamp", current_timestamp()) \
             .withColumn("UpdatedBy", expr("current_user()")) \
             .withColumn("CreatedBy", expr("current_user()")) \
             .withColumn("FilePath", col("_metadata.file_path"))

        return df


# COMMAND ----------

# MAGIC %md
# MAGIC ##*Write class*

# COMMAND ----------


class Write:

    def Write_dataframe(
        self,
        df,
        fullTargettablename,
        load_type
    ):

        load_type = load_type.strip().lower()

        if load_type == "overwrite":

            spark.sql("DROP TABLE IF EXISTS " + fullTargettablename)

            df.write.mode("overwrite") \
                .format("delta") \
                .saveAsTable(fullTargettablename)

        elif load_type == "append":

            df.write.mode("append") \
                .format("delta") \
                .saveAsTable(fullTargettablename)

        else:
            raise ValueError(
                f"Invalid load type: {load_type}. "
                "Use overwrite or append."
            )

        return (
            f"Table {fullTargettablename} "
            "loaded successfully"
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ##**Initialize classes and run ID**

# COMMAND ----------

validationobj = FileValidation()
readobj = Read()
writeobj = Write()

# Unique notebook execution ID
run_id = str(uuid.uuid4())

# Audit table
audit_table = "commondb.audit.audit_table"

# Parallel workers
max_workers = 2

print(f"Bronze execution started. Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##*Main table processing function*

# COMMAND ----------

def process_table(row):

    config = row.asDict()

    # Metadata extraction
    source_layer = config["layer"]

    source_path = config["source_path"]
    schema_folder = spark.sql("SELECT date_format(current_date(), 'yyyy-MM-dd')").first()[0]
    Tablefoldername = config["source_table"]

    file_type = config["file_type"]

    Databasename = config["target_database"]
    Schemaname = config["target_schema"]
    Tablename = config["target_table"]

    load_type = config["load_type"]

    # Construct source path
    source_updated_path = (
        f"{source_path}/{schema_folder}/{Tablefoldername}/"
    )

    # Construct target table name
    fullTargettablename = (
        f"{Databasename.strip().lower()}."
        f"{Schemaname.strip().lower()}."
        f"{Tablename.strip().lower()}"
    )

    # Default audit values
    row_count = 0
    status = "SUCCESS"
    error_message = None

    try:

        print(
            f"STARTED: Bronze | {fullTargettablename}"
        )

        # STEP 1: File validation
        validationobj.File_validation(
            source_path,
            schema_folder,
            Tablefoldername
        )

        print(
            f"VALIDATION SUCCESS: {fullTargettablename}"
        )

        # STEP 2: Read source data
        df_read = readobj.Read_dataframe(
            source_updated_path,
            file_type
        )

        print(
            f"READ SUCCESS: {fullTargettablename}"
        )

        # STEP 3: Calculate row count
        row_count = df_read.count()

        # STEP 4: Write to Bronze Delta table
        result = writeobj.Write_dataframe(
            df_read,
            fullTargettablename,
            load_type
        )

        print(
            f"SUCCESS: {result} | Row count: {row_count}"
        )

    except Exception as e:

        status = "FAILURE"
        error_message = str(e)

        # Print actual exception
        print(
            f"BRONZE LOAD FAILED: {fullTargettablename}"
        )

        print(
            f"ERROR MESSAGE: {error_message}"
        )

    # Return audit information
    return {
        "run_id": run_id,
        "processing_layer": "Bronze",
        "source_layer": source_layer,
        "table_name": fullTargettablename,
        "execution_date": datetime.now(timezone.utc).replace(tzinfo=None),
        "row_count": row_count,
        "status": status,
        "error_message": error_message,
        "load_type": load_type
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ##*Filter metadata and execute parallel processing*

# COMMAND ----------

# Filter active RAW layer tables
df_table = spark.table('commondb.audit.control_table').filter(
    (lower(trim(col("layer"))) == "raw") &
    (
        lower(trim(col("is_active").cast("string"))) == "true"
    )
)

# Collect metadata records only
table_list = df_table.collect()

print(f"Total tables identified: {len(table_list)}")

# Audit records are small metadata dictionaries,
# not the actual source DataFrames.
audit_records = []

with ThreadPoolExecutor(max_workers=max_workers) as executor:

    future_tasks = {
        executor.submit(process_table, row): row
        for row in table_list
    }

    for future in as_completed(future_tasks):

        try:

            audit_record = future.result()

            audit_records.append(audit_record)

        except Exception as e:

            # Unexpected error outside process_table
            print(f"UNEXPECTED THREAD ERROR: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##*Create audit DataFrame and write to Delta*

# COMMAND ----------

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


if audit_records:

    audit_df = spark.createDataFrame(
        audit_records,
        schema=audit_schema
    )

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable('commondb.audit.audit_table')

    print("Audit records inserted successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##*Final execution summary*

# COMMAND ----------

total_tables = len(audit_records)

success_count = sum(
    1 for r in audit_records
    if r["status"] == "SUCCESS"
)

failure_count = sum(
    1 for r in audit_records
    if r["status"] == "FAILURE"
)

print("========================================")
print("       BRONZE EXECUTION SUMMARY")
print("========================================")

print(f"Run ID          : {run_id}")
print(f"Total Tables    : {total_tables}")
print(f"Successful      : {success_count}")
print(f"Failed          : {failure_count}")

print("========================================")

if failure_count > 0:

    raise RuntimeError(
        f"Bronze execution failed for {failure_count} tables. "
        f"Please check audit table: {audit_table}. "
        f"Run ID: {run_id}"
    )

print("All Bronze tables processed successfully!")