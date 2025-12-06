# PySpark example (run in Databricks / EMR / local)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, sha2, concat_ws

spark = SparkSession.builder.appName("FRS_ETL").getOrCreate()

# 1. read raw CSV from S3
raw = spark.read.option("header", True).csv("s3://frs-raw/transactions/2025-12-05/*.csv")

# 2. basic parsing & typing
clean = (raw
    .withColumn("txn_ts", to_timestamp(col("txn_time"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("currency", col("currency"))
    .withColumn("ingest_ts", lit(current_timestamp()))
)

# 3. dedupe using txn_id and checksum
deduped = clean.dropDuplicates(["txn_id"])

# 4. create a surrogate hash for PII (example)
pseudonymized = deduped.withColumn("cust_hash", sha2(concat_ws("||", col("customer_name"), col("customer_dob")), 256))

# 5. write to Bronze (Parquet partitioned by date)
(pseudonymized.write
   .mode("append")
   .partitionBy("ingest_date")
   .parquet("s3://frs-bronze/transactions/"))
