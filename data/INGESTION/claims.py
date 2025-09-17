from pyspark.sql import SparkSession, functions as F

# -----------------------------
# Spark session
# -----------------------------
spark = (
    SparkSession.builder
        .appName("Healthcare Claims Ingestion")
        # If you have the connector jar on the classpath you don't need extra config here.
        # .config("spark.sql.execution.arrow.pyspark.enabled", "true")  # optional
        .getOrCreate()
)

# -----------------------------
# Config
# -----------------------------
BUCKET_NAME = "healthcare-bucket-22052025"
CLAIMS_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"

# BigQuery: MUST be project.dataset.table with NO backticks
BQ_TABLE = "my-project-hospital-erm.bronze_dataset.claims"

# temporaryGcsBucket MUST be just the bucket name (no gs://, no folder)
TEMP_GCS_BUCKET = BUCKET_NAME

# -----------------------------
# Read → Transform
# -----------------------------
claims_df = (
    spark.read.csv(CLAIMS_PATH, header=True)    # add inferSchema=True if you want Spark to guess types
         
         .withColumn(
             "datasource",
             F.when(F.input_file_name().contains("hospital2"), F.lit("hospb"))
              .when(F.input_file_name().contains("hospital1"), F.lit("hospa"))
              .otherwise(F.lit("None"))
         )
         .withColumn("ingest_ts", F.current_timestamp())  # optional: helpful audit column
)
claims_df=claims_df.dropDuplicates()
claims_df.show(100)

# -----------------------------
# Write to BigQuery
# -----------------------------
(
    claims_df.write
        .format("bigquery")
        .mode("overwrite")  # change to "append" if you don't want to replace existing table data
        .option("table", BQ_TABLE)
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        # .option("writeMethod", "direct")  # optional: enable if your connector/runtime supports it
        .mode("overwrite")
        .save()
)

print("✅ Claims data written to BigQuery:", BQ_TABLE)
