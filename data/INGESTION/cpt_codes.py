from pyspark.sql import SparkSession

spark=SparkSession.builder\
                   .appName("CPT Codes Ingestion")\
                   .getOrCreate()

BUCKET_NAME = "healthcare-bucket-22052025"
CPT_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"

# BigQuery: MUST be project.dataset.table with NO backticks
BQ_TABLE = "my-project-hospital-erm.bronze_dataset.cptcodes"

# temporaryGcsBucket MUST be just the bucket name (no gs://, no folder)
TEMP_GCS_BUCKET = BUCKET_NAME

cptcodes_df=spark.read.csv(CPT_PATH, header=True)

for col in cptcodes_df.columns:
    new_col=col.replace(" ","_").lower()
    cptcodes_df=cptcodes_df.withColumnRenamed(col, new_col)


(
    cptcodes_df.write
        .format("bigquery")
        .mode("overwrite")  # change to "append" if you don't want to replace existing table data
        .option("table", BQ_TABLE)
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        # .option("writeMethod", "direct")  # optional: enable if your connector/runtime supports it
        .mode("overwrite")
        .save()
)

print("✅ Claims data written to BigQuery:", BQ_TABLE)