# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

# Create Spark session
spark = SparkSession.builder \
    .appName("Healthcare Claims Ingestion") \
    .getOrCreate()

BUCKET_NAME = "healthcare-bucket-22032025"
BQ_TABLE = "avd-databricks-demo.bronze_dataset.claims"
TEMP_GCS_BUCKET = "healthcare-bucket-22032025/temp/"

claims_df=spark.read.csv(f"gs://{BUCKET_NAME}/landing/claims/*.csv",header=True)

claims_df = claims_df.withColumn(
    "datasource",
    f.when(f.input_file_name().contains("hospital1"), "hosa").when(f.input_file_name().contains("hospital2"), "hosb")
     .otherwise(None)
)

# Write DataFrame to BigQuery
claims_df.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
    .mode("overwrite") \
    .save()