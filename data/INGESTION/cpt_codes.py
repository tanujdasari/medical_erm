# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

# Create Spark session
spark = SparkSession.builder \
    .appName("CPT Codes Ingestion") \
    .getOrCreate()

BUCKET_NAME = "healthcare-bucket-22032025"
BQ_TABLE = "avd-databricks-demo.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = "healthcare-bucket-22032025/temp/"

# Read the CSV file
cptcodes_df = spark.read.csv(f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv", header=True)

# Replace whitespaces in column names with underscores and convert to lowercase
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)

# Write DataFrame to BigQuery
cptcodes_df.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
    .mode("overwrite") \
    .save()
