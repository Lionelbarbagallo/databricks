# Databricks notebook source
from pyspark.sql.functions import *
import os
# import logging

# COMMAND ----------

try:
  s3_stage = dbutils.widgets.get("lio_prod_env")
except:
  s3_stage = 'dev'

# COMMAND ----------

s3_source_dir = f"prueba-lio-{s3_stage}/source"
s3_sink_dir = f"prueba-lio-{s3_stage}/sink"
#logging.info(f"Using S3 source dir: {s3_source_dir}")
#logging.info(f"Using S3 sink dir: {s3_sink_dir}")
print(f"Using S3 source dir: {s3_source_dir}")
print(f"Using S3 sink dir: {s3_sink_dir}")


# COMMAND ----------

raw_fire_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

raw_fire_df.cache()

# COMMAND ----------

raw_fire_df.write.mode("overwrite").format("parquet").save(f"s3://{s3_sink_dir}/")
