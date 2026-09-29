# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — land it, change nothing
# COMMAND ----------
MY_ID="yourname"; VOL=f"/Volumes/workspace/capstone_{MY_ID}/raw"; CATALOG="workspace"; SCHEMA=f"capstone_{MY_ID}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
# COMMAND ----------
import pyspark.sql.functions as F
def read_raw(name):
    base=spark.read.option("header",True).csv(f"{VOL}/{name}")
    return base.withColumn("_source_file",F.input_file_name()).withColumn("_ingested_at",F.current_timestamp()).withColumn("_row_hash",F.sha2(F.concat_ws("||",*[F.col(c) for c in base.columns]),256))
for tbl in ["sellers","orders","order_items","order_reviews"]:
    df=read_raw(tbl); df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_{tbl}"); print(tbl,df.count())
