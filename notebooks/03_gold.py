# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — the shape that travels
# MAGIC One row per seller per month. late_rate = SUM(late_orders) / SUM(orders).
# COMMAND ----------
MY_ID="yourname"; CATALOG="workspace"; SCHEMA=f"capstone_{MY_ID}"
import pyspark.sql.functions as F
orders=spark.table(f"{CATALOG}.{SCHEMA}.silver_orders"); items=spark.table(f"{CATALOG}.{SCHEMA}.silver_order_items"); sellers=spark.table(f"{CATALOG}.{SCHEMA}.silver_sellers"); reviews=spark.table(f"{CATALOG}.{SCHEMA}.silver_order_reviews")
order_seller=items.select("order_id","seller_id").distinct()
om=orders.join(order_seller,"order_id").withColumn("month",F.trunc("order_purchase_ts","month"))
item_rev=items.groupBy("order_id").agg(F.sum(F.col("quantity")*F.col("unit_price")).alias("revenue"))
base=om.join(item_rev,"order_id","left").join(reviews.select("order_id","review_score"),"order_id","left")
gold=base.groupBy("seller_id","month").agg(F.count("order_id").alias("orders"),F.sum("revenue").alias("revenue"),F.sum(F.when(F.col("is_late")==True,1).otherwise(0)).alias("late_orders"),F.avg("review_score").alias("avg_review_score")).withColumn("late_rate",F.round(F.col("late_orders")/F.col("orders"),4)).join(sellers,"seller_id").select("seller_id","seller_state","month","orders","revenue","late_orders","late_rate","avg_review_score")
gold.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_seller_month")
VOL=f"/Volumes/workspace/capstone_{MY_ID}/raw"; gold.coalesce(1).write.mode("overwrite").option("header",True).csv(f"{VOL}/export/gold_seller_month")
