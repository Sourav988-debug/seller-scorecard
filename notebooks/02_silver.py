# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — the decisions, made and recorded
# COMMAND ----------
MY_ID="yourname"; CATALOG="workspace"; SCHEMA=f"capstone_{MY_ID}"
from pyspark.sql import Window
import pyspark.sql.functions as F
sellers=spark.table(f"{CATALOG}.{SCHEMA}.bronze_sellers"); orders=spark.table(f"{CATALOG}.{SCHEMA}.bronze_orders"); items=spark.table(f"{CATALOG}.{SCHEMA}.bronze_order_items"); reviews=spark.table(f"{CATALOG}.{SCHEMA}.bronze_order_reviews")
items_cast=items.selectExpr("order_id","cast(item_no as int) as item_no","seller_id","product_id","try_cast(quantity as int) as quantity","try_cast(unit_price as double) as unit_price","try_cast(freight_value as double) as freight_value")
orders_cast=orders.selectExpr("order_id","customer_id","order_status","try_cast(order_purchase_ts as timestamp) as order_purchase_ts","try_cast(estimated_delivery_at as timestamp) as estimated_delivery_at","try_cast(delivered_at as timestamp) as delivered_at","_ingested_at")
print("unit_price NULLs after try_cast:",items_cast.filter("unit_price is null").count())
w=Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
orders_dedup=orders_cast.withColumn("rn",F.row_number().over(w)).filter("rn=1").drop("rn")
print("orders before:",orders.count(),"after:",orders_dedup.count())
orphans=items_cast.join(sellers,"seller_id","left_anti"); items_clean=items_cast.join(sellers.select("seller_id"),"seller_id","left_semi")
orphans.withColumn("reason",F.lit("unknown_seller")).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_rejects")
print("order_items total:",items_cast.count(),"orphans:",orphans.count(),"kept:",items_clean.count())
orders_final=orders_dedup.withColumn("is_late",F.when(F.col("delivered_at").isNull(),F.lit(None)).otherwise(F.col("delivered_at")>F.col("estimated_delivery_at")))
sellers.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_sellers"); orders_final.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_orders"); items_clean.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_order_items")
reviews.selectExpr("review_id","order_id","try_cast(review_score as int) as review_score","try_cast(review_ts as timestamp) as review_ts").write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_order_reviews")
