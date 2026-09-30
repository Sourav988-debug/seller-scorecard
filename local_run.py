"""
Local runner for the Seller Scorecard capstone.

Mirrors the Databricks Bronze -> Silver -> Gold pipeline locally with PySpark.
Run: pip install pyspark==3.5.1 && python local_run.py
"""
import shutil, os
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

VOL = "./data/_raw_tmp"
shutil.rmtree(VOL, ignore_errors=True)
os.makedirs(VOL, exist_ok=True)

spark = SparkSession.builder.appName("capstone-local").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def W(d, n):
    d.write.mode("overwrite").option("header", True).csv(f"{VOL}/{n}")

ST = "array('KA','MH','TN','DL','WB','GJ','TG','KL')"
LR = "round(0.02 + 0.33 * (id % 40) / 39, 3)"

W(spark.range(40).selectExpr(
    "format_string('S%03d', id + 1) as seller_id",
    f"element_at({ST}, cast(id % 8 as int) + 1) as seller_state",
    "format_string('Hub%02d', id % 8 + 1) as seller_city"
), "sellers")

o = spark.range(6000).selectExpr(
    "id", "format_string('O%06d', id) as order_id",
    "format_string('S%03d', id % 40 + 1) as seller_id",
    "format_string('C%05d', pmod(hash(id, 1), 4000)) as customer_id",
    "date_add(date'2025-01-01', cast(cast(id / 40 as int) * 181 / 150 as int)) as dy",
    f"{LR} as lr")
o = o.selectExpr("*", "id >= 5700 as undel", "cast(dy as timestamp) as ts",
    "if(month(dy) >= 4 and id % 4 = 0, least(0.9, lr * 3), lr) as lr2")
o = o.selectExpr("*", "pmod(hash(id, 2), 1000) < lr2 * 1000 as is_late")
o = o.selectExpr("id", "order_id", "seller_id", "customer_id", "is_late", "ts",
    "if(undel, 'shipped', 'delivered') as order_status",
    "timestampadd(day, 7, ts) as estimated_delivery_at",
    "if(undel, null, timestampadd(day, if(is_late, 8, 2) + pmod(hash(id, 3), 4), ts)) as delivered_at")

od = o.selectExpr("id", "order_id", "customer_id", "order_status",
    "ts as order_purchase_ts", "estimated_delivery_at", "delivered_at")
W(od.unionByName(od.filter("id between 1000 and 1119")).drop("id"), "orders")

it = o.withColumn("item_no", F.expr("explode(sequence(1, cast(id % 4 + 1 as int)))"))
W(it.selectExpr("order_id", "item_no",
    "if(id between 3000 and 3089 and item_no = 1, 'S999', seller_id) as seller_id",
    "format_string('PR%04d', pmod(hash(id, item_no, 4), 800)) as product_id",
    "pmod(hash(id, item_no, 5), 3) + 1 as quantity",
    "if(id between 4000 and 4199 and item_no = 1, 'NA', string(round(exp(4.79 + randn(42) * 0.5), 2))) as unit_price",
    "round(15 + pmod(hash(id, item_no, 6), 4000) / 100, 2) as freight_value"), "order_items")

W(o.selectExpr("format_string('RV%06d', id) as review_id", "order_id",
    "if(is_late, greatest(1, pmod(hash(id, 7), 5)), pmod(hash(id, 7), 5) + 1) as review_score",
    "timestampadd(day, 12, ts) as review_ts"), "order_reviews")

def read_raw(name):
    df = spark.read.option("header", True).csv(f"{VOL}/{name}")
    return (df.withColumn("_source_file", F.input_file_name())
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_row_hash", F.sha2(F.concat_ws("||", *[F.col(c) for c in df.columns]), 256)))

bronze = {t: read_raw(t) for t in ["sellers", "orders", "order_items", "order_reviews"]}
sellers, orders, items, reviews = (bronze["sellers"], bronze["orders"], bronze["order_items"], bronze["order_reviews"])

items_cast = items.selectExpr("order_id", "cast(item_no as int) as item_no", "seller_id", "product_id",
    "try_cast(quantity as int) as quantity", "try_cast(unit_price as double) as unit_price",
    "try_cast(freight_value as double) as freight_value")
orders_cast = orders.selectExpr("order_id", "customer_id", "order_status",
    "try_cast(order_purchase_ts as timestamp) as order_purchase_ts",
    "try_cast(estimated_delivery_at as timestamp) as estimated_delivery_at",
    "try_cast(delivered_at as timestamp) as delivered_at", "_ingested_at")

w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
orders_dedup = orders_cast.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
orphans = items_cast.join(sellers, "seller_id", "left_anti")
items_clean = items_cast.join(sellers.select("seller_id"), "seller_id", "left_semi")
orders_final = orders_dedup.withColumn("is_late",
    F.when(F.col("delivered_at").isNull(), F.lit(None))
     .otherwise(F.col("delivered_at") > F.col("estimated_delivery_at")))
reviews_clean = reviews.selectExpr("review_id", "order_id",
    "try_cast(review_score as int) as review_score", "try_cast(review_ts as timestamp) as review_ts")

print("Bronze:", {k: v.count() for k, v in bronze.items()})
print("unit_price NULLs:", items_cast.filter("unit_price is null").count())
print("orders dedupe:", orders.count(), "->", orders_dedup.count())
print("items orphans/kept:", orphans.count(), items_clean.count())

order_seller = items_clean.select("order_id", "seller_id").distinct()
om = orders_final.join(order_seller, "order_id").withColumn("month", F.trunc("order_purchase_ts", "month"))
item_rev = items_clean.groupBy("order_id").agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"))
base = om.join(item_rev, "order_id", "left").join(reviews_clean.select("order_id", "review_score"), "order_id", "left")

gold = (base.groupBy("seller_id", "month")
        .agg(F.count("order_id").alias("orders"), F.sum("revenue").alias("revenue"),
             F.sum(F.when(F.col("is_late") == True, 1).otherwise(0)).alias("late_orders"),
             F.avg("review_score").alias("avg_review_score"))
        .withColumn("late_rate", F.round(F.col("late_orders") / F.col("orders"), 4))
        .join(sellers, "seller_id")
        .select("seller_id", "seller_state", "month", "orders", "revenue", "late_orders", "late_rate", "avg_review_score")
        .orderBy("seller_id", "month"))

print("Gold rows:", gold.count())
gold.toPandas().to_csv("data/gold_seller_month.csv", index=False)
sellers.toPandas().to_csv("data/dim_sellers.csv", index=False)
shutil.rmtree(VOL, ignore_errors=True)
spark.stop()
