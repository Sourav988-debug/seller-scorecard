USE DATABASE SELLER_SCORECARD_DB; USE WAREHOUSE SCORECARD_WH;
CREATE STREAM IF NOT EXISTS RAW.GOLD_SELLER_MONTH_STREAM ON TABLE RAW.GOLD_SELLER_MONTH_RAW;
CREATE OR REPLACE TASK ANALYTICS.REFRESH_GOLD_SELLER_MONTH WAREHOUSE=SCORECARD_WH SCHEDULE='USING CRON 0 * * * * UTC' WHEN SYSTEM$STREAM_HAS_DATA('RAW.GOLD_SELLER_MONTH_STREAM')
AS MERGE INTO ANALYTICS.GOLD_SELLER_MONTH tgt USING RAW.GOLD_SELLER_MONTH_STREAM src
ON tgt.seller_id=src.seller_id AND tgt.month=src.month
WHEN MATCHED THEN UPDATE SET orders=src.orders,revenue=src.revenue,late_orders=src.late_orders,late_rate=src.late_rate,avg_review_score=src.avg_review_score
WHEN NOT MATCHED THEN INSERT(seller_id,seller_state,month,orders,revenue,late_orders,late_rate,avg_review_score)
VALUES(src.seller_id,src.seller_state,src.month,src.orders,src.revenue,src.late_orders,src.late_rate,src.avg_review_score);
ALTER TASK ANALYTICS.REFRESH_GOLD_SELLER_MONTH RESUME;