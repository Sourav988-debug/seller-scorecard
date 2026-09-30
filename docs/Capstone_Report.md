# RetailLens Marketplace - Seller Scorecard

**Databricks and Snowflake Capstone 2026**  
**Topic:** 01 - Marketplace Seller Scorecard  
**Student:** Sourav Kumar Singh  
**Roll Number:** 2330125  
**Submitted to:** Charan Shetty  
**Date:** 29 September 2026

## Project objective

Build a seller-level marketplace scorecard that combines operational, commercial, and customer-experience metrics. The pipeline follows Bronze -> Silver -> Gold processing in Databricks, uses Snowflake for serving and SQL analysis, and exposes the final results through a static dashboard.

## Dataset and reproducibility

The marketplace dataset is synthetic and Olist-shaped. It is generated deterministically with **seed 42** and covers **January-June 2025**.

- Sellers: 40
- Orders generated: 6,000
- Order items: 15,000
- Reviews: 6,000
- Gold seller-month rows: 240

The generator intentionally introduces data-quality cases used to demonstrate the pipeline:

- 120 duplicate order records
- 90 orphan order-item records
- 200 invalid unit-price values that become NULL after `try_cast`

## Pipeline checkpoints

| Checkpoint | Result |
|---|---:|
| Bronze orders / order_items / reviews / sellers | 6,120 / 15,000 / 6,000 / 40 |
| Invalid unit prices after `try_cast` | 200 |
| Orders after deduplication | 6,000 |
| Orphan order items rejected | 90 |
| Gold rows | 240 |

## Gold metrics

The dashboard is based on the generated scorecard outputs:

- Sellers: 40
- Gold orders represented: 5,977
- Revenue: Rs 40,14,077
- Late-delivery rate: 20.58%
- Average review score: 2.83 / 5

Late rate is calculated as `SUM(late_orders) / SUM(orders)` rather than averaging monthly percentages.

## Escalation analysis

The April escalation analysis compares pre-April and post-March delivery performance. Ten sellers are flagged for substantial increases in late-delivery rate.

Example: seller S021 moves from **9.33%** before April to **49.32%** from April onward, a **5.29x** increase.

## Technology

- Databricks / PySpark
- Snowflake SQL
- Python
- Static HTML
- Inline SVG dashboard visualizations
- GitHub
- Vercel deployment target

## Repository links

- GitHub: https://github.com/Sourav988-debug/seller-scorecard
- Dashboard source: `dashboard/index.html`
- Dashboard data: `dashboard/data.json`
- Local pipeline: `local_run.py`

## Local reproduction

Install the pinned dependency set:

`pip install -r requirements.txt`

Then run:

`python local_run.py`

The runner regenerates the local raw data and the Gold seller-month output. It is intended for local verification; the Databricks notebooks remain the submission pipeline.

## Snowflake

Run the SQL files in order:

1. `sql/01_snowflake_setup.sql`
2. `sql/02_streams_tasks.sql`
3. `sql/03_analysis_queries.sql`

Credentials and account URLs are intentionally not committed.
