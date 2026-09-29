# RetailLens Marketplace — Seller Scorecard
Databricks & Snowflake Capstone · Topic 01 · Seed 42

A one-row-per-seller-per-month scorecard covering orders, revenue, late-delivery rate, and average review score. The project is structured as Bronze → Silver → Gold on Databricks, exported to Snowflake, and presented through a static dashboard.

## Repository structure

- `notebooks/` contains the generator and Bronze, Silver, and Gold transformations.
- `sql/` contains Snowflake setup, Streams/Tasks, and analysis queries.
- `data/` contains the generated Gold and dashboard rollup outputs.
- `dashboard/` contains the deployable static scorecard.
- `docs/` contains the capstone report.
- `local_run.py` mirrors the pipeline locally for checkpoint verification.

## Dashboard

The dashboard uses `dashboard/data.json` and is fully self-contained. It does not depend on an external Chart.js CDN, so the charts and tables continue to render when a deployment blocks third-party CDN resources.

Open `dashboard/index.html` locally or serve the `dashboard/` directory as a static site.

## Pipeline checkpoints

The project is designed around these expected checkpoints:

| Check | Expected |
|---|---:|
| Bronze orders / order_items / reviews / sellers | 6,120 / 15,000 / 6,000 / 40 |
| unit_price NULLs after try_cast | 200 |
| Orders after dedupe | 6,000 |
| Orphan order_items rejected | 90 |
| Gold rows | 240 |

The Gold late rate is calculated as:

`SUM(late_orders) / SUM(orders)`

rather than averaging monthly late-rate percentages.

## Snowflake

Run the SQL scripts in `sql/` in order. The setup script creates the warehouse, database, schemas, stage, landing table, analytics table, and read-only analyst role. The Streams/Tasks script adds incremental refresh, and the analysis script contains the leaderboard, escalation, state rollup, and access-control checks.

Do not commit real Snowflake credentials or account URLs. The SQL files intentionally use placeholders.

## Reproducibility

Synthetic marketplace data is generated deterministically from seed 42. The dashboard data therefore remains reproducible while avoiding a dependency on a downloaded source dataset.
