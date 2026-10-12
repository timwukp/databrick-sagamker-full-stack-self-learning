# Use case: global retail, from till to forecast

One use case, two solutions. The same synthetic data goes from store POS all the way to a “warehouse inventory forecast” and a “store sales forecast”, implemented once on Databricks and once on AWS (SageMaker and related services). The [3D lab](../../en/lab/retail-3d.html) on the site is the interactive version; this document is the text version.

## The business problem

A retailer with thousands of stores in Southeast Asia wants to reduce both stockouts (can't sell) and excess inventory (won't sell). This is really two forecasts running on two different clocks:

| | Warehouse inventory forecast | Store sales forecast |
|---|---|---|
| Grain | Each warehouse (DC) × SKU × day | Each store × SKU × day |
| Horizon | Next 30 days | The rest of today |
| Frequency | Nightly batch | Intraday, real time (for example every 15 minutes) |
| Users | Planner / replenishment planning | Store manager |
| Output | Suggested purchase order, sent back to ERP for approval | An action like “restock 60 from the back room before noon” |
| Serving | batch inference written to a table | real-time endpoint |

Success is measured by stockout rate, waste rate and labor cost, not just model accuracy (wMAPE).

## The world in the synthetic data

| Type | Contents |
|---|---|
| Stores | `SG-ORCH-001` (Singapore, UTC+8), `MY-KUL-014` (Kuala Lumpur, UTC+8), `TH-BKK-007` (Bangkok, UTC+7) |
| Warehouses | `DC-SG-01` supplies the Singapore and Malaysia stores, `DC-TH-02` supplies the Thailand stores |
| SKUs | `SKU-20871` instant noodles 5-pack, `SKU-10023` oat milk 1L, `SKU-31045` sunscreen SPF50 |
| Business day | 2026-10-09 (Friday); `SKU-20871` is 20% off in SG and MY from the 9th to the 12th |
| Source systems | POS (JSON events), ERP / WMS (SAP table `MARD` as the example), promotion calendar (CSV), weather and holidays (files or API) |

## Problems planted in the raw data

Each step removes some of them, which is the fastest way to understand bronze → silver → gold:

| Problem | Where | Handled by |
|---|---|---|
| The POS resends the same checkout (E2) | Still in Kafka and bronze | silver dedupes on `event_id` |
| Network outage: a 23:58 transaction arrives at 00:07 (E3) | Kafka timestamp is later than event time | silver computes `business_date` from event time, so it still counts for the 9th |
| New POS firmware adds a `loyalty_id` field (E4) | Raw JSON | schema evolution; null for old data |
| Failed scan: no SKU, quantity 0 (E5) | Raw JSON | silver expectation drops and counts it |
| One checkout has several items | Nested `lines` array | silver flattens it to one row per item |
| Time is a “local-time string” | `event_ts_local` + `tz_offset` | silver converts to a UTC timestamp |
| SAP material numbers zero-padded to 18 digits, column names are German abbreviations | `MATNR`, `WERKS`, `LABST` | silver renames to `sku`, `dc_id`, `on_hand_qty` |
| Vendor file has an extra column | Promotion CSV | Auto Loader `_rescued_data`; Glue needs a catalog update |
| ERP CDC is 6 hours late | No new commits on the `sap_mard` table | anomaly detection reports a freshness anomaly |

## Nine stages

1. Sources — POS, ERP, promotions and weather
2. Ingest — Kafka / MSK, Auto Loader / Glue, Lakeflow Connect / zero-ETL or DMS
3. Raw (bronze) — stored as is; streaming features are also computed from here
4. Clean (silver) — dedupe, validate, flatten, time zones, types
5. Curated (gold) — one day × one store × one SKU; warehouse end-of-day stock
6. Features — offline feature tables (point-in-time) and the online feature store
7. Train and register — many-models training, MLflow, model registry
8. Serve — nightly batch scoring, intraday real-time endpoint, monitoring and feedback loop
9. Use — purchase suggestions, store manager app, GenAI agent

Three more “layers” span all stages: governance (Unity Catalog vs SageMaker Catalog + Lake Formation + IAM), storage (Delta vs Iceberg, both on S3), and workspace and orchestration (one workspace + Lakeflow Jobs vs Unified Studio + SageMaker AI Studio + Pipelines / Step Functions).

## How the numbers reconcile

The numbers in this synthetic data are designed to reconcile, and `verify.py` recomputes them every time:

- Each line item: `net_amount = qty × unit_price − discount` (for example the noodles in E1: 2 × 6.90 − 2.76 = 11.04).
- Gold revenue: `units × unit price × (1 − discount)`, for example SG noodles 186 × 6.90 × 0.8 = 1026.72.
- Purchase suggestion: next 7 days p50 total 2456 + safety stock 420 − end-of-day stock 1180 − in transit 600 = 1096, rounded up to the case size of 24 to get 1104 (46 cases).
- Streaming features: last 15 minutes 2 + 4 + 1 = 7 units, last 60 minutes 13 units in total.
- Agent trace: child span latencies add up to 1830 + 210 + 340 + 1650 + 90 = 4120 ms, equal to the parent span.

The gold layer uses totals for the “whole day” (for example 186 units), while silver before it shows only 4 sample rows; E1 contributes 2 of them.

## Where to look

- Interactive 3D diagram and data for every step: [lab/retail-3d.html](../../en/lab/retail-3d.html)
- Before / After data and schema for every step (text version): [data-walkthrough.md](data-walkthrough.md)
- Component map: [databricks-vs-aws-mapping.md](databricks-vs-aws-mapping.md)
- Downloadable sample data and schemas: [`data/`](../../data), index at [`data/manifest.json`](../../data/manifest.json)
- Hands-on: [`labs/01_retail_medallion.py`](../../labs/01_retail_medallion.py) (Databricks Free Edition)
