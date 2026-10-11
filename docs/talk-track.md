# Explain it in about 90 seconds

A 90-second English walkthrough of the Databricks design, with the same design on AWS for comparison. Same text as the [3D lab](../lab/retail-3d.html#talk).

1. **Frame the problem.** A retailer with thousands of stores wants fewer stock-outs and less overstock. That is two forecasts on two clocks: a nightly 30-day forecast per warehouse and SKU for replenishment, and an intraday sales forecast per store for top-ups and staffing.
2. **Ingest.** POS events go through Kafka, which absorbs promo-day bursts and lets us replay. ERP changes come in by CDC through Lakeflow Connect, and promo and weather files through Auto Loader.
3. **Pipelines.** Lakeflow declarative pipelines take bronze to silver to gold. Silver handles dedupe, late events and store time zones, with expectations quarantining bad rows. Gold is daily sales and stock per store, warehouse and SKU, and it feeds both BI and ML.
4. **Features and training.** Features live in Feature Store with point-in-time joins, so we never train on the future. We train one model per series in parallel with applyInPandas, track every run in MLflow, and register models in Unity Catalog with champion and challenger aliases.
5. **Serving.** Inventory is a nightly batch job that writes a forecast table back to the ERP. Sales is intraday: streaming features go straight to the online feature store on Lakebase, and a Model Serving endpoint sits behind the store app.
6. **Govern and observe.** Unity Catalog is one permission model and one lineage graph across all of it. Data quality monitoring catches late or partial feeds and drift on inference tables, and that triggers retraining.
7. **Where GenAI adds the most.** On top, a replenishment agent: planners ask Genie why a forecast moved, and an Agent Bricks agent drafts orders for a human to approve, with models and tools going through Unity Gateway.

## The same design on AWS

MSK for the stream, zero-ETL or DMS for the ERP, Glue or EMR Serverless authored in SageMaker Unified Studio, Iceberg on S3 Tables, Managed Flink for streaming features, SageMaker Feature Store, training jobs or Canvas time series, managed MLflow, Model Registry, a real-time endpoint and Batch Transform, Data Capture with MLflow Apps and Evidently for drift (Model Monitor is closed to new customers), and Quick plus AgentCore on top.

It works well. The trade-off is how many services and permission models the team operates and keeps aligned: SageMaker Catalog, Lake Formation and IAM on the governance side, and Unified Studio next to SageMaker AI Studio on the tooling side.

筆記：比較兩邊時要公平、具體。Databricks 的論點是「少一層要對齊的東西」，不是「AWS 做不到」。Kafka、S3 兩邊都在，這是共存（Databricks on AWS）。
