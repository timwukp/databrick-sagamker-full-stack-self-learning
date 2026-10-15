# Common mistakes

Databricks vs SageMaker comparisons online and from AI often have outdated names, wrong data flows and overpromises. Below are real examples we ran into, with the correct wording checked against official docs.

| Original claim | Problem | Change to |
|---|---|---|
| The whole thing stops at “forecast → BI” | No GenAI / agent layer. In 2026 Databricks is pushing Agent Bricks, Genie and Unity Gateway. | Add a GenAI layer (purple in the diagram). |
| Delta Live Tables (DLT) | Renamed. The official docs title is Spark Declarative Pipelines; the product is called Lakeflow pipelines. | Lakeflow Spark Declarative Pipelines |
| Kafka → Auto Loader; or 'replace the Kafka consumer with Auto Loader' | Auto Loader (cloudFiles) only reads files on cloud storage; it does not read Kafka. The two halves of the statement also contradict each other. | Use the Structured Streaming Kafka source for Kafka; use Auto Loader only for files. |
| Train Prophet / LightGBM with Mosaic AI Training | That is foundation model fine-tuning. The standard approach for tens of thousands of store×SKU time series is many-models. | Parallel training with groupBy + applyInPandas, logged in MLflow, registered to UC. |
| Model Serving 'has Lakehouse Monitoring built in' | Mixes up two things, and Lakehouse Monitoring has been renamed. | Model Serving captures request/response automatically; Data quality monitoring (anomaly detection + data profiling, the latter formerly Lakehouse Monitoring) handles quality and drift. |
| “Build a Databricks Feature Store” in Silver | Feature Store is now a UC object (Feature Views or feature tables; the non-UC Workspace Feature Store is deprecated); online serving is the Lakebase-backed Online Feature Store. | Feature Store in Unity Catalog + Online Feature Store (Lakebase) |
| Kafka ingests into Bronze with 'microsecond latency' | Not true. | Do not quote numbers; if lower latency is needed, mention Structured Streaming real-time mode. |
| Use SageMaker Experiments on the AWS side | AWS now promotes managed MLflow on SageMaker AI (MLflow Apps, available in the Singapore Region); Experiments remains only in Studio Classic. | Managed MLflow on SageMaker AI |
| Use SageMaker Model Monitor for monitoring on the AWS side | Official docs: Model Monitor is closed to new customers (same as Amazon Forecast). | Data Capture + an open-source monitoring stack (SageMaker AI MLflow Apps + Evidently AI) + CloudWatch + Quick Sight. |
| 'Zero-ETL vs Auto Loader, a tie' | Different things: zero-ETL is near real-time replication from operational databases and SaaS (such as SAP, Salesforce); Auto Loader is incremental file ingestion. | Zero-ETL ↔ Lakeflow Connect; Auto Loader ↔ S3 events + Glue. |
| 'The two are now fully equivalent, 1:1' | This amounts to saying the products have no differences, which is also inaccurate (for example, AWS Feature Store and Canvas are still in SageMaker AI Studio). | Talk about the difference: one governance and lineage system covers models and agent tools; at the same time, fairly acknowledge AWS strengths. |
| Delta Sharing | Renamed. The official docs now call it OpenSharing (the old /delta-sharing URL redirects to /opensharing). | OpenSharing (formerly Delta Sharing) |
| ‘Use Lakehouse Federation to bring the ERP data in’ | Federation queries the source in place, read-only, without copying. It is not an ingestion tool and keeps no history. | Lakehouse Federation for occasional queries; Lakeflow Connect for volume, low latency or SCD type 2 history. |
| 'UC finds the failing upstream in one second' | Overstated. | 'Lineage can trace from the endpoint all the way back to the POS table.' |
