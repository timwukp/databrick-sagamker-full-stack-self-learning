# 常見的錯誤說法

網路上和 AI 生成的 Databricks vs SageMaker 比較，常見過期名稱、錯誤數據流與過度承諾。以下是實際遇過的例子，以及對照官方文件後的正確說法。

| 常見寫法 | 問題 | 正確說法 |
|---|---|---|
| 整套只到「預測 → BI」 | 沒有 GenAI / agent 層。2026 年 Databricks 主推 Agent Bricks、Genie、Unity Gateway。 | 加上 GenAI 層（圖中紫色）。 |
| Delta Live Tables (DLT) | 已改名。官方文件標題是 Spark Declarative Pipelines，產品稱 Lakeflow pipelines。 | Lakeflow Spark Declarative Pipelines |
| Kafka → Auto Loader；或「用 Auto Loader 取代 Kafka 消費端」 | Auto Loader（cloudFiles）只讀雲端儲存上的檔案，不讀 Kafka。兩段說法也互相矛盾。 | Kafka 用 Structured Streaming Kafka source；檔案才用 Auto Loader。 |
| 用 Mosaic AI Training 訓練 Prophet / LightGBM | 那是基礎模型微調。數萬條 store×SKU 時間序列的標準做法是 many-models。 | groupBy + applyInPandas 平行訓練，MLflow 記錄，註冊到 UC。 |
| Model Serving「內建 Lakehouse Monitoring」 | 混了兩件事，而且 Lakehouse Monitoring 已改名。 | Model Serving 自動擷取 request/response；Data quality monitoring（anomaly detection + data profiling，後者前身是 Lakehouse Monitoring）負責品質與漂移。 |
| 在 Silver「建立 Databricks Feature Store」 | Feature Store 現在是 UC 物件（Feature Views 或 feature tables；不在 UC 的 Workspace Feature Store 已 deprecated）；線上服務是 Lakebase 驅動的 Online Feature Store。 | Feature Store in Unity Catalog + Online Feature Store（Lakebase） |
| Kafka「微秒級延遲」注入 Bronze | 不實。 | 不講數字；需要更低延遲時提 Structured Streaming real-time mode。 |
| AWS 側用 SageMaker Experiments | AWS 現在主推 SageMaker AI 上的 managed MLflow（MLflow Apps，新加坡區可用）；Experiments 只剩 Studio Classic。 | Managed MLflow on SageMaker AI |
| AWS 側監控用 SageMaker Model Monitor | 官方文件：Model Monitor 已不開放新客戶（和 Amazon Forecast 一樣）。 | Data Capture + 開源監控方案（SageMaker AI MLflow Apps + Evidently AI）+ CloudWatch + Quick Sight。 |
| 「Zero-ETL 對 Auto Loader，平手」 | 不同東西：zero-ETL 是營運資料庫和 SaaS（如 SAP、Salesforce）的近即時複製；Auto Loader 是檔案增量吸入。 | Zero-ETL ↔ Lakeflow Connect；Auto Loader ↔ S3 事件 + Glue。 |
| 「兩者已 1:1 完全對等」 | 這等於說產品沒有差異，也不準確（例如 AWS 的 Feature Store、Canvas 仍在 SageMaker AI Studio）。 | 講差異：一套治理與 lineage 管到模型與 agent 工具；同時公平承認 AWS 強項。 |
| 「UC 一秒查出哪個上游出錯」 | 誇大。 | 「lineage 能從 endpoint 一路追到 POS 表」。 |
