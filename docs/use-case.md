# Use case: global retail, from till to forecast

一個 use case、兩套方案。同一份合成數據，從門店 POS 一路走到「倉庫庫存預測」和「門店銷售預測」，分別用 Databricks 和 AWS（SageMaker 與相關服務）實作。網站上的 [3D lab](../lab/retail-3d.html) 是互動版，這份文件是文字版。

## 業務問題

一家在東南亞有數千家門店的零售商，想同時減少缺貨（賣不到）和庫存積壓（賣不掉）。這其實是兩個預測，跑在兩種時鐘上：

| | 倉庫庫存預測 (warehouse inventory) | 門店銷售預測 (store sales) |
|---|---|---|
| 粒度 | 每個倉庫 (DC) × SKU × 日 | 每家店 × SKU × 日 |
| 預測期 | 未來 30 天 | 今天剩下的時間 |
| 頻率 | 每晚批次 | 日內即時（例如每 15 分鐘） |
| 使用者 | 採購 / 補貨計畫 | 店經理 |
| 輸出 | 建議採購單，送回 ERP 等人核准 | 「中午前從後倉補 60 個」這種動作 |
| 服務方式 | batch inference 寫成一張表 | real-time endpoint |

成功指標是缺貨率、報廢率、人力成本，不只是模型準確度（wMAPE）。

## 合成數據裡的世界

| 類型 | 內容 |
|---|---|
| 門店 | `SG-ORCH-001`（新加坡，UTC+8）、`MY-KUL-014`（吉隆坡，UTC+8）、`TH-BKK-007`（曼谷，UTC+7） |
| 倉庫 | `DC-SG-01` 供應新加坡與馬來西亞門店、`DC-TH-02` 供應泰國門店 |
| SKU | `SKU-20871` 泡麵五入、`SKU-10023` 燕麥奶 1L、`SKU-31045` 防曬乳 SPF50 |
| 營業日 | 2026-10-09（星期五）；`SKU-20871` 在 SG、MY 9–12 號打八折 |
| 來源系統 | POS（JSON 事件）、ERP / WMS（以 SAP 表 `MARD` 為例）、促銷日曆（CSV）、天氣與節日（檔案或 API） |

## 刻意埋進原始數據的問題

每一步處理掉一部分，這是理解 bronze → silver → gold 最快的方法：

| 問題 | 在哪裡 | 誰處理 |
|---|---|---|
| 同一筆結帳被 POS 重送（E2） | Kafka、bronze 都還在 | silver 用 `event_id` 去重 |
| 斷網，23:58 的交易 00:07 才送到（E3） | Kafka timestamp 晚於事件時間 | silver 用事件時間算 `business_date`，仍算 9 號 |
| 新版 POS 韌體多一個 `loyalty_id` 欄位（E4） | 原始 JSON | schema evolution；舊資料為 null |
| 掃描失敗：沒有 SKU、數量 0（E5） | 原始 JSON | silver 的 expectation 丟掉並計數 |
| 一筆結帳有多個商品 | 巢狀 `lines` 陣列 | silver 攤平成一行一個商品 |
| 時間是「當地時間字串」 | `event_ts_local` + `tz_offset` | silver 轉成 UTC timestamp |
| SAP 物料號補零到 18 位、欄位是德文縮寫 | `MATNR`、`WERKS`、`LABST` | silver 改成 `sku`、`dc_id`、`on_hand_qty` |
| 廠商檔案多一個欄位 | 促銷 CSV | Auto Loader 的 `_rescued_data`；Glue 要更新 catalog |
| ERP 的 CDC 晚了 6 小時 | `sap_mard` 表沒有新 commit | anomaly detection 報 freshness 異常 |

## 九個階段

1. Sources — POS、ERP、促銷與天氣
2. Ingest — Kafka / MSK、Auto Loader / Glue、Lakeflow Connect / zero-ETL 或 DMS
3. Raw (bronze) — 原封不動保存；串流特徵也從這裡開始算
4. Clean (silver) — 去重、驗證、攤平、時區、型別
5. Curated (gold) — 一天 × 一店 × 一 SKU；倉庫期末庫存
6. Features — 離線特徵表（point-in-time）與線上特徵庫
7. Train and register — many-models 訓練、MLflow、模型註冊
8. Serve — 每晚批次打分、日內即時 endpoint、監控與回饋迴圈
9. Use — 採購建議、店經理 app、GenAI agent

另外三個「層」橫跨所有階段：治理（Unity Catalog 對 SageMaker Catalog + Lake Formation + IAM）、儲存（Delta 對 Iceberg，都在 S3）、工作與排程（一個 workspace + Lakeflow Jobs 對 Unified Studio + SageMaker AI Studio + Pipelines / Step Functions）。

## 數字如何前後對得上

這份合成數據的數字是刻意設計成可以對帳的，`verify.py` 每次都會重算：

- 每一行商品：`net_amount = qty × unit_price − discount`（例如 E1 的泡麵：2 × 6.90 − 2.76 = 11.04）。
- Gold 的營收：`units × 單價 × (1 − 折扣)`，例如 SG 泡麵 186 × 6.90 × 0.8 = 1026.72。
- 採購建議：未來 7 天 p50 合計 2456 + 安全庫存 420 − 期末庫存 1180 − 在途 600 = 1096，按箱規 24 取整成 1104（46 箱）。
- 串流特徵：最近 15 分鐘 2 + 4 + 1 = 7 個，最近 60 分鐘共 13 個。
- Agent trace：子 span 延遲加總 1830 + 210 + 340 + 1650 + 90 = 4120 ms，等於父 span。

Gold 那一層用的是「整天」的總數（例如 186 個），前面 silver 只展示其中 4 行樣本；E1 貢獻其中 2 個。

## 去哪裡看

- 互動 3D 圖與每一步的資料：[lab/retail-3d.html](../lab/retail-3d.html)
- 每一步的處理前 / 處理後資料與 schema（文字版）：[data-walkthrough.md](data-walkthrough.md)
- 元件對照表：[databricks-vs-aws-mapping.md](databricks-vs-aws-mapping.md)
- 可下載的樣本數據與 schema：[`data/`](../data)，索引在 [`data/manifest.json`](../data/manifest.json)
- 動手做：[`labs/01_retail_medallion.py`](../labs/01_retail_medallion.py)（Databricks Free Edition）
