# Data walkthrough: every step, before and after

Generated from `lab/retail-3d.html` by `scripts/export_from_lab.mjs`. The same content is interactive in the
[3D lab](../lab/retail-3d.html#data). Sample rows are synthetic; columns, types and system fields follow the official docs.

Legend for row notes: duplicate, fails check, late, schema change, changed, look here, alert, superseded.

## Steps

1. [Sources: Store POS terminals](#step-1)
2. [Sources: Promo calendar, weather, holidays](#step-2)
3. [Sources: ERP and WMS (SAP, Oracle)](#step-3)
4. [Ingest: Apache Kafka](#step-4)
5. [Ingest: Auto Loader](#step-5)
6. [Ingest: Lakeflow Connect](#step-6)
7. [Raw (bronze): Bronze streaming tables](#step-7)
8. [Clean (silver): Silver: Lakeflow Spark Declarative Pipelines](#step-8)
9. [Curated (gold): Gold materialized views](#step-9)
10. [Curated (gold): Databricks SQL warehouse](#step-10)
11. [Curated (gold): OpenSharing (formerly Delta Sharing)](#step-11)
12. [Features: Feature Store in Unity Catalog](#step-12)
13. [Raw (bronze): Structured Streaming, real-time mode](#step-13)
14. [Features: Online Feature Store](#step-14)
15. [Train and register: Many-models training](#step-15)
16. [Train and register: MLflow 3, managed](#step-16)
17. [Train and register: Models in Unity Catalog](#step-17)
18. [Serve: Batch inference job](#step-18)
19. [Use: Warehouse inventory forecast](#step-19)
20. [Serve: Model Serving endpoint](#step-20)
21. [Use: Store sales forecast](#step-21)
22. [Serve: Data quality monitoring](#step-22)
23. [Use: Genie + Agent Bricks](#step-23)
24. [Layer: Governance](#step-24)
25. [Layer: Storage](#step-25)
26. [Layer: Where people work and how jobs run](#step-26)
27. [Layer: Foundation: identity, security, cost, CI/CD](#step-27)

<a id="step-1"></a>
## 1. Sources: Store POS terminals

### On Databricks: Sources: Store POS terminals

每結帳一次，POS 就送出一筆 JSON 事件；一筆事件裡有多個商品（巢狀的 lines）。

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**POS event payload, one JSON document per basket**

| column | type | tag | note |
|---|---|---|---|
| `event_id` | string |  |  |
| `store_id` | string |  |  |
| `terminal_id` | string |  |  |
| `event_ts_local` | string |  | local time, as text |
| `tz_offset` | string |  |  |
| `currency` | string |  |  |
| `lines` | array<struct<sku,qty,unit_price,discount>> |  | several products in one basket |
| `payment` | struct<method,amount> |  |  |
| `send_attempt` | int |  |  |
| `loyalty_id` | string |  | only sent by new POS firmware |

- **ok** E1 normal basket (2 lines)
- **dup** E2 the same basket re-sent after a network retry
- **late** E3 Bangkok, sold 23:58, reached Kafka at 00:07 the next day
- **evo** E4 new POS firmware adds loyalty_id
- **bad** E5 scanner glitch: no SKU, qty 0

```json
[
  {
    "event_id": "pos-SG001-T03-000184",
    "store_id": "SG-ORCH-001",
    "terminal_id": "T03",
    "event_ts_local": "2026-10-09 18:42:07",
    "tz_offset": "+08:00",
    "currency": "SGD",
    "lines": [
      {
        "sku": "SKU-20871",
        "qty": 2,
        "unit_price": 6.9,
        "discount": 2.76
      },
      {
        "sku": "SKU-10023",
        "qty": 1,
        "unit_price": 3.95,
        "discount": 0
      }
    ],
    "payment": {
      "method": "card",
      "amount": 14.99
    },
    "send_attempt": 1
  },
  {
    "event_id": "pos-SG001-T03-000184",
    "store_id": "SG-ORCH-001",
    "terminal_id": "T03",
    "event_ts_local": "2026-10-09 18:42:07",
    "tz_offset": "+08:00",
    "currency": "SGD",
    "lines": [
      {
        "sku": "SKU-20871",
        "qty": 2,
        "unit_price": 6.9,
        "discount": 2.76
      },
      {
        "sku": "SKU-10023",
        "qty": 1,
        "unit_price": 3.95,
        "discount": 0
      }
    ],
    "payment": {
      "method": "card",
      "amount": 14.99
    },
    "send_attempt": 2
  },
  {
    "event_id": "pos-TH007-T01-002291",
    "store_id": "TH-BKK-007",
    "terminal_id": "T01",
    "event_ts_local": "2026-10-09 23:58:40",
    "tz_offset": "+07:00",
    "currency": "THB",
    "lines": [
      {
        "sku": "SKU-31045",
        "qty": 1,
        "unit_price": 289,
        "discount": 0
      }
    ],
    "payment": {
      "method": "qr",
      "amount": 289
    },
    "send_attempt": 1
  },
  {
    "event_id": "pos-MY014-T02-000977",
    "store_id": "MY-KUL-014",
    "terminal_id": "T02",
    "event_ts_local": "2026-10-09 12:15:33",
    "tz_offset": "+08:00",
    "currency": "MYR",
    "lines": [
      {
        "sku": "SKU-20871",
        "qty": 3,
        "unit_price": 7.5,
        "discount": 4.5
      }
    ],
    "payment": {
      "method": "ewallet",
      "amount": 18
    },
    "send_attempt": 1,
    "loyalty_id": "L-55821"
  },
  {
    "event_id": "pos-SG001-T05-000201",
    "store_id": "SG-ORCH-001",
    "terminal_id": "T05",
    "event_ts_local": "2026-10-09 19:03:12",
    "tz_offset": "+08:00",
    "currency": "SGD",
    "lines": [
      {
        "sku": null,
        "qty": 0,
        "unit_price": 3.95,
        "discount": 0
      }
    ],
    "payment": {
      "method": "cash",
      "amount": 0
    },
    "send_attempt": 1
  }
]
```

#### 這一步改了什麼

原始事件刻意埋了六個典型問題：時間是當地時間的「字串」、一筆事件有多個商品、會重送（E2）、會晚到（E3）、韌體升級多出新欄位（E4）、偶爾有壞資料（E5）。後面每一步，就是在逐一處理它們。

[Open this step in the 3D lab](../lab/retail-3d.html#step=pos&plat=dbx)

### On AWS: Sources: Store POS terminals

每結帳一次，POS 就送出一筆 JSON 事件；一筆事件裡有多個商品（巢狀的 lines）。

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**POS event payload, one JSON document per basket**

| column | type | tag | note |
|---|---|---|---|
| `event_id` | string |  |  |
| `store_id` | string |  |  |
| `terminal_id` | string |  |  |
| `event_ts_local` | string |  | local time, as text |
| `tz_offset` | string |  |  |
| `currency` | string |  |  |
| `lines` | array<struct<sku,qty,unit_price,discount>> |  | several products in one basket |
| `payment` | struct<method,amount> |  |  |
| `send_attempt` | int |  |  |
| `loyalty_id` | string |  | only sent by new POS firmware |

- **ok** E1 normal basket (2 lines)
- **dup** E2 the same basket re-sent after a network retry
- **late** E3 Bangkok, sold 23:58, reached Kafka at 00:07 the next day
- **evo** E4 new POS firmware adds loyalty_id
- **bad** E5 scanner glitch: no SKU, qty 0

```json
[
  {
    "event_id": "pos-SG001-T03-000184",
    "store_id": "SG-ORCH-001",
    "terminal_id": "T03",
    "event_ts_local": "2026-10-09 18:42:07",
    "tz_offset": "+08:00",
    "currency": "SGD",
    "lines": [
      {
        "sku": "SKU-20871",
        "qty": 2,
        "unit_price": 6.9,
        "discount": 2.76
      },
      {
        "sku": "SKU-10023",
        "qty": 1,
        "unit_price": 3.95,
        "discount": 0
      }
    ],
    "payment": {
      "method": "card",
      "amount": 14.99
    },
    "send_attempt": 1
  },
  {
    "event_id": "pos-SG001-T03-000184",
    "store_id": "SG-ORCH-001",
    "terminal_id": "T03",
    "event_ts_local": "2026-10-09 18:42:07",
    "tz_offset": "+08:00",
    "currency": "SGD",
    "lines": [
      {
        "sku": "SKU-20871",
        "qty": 2,
        "unit_price": 6.9,
        "discount": 2.76
      },
      {
        "sku": "SKU-10023",
        "qty": 1,
        "unit_price": 3.95,
        "discount": 0
      }
    ],
    "payment": {
      "method": "card",
      "amount": 14.99
    },
    "send_attempt": 2
  },
  {
    "event_id": "pos-TH007-T01-002291",
    "store_id": "TH-BKK-007",
    "terminal_id": "T01",
    "event_ts_local": "2026-10-09 23:58:40",
    "tz_offset": "+07:00",
    "currency": "THB",
    "lines": [
      {
        "sku": "SKU-31045",
        "qty": 1,
        "unit_price": 289,
        "discount": 0
      }
    ],
    "payment": {
      "method": "qr",
      "amount": 289
    },
    "send_attempt": 1
  },
  {
    "event_id": "pos-MY014-T02-000977",
    "store_id": "MY-KUL-014",
    "terminal_id": "T02",
    "event_ts_local": "2026-10-09 12:15:33",
    "tz_offset": "+08:00",
    "currency": "MYR",
    "lines": [
      {
        "sku": "SKU-20871",
        "qty": 3,
        "unit_price": 7.5,
        "discount": 4.5
      }
    ],
    "payment": {
      "method": "ewallet",
      "amount": 18
    },
    "send_attempt": 1,
    "loyalty_id": "L-55821"
  },
  {
    "event_id": "pos-SG001-T05-000201",
    "store_id": "SG-ORCH-001",
    "terminal_id": "T05",
    "event_ts_local": "2026-10-09 19:03:12",
    "tz_offset": "+08:00",
    "currency": "SGD",
    "lines": [
      {
        "sku": null,
        "qty": 0,
        "unit_price": 3.95,
        "discount": 0
      }
    ],
    "payment": {
      "method": "cash",
      "amount": 0
    },
    "send_attempt": 1
  }
]
```

#### 這一步改了什麼

原始事件刻意埋了六個典型問題：時間是當地時間的「字串」、一筆事件有多個商品、會重送（E2）、會晚到（E3）、韌體升級多出新欄位（E4）、偶爾有壞資料（E5）。後面每一步，就是在逐一處理它們。

[Open this step in the 3D lab](../lab/retail-3d.html#step=pos&plat=aws)

<a id="step-2"></a>
## 2. Sources: Promo calendar, weather, holidays

### On Databricks: Sources: Promo calendar, weather, holidays

促銷部門每週丟一份 CSV 到 S3；天氣與節日也是類似的外部檔案或 API。

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**s3://retail-landing/promo/2026-10-08/promo_SEA.csv**

| column | type | tag | note |
|---|---|---|---|
| `sku` | text |  |  |
| `store_scope` | text |  | two countries packed into one cell (SG and MY) |
| `start_date` | text |  |  |
| `end_date` | text |  |  |
| `promo_type` | text |  |  |
| `discount_pct` | text |  |  |

```text
sku,store_scope,start_date,end_date,promo_type,discount_pct
SKU-20871,SG|MY,2026-10-09,2026-10-12,PCT_OFF,20
SKU-31045,TH,2026-10-20,2026-10-26,PCT_OFF,15
SKU-10023,SG,2026-10-16,2026-10-18,BUY2_SAVE,10
```

#### 這一步改了什麼

CSV 沒有型別：日期和數字都只是文字；store_scope 用 | 把多個國家串在一起——下游要拆開、轉型別，才能和銷售資料 join。

[Open this step in the 3D lab](../lab/retail-3d.html#step=ext&plat=dbx)

### On AWS: Sources: Promo calendar, weather, holidays

促銷部門每週丟一份 CSV 到 S3；天氣與節日也是類似的外部檔案或 API。

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**s3://retail-landing/promo/2026-10-08/promo_SEA.csv**

| column | type | tag | note |
|---|---|---|---|
| `sku` | text |  |  |
| `store_scope` | text |  | two countries packed into one cell (SG and MY) |
| `start_date` | text |  |  |
| `end_date` | text |  |  |
| `promo_type` | text |  |  |
| `discount_pct` | text |  |  |

```text
sku,store_scope,start_date,end_date,promo_type,discount_pct
SKU-20871,SG|MY,2026-10-09,2026-10-12,PCT_OFF,20
SKU-31045,TH,2026-10-20,2026-10-26,PCT_OFF,15
SKU-10023,SG,2026-10-16,2026-10-18,BUY2_SAVE,10
```

#### 這一步改了什麼

CSV 沒有型別：日期和數字都只是文字；store_scope 用 | 把多個國家串在一起——下游要拆開、轉型別，才能和銷售資料 join。

[Open this step in the 3D lab](../lab/retail-3d.html#step=ext&plat=aws)

<a id="step-3"></a>
## 3. Sources: ERP and WMS (SAP, Oracle)

### On Databricks: Sources: ERP and WMS (SAP, Oracle)

ERP（以 SAP 為例）的庫存表。這是 system of record，平台只讀、不改。

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**SAP table MARD (stock per plant and storage location)**

| column | type | tag | note |
|---|---|---|---|
| `MATNR` | CHAR(18) | pk | material number, zero-padded (40 chars on S/4HANA) |
| `WERKS` | CHAR(4) | pk | plant = warehouse |
| `LGORT` | CHAR(4) | pk | storage location |
| `LABST` | QUAN(13,3) |  | unrestricted stock |

| row note | MATNR | WERKS | LGORT | LABST |
|---|---|---|---|---|
| chg: 23:30 SGT picking for store deliveries: 1240 → 1180 | 000000000000020871 | SG01 | 0001 | 1180.000 |
|  | 000000000000031045 | TH02 | 0001 | 96.000 |
|  | 000000000000010023 | SG01 | 0001 | 3310.000 |

#### 這一步改了什麼

SAP 欄位名是德文縮寫：MATNR 物料號、WERKS 工廠、LGORT 儲位、LABST 可用庫存；物料號補零到 18 位。這些到 silver 都要改成業務名稱與 SKU 格式。

[Open this step in the 3D lab](../lab/retail-3d.html#step=erp&plat=dbx)

### On AWS: Sources: ERP and WMS (SAP, Oracle)

ERP（以 SAP 為例）的庫存表。這是 system of record，平台只讀、不改。

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**SAP table MARD (stock per plant and storage location)**

| column | type | tag | note |
|---|---|---|---|
| `MATNR` | CHAR(18) | pk | material number, zero-padded (40 chars on S/4HANA) |
| `WERKS` | CHAR(4) | pk | plant = warehouse |
| `LGORT` | CHAR(4) | pk | storage location |
| `LABST` | QUAN(13,3) |  | unrestricted stock |

| row note | MATNR | WERKS | LGORT | LABST |
|---|---|---|---|---|
| chg: 23:30 SGT picking for store deliveries: 1240 → 1180 | 000000000000020871 | SG01 | 0001 | 1180.000 |
|  | 000000000000031045 | TH02 | 0001 | 96.000 |
|  | 000000000000010023 | SG01 | 0001 | 3310.000 |

#### 這一步改了什麼

SAP 欄位名是德文縮寫：MATNR 物料號、WERKS 工廠、LGORT 儲位、LABST 可用庫存；物料號補零到 18 位。這些到 silver 都要改成業務名稱與 SKU 格式。

[Open this step in the 3D lab](../lab/retail-3d.html#step=erp&plat=aws)

<a id="step-4"></a>
## 4. Ingest: Apache Kafka

### On Databricks: Ingest: Apache Kafka

POS 事件寫進 Kafka topic。Kafka 不解析內容，只存 bytes 加上位置資訊。

#### Before

**Events sent by the tills (from step 1)**

| column | type | tag | note |
|---|---|---|---|
| `event_id` | string |  |  |
| `store_id` | string |  |  |
| `send_attempt` | int |  |  |
| `bytes` | int |  |  |

| row note | event_id | store_id | send_attempt | bytes |
|---|---|---|---|---|
| evo:  | pos-MY014-T02-000977 | MY-KUL-014 | 1 | 412 |
|  | pos-SG001-T03-000184 | SG-ORCH-001 | 1 | 398 |
| dup: retry | pos-SG001-T03-000184 | SG-ORCH-001 | 2 | 398 |
| bad:  | pos-SG001-T05-000201 | SG-ORCH-001 | 1 | 341 |
| late:  | pos-TH007-T01-002291 | TH-BKK-007 | 1 | 356 |

#### After

**Topic pos.sales.v1, as read by Spark Structured Streaming**

| column | type | tag | note |
|---|---|---|---|
| `key` | BINARY |  | store_id bytes (shown decoded) |
| `value` | BINARY |  | the JSON payload bytes |
| `topic` | STRING |  |  |
| `partition` | INT |  | same key, same partition |
| `offset` | BIGINT |  | position inside the partition |
| `timestamp` | TIMESTAMP |  | producer time, UTC |
| `timestampType` | INT |  | 0 = CreateTime |

| row note | key | value | topic | partition | offset | timestamp | timestampType |
|---|---|---|---|---|---|---|---|
| evo: E4 carries the new loyalty_id field | MY-KUL-014 | {"event_id":"pos-MY014-T02-000977",…,"loyalty_id":"L-55821"} | pos.sales.v1 | 1 | 775310 | 2026-10-09 04:15:34.010 | 0 |
|  | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":1} | pos.sales.v1 | 4 | 1048576 | 2026-10-09 10:42:08.120 | 0 |
| dup: E2: same event_id, sent again 3 s later | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":2} | pos.sales.v1 | 4 | 1048577 | 2026-10-09 10:42:11.402 | 0 |
| bad: E5: no SKU, qty 0 | SG-ORCH-001 | {"event_id":"pos-SG001-T05-000201",…,"sku":null,"qty":0} | pos.sales.v1 | 4 | 1048590 | 2026-10-09 11:03:13.551 | 0 |
| late: E3: sold 16:58 UTC, sent 17:07 UTC after the store network came back | TH-BKK-007 | {"event_id":"pos-TH007-T01-002291",…} | pos.sales.v1 | 9 | 532118 | 2026-10-09 17:07:12.004 | 0 |

#### 這一步改了什麼

Kafka 的 schema 永遠是這七欄（key、value、topic、partition、offset、timestamp、timestampType），你的欄位都被包在 value 這個 BINARY 裡。用 store_id 當 key，同一家店永遠進同一個 partition，順序有保證。POS 端重送造成的重複，Kafka 不會幫你去掉——那是 silver 的工作。

#### Code (Bash)

```
kafka-topics.sh --create --topic pos.sales.v1 \
  --partitions 12 --replication-factor 3 \
  --bootstrap-server <brokers>        # Confluent, self-managed, or Amazon MSK
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=stream&plat=dbx)

### On AWS: Ingest: Amazon MSK

POS 事件寫進 Kafka topic。Kafka 不解析內容，只存 bytes 加上位置資訊。

#### Before

**Events sent by the tills (from step 1)**

| column | type | tag | note |
|---|---|---|---|
| `event_id` | string |  |  |
| `store_id` | string |  |  |
| `send_attempt` | int |  |  |
| `bytes` | int |  |  |

| row note | event_id | store_id | send_attempt | bytes |
|---|---|---|---|---|
| evo:  | pos-MY014-T02-000977 | MY-KUL-014 | 1 | 412 |
|  | pos-SG001-T03-000184 | SG-ORCH-001 | 1 | 398 |
| dup: retry | pos-SG001-T03-000184 | SG-ORCH-001 | 2 | 398 |
| bad:  | pos-SG001-T05-000201 | SG-ORCH-001 | 1 | 341 |
| late:  | pos-TH007-T01-002291 | TH-BKK-007 | 1 | 356 |

#### After

**Topic pos.sales.v1, as read by Spark Structured Streaming**

| column | type | tag | note |
|---|---|---|---|
| `key` | BINARY |  | store_id bytes (shown decoded) |
| `value` | BINARY |  | the JSON payload bytes |
| `topic` | STRING |  |  |
| `partition` | INT |  | same key, same partition |
| `offset` | BIGINT |  | position inside the partition |
| `timestamp` | TIMESTAMP |  | producer time, UTC |
| `timestampType` | INT |  | 0 = CreateTime |

| row note | key | value | topic | partition | offset | timestamp | timestampType |
|---|---|---|---|---|---|---|---|
| evo: E4 carries the new loyalty_id field | MY-KUL-014 | {"event_id":"pos-MY014-T02-000977",…,"loyalty_id":"L-55821"} | pos.sales.v1 | 1 | 775310 | 2026-10-09 04:15:34.010 | 0 |
|  | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":1} | pos.sales.v1 | 4 | 1048576 | 2026-10-09 10:42:08.120 | 0 |
| dup: E2: same event_id, sent again 3 s later | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":2} | pos.sales.v1 | 4 | 1048577 | 2026-10-09 10:42:11.402 | 0 |
| bad: E5: no SKU, qty 0 | SG-ORCH-001 | {"event_id":"pos-SG001-T05-000201",…,"sku":null,"qty":0} | pos.sales.v1 | 4 | 1048590 | 2026-10-09 11:03:13.551 | 0 |
| late: E3: sold 16:58 UTC, sent 17:07 UTC after the store network came back | TH-BKK-007 | {"event_id":"pos-TH007-T01-002291",…} | pos.sales.v1 | 9 | 532118 | 2026-10-09 17:07:12.004 | 0 |

#### 這一步改了什麼

MSK 就是 Kafka，這一格兩邊完全一樣：同一個 topic、同一份 schema。若改用 Kinesis Data Streams，對應概念是 partition key、shard、sequence number。POS 重送造成的重複，同樣要在下游去重。

#### Code (Bash)

```
kafka-topics.sh --create --topic pos.sales.v1 \
  --partitions 12 --replication-factor 3 \
  --bootstrap-server <brokers>        # Confluent, self-managed, or Amazon MSK
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=stream&plat=aws)

<a id="step-5"></a>
## 5. Ingest: Auto Loader

### On Databricks: Ingest: Auto Loader

把 S3 上新到的檔案增量吸進一張表；已處理過的檔案不重讀。

#### Before

**s3://retail-landing/promo/ (two new files)**

| column | type | tag | note |
|---|---|---|---|
| `sku` | text |  |  |
| `store_scope` | text |  |  |
| `start_date` | text |  |  |
| `end_date` | text |  |  |
| `promo_type` | text |  |  |
| `discount_pct` | text |  |  |
| `max_per_customer` | text |  | only in the vendor file |

```text
promo_SEA.csv
sku,store_scope,start_date,end_date,promo_type,discount_pct
SKU-20871,SG|MY,2026-10-09,2026-10-12,PCT_OFF,20
SKU-31045,TH,2026-10-20,2026-10-26,PCT_OFF,15
SKU-10023,SG,2026-10-16,2026-10-18,BUY2_SAVE,10

promo_vendorX.csv   (a vendor added a column)
sku,store_scope,start_date,end_date,promo_type,discount_pct,max_per_customer
SKU-20871,TH,2026-11-01,2026-11-03,PCT_OFF,10,4
```

#### After

**retail.bronze.promo_calendar (streaming table via Auto Loader)**

| column | type | tag | note |
|---|---|---|---|
| `sku` | STRING |  |  |
| `store_scope` | STRING |  |  |
| `start_date` | STRING |  | still text |
| `end_date` | STRING |  |  |
| `promo_type` | STRING |  |  |
| `discount_pct` | STRING |  | CSV columns are inferred as STRING by default |
| `_rescued_data` | STRING | sys | added by Auto Loader |
| `_source_file` | STRING | sys | from _metadata.file_path |

| row note | sku | store_scope | start_date | end_date | promo_type | discount_pct | _rescued_data | _source_file |
|---|---|---|---|---|---|---|---|---|
|  | SKU-20871 | SG\|MY | 2026-10-09 | 2026-10-12 | PCT_OFF | 20 | *null* | …/promo_SEA.csv |
|  | SKU-31045 | TH | 2026-10-20 | 2026-10-26 | PCT_OFF | 15 | *null* | …/promo_SEA.csv |
|  | SKU-10023 | SG | 2026-10-16 | 2026-10-18 | BUY2_SAVE | 10 | *null* | …/promo_SEA.csv |
| evo: rescue mode: the unknown column is kept in _rescued_data, not dropped | SKU-20871 | TH | 2026-11-01 | 2026-11-03 | PCT_OFF | 10 | {"max_per_customer":"4","_file_path":"…/promo_vendorX.csv"} | …/promo_vendorX.csv |

#### 這一步改了什麼

Auto Loader（cloudFiles）只處理新到的檔案。兩個要記住的細節：(1) CSV / JSON 欄位預設一律推斷成 STRING（除非開 cloudFiles.inferColumnTypes），型別轉換留給 silver；(2) 自動加上 _rescued_data，不符合 schema 的值會被收進這一欄而不是消失。遇到新欄位時，預設的 addNewColumns 模式會停下、更新 schema、重啟；rescue 模式則像上表這樣先收著。

#### Code (SQL (Lakeflow pipeline))

```
CREATE OR REFRESH STREAMING TABLE retail.bronze.promo_calendar AS
SELECT *, _metadata.file_path AS _source_file
FROM STREAM read_files(
  's3://retail-landing/promo/',
  format => 'csv', header => true);
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=files&plat=dbx)

### On AWS: Ingest: S3 landing + AWS Glue jobs

把 S3 上新到的檔案增量吸進一張表；已處理過的檔案不重讀。

#### Before

**s3://retail-landing/promo/ (two new files)**

| column | type | tag | note |
|---|---|---|---|
| `sku` | text |  |  |
| `store_scope` | text |  |  |
| `start_date` | text |  |  |
| `end_date` | text |  |  |
| `promo_type` | text |  |  |
| `discount_pct` | text |  |  |
| `max_per_customer` | text |  | only in the vendor file |

```text
promo_SEA.csv
sku,store_scope,start_date,end_date,promo_type,discount_pct
SKU-20871,SG|MY,2026-10-09,2026-10-12,PCT_OFF,20
SKU-31045,TH,2026-10-20,2026-10-26,PCT_OFF,15
SKU-10023,SG,2026-10-16,2026-10-18,BUY2_SAVE,10

promo_vendorX.csv   (a vendor added a column)
sku,store_scope,start_date,end_date,promo_type,discount_pct,max_per_customer
SKU-20871,TH,2026-11-01,2026-11-03,PCT_OFF,10,4
```

#### After

**Glue Data Catalog: bronze.promo_calendar (Glue job with job bookmarks)**

| column | type | tag | note |
|---|---|---|---|
| `sku` | string |  |  |
| `store_scope` | string |  |  |
| `start_date` | string |  |  |
| `end_date` | string |  |  |
| `promo_type` | string |  |  |
| `discount_pct` | bigint |  | Glue infers numbers |
| `max_per_customer` | bigint | new | new column added to the table |
| `ingest_date` | string | part | partition column |

| row note | sku | store_scope | start_date | end_date | promo_type | discount_pct | max_per_customer | ingest_date |
|---|---|---|---|---|---|---|---|---|
|  | SKU-20871 | SG\|MY | 2026-10-09 | 2026-10-12 | PCT_OFF | 20 | *null* | 2026-10-08 |
|  | SKU-31045 | TH | 2026-10-20 | 2026-10-26 | PCT_OFF | 15 | *null* | 2026-10-08 |
|  | SKU-10023 | SG | 2026-10-16 | 2026-10-18 | BUY2_SAVE | 10 | *null* | 2026-10-08 |
| evo: schema changed: catalog must be updated | SKU-20871 | TH | 2026-11-01 | 2026-11-03 | PCT_OFF | 10 | 4 | 2026-10-30 |

#### 這一步改了什麼

Glue job 用 job bookmark 記住處理過哪些檔案（靠 transformation_ctx，最後 job.commit() 存檔）。型別由 Glue 推斷，數字會變 bigint。遇到新欄位，DynamicFrame 會帶出來，但 catalog 的 schema 要另外更新——沒有像 _rescued_data 這種「先收著」的欄位。

#### Code (Python (AWS Glue job))

```
src = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", format="csv",
    connection_options={"paths": ["s3://retail-landing/promo/"]},
    format_options={"withHeader": True},
    transformation_ctx="promo_src")          # bookmark key
glueContext.write_dynamic_frame.from_catalog(
    frame=src, database="bronze", table_name="promo_calendar")
job.commit()                                 # saves the bookmark
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=files&plat=aws)

<a id="step-6"></a>
## 6. Ingest: Lakeflow Connect

### On Databricks: Ingest: Lakeflow Connect

持續抓 ERP 資料庫的變更（CDC），不用每晚整表重倒。

#### Before

**SAP MARD now (step 3)**

| column | type | tag | note |
|---|---|---|---|
| `MATNR` | CHAR(18) | pk |  |
| `WERKS` | CHAR(4) | pk |  |
| `LGORT` | CHAR(4) | pk |  |
| `LABST` | QUAN(13,3) |  |  |

| row note | MATNR | WERKS | LGORT | LABST |
|---|---|---|---|---|
| chg: was 1240 until 23:30 SGT | 000000000000020871 | SG01 | 0001 | 1180.000 |
|  | 000000000000031045 | TH02 | 0001 | 96.000 |
|  | 000000000000010023 | SG01 | 0001 | 3310.000 |

#### After

**retail.bronze.sap_mard (history tracking = SCD type 2)**

| column | type | tag | note |
|---|---|---|---|
| `MATNR` | STRING | pk |  |
| `WERKS` | STRING | pk |  |
| `LGORT` | STRING | pk |  |
| `LABST` | DECIMAL(13,3) |  |  |
| `__START_AT` | TIMESTAMP | sys | this version valid from |
| `__END_AT` | TIMESTAMP | sys | null = current version |

| row note | MATNR | WERKS | LGORT | LABST | __START_AT | __END_AT |
|---|---|---|---|---|---|---|
| closed: old version closed, not overwritten | 000000000000020871 | SG01 | 0001 | 1240.000 | 2026-10-09 14:00:00 | 2026-10-09 15:30:00 |
| hl: current version | 000000000000020871 | SG01 | 0001 | 1180.000 | 2026-10-09 15:30:00 | *null* |
|  | 000000000000031045 | TH02 | 0001 | 96.000 | 2026-10-08 17:00:00 | *null* |
|  | 000000000000010023 | SG01 | 0001 | 3310.000 | 2026-10-09 09:10:00 | *null* |

#### 這一步改了什麼

SCD type 2 的意思：每次變更都保留一個版本，用 __START_AT / __END_AT 標出有效期間。舊版本被「關閉」而不是被覆蓋，所以能回答「昨晚 10 點庫存是多少」——這正是訓練模型時要的歷史。資料庫型 ERP 可用 Lakeflow Connect 的資料庫 connector；SAP 實務上也常走 SAP Business Data Cloud 的 OpenSharing（前身 Delta Sharing），依客戶 SAP 版本而定。

#### Code (SQL (Lakeflow pipeline, from a change feed))

```
CREATE OR REFRESH STREAMING TABLE retail.bronze.sap_mard;

CREATE FLOW sap_mard_cdc AS AUTO CDC INTO retail.bronze.sap_mard
FROM STREAM(retail.raw.sap_mard_changes)
KEYS (MATNR, WERKS, LGORT)
SEQUENCE BY change_ts
STORED AS SCD TYPE 2;
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=cdc&plat=dbx)

### On AWS: Ingest: Zero-ETL or AWS DMS

持續抓 ERP 資料庫的變更（CDC），不用每晚整表重倒。

#### Before

**SAP MARD now (step 3)**

| column | type | tag | note |
|---|---|---|---|
| `MATNR` | CHAR(18) | pk |  |
| `WERKS` | CHAR(4) | pk |  |
| `LGORT` | CHAR(4) | pk |  |
| `LABST` | QUAN(13,3) |  |  |

| row note | MATNR | WERKS | LGORT | LABST |
|---|---|---|---|---|
| chg: was 1240 until 23:30 SGT | 000000000000020871 | SG01 | 0001 | 1180.000 |
|  | 000000000000031045 | TH02 | 0001 | 96.000 |
|  | 000000000000010023 | SG01 | 0001 | 3310.000 |

#### After

**s3://retail-raw/dms/SAPSR3/MARD/ (DMS CDC files, Parquet)**

| column | type | tag | note |
|---|---|---|---|
| `Op` | string | sys | I / U / D, added by DMS |
| `cdc_ts` | timestamp | sys | commit time (TimestampColumnName setting) |
| `MATNR` | string |  |  |
| `WERKS` | string |  |  |
| `LGORT` | string |  |  |
| `LABST` | decimal(13,3) |  |  |

| row note | Op | cdc_ts | MATNR | WERKS | LGORT | LABST |
|---|---|---|---|---|---|---|
| chg: only the change arrives; merging it into a current or history table is your job | U | 2026-10-09 15:30:02 | 000000000000020871 | SG01 | 0001 | 1180.000 |

#### 這一步改了什麼

DMS 的 CDC 檔每一列多一個 Op 欄（I / U / D），只帶「變更」本身；要把它 merge 成最新狀態或 SCD2 歷史表，得自己在 Glue / EMR 用 MERGE INTO 寫。Zero-ETL 則直接給你一張複製好的表（最新狀態），但不保留每個版本。

#### Code (JSON (DMS S3 target endpoint settings))

```
{
  "BucketName": "retail-raw",
  "BucketFolder": "dms",
  "DataFormat": "parquet",
  "CdcPath": "cdc",
  "TimestampColumnName": "cdc_ts"
}
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=cdc&plat=aws)

<a id="step-7"></a>
## 7. Raw (bronze): Bronze streaming tables

### On Databricks: Raw (bronze): Bronze streaming tables

原封不動落地：每筆 Kafka 訊息變成表裡的一行，加上「從哪來、何時到」。

#### Before

**Topic pos.sales.v1, as read by Spark Structured Streaming**

| column | type | tag | note |
|---|---|---|---|
| `key` | BINARY |  | store_id bytes (shown decoded) |
| `value` | BINARY |  | the JSON payload bytes |
| `topic` | STRING |  |  |
| `partition` | INT |  | same key, same partition |
| `offset` | BIGINT |  | position inside the partition |
| `timestamp` | TIMESTAMP |  | producer time, UTC |
| `timestampType` | INT |  | 0 = CreateTime |

| row note | key | value | topic | partition | offset | timestamp | timestampType |
|---|---|---|---|---|---|---|---|
| evo: E4 carries the new loyalty_id field | MY-KUL-014 | {"event_id":"pos-MY014-T02-000977",…,"loyalty_id":"L-55821"} | pos.sales.v1 | 1 | 775310 | 2026-10-09 04:15:34.010 | 0 |
|  | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":1} | pos.sales.v1 | 4 | 1048576 | 2026-10-09 10:42:08.120 | 0 |
| dup: E2: same event_id, sent again 3 s later | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":2} | pos.sales.v1 | 4 | 1048577 | 2026-10-09 10:42:11.402 | 0 |
| bad: E5: no SKU, qty 0 | SG-ORCH-001 | {"event_id":"pos-SG001-T05-000201",…,"sku":null,"qty":0} | pos.sales.v1 | 4 | 1048590 | 2026-10-09 11:03:13.551 | 0 |
| late: E3: sold 16:58 UTC, sent 17:07 UTC after the store network came back | TH-BKK-007 | {"event_id":"pos-TH007-T01-002291",…} | pos.sales.v1 | 9 | 532118 | 2026-10-09 17:07:12.004 | 0 |

#### After

**retail.bronze.pos_events (streaming table)**

| column | type | tag | note |
|---|---|---|---|
| `kafka_key` | STRING |  |  |
| `payload` | STRING |  | raw JSON, untouched |
| `partition` | INT |  |  |
| `offset` | BIGINT |  |  |
| `kafka_ts` | TIMESTAMP |  |  |
| `_ingested_at` | TIMESTAMP | sys | when the platform wrote it |

| row note | kafka_key | payload | partition | offset | kafka_ts | _ingested_at |
|---|---|---|---|---|---|---|
| evo: still here | MY-KUL-014 | {"event_id":"pos-MY014-T02-000977",…} | 1 | 775310 | 2026-10-09 04:15:34.010 | 2026-10-09 04:15:36.2 |
|  | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":1} | 4 | 1048576 | 2026-10-09 10:42:08.120 | 2026-10-09 10:42:09.9 |
| dup: still here: bronze keeps everything | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":2} | 4 | 1048577 | 2026-10-09 10:42:11.402 | 2026-10-09 10:42:12.7 |
| bad: still here | SG-ORCH-001 | {"event_id":"pos-SG001-T05-000201",…} | 4 | 1048590 | 2026-10-09 11:03:13.551 | 2026-10-09 11:03:15.0 |
| late: still here | TH-BKK-007 | {"event_id":"pos-TH007-T01-002291",…} | 9 | 532118 | 2026-10-09 17:07:12.004 | 2026-10-09 17:07:13.8 |

#### 這一步改了什麼

Bronze 只做一件事：完整保存。重複（E2）、壞資料（E5）、晚到（E3）全部留著，payload 也還是原始 JSON 字串。好處是：日後清洗邏輯改了，可以從 bronze 重算 silver 和 gold，不必回頭找來源系統。

#### Code (SQL (Lakeflow pipeline))

```
CREATE OR REFRESH STREAMING TABLE retail.bronze.pos_events AS
SELECT CAST(key AS STRING)   AS kafka_key,
       CAST(value AS STRING) AS payload,
       partition, offset,
       timestamp             AS kafka_ts,
       current_timestamp()   AS _ingested_at
FROM STREAM read_kafka(
  bootstrapServers => '<brokers>',
  subscribe        => 'pos.sales.v1');
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=bronze&plat=dbx)

### On AWS: Raw (bronze): Raw Iceberg tables in S3 Tables

原封不動落地：每筆 Kafka 訊息變成表裡的一行，加上「從哪來、何時到」。

#### Before

**Topic pos.sales.v1, as read by Spark Structured Streaming**

| column | type | tag | note |
|---|---|---|---|
| `key` | BINARY |  | store_id bytes (shown decoded) |
| `value` | BINARY |  | the JSON payload bytes |
| `topic` | STRING |  |  |
| `partition` | INT |  | same key, same partition |
| `offset` | BIGINT |  | position inside the partition |
| `timestamp` | TIMESTAMP |  | producer time, UTC |
| `timestampType` | INT |  | 0 = CreateTime |

| row note | key | value | topic | partition | offset | timestamp | timestampType |
|---|---|---|---|---|---|---|---|
| evo: E4 carries the new loyalty_id field | MY-KUL-014 | {"event_id":"pos-MY014-T02-000977",…,"loyalty_id":"L-55821"} | pos.sales.v1 | 1 | 775310 | 2026-10-09 04:15:34.010 | 0 |
|  | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":1} | pos.sales.v1 | 4 | 1048576 | 2026-10-09 10:42:08.120 | 0 |
| dup: E2: same event_id, sent again 3 s later | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":2} | pos.sales.v1 | 4 | 1048577 | 2026-10-09 10:42:11.402 | 0 |
| bad: E5: no SKU, qty 0 | SG-ORCH-001 | {"event_id":"pos-SG001-T05-000201",…,"sku":null,"qty":0} | pos.sales.v1 | 4 | 1048590 | 2026-10-09 11:03:13.551 | 0 |
| late: E3: sold 16:58 UTC, sent 17:07 UTC after the store network came back | TH-BKK-007 | {"event_id":"pos-TH007-T01-002291",…} | pos.sales.v1 | 9 | 532118 | 2026-10-09 17:07:12.004 | 0 |

#### After

**s3tablescatalog/retail-lake.bronze.pos_events (Iceberg)**

| column | type | tag | note |
|---|---|---|---|
| `kafka_key` | string |  |  |
| `payload` | string |  | raw JSON, untouched |
| `partition` | int |  |  |
| `offset` | long |  |  |
| `kafka_ts` | timestamptz |  |  |
| `_ingested_at` | timestamptz | sys | when the platform wrote it |

| row note | kafka_key | payload | partition | offset | kafka_ts | _ingested_at |
|---|---|---|---|---|---|---|
| evo: still here | MY-KUL-014 | {"event_id":"pos-MY014-T02-000977",…} | 1 | 775310 | 2026-10-09 04:15:34.010 | 2026-10-09 04:15:36.2 |
|  | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":1} | 4 | 1048576 | 2026-10-09 10:42:08.120 | 2026-10-09 10:42:09.9 |
| dup: still here: bronze keeps everything | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":2} | 4 | 1048577 | 2026-10-09 10:42:11.402 | 2026-10-09 10:42:12.7 |
| bad: still here | SG-ORCH-001 | {"event_id":"pos-SG001-T05-000201",…} | 4 | 1048590 | 2026-10-09 11:03:13.551 | 2026-10-09 11:03:15.0 |
| late: still here | TH-BKK-007 | {"event_id":"pos-TH007-T01-002291",…} | 9 | 532118 | 2026-10-09 17:07:12.004 | 2026-10-09 17:07:13.8 |

#### 這一步改了什麼

AWS 上同一張表是 S3 Tables 裡的 Iceberg 表（型別名稱改用 Iceberg：string、long、timestamptz…），通常由 Amazon Data Firehose 從 MSK 直接寫入，或用 Glue streaming job。概念完全相同：原封不動保存、加中繼欄位。

#### Code (JSON (Firehose: MSK → Iceberg table in S3 Tables))

```
{
  "DeliveryStreamType": "MSKAsSource",
  "IcebergDestinationConfiguration": {
    "CatalogConfiguration": {
      "CatalogARN": "arn:aws:glue:ap-southeast-1:111122223333:catalog/s3tablescatalog/retail-lake" },
    "DestinationTableConfigurationList": [
      { "DestinationDatabaseName": "bronze", "DestinationTableName": "pos_events" } ]
  }
}
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=bronze&plat=aws)

<a id="step-8"></a>
## 8. Clean (silver): Silver: Lakeflow Spark Declarative Pipelines

### On Databricks: Clean (silver): Silver: Lakeflow Spark Declarative Pipelines

清洗與標準化：去重、丟壞資料、攤平巢狀、轉時區與型別。

#### Before

**retail.bronze.pos_events (streaming table)**

| column | type | tag | note |
|---|---|---|---|
| `kafka_key` | STRING |  |  |
| `payload` | STRING |  | raw JSON, untouched |
| `partition` | INT |  |  |
| `offset` | BIGINT |  |  |
| `kafka_ts` | TIMESTAMP |  |  |
| `_ingested_at` | TIMESTAMP | sys | when the platform wrote it |

| row note | kafka_key | payload | partition | offset | kafka_ts | _ingested_at |
|---|---|---|---|---|---|---|
| evo: still here | MY-KUL-014 | {"event_id":"pos-MY014-T02-000977",…} | 1 | 775310 | 2026-10-09 04:15:34.010 | 2026-10-09 04:15:36.2 |
|  | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":1} | 4 | 1048576 | 2026-10-09 10:42:08.120 | 2026-10-09 10:42:09.9 |
| dup: still here: bronze keeps everything | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":2} | 4 | 1048577 | 2026-10-09 10:42:11.402 | 2026-10-09 10:42:12.7 |
| bad: still here | SG-ORCH-001 | {"event_id":"pos-SG001-T05-000201",…} | 4 | 1048590 | 2026-10-09 11:03:13.551 | 2026-10-09 11:03:15.0 |
| late: still here | TH-BKK-007 | {"event_id":"pos-TH007-T01-002291",…} | 9 | 532118 | 2026-10-09 17:07:12.004 | 2026-10-09 17:07:13.8 |

#### After

**retail.silver.sales_lines (one row per product line)**

| column | type | tag | note |
|---|---|---|---|
| `event_id` | STRING |  |  |
| `line_no` | INT |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `qty` | INT |  |  |
| `unit_price` | DECIMAL(10,2) |  |  |
| `discount` | DECIMAL(10,2) |  |  |
| `net_amount` | DECIMAL(12,2) |  | qty × price − discount |
| `currency` | STRING |  |  |
| `event_ts_utc` | TIMESTAMP |  | local text + offset → UTC |
| `business_date` | DATE |  | the store's local date |
| `loyalty_id` | STRING |  | null for old POS firmware |

| row note | event_id | line_no | store_id | sku | qty | unit_price | discount | net_amount | currency | event_ts_utc | business_date | loyalty_id |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| evo: loyalty_id added by schema evolution | pos-MY014-T02-000977 | 1 | MY-KUL-014 | SKU-20871 | 3 | 7.50 | 4.50 | 18.00 | MYR | 2026-10-09 04:15:33 | 2026-10-09 | L-55821 |
|  | pos-SG001-T03-000184 | 1 | SG-ORCH-001 | SKU-20871 | 2 | 6.90 | 2.76 | 11.04 | SGD | 2026-10-09 10:42:07 | 2026-10-09 | *null* |
|  | pos-SG001-T03-000184 | 2 | SG-ORCH-001 | SKU-10023 | 1 | 3.95 | 0.00 | 3.95 | SGD | 2026-10-09 10:42:07 | 2026-10-09 | *null* |
| late: arrived after local midnight, still business_date 9 Oct (event time, not arrival time) | pos-TH007-T01-002291 | 1 | TH-BKK-007 | SKU-31045 | 1 | 289.00 | 0.00 | 289.00 | THB | 2026-10-09 16:58:40 | 2026-10-09 | *null* |

**Pipeline event log: data quality for this update**

| column | type | tag | note |
|---|---|---|---|
| `rule` | STRING |  |  |
| `rows_dropped` | BIGINT |  |  |
| `example` | STRING |  |  |

| row note | rule | rows_dropped | example |
|---|---|---|---|
| dup:  | dedupe on event_id | 1 | pos-SG001-T03-000184, send_attempt 2 |
| bad:  | valid_line: sku IS NOT NULL AND qty <> 0 | 1 | pos-SG001-T05-000201 |

**retail.silver.dc_stock (current stock, business names)**

| column | type | tag | note |
|---|---|---|---|
| `dc_id` | STRING | pk | WERKS 'SG01' → 'DC-SG-01' |
| `sku` | STRING | pk | MATNR '000000000000020871' → 'SKU-20871' |
| `on_hand_qty` | INT |  |  |
| `as_of` | TIMESTAMP |  |  |

| dc_id | sku | on_hand_qty | as_of |
|---|---|---|---|
| DC-SG-01 | SKU-20871 | 1180 | 2026-10-09 15:30:00 |
| DC-TH-02 | SKU-31045 | 96 | 2026-10-08 17:00:00 |
| DC-SG-01 | SKU-10023 | 3310 | 2026-10-09 09:10:00 |

#### 這一步改了什麼

Silver 依序做六件事：(1) 用 event_id 去重，E2 消失；(2) expectation 丟掉 E5 並計數，數字寫進 event log；(3) 把巢狀 lines 攤平成一行一個商品，E1 變兩行；(4) 當地時間字串加時區 → UTC timestamp，另外保留門店當地的 business_date，所以晚到的 E3 仍算 9 號；(5) 金額轉 DECIMAL；(6) loyalty_id 經 schema evolution 加入，舊資料為 null。ERP 庫存也在這層改成業務名稱（下方第二張表）。

#### Code (Python (Lakeflow pipeline))

```
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="retail.silver.sales_lines")
@dp.expect_or_drop("valid_line", "sku IS NOT NULL AND qty <> 0")
def sales_lines():
    e = (spark.readStream.table("retail.bronze.pos_events")
           .select(F.from_json("payload", POS_SCHEMA).alias("e")).select("e.*")
           .withColumn("event_ts_utc", F.to_timestamp(
               F.concat_ws(" ", "event_ts_local", "tz_offset"),
               "yyyy-MM-dd HH:mm:ss XXX"))
           .withWatermark("event_ts_utc", "2 hours")
           .dropDuplicatesWithinWatermark(["event_id"]))
    return (e.select("*", F.posexplode("lines").alias("pos", "l"))
             .select("event_id", (F.col("pos") + 1).alias("line_no"),
                     "store_id", "l.sku", "l.qty", "l.unit_price", "l.discount",
                     (F.col("l.qty") * F.col("l.unit_price") - F.col("l.discount"))
                         .cast("decimal(12,2)").alias("net_amount"),
                     "currency", "event_ts_utc",
                     F.to_date("event_ts_local").alias("business_date"),
                     "loyalty_id"))
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=silver&plat=dbx)

### On AWS: Clean (silver): AWS Glue ETL or EMR Serverless

清洗與標準化：去重、丟壞資料、攤平巢狀、轉時區與型別。

#### Before

**s3tablescatalog/retail-lake.bronze.pos_events (Iceberg)**

| column | type | tag | note |
|---|---|---|---|
| `kafka_key` | string |  |  |
| `payload` | string |  | raw JSON, untouched |
| `partition` | int |  |  |
| `offset` | long |  |  |
| `kafka_ts` | timestamptz |  |  |
| `_ingested_at` | timestamptz | sys | when the platform wrote it |

| row note | kafka_key | payload | partition | offset | kafka_ts | _ingested_at |
|---|---|---|---|---|---|---|
| evo: still here | MY-KUL-014 | {"event_id":"pos-MY014-T02-000977",…} | 1 | 775310 | 2026-10-09 04:15:34.010 | 2026-10-09 04:15:36.2 |
|  | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":1} | 4 | 1048576 | 2026-10-09 10:42:08.120 | 2026-10-09 10:42:09.9 |
| dup: still here: bronze keeps everything | SG-ORCH-001 | {"event_id":"pos-SG001-T03-000184",…,"send_attempt":2} | 4 | 1048577 | 2026-10-09 10:42:11.402 | 2026-10-09 10:42:12.7 |
| bad: still here | SG-ORCH-001 | {"event_id":"pos-SG001-T05-000201",…} | 4 | 1048590 | 2026-10-09 11:03:13.551 | 2026-10-09 11:03:15.0 |
| late: still here | TH-BKK-007 | {"event_id":"pos-TH007-T01-002291",…} | 9 | 532118 | 2026-10-09 17:07:12.004 | 2026-10-09 17:07:13.8 |

#### After

**s3tablescatalog/retail-lake.silver.sales_lines (one row per product line)**

| column | type | tag | note |
|---|---|---|---|
| `event_id` | string |  |  |
| `line_no` | int |  |  |
| `store_id` | string |  |  |
| `sku` | string |  |  |
| `qty` | int |  |  |
| `unit_price` | decimal(10,2) |  |  |
| `discount` | decimal(10,2) |  |  |
| `net_amount` | decimal(12,2) |  | qty × price − discount |
| `currency` | string |  |  |
| `event_ts_utc` | timestamptz |  | local text + offset → UTC |
| `business_date` | date |  | the store's local date |
| `loyalty_id` | string |  | null for old POS firmware |

| row note | event_id | line_no | store_id | sku | qty | unit_price | discount | net_amount | currency | event_ts_utc | business_date | loyalty_id |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| evo: loyalty_id added by schema evolution | pos-MY014-T02-000977 | 1 | MY-KUL-014 | SKU-20871 | 3 | 7.50 | 4.50 | 18.00 | MYR | 2026-10-09 04:15:33 | 2026-10-09 | L-55821 |
|  | pos-SG001-T03-000184 | 1 | SG-ORCH-001 | SKU-20871 | 2 | 6.90 | 2.76 | 11.04 | SGD | 2026-10-09 10:42:07 | 2026-10-09 | *null* |
|  | pos-SG001-T03-000184 | 2 | SG-ORCH-001 | SKU-10023 | 1 | 3.95 | 0.00 | 3.95 | SGD | 2026-10-09 10:42:07 | 2026-10-09 | *null* |
| late: arrived after local midnight, still business_date 9 Oct (event time, not arrival time) | pos-TH007-T01-002291 | 1 | TH-BKK-007 | SKU-31045 | 1 | 289.00 | 0.00 | 289.00 | THB | 2026-10-09 16:58:40 | 2026-10-09 | *null* |

**Glue Data Quality results (DQDL ruleset)**

| column | type | tag | note |
|---|---|---|---|
| `rule` | string |  |  |
| `outcome` | string |  |  |
| `failed_rows` | long |  |  |

| row note | rule | outcome | failed_rows |
|---|---|---|---|
| bad:  | IsComplete "sku" | Failed | 1 |
| bad:  | ColumnValues "qty" != 0 | Failed | 1 |

**s3tablescatalog/retail-lake.silver.dc_stock (current stock, business names)**

| column | type | tag | note |
|---|---|---|---|
| `dc_id` | string | pk | WERKS 'SG01' → 'DC-SG-01' |
| `sku` | string | pk | MATNR '000000000000020871' → 'SKU-20871' |
| `on_hand_qty` | int |  |  |
| `as_of` | timestamptz |  |  |

| dc_id | sku | on_hand_qty | as_of |
|---|---|---|---|
| DC-SG-01 | SKU-20871 | 1180 | 2026-10-09 15:30:00 |
| DC-TH-02 | SKU-31045 | 96 | 2026-10-08 17:00:00 |
| DC-SG-01 | SKU-10023 | 3310 | 2026-10-09 09:10:00 |

#### 這一步改了什麼

同樣的邏輯在 Glue 或 EMR Serverless 的 Spark 裡自己寫（dropDuplicates、explode、to_timestamp），寫進 S3 Tables 的 Iceberg 表。資料品質用 Glue Data Quality 的 DQDL 規則，結果另外存；要「丟掉壞行」得自己 filter，不像 expectation 一個 decorator 就同時擋下並計數。

#### Code (DQDL + PySpark (AWS Glue))

```
# Glue Data Quality ruleset
Rules = [ IsComplete "sku", ColumnValues "qty" != 0 ]

# Spark job (Glue / EMR Serverless)
df = (bronze.dropDuplicates(["event_id"])
            .withColumn("l", F.explode("lines"))
            .filter("l.sku IS NOT NULL AND l.qty <> 0"))
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=silver&plat=aws)

<a id="step-9"></a>
## 9. Curated (gold): Gold materialized views

### On Databricks: Curated (gold): Gold materialized views

業務口徑：一天 × 一店 × 一個 SKU 一行；再 join 促銷日曆與倉庫庫存。

#### Before

**retail.silver.sales_lines (one row per product line)**

| column | type | tag | note |
|---|---|---|---|
| `event_id` | STRING |  |  |
| `line_no` | INT |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `qty` | INT |  |  |
| `unit_price` | DECIMAL(10,2) |  |  |
| `discount` | DECIMAL(10,2) |  |  |
| `net_amount` | DECIMAL(12,2) |  | qty × price − discount |
| `currency` | STRING |  |  |
| `event_ts_utc` | TIMESTAMP |  | local text + offset → UTC |
| `business_date` | DATE |  | the store's local date |
| `loyalty_id` | STRING |  | null for old POS firmware |

| row note | event_id | line_no | store_id | sku | qty | unit_price | discount | net_amount | currency | event_ts_utc | business_date | loyalty_id |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| evo: loyalty_id added by schema evolution | pos-MY014-T02-000977 | 1 | MY-KUL-014 | SKU-20871 | 3 | 7.50 | 4.50 | 18.00 | MYR | 2026-10-09 04:15:33 | 2026-10-09 | L-55821 |
|  | pos-SG001-T03-000184 | 1 | SG-ORCH-001 | SKU-20871 | 2 | 6.90 | 2.76 | 11.04 | SGD | 2026-10-09 10:42:07 | 2026-10-09 | *null* |
|  | pos-SG001-T03-000184 | 2 | SG-ORCH-001 | SKU-10023 | 1 | 3.95 | 0.00 | 3.95 | SGD | 2026-10-09 10:42:07 | 2026-10-09 | *null* |
| late: arrived after local midnight, still business_date 9 Oct (event time, not arrival time) | pos-TH007-T01-002291 | 1 | TH-BKK-007 | SKU-31045 | 1 | 289.00 | 0.00 | 289.00 | THB | 2026-10-09 16:58:40 | 2026-10-09 | *null* |

#### After

**retail.gold.daily_store_sku (materialized view)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | DATE |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `units` | BIGINT |  |  |
| `net_revenue` | DECIMAL(14,2) |  |  |
| `currency` | STRING |  |  |
| `txn_count` | BIGINT |  |  |
| `is_promo` | BOOLEAN |  | joined from the promo calendar |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: includes E1's 2 units plus the rest of the day | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | 1026.72 | SGD | 141 | true |
|  | 2026-10-09 | SG-ORCH-001 | SKU-10023 | 64 | 252.80 | SGD | 60 | false |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | 1398.00 | MYR | 170 | true |
|  | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | 6069.00 | THB | 21 | false |

_Full-day totals. The four silver rows above are part of these numbers (for example E1 contributes 2 of the 186 units)._

**retail.gold.dc_stock_daily**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | DATE |  |  |
| `dc_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `on_hand_eod` | INT |  | end-of-day stock |
| `open_po_qty` | INT |  | ordered, not yet received |
| `served_stores` | INT |  |  |

| business_date | dc_id | sku | on_hand_eod | open_po_qty | served_stores |
|---|---|---|---|---|---|
| 2026-10-09 | DC-SG-01 | SKU-20871 | 1180 | 600 | 38 |
| 2026-10-09 | DC-SG-01 | SKU-10023 | 3310 | 0 | 38 |
| 2026-10-09 | DC-TH-02 | SKU-31045 | 96 | 0 | 22 |

#### 這一步改了什麼

Gold 是給人和模型用的「答案表」：從 silver 聚合出 units、營收、交易數，join 促銷日曆得到 is_promo；另一張是每個倉庫每天的期末庫存與在途採購。BI 儀表板和 ML 讀的是同一份 gold，數字才會一致。materialized view 會自動增量刷新。

#### Code (SQL (Lakeflow pipeline))

```
CREATE OR REFRESH MATERIALIZED VIEW retail.gold.daily_store_sku AS
SELECT s.business_date, s.store_id, s.sku,
       SUM(s.qty)                  AS units,
       SUM(s.net_amount)           AS net_revenue,
       ANY_VALUE(s.currency)       AS currency,
       COUNT(DISTINCT s.event_id)  AS txn_count,
       BOOL_OR(p.sku IS NOT NULL)  AS is_promo
FROM retail.silver.sales_lines s
LEFT JOIN retail.silver.promo_calendar p
  ON  p.sku = s.sku
  AND p.country = LEFT(s.store_id, 2)
  AND s.business_date BETWEEN p.start_date AND p.end_date
GROUP BY ALL;
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=gold&plat=dbx)

### On AWS: Curated (gold): Curated Iceberg tables

業務口徑：一天 × 一店 × 一個 SKU 一行；再 join 促銷日曆與倉庫庫存。

#### Before

**s3tablescatalog/retail-lake.silver.sales_lines (one row per product line)**

| column | type | tag | note |
|---|---|---|---|
| `event_id` | string |  |  |
| `line_no` | int |  |  |
| `store_id` | string |  |  |
| `sku` | string |  |  |
| `qty` | int |  |  |
| `unit_price` | decimal(10,2) |  |  |
| `discount` | decimal(10,2) |  |  |
| `net_amount` | decimal(12,2) |  | qty × price − discount |
| `currency` | string |  |  |
| `event_ts_utc` | timestamptz |  | local text + offset → UTC |
| `business_date` | date |  | the store's local date |
| `loyalty_id` | string |  | null for old POS firmware |

| row note | event_id | line_no | store_id | sku | qty | unit_price | discount | net_amount | currency | event_ts_utc | business_date | loyalty_id |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| evo: loyalty_id added by schema evolution | pos-MY014-T02-000977 | 1 | MY-KUL-014 | SKU-20871 | 3 | 7.50 | 4.50 | 18.00 | MYR | 2026-10-09 04:15:33 | 2026-10-09 | L-55821 |
|  | pos-SG001-T03-000184 | 1 | SG-ORCH-001 | SKU-20871 | 2 | 6.90 | 2.76 | 11.04 | SGD | 2026-10-09 10:42:07 | 2026-10-09 | *null* |
|  | pos-SG001-T03-000184 | 2 | SG-ORCH-001 | SKU-10023 | 1 | 3.95 | 0.00 | 3.95 | SGD | 2026-10-09 10:42:07 | 2026-10-09 | *null* |
| late: arrived after local midnight, still business_date 9 Oct (event time, not arrival time) | pos-TH007-T01-002291 | 1 | TH-BKK-007 | SKU-31045 | 1 | 289.00 | 0.00 | 289.00 | THB | 2026-10-09 16:58:40 | 2026-10-09 | *null* |

#### After

**s3tablescatalog/retail-lake.gold.daily_store_sku (Iceberg, refreshed by a scheduled job)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | date |  |  |
| `store_id` | string |  |  |
| `sku` | string |  |  |
| `units` | long |  |  |
| `net_revenue` | decimal(14,2) |  |  |
| `currency` | string |  |  |
| `txn_count` | long |  |  |
| `is_promo` | boolean |  | joined from the promo calendar |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: includes E1's 2 units plus the rest of the day | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | 1026.72 | SGD | 141 | true |
|  | 2026-10-09 | SG-ORCH-001 | SKU-10023 | 64 | 252.80 | SGD | 60 | false |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | 1398.00 | MYR | 170 | true |
|  | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | 6069.00 | THB | 21 | false |

_Full-day totals. The four silver rows above are part of these numbers (for example E1 contributes 2 of the 186 units)._

**s3tablescatalog/retail-lake.gold.dc_stock_daily**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | date |  |  |
| `dc_id` | string |  |  |
| `sku` | string |  |  |
| `on_hand_eod` | int |  | end-of-day stock |
| `open_po_qty` | int |  | ordered, not yet received |
| `served_stores` | int |  |  |

| business_date | dc_id | sku | on_hand_eod | open_po_qty | served_stores |
|---|---|---|---|---|---|
| 2026-10-09 | DC-SG-01 | SKU-20871 | 1180 | 600 | 38 |
| 2026-10-09 | DC-SG-01 | SKU-10023 | 3310 | 0 | 38 |
| 2026-10-09 | DC-TH-02 | SKU-31045 | 96 | 0 | 22 |

#### 這一步改了什麼

AWS 上同樣的 SQL 可以在 Athena 或 Glue Spark 跑，結果寫成 Iceberg 表；差別是刷新：沒有自動增量刷新的 materialized view，要自己排程 CTAS / MERGE INTO（或改用 Redshift 的 materialized view）。

#### Code (SQL (Athena, Iceberg))

```
MERGE INTO gold.daily_store_sku t
USING (SELECT business_date, store_id, sku, SUM(qty) AS units, …
       FROM silver.sales_lines WHERE business_date = DATE '2026-10-09'
       GROUP BY 1, 2, 3) s
ON  t.business_date = s.business_date AND t.store_id = s.store_id AND t.sku = s.sku
WHEN MATCHED THEN UPDATE SET units = s.units, …
WHEN NOT MATCHED THEN INSERT (business_date, store_id, sku, units, …)
                      VALUES (s.business_date, s.store_id, s.sku, s.units, …);
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=gold&plat=aws)

<a id="step-10"></a>
## 10. Curated (gold): Databricks SQL warehouse

### On Databricks: Curated (gold): Databricks SQL warehouse

BI 和 Genie 不直接讀檔案：一個 SQL 引擎對 gold 表跑查詢，權限照治理層。這裡問：促銷讓哪些店多賣了多少？

#### Before

**retail.gold.daily_store_sku (materialized view)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | DATE |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `units` | BIGINT |  |  |
| `net_revenue` | DECIMAL(14,2) |  |  |
| `currency` | STRING |  |  |
| `txn_count` | BIGINT |  |  |
| `is_promo` | BOOLEAN |  | joined from the promo calendar |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: includes E1's 2 units plus the rest of the day | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | 1026.72 | SGD | 141 | true |
|  | 2026-10-09 | SG-ORCH-001 | SKU-10023 | 64 | 252.80 | SGD | 60 | false |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | 1398.00 | MYR | 170 | true |
|  | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | 6069.00 | THB | 21 | false |

_Full-day totals. The four silver rows above are part of these numbers (for example E1 contributes 2 of the 186 units)._

#### After

**Dashboard query: promo uplift per store and SKU (run on a SQL warehouse)**

| column | type | tag | note |
|---|---|---|---|
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `units_today` | BIGINT |  | gold.daily_store_sku.units |
| `units_same_day_last_week` | INT |  | feature table: units_lag_7 |
| `uplift_pct` | DOUBLE |  | (today − last week) ÷ last week × 100 |
| `is_promo` | BOOLEAN |  |  |

| row note | store_id | sku | units_today | units_same_day_last_week | uplift_pct | is_promo |
|---|---|---|---|---|---|---|
| hl: the promo roughly doubles units; the agent quotes 92 → 186 later | SG-ORCH-001 | SKU-20871 | 186 | 92 | 102.2 | true |
|  | MY-KUL-014 | SKU-20871 | 233 | 118 | 97.5 | true |
|  | TH-BKK-007 | SKU-31045 | 21 | 19 | 10.5 | false |
|  | SG-ORCH-001 | SKU-10023 | 64 | 61 | 4.9 | false |

_Same numbers a planner sees in an AI/BI dashboard, in Genie, or in Power BI connected to the same warehouse._

#### 這一步改了什麼

SQL warehouse 是專門跑 SQL 和 BI 的運算：serverless、閒置自動停止，Photon 是預設引擎。不管查詢是從 AI/BI dashboard、Genie 還是 Power BI 發出，都套用 UC 的 row filter 與 column mask（見治理層），泰國採購看到的仍然只有 TH。每一筆查詢都記在 system tables，可以稽核誰查了什麼。

#### Code (SQL (Databricks SQL warehouse))

```
SELECT g.store_id, g.sku,
       g.units                                                   AS units_today,
       f.units_lag_7                                             AS units_same_day_last_week,
       ROUND(100.0 * (g.units - f.units_lag_7) / f.units_lag_7, 1) AS uplift_pct,
       g.is_promo
FROM retail.gold.daily_store_sku g
JOIN retail.features.store_sku_daily f USING (store_id, sku, business_date)
WHERE g.business_date = DATE'2026-10-09'
ORDER BY uplift_pct DESC;
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=sqlwh&plat=dbx)

### On AWS: Curated (gold): Amazon Athena or Redshift Serverless

BI 和 Genie 不直接讀檔案：一個 SQL 引擎對 gold 表跑查詢，權限照治理層。這裡問：促銷讓哪些店多賣了多少？

#### Before

**s3tablescatalog/retail-lake.gold.daily_store_sku (Iceberg, refreshed by a scheduled job)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | date |  |  |
| `store_id` | string |  |  |
| `sku` | string |  |  |
| `units` | long |  |  |
| `net_revenue` | decimal(14,2) |  |  |
| `currency` | string |  |  |
| `txn_count` | long |  |  |
| `is_promo` | boolean |  | joined from the promo calendar |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: includes E1's 2 units plus the rest of the day | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | 1026.72 | SGD | 141 | true |
|  | 2026-10-09 | SG-ORCH-001 | SKU-10023 | 64 | 252.80 | SGD | 60 | false |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | 1398.00 | MYR | 170 | true |
|  | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | 6069.00 | THB | 21 | false |

_Full-day totals. The four silver rows above are part of these numbers (for example E1 contributes 2 of the 186 units)._

#### After

**Dashboard query: promo uplift per store and SKU (Athena)**

| column | type | tag | note |
|---|---|---|---|
| `store_id` | string |  |  |
| `sku` | string |  |  |
| `units_today` | bigint |  |  |
| `units_same_day_last_week` | bigint |  | offline feature store: units_lag_7 |
| `uplift_pct` | double |  |  |
| `is_promo` | boolean |  |  |

| row note | store_id | sku | units_today | units_same_day_last_week | uplift_pct | is_promo |
|---|---|---|---|---|---|---|
| hl: same answer, different engine | SG-ORCH-001 | SKU-20871 | 186 | 92 | 102.2 | true |
|  | MY-KUL-014 | SKU-20871 | 233 | 118 | 97.5 | true |
|  | TH-BKK-007 | SKU-31045 | 21 | 19 | 10.5 | false |
|  | SG-ORCH-001 | SKU-10023 | 64 | 61 | 4.9 | false |

#### 這一步改了什麼

Athena 直接在 Iceberg 表上跑同一段 SQL，按掃描量計費；高並發的儀表板通常改用 Redshift Serverless。Lake Formation 的 data cells filter 一樣生效。差別在 join 的另一邊：AWS 的特徵在 SageMaker Feature Store 的 offline store（Glue 裡的一張表），key 是拼起來的 store_sku，而且保留每次寫入的歷史，查詢時要自己取最新一筆。

#### Code (SQL (Athena))

```
SELECT g.store_id, g.sku, g.units AS units_today,
       f.units_lag_7 AS units_same_day_last_week,
       ROUND(100.0 * (g.units - f.units_lag_7) / f.units_lag_7, 1) AS uplift_pct,
       g.is_promo
FROM gold.daily_store_sku g
JOIN "sagemaker_featurestore"."store_sku_daily_1728461102" f
  ON f.store_sku = g.store_id || '#' || g.sku
 AND f.is_deleted = false
WHERE g.business_date = DATE '2026-10-09'
ORDER BY uplift_pct DESC;
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=sqlwh&plat=aws)

<a id="step-11"></a>
## 11. Curated (gold): OpenSharing (formerly Delta Sharing)

### On Databricks: Curated (gold): OpenSharing (formerly Delta Sharing)

把 sell-through 分享給供應商：對方查的是活的表，不是每天寄一份 CSV。供應商 A 只該看到自己的 SKU，而且看不到營收。

#### Before

**retail.gold.daily_store_sku (materialized view)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | DATE |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `units` | BIGINT |  |  |
| `net_revenue` | DECIMAL(14,2) |  |  |
| `currency` | STRING |  |  |
| `txn_count` | BIGINT |  |  |
| `is_promo` | BOOLEAN |  | joined from the promo calendar |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: includes E1's 2 units plus the rest of the day | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | 1026.72 | SGD | 141 | true |
|  | 2026-10-09 | SG-ORCH-001 | SKU-10023 | 64 | 252.80 | SGD | 60 | false |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | 1398.00 | MYR | 170 | true |
|  | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | 6069.00 | THB | 21 | false |

_Full-day totals. The four silver rows above are part of these numbers (for example E1 contributes 2 of the 186 units)._

#### After

**What supplier A sees: retail.share.daily_sku_supplier_a (shared view)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | DATE |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `units` | BIGINT |  |  |
| `is_promo` | BOOLEAN |  |  |

| row note | business_date | store_id | sku | units | is_promo |
|---|---|---|---|---|---|
| hl: only SKU-20871; net_revenue, currency and txn_count are not shared | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | true |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | true |

**Share objects in Unity Catalog**

| column | type | tag | note |
|---|---|---|---|
| `object` | STRING |  |  |
| `name` | STRING |  |  |
| `detail` | STRING |  |  |

| object | name | detail |
|---|---|---|
| share | supplier_a | view retail.share.daily_sku_supplier_a |
| recipient | supplier_a_team | open sharing: activation link, no Databricks account needed |
| grant | SELECT ON SHARE supplier_a | to recipient supplier_a_team; revoke to stop sharing |

#### 這一步改了什麼

OpenSharing 分享的是 UC 裡的表或 view：這裡先用一個 view 只留供應商自己的 SKU、拿掉營收欄，再把 view 加進 share。對方讀到的永遠是最新版本，不必每天匯出；要停止就撤銷 recipient。對方如果也用 Databricks，表會直接出現在他自己的 Unity Catalog。

#### Code (SQL (Unity Catalog, OpenSharing))

```
CREATE VIEW retail.share.daily_sku_supplier_a AS
SELECT business_date, store_id, sku, units, is_promo
FROM retail.gold.daily_store_sku
WHERE sku = 'SKU-20871';

CREATE SHARE supplier_a;
ALTER SHARE supplier_a ADD VIEW retail.share.daily_sku_supplier_a;
CREATE RECIPIENT supplier_a_team;          -- open sharing: returns an activation link
GRANT SELECT ON SHARE supplier_a TO RECIPIENT supplier_a_team;
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=share&plat=dbx)

### On AWS: Curated (gold): Lake Formation cross-account sharing

把 sell-through 分享給供應商：對方查的是活的表，不是每天寄一份 CSV。供應商 A 只該看到自己的 SKU，而且看不到營收。

#### Before

**s3tablescatalog/retail-lake.gold.daily_store_sku (Iceberg, refreshed by a scheduled job)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | date |  |  |
| `store_id` | string |  |  |
| `sku` | string |  |  |
| `units` | long |  |  |
| `net_revenue` | decimal(14,2) |  |  |
| `currency` | string |  |  |
| `txn_count` | long |  |  |
| `is_promo` | boolean |  | joined from the promo calendar |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: includes E1's 2 units plus the rest of the day | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | 1026.72 | SGD | 141 | true |
|  | 2026-10-09 | SG-ORCH-001 | SKU-10023 | 64 | 252.80 | SGD | 60 | false |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | 1398.00 | MYR | 170 | true |
|  | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | 6069.00 | THB | 21 | false |

_Full-day totals. The four silver rows above are part of these numbers (for example E1 contributes 2 of the 186 units)._

#### After

**What supplier A's AWS account sees (resource link to the shared table)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | date |  |  |
| `store_id` | string |  |  |
| `sku` | string |  |  |
| `units` | bigint |  |  |
| `is_promo` | boolean |  |  |

| row note | business_date | store_id | sku | units | is_promo |
|---|---|---|---|---|---|
| hl: data cells filter: SKU-20871 rows, revenue columns excluded | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | true |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | true |

#### 這一步改了什麼

Lake Formation 可以把一張表或一個 data cells filter 授權給另一個 AWS 帳號，透過 AWS RAM 送達；對方建立 resource link 之後用 Athena 查。前提是供應商也在 AWS，而且雙方都要管理跨帳號授權。供應商不在 AWS 時，需要另找方式，例如排程匯出檔案。

#### Code (Python (boto3, Lake Formation))

```
lf.create_data_cells_filter(TableData={
    "TableCatalogId": ACCOUNT_ID, "DatabaseName": "gold",
    "TableName": "daily_store_sku", "Name": "supplier_a",
    "RowFilter": {"FilterExpression": "sku = 'SKU-20871'"},
    "ColumnNames": ["business_date", "store_id", "sku", "units", "is_promo"]})

lf.grant_permissions(
    Principal={"DataLakePrincipalIdentifier": SUPPLIER_ACCOUNT_ID},
    Resource={"DataCellsFilter": {"TableCatalogId": ACCOUNT_ID, "DatabaseName": "gold",
                                  "TableName": "daily_store_sku", "Name": "supplier_a"}},
    Permissions=["SELECT"])
# the supplier accepts the AWS RAM invitation, then creates a resource link for Athena
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=share&plat=aws)

<a id="step-12"></a>
## 12. Features: Feature Store in Unity Catalog

### On Databricks: Features: Feature Store in Unity Catalog

把 gold 變成模型要的特徵；每個特徵值都帶「當時」的日期。

#### Before

**retail.gold.daily_store_sku (materialized view)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | DATE |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `units` | BIGINT |  |  |
| `net_revenue` | DECIMAL(14,2) |  |  |
| `currency` | STRING |  |  |
| `txn_count` | BIGINT |  |  |
| `is_promo` | BOOLEAN |  | joined from the promo calendar |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: includes E1's 2 units plus the rest of the day | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | 1026.72 | SGD | 141 | true |
|  | 2026-10-09 | SG-ORCH-001 | SKU-10023 | 64 | 252.80 | SGD | 60 | false |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | 1398.00 | MYR | 170 | true |
|  | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | 6069.00 | THB | 21 | false |

_Full-day totals. The four silver rows above are part of these numbers (for example E1 contributes 2 of the 186 units)._

#### After

**retail.features.store_sku_daily (feature table)**

| column | type | tag | note |
|---|---|---|---|
| `store_id` | STRING | pk |  |
| `sku` | STRING | pk |  |
| `business_date` | DATE | ts | TIMESERIES key: enables point-in-time joins |
| `units_lag_7` | INT |  | same weekday last week |
| `units_mean_28` | DOUBLE |  | 28-day average |
| `dow` | INT |  | 1 = Monday |
| `is_promo` | BOOLEAN |  |  |
| `discount_pct` | DOUBLE |  |  |
| `promo_days_left` | INT |  |  |

| store_id | sku | business_date | units_lag_7 | units_mean_28 | dow | is_promo | discount_pct | promo_days_left |
|---|---|---|---|---|---|---|---|---|
| SG-ORCH-001 | SKU-20871 | 2026-10-09 | 92 | 88.4 | 5 | true | 20 | 3 |
| SG-ORCH-001 | SKU-10023 | 2026-10-09 | 61 | 58.9 | 5 | false | 0 | 0 |
| MY-KUL-014 | SKU-20871 | 2026-10-09 | 118 | 109.6 | 5 | true | 20 | 3 |
| TH-BKK-007 | SKU-31045 | 2026-10-09 | 19 | 17.3 | 5 | false | 0 | 0 |

#### 這一步改了什麼

Feature table 就是一張有主鍵的 Delta 表：(store_id, sku) 加上 business_date 這個 TIMESERIES 鍵。訓練時做 point-in-time join：每個樣本只拿到「當天以前」已知的特徵，不會偷看未來。模型會記住它用了哪些特徵，lineage 一路連回 gold。

#### Code (Python (Feature Engineering in Unity Catalog))

```
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
fe.create_table(
    name="retail.features.store_sku_daily",
    primary_keys=["store_id", "sku", "business_date"],
    timeseries_columns="business_date",
    df=features_df)
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=fs_off&plat=dbx)

### On AWS: Features: SageMaker Feature Store, offline

把 gold 變成模型要的特徵；每個特徵值都帶「當時」的日期。

#### Before

**s3tablescatalog/retail-lake.gold.daily_store_sku (Iceberg, refreshed by a scheduled job)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | date |  |  |
| `store_id` | string |  |  |
| `sku` | string |  |  |
| `units` | long |  |  |
| `net_revenue` | decimal(14,2) |  |  |
| `currency` | string |  |  |
| `txn_count` | long |  |  |
| `is_promo` | boolean |  | joined from the promo calendar |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: includes E1's 2 units plus the rest of the day | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | 1026.72 | SGD | 141 | true |
|  | 2026-10-09 | SG-ORCH-001 | SKU-10023 | 64 | 252.80 | SGD | 60 | false |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | 1398.00 | MYR | 170 | true |
|  | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | 6069.00 | THB | 21 | false |

_Full-day totals. The four silver rows above are part of these numbers (for example E1 contributes 2 of the 186 units)._

#### After

**Feature group store-sku-daily, offline store (Iceberg table format)**

| column | type | tag | note |
|---|---|---|---|
| `store_sku` | String | rid | record identifier: store and SKU joined |
| `event_time` | String | et | required event time |
| `units_lag_7` | Integral |  |  |
| `units_mean_28` | Fractional |  |  |
| `dow` | Integral |  |  |
| `is_promo` | Integral |  | no boolean type: 1 / 0 |
| `discount_pct` | Fractional |  |  |
| `promo_days_left` | Integral |  |  |
| `write_time` | timestamp | sys | added by Feature Store |
| `api_invocation_time` | timestamp | sys | added by Feature Store |
| `is_deleted` | boolean | sys | added by Feature Store |

| store_sku | event_time | units_lag_7 | units_mean_28 | dow | is_promo | discount_pct | promo_days_left | write_time | api_invocation_time | is_deleted |
|---|---|---|---|---|---|---|---|---|---|---|
| SG-ORCH-001#SKU-20871 | 2026-10-09T23:59:59+08:00 | 92 | 88.4 | 5 | 1 | 20 | 3 | 2026-10-09 16:12:40 | 2026-10-09 16:05:02 | false |
| SG-ORCH-001#SKU-10023 | 2026-10-09T23:59:59+08:00 | 61 | 58.9 | 5 | 0 | 0 | 0 | 2026-10-09 16:12:40 | 2026-10-09 16:05:02 | false |
| MY-KUL-014#SKU-20871 | 2026-10-09T23:59:59+08:00 | 118 | 109.6 | 5 | 1 | 20 | 3 | 2026-10-09 16:12:41 | 2026-10-09 16:05:03 | false |
| TH-BKK-007#SKU-31045 | 2026-10-09T23:59:59+07:00 | 19 | 17.3 | 5 | 0 | 0 | 0 | 2026-10-09 17:12:05 | 2026-10-09 17:05:01 | false |

#### 這一步改了什麼

SageMaker Feature Store 的模型不一樣：一個 record identifier（只能一個欄位，所以把店和 SKU 拼起來）加一個 event time；特徵型別只有 Integral、Fractional、String 三種，布林要轉 0/1；離線庫自動多三欄 write_time、api_invocation_time、is_deleted（PutRecord 後約 15 分鐘內寫入 S3）。

#### Code (Python (SageMaker Python SDK))

```
fg = FeatureGroup(name="store-sku-daily", sagemaker_session=sess)
fg.load_feature_definitions(data_frame=features_df)
fg.create(
    s3_uri="s3://retail-fs/offline",
    record_identifier_name="store_sku",
    event_time_feature_name="event_time",
    role_arn=ROLE,
    enable_online_store=True,
    table_format=TableFormatEnum.ICEBERG)
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=fs_off&plat=aws)

<a id="step-13"></a>
## 13. Raw (bronze): Structured Streaming, real-time mode

### On Databricks: Raw (bronze): Structured Streaming, real-time mode

從即時事件流算出「最近 15 / 60 分鐘賣了幾個」，直接寫進線上特徵庫。

#### Before

**Parsed sales lines, last hour (Saturday 10 Oct)**

| column | type | tag | note |
|---|---|---|---|
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `qty` | INT |  |  |
| `event_ts_utc` | TIMESTAMP |  |  |

| row note | store_id | sku | qty | event_ts_utc |
|---|---|---|---|---|
|  | SG-ORCH-001 | SKU-20871 | 2 | 2026-10-10 01:16:05 |
|  | SG-ORCH-001 | SKU-20871 | 1 | 2026-10-10 01:31:47 |
|  | SG-ORCH-001 | SKU-20871 | 3 | 2026-10-10 01:58:20 |
| hl: inside the last 15 min | SG-ORCH-001 | SKU-20871 | 2 | 2026-10-10 02:03:11 |
| hl:  | SG-ORCH-001 | SKU-20871 | 4 | 2026-10-10 02:09:40 |
| hl:  | SG-ORCH-001 | SKU-20871 | 1 | 2026-10-10 02:13:02 |

#### After

**Stream features for SG-ORCH-001 × SKU-20871**

| column | type | tag | note |
|---|---|---|---|
| `store_id` | STRING | pk |  |
| `sku` | STRING | pk |  |
| `window_end` | TIMESTAMP | ts |  |
| `units_15m` | INT |  | 2 + 4 + 1 |
| `units_60m` | INT |  | all six events |

| store_id | sku | window_end | units_15m | units_60m |
|---|---|---|---|---|
| SG-ORCH-001 | SKU-20871 | 2026-10-10 02:15:00 | 7 | 13 |

#### 這一步改了什麼

逐筆事件 → 滑動視窗聚合：每 5 分鐘滑一次，算最近 15 分鐘（7 個）與 60 分鐘（13 個）。Databricks 用 Structured Streaming（real-time mode）經 Stream Feature Views 直接寫進 Online Feature Store；同一個 Spark 引擎也做批次。

#### Code (Python (Structured Streaming))

```
from pyspark.sql import functions as F
feats = (spark.readStream.table("retail.silver.sales_lines")
    .withWatermark("event_ts_utc", "10 minutes")
    .groupBy("store_id", "sku",
             F.window("event_ts_utc", "15 minutes", "5 minutes"))
    .agg(F.sum("qty").alias("units_15m")))
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=rtc&plat=dbx)

### On AWS: Raw (bronze): Managed Service for Apache Flink

從即時事件流算出「最近 15 / 60 分鐘賣了幾個」，直接寫進線上特徵庫。

#### Before

**Parsed sales lines, last hour (Saturday 10 Oct)**

| column | type | tag | note |
|---|---|---|---|
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `qty` | INT |  |  |
| `event_ts_utc` | TIMESTAMP |  |  |

| row note | store_id | sku | qty | event_ts_utc |
|---|---|---|---|---|
|  | SG-ORCH-001 | SKU-20871 | 2 | 2026-10-10 01:16:05 |
|  | SG-ORCH-001 | SKU-20871 | 1 | 2026-10-10 01:31:47 |
|  | SG-ORCH-001 | SKU-20871 | 3 | 2026-10-10 01:58:20 |
| hl: inside the last 15 min | SG-ORCH-001 | SKU-20871 | 2 | 2026-10-10 02:03:11 |
| hl:  | SG-ORCH-001 | SKU-20871 | 4 | 2026-10-10 02:09:40 |
| hl:  | SG-ORCH-001 | SKU-20871 | 1 | 2026-10-10 02:13:02 |

#### After

**Feature Store record written by Flink (PutRecord)**

| column | type | tag | note |
|---|---|---|---|
| `store_sku` | String | rid | record identifier = store#sku |
| `event_time` | String | et | required event time |
| `units_15m` | Integral |  |  |
| `units_60m` | Integral |  |  |

| store_sku | event_time | units_15m | units_60m |
|---|---|---|---|
| SG-ORCH-001#SKU-20871 | 2026-10-10T02:15:00Z | 7 | 13 |

#### 這一步改了什麼

AWS 常見是 Managed Service for Apache Flink 算滑動視窗，再呼叫 PutRecord 寫進 Feature Store。注意兩件事：Feature Store 要一個 record identifier（這裡把店和 SKU 拼成 store_sku）和一個 event time；批次用 Spark、串流用 Flink，是兩套引擎。

#### Code (SQL (Flink SQL on Managed Flink))

```
SELECT store_id, sku, window_end, SUM(qty) AS units_15m
FROM TABLE(
  HOP(TABLE pos_lines, DESCRIPTOR(event_ts),
      INTERVAL '5' MINUTES, INTERVAL '15' MINUTES))
GROUP BY store_id, sku, window_start, window_end;
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=rtc&plat=aws)

<a id="step-14"></a>
## 14. Features: Online Feature Store

### On Databricks: Features: Online Feature Store

線上特徵庫：每個 key 只留最新一筆，給毫秒級查詢。

#### Before

**Inputs to the online store**

| column | type | tag | note |
|---|---|---|---|
| `source` | STRING |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `latest values` | STRING |  |  |

| source | store_id | sku | latest values |
|---|---|---|---|
| feature table (nightly) | SG-ORCH-001 | SKU-20871 | units_lag_7 97, units_mean_28 89.6, is_promo true, promo_days_left 2 |
| stream features (step: real-time) | SG-ORCH-001 | SKU-20871 | units_15m 7, units_60m 13 at 02:15 UTC |

#### After

**Online Feature Store (Lakebase Postgres): one row per key**

| column | type | tag | note |
|---|---|---|---|
| `store_id` | STRING | pk |  |
| `sku` | STRING | pk |  |
| `units_lag_7` | INT |  |  |
| `units_mean_28` | DOUBLE |  |  |
| `is_promo` | BOOLEAN |  |  |
| `promo_days_left` | INT |  |  |
| `units_15m` | INT |  |  |
| `units_60m` | INT |  |  |
| `_updated_at` | TIMESTAMP | sys |  |

| store_id | sku | units_lag_7 | units_mean_28 | is_promo | promo_days_left | units_15m | units_60m | _updated_at |
|---|---|---|---|---|---|---|---|---|
| SG-ORCH-001 | SKU-20871 | 97 | 89.6 | true | 2 | 7 | 13 | 2026-10-10 02:15:04 |

#### 這一步改了什麼

離線 feature table 給訓練用、線上副本給服務用，兩者來自同一份定義——避免 training/serving skew（訓練與上線算法不一致）。線上庫由 Lakebase 驅動，本質上是 Postgres，所以也能直接用 SQL 查。

#### Code (SQL (Postgres, Lakebase))

```
SELECT units_lag_7, units_mean_28, is_promo, units_15m, units_60m
FROM store_sku_daily_online
WHERE store_id = 'SG-ORCH-001' AND sku = 'SKU-20871';
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=fs_on&plat=dbx)

### On AWS: Features: SageMaker Feature Store, online

線上特徵庫：每個 key 只留最新一筆，給毫秒級查詢。

#### Before

**Inputs to the online store**

| column | type | tag | note |
|---|---|---|---|
| `source` | STRING |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `latest values` | STRING |  |  |

| source | store_id | sku | latest values |
|---|---|---|---|
| feature table (nightly) | SG-ORCH-001 | SKU-20871 | units_lag_7 97, units_mean_28 89.6, is_promo true, promo_days_left 2 |
| stream features (step: real-time) | SG-ORCH-001 | SKU-20871 | units_15m 7, units_60m 13 at 02:15 UTC |

#### After

**GetRecord response**

| column | type | tag | note |
|---|---|---|---|
| `Record` | list<struct<FeatureName,ValueAsString>> |  | every value is a string |

- All values come back as strings

```json
{
  "Record": [
    {
      "FeatureName": "store_sku",
      "ValueAsString": "SG-ORCH-001#SKU-20871"
    },
    {
      "FeatureName": "event_time",
      "ValueAsString": "2026-10-10T10:15:00+08:00"
    },
    {
      "FeatureName": "units_lag_7",
      "ValueAsString": "97"
    },
    {
      "FeatureName": "units_mean_28",
      "ValueAsString": "89.6"
    },
    {
      "FeatureName": "is_promo",
      "ValueAsString": "1"
    },
    {
      "FeatureName": "promo_days_left",
      "ValueAsString": "2"
    },
    {
      "FeatureName": "units_15m",
      "ValueAsString": "7"
    },
    {
      "FeatureName": "units_60m",
      "ValueAsString": "13"
    }
  ]
}
```

#### 這一步改了什麼

線上庫同樣只保留最新 event time 的那筆。GetRecord 回來的是 FeatureName / ValueAsString 的清單，數字也是字串，呼叫端要自己轉型。

#### Code (Python (boto3))

```
fs = boto3.client("sagemaker-featurestore-runtime")
rec = fs.get_record(
    FeatureGroupName="store-sku-daily",
    RecordIdentifierValueAsString="SG-ORCH-001#SKU-20871")
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=fs_on&plat=aws)

<a id="step-15"></a>
## 15. Train and register: Many-models training

### On Databricks: Train and register: Many-models training

Many-models：每個 店 × SKU 序列各訓練一個小模型，平行跑。

#### Before

**Training set for one series (SG-ORCH-001 × SKU-20871), last 4 of 730 days**

| column | type | tag | note |
|---|---|---|---|
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `business_date` | DATE |  |  |
| `units` | BIGINT |  | label |
| `units_lag_7` | INT |  |  |
| `units_mean_28` | DOUBLE |  |  |
| `is_promo` | BOOLEAN |  |  |
| `dow` | INT |  |  |

| row note | store_id | sku | business_date | units | units_lag_7 | units_mean_28 | is_promo | dow |
|---|---|---|---|---|---|---|---|---|
|  | SG-ORCH-001 | SKU-20871 | 2026-10-06 | 84 | 90 | 87.9 | false | 2 |
|  | SG-ORCH-001 | SKU-20871 | 2026-10-07 | 88 | 86 | 88 | false | 3 |
|  | SG-ORCH-001 | SKU-20871 | 2026-10-08 | 91 | 89 | 88.1 | false | 4 |
| chg: promo day: units double | SG-ORCH-001 | SKU-20871 | 2026-10-09 | 186 | 92 | 88.4 | true | 5 |

#### After

**applyInPandas output: one row per series**

| column | type | tag | note |
|---|---|---|---|
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `n_train_days` | INT |  |  |
| `wmape_holdout` | DOUBLE |  |  |
| `model_path` | STRING |  |  |

| row note | store_id | sku | n_train_days | wmape_holdout | model_path |
|---|---|---|---|---|---|
|  | SG-ORCH-001 | SKU-20871 | 730 | 0.142 | …/series/SG-ORCH-001/SKU-20871.pkl |
|  | SG-ORCH-001 | SKU-10023 | 730 | 0.118 | …/series/SG-ORCH-001/SKU-10023.pkl |
|  | MY-KUL-014 | SKU-20871 | 512 | 0.171 | …/series/MY-KUL-014/SKU-20871.pkl |
| drift: slow mover: higher error is normal | TH-BKK-007 | SKU-31045 | 730 | 0.264 | …/series/TH-BKK-007/SKU-31045.pkl |

_48,210 rows in the real run, one per series._

#### 這一步改了什麼

groupBy(store, sku) 把 4.8 萬條序列分給 Spark 的各個 worker，每條各自訓練。applyInPandas 必須先宣告輸出 schema（上表五欄）——這就是 schema 從「訓練資料」變成「訓練結果」的地方。實務上會把所有小模型包成一個 MLflow pyfunc「路由模型」再註冊，上線時依 store、sku 選對的小模型。

#### Code (Python (PySpark))

```
schema = "store_id string, sku string, n_train_days int, wmape_holdout double, model_path string"

results = (train_df.groupBy("store_id", "sku")
                   .applyInPandas(fit_one_series, schema=schema))
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=train&plat=dbx)

### On AWS: Train and register: SageMaker AI training jobs

Many-models：每個 店 × SKU 序列各訓練一個小模型，平行跑。

#### Before

**Training set for one series (SG-ORCH-001 × SKU-20871), last 4 of 730 days**

| column | type | tag | note |
|---|---|---|---|
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `business_date` | DATE |  |  |
| `units` | BIGINT |  | label |
| `units_lag_7` | INT |  |  |
| `units_mean_28` | DOUBLE |  |  |
| `is_promo` | BOOLEAN |  |  |
| `dow` | INT |  |  |

| row note | store_id | sku | business_date | units | units_lag_7 | units_mean_28 | is_promo | dow |
|---|---|---|---|---|---|---|---|---|
|  | SG-ORCH-001 | SKU-20871 | 2026-10-06 | 84 | 90 | 87.9 | false | 2 |
|  | SG-ORCH-001 | SKU-20871 | 2026-10-07 | 88 | 86 | 88 | false | 3 |
|  | SG-ORCH-001 | SKU-20871 | 2026-10-08 | 91 | 89 | 88.1 | false | 4 |
| chg: promo day: units double | SG-ORCH-001 | SKU-20871 | 2026-10-09 | 186 | 92 | 88.4 | true | 5 |

#### After

**DescribeTrainingJob (excerpt)**

| column | type | tag | note |
|---|---|---|---|
| `TrainingJobName` | string |  |  |
| `TrainingJobStatus` | string |  |  |
| `ModelArtifacts.S3ModelArtifacts` | string |  | model.tar.gz in S3 |
| `FinalMetricDataList` | list |  | parsed from logs by regex |
| `BillableTimeInSeconds` | int |  |  |


```json
{
  "TrainingJobName": "demand-lgbm-sea-20261009",
  "TrainingJobStatus": "Completed",
  "ModelArtifacts": {
    "S3ModelArtifacts": "s3://retail-ml/models/demand-lgbm-sea-20261009/output/model.tar.gz"
  },
  "FinalMetricDataList": [
    {
      "MetricName": "validation:wmape",
      "Value": 0.187
    }
  ],
  "BillableTimeInSeconds": 2310
}
```

#### 這一步改了什麼

SageMaker 的單位是 training job：你的腳本在容器裡自己 fan-out（或拆成多個 job），產物是 S3 上的 model.tar.gz，指標靠 regex 從日誌抓。每條序列的結果表要腳本自己寫到 S3。低程式碼可改用 SageMaker Canvas 的時間序列預測（以 item ID 為單位）；Amazon Forecast 已不開放新客戶。

#### Code (Python (SageMaker Python SDK))

```
from sagemaker.sklearn import SKLearn
est = SKLearn(entry_point="train_many.py", framework_version="1.2-1",
              role=ROLE, instance_type="ml.m5.4xlarge", instance_count=4,
              metric_definitions=[{"Name": "validation:wmape",
                                   "Regex": "wmape=([0-9.]+)"}])
est.fit({"train": "s3://retail-ml/train/2026-10-09/"})
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=train&plat=aws)

<a id="step-16"></a>
## 16. Train and register: MLflow 3, managed

### On Databricks: Train and register: MLflow 3, managed

一次訓練 = 一個 MLflow run：記下參數、指標和產物。

#### Before

**Training configuration**

| column | type | tag | note |
|---|---|---|---|
| `model_type` | string |  |  |
| `objective` | string |  |  |
| `num_leaves` | int |  |  |
| `learning_rate` | double |  |  |
| `horizon_days` | int |  |  |
| `train_window_days` | int |  |  |
| `series` | int |  |  |


```json
{
  "model_type": "lightgbm",
  "objective": "tweedie",
  "num_leaves": 31,
  "learning_rate": 0.05,
  "horizon_days": 30,
  "train_window_days": 730,
  "series": 48210
}
```

#### After

**MLflow run in experiment /Shared/retail/demand**

| column | type | tag | note |
|---|---|---|---|
| `field` | STRING |  |  |
| `value` | STRING |  |  |

| row note | field | value |
|---|---|---|
|  | run_id | 4f2c9d1e8a7b4c06 |
|  | status | FINISHED |
|  | params.num_leaves | 31 |
|  | params.learning_rate | 0.05 |
|  | params.horizon_days | 30 |
| hl: the number the gate compares | metrics.wmape_holdout | 0.187 |
|  | metrics.bias | -0.021 |
|  | metrics.series_failed | 12 |
|  | tags.region | SEA |
|  | artifacts | model/, per_series_metrics.parquet, wmape_by_store.png |

#### 這一步改了什麼

兩邊都是 MLflow，所以這一格幾乎一樣：參數、指標、產物、tag。wMAPE（加權平均絕對百分比誤差）是零售預測最常用的指標。差別只在 tracking server 在哪：Databricks 內建，和 Unity Catalog 原生整合。

#### Code (Python (MLflow))

```
mlflow.set_experiment("/Shared/retail/demand")
with mlflow.start_run():
    mlflow.log_params({"num_leaves": 31, "learning_rate": 0.05, "horizon_days": 30})
    mlflow.log_metric("wmape_holdout", 0.187)
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=mlflow&plat=dbx)

### On AWS: Train and register: Managed MLflow on SageMaker AI

一次訓練 = 一個 MLflow run：記下參數、指標和產物。

#### Before

**Training configuration**

| column | type | tag | note |
|---|---|---|---|
| `model_type` | string |  |  |
| `objective` | string |  |  |
| `num_leaves` | int |  |  |
| `learning_rate` | double |  |  |
| `horizon_days` | int |  |  |
| `train_window_days` | int |  |  |
| `series` | int |  |  |


```json
{
  "model_type": "lightgbm",
  "objective": "tweedie",
  "num_leaves": 31,
  "learning_rate": 0.05,
  "horizon_days": 30,
  "train_window_days": 730,
  "series": 48210
}
```

#### After

**MLflow run in experiment /Shared/retail/demand**

| column | type | tag | note |
|---|---|---|---|
| `field` | STRING |  |  |
| `value` | STRING |  |  |

| row note | field | value |
|---|---|---|
|  | run_id | 4f2c9d1e8a7b4c06 |
|  | status | FINISHED |
|  | params.num_leaves | 31 |
|  | params.learning_rate | 0.05 |
|  | params.horizon_days | 30 |
| hl: the number the gate compares | metrics.wmape_holdout | 0.187 |
|  | metrics.bias | -0.021 |
|  | metrics.series_failed | 12 |
|  | tags.region | SEA |
|  | artifacts | model/, per_series_metrics.parquet, wmape_by_store.png |

#### 這一步改了什麼

同一個 MLflow API；tracking server 是 SageMaker AI 的 MLflow App，用它的 ARN 當 tracking URI。模型可設定自動註冊到 SageMaker Model Registry。

#### Code (Python (MLflow on SageMaker AI))

```
mlflow.set_tracking_uri(
    "arn:aws:sagemaker:ap-southeast-1:111122223333:mlflow-app/app-EXAMPLE")
mlflow.set_experiment("retail-demand")
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=mlflow&plat=aws)

<a id="step-17"></a>
## 17. Train and register: Models in Unity Catalog

### On Databricks: Train and register: Models in Unity Catalog

把訓練好的模型註冊成版本，用 alias 決定誰上線。

#### Before

**Logged model from run 4f2c9d1e8a7b4c06**

| column | type | tag | note |
|---|---|---|---|
| `artifact_path` | string |  |  |
| `flavor` | string |  |  |
| `signature` | struct<inputs,outputs> |  |  |


```json
{
  "artifact_path": "model",
  "flavor": "python_function",
  "signature": {
    "inputs": "store_id, sku, units_lag_7, units_mean_28, is_promo, …",
    "outputs": "p10, p50, p90"
  }
}
```

#### After

**retail.ml.demand_forecaster: model versions in Unity Catalog**

| column | type | tag | note |
|---|---|---|---|
| `version` | INT |  |  |
| `aliases` | ARRAY<STRING> |  |  |
| `run_id` | STRING |  |  |
| `wmape_holdout` | DOUBLE |  |  |
| `created_at` | TIMESTAMP |  |  |
| `status` | STRING |  |  |

| row note | version | aliases | run_id | wmape_holdout | created_at | status |
|---|---|---|---|---|---|---|
|  | 6 | [] | 9b1e07c2… | 0.201 | 2026-10-02 18:20:00 | READY |
| hl: serving now | 7 | ["champion"] | 4f2c9d1e… | 0.187 | 2026-10-09 18:05:00 | READY |
| evo: retrained after the drift alert (feedback loop) | 8 | ["challenger"] | c7a03f55… | 0.183 | 2026-10-10 03:05:00 | READY |

#### 這一步改了什麼

模型名稱和表一樣是三段式（catalog.schema.model），版本號自動遞增。上線靠 alias：endpoint 指向 @champion；換版就是把 alias 指到 8，回滾就是指回 7。權限與 lineage 和資料表同一套。

#### Code (Python (MLflow + Unity Catalog))

```
mlflow.set_registry_uri("databricks-uc")
mv = mlflow.register_model(f"runs:/{run_id}/model", "retail.ml.demand_forecaster")
MlflowClient().set_registered_model_alias(
    "retail.ml.demand_forecaster", "challenger", mv.version)
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=registry&plat=dbx)

### On AWS: Train and register: SageMaker Model Registry

把訓練好的模型註冊成版本，用 alias 決定誰上線。

#### Before

**Logged model from run 4f2c9d1e8a7b4c06**

| column | type | tag | note |
|---|---|---|---|
| `artifact_path` | string |  |  |
| `flavor` | string |  |  |
| `signature` | struct<inputs,outputs> |  |  |


```json
{
  "artifact_path": "model",
  "flavor": "python_function",
  "signature": {
    "inputs": "store_id, sku, units_lag_7, units_mean_28, is_promo, …",
    "outputs": "p10, p50, p90"
  }
}
```

#### After

**Model package group demand-forecaster**

| column | type | tag | note |
|---|---|---|---|
| `ModelPackageVersion` | int |  |  |
| `ModelApprovalStatus` | string |  | the deployment gate |
| `ModelPackageArn` | string |  |  |
| `CreationTime` | timestamp |  |  |

| row note | ModelPackageVersion | ModelApprovalStatus | ModelPackageArn | CreationTime |
|---|---|---|---|---|
|  | 6 | Approved | arn:aws:sagemaker:…:model-package/demand-forecaster/6 | 2026-10-02 18:20:00 |
| hl: deployed | 7 | Approved | arn:aws:sagemaker:…:model-package/demand-forecaster/7 | 2026-10-09 18:05:00 |
| evo: waiting for a human | 8 | PendingManualApproval | arn:aws:sagemaker:…:model-package/demand-forecaster/8 | 2026-10-10 03:05:00 |

#### 這一步改了什麼

SageMaker Model Registry 用 model package group 管版本；上線閘門是 ModelApprovalStatus（PendingManualApproval → Approved），通常由 EventBridge 事件觸發部署。權限走 IAM，和資料權限（Lake Formation）是兩套。

#### Code (Python (boto3))

```
sm.update_model_package(
    ModelPackageArn=arn_v8,
    ModelApprovalStatus="Approved")   # EventBridge rule triggers the deploy
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=registry&plat=aws)

<a id="step-18"></a>
## 18. Serve: Batch inference job

### On Databricks: Serve: Batch inference job

每晚對每個倉庫 × SKU × 未來 30 天打分，結果寫成一張表。

#### Before

**Scoring input for tonight (DC level)**

| column | type | tag | note |
|---|---|---|---|
| `dc_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `forecast_date` | DATE |  |  |
| `horizon_day` | INT |  |  |
| `units_mean_28_dc` | DOUBLE |  |  |
| `is_promo` | BOOLEAN |  |  |
| `promo_days_left` | INT |  |  |

| dc_id | sku | forecast_date | horizon_day | units_mean_28_dc | is_promo | promo_days_left |
|---|---|---|---|---|---|---|
| DC-SG-01 | SKU-20871 | 2026-10-10 | 1 | 412.5 | true | 2 |
| DC-SG-01 | SKU-20871 | 2026-10-13 | 4 | 412.5 | false | 0 |
| DC-TH-02 | SKU-31045 | 2026-10-10 | 1 | 58.1 | false | 0 |

#### After

**retail.gold.dc_sku_forecast**

| column | type | tag | note |
|---|---|---|---|
| `dc_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `forecast_date` | DATE |  |  |
| `horizon_day` | INT |  |  |
| `p10` | DOUBLE |  |  |
| `p50` | DOUBLE |  | median: used for ordering |
| `p90` | DOUBLE |  | high case: used for risk |
| `model_version` | INT |  |  |
| `scored_at` | TIMESTAMP |  |  |

| row note | dc_id | sku | forecast_date | horizon_day | p10 | p50 | p90 | model_version | scored_at |
|---|---|---|---|---|---|---|---|---|---|
|  | DC-SG-01 | SKU-20871 | 2026-10-10 | 1 | 398 | 452 | 521 | 7 | 2026-10-09 18:30:00 |
|  | DC-SG-01 | SKU-20871 | 2026-10-11 | 2 | 410 | 468 | 540 | 7 | 2026-10-09 18:30:00 |
|  | DC-SG-01 | SKU-20871 | 2026-10-12 | 3 | 385 | 441 | 509 | 7 | 2026-10-09 18:30:00 |
| chg: promo ends 12 Oct: demand drops | DC-SG-01 | SKU-20871 | 2026-10-13 | 4 | 240 | 288 | 345 | 7 | 2026-10-09 18:30:00 |
|  | DC-TH-02 | SKU-31045 | 2026-10-10 | 1 | 48 | 63 | 81 | 7 | 2026-10-09 18:30:00 |

_30 rows per DC × SKU in reality (horizon 1–30); 5 shown._

#### 這一步改了什麼

輸出不是一個數字，而是分位數：p10 / p50 / p90。補貨用 p50 加安全庫存，缺貨風險看 p90。注意 13 號促銷結束，預測從 441 掉到 288——模型學到了促銷效應。這張 gold 表會同時被 ERP、儀表板和 agent 讀取。

#### Code (Python (batch scoring in a Lakeflow Jobs task))

```
predict = mlflow.pyfunc.spark_udf(
    spark, "models:/retail.ml.demand_forecaster@champion")
scored = score_df.withColumn("pred", predict(*feature_cols))
scored.write.mode("overwrite").saveAsTable("retail.gold.dc_sku_forecast")
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=batch&plat=dbx)

### On AWS: Serve: SageMaker Batch Transform

每晚對每個倉庫 × SKU × 未來 30 天打分，結果寫成一張表。

#### Before

**Scoring input for tonight (DC level)**

| column | type | tag | note |
|---|---|---|---|
| `dc_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `forecast_date` | DATE |  |  |
| `horizon_day` | INT |  |  |
| `units_mean_28_dc` | DOUBLE |  |  |
| `is_promo` | BOOLEAN |  |  |
| `promo_days_left` | INT |  |  |

| dc_id | sku | forecast_date | horizon_day | units_mean_28_dc | is_promo | promo_days_left |
|---|---|---|---|---|---|---|
| DC-SG-01 | SKU-20871 | 2026-10-10 | 1 | 412.5 | true | 2 |
| DC-SG-01 | SKU-20871 | 2026-10-13 | 4 | 412.5 | false | 0 |
| DC-TH-02 | SKU-31045 | 2026-10-10 | 1 | 58.1 | false | 0 |

#### After

**s3://retail-ml/batch-out/2026-10-09/part-00000.csv.out**

| column | type | tag | note |
|---|---|---|---|
| `dc_id` | text |  | joined back from the input |
| `sku` | text |  |  |
| `forecast_date` | text |  |  |
| `horizon_day` | text |  |  |
| `p10` | text |  |  |
| `p50` | text |  |  |
| `p90` | text |  |  |

```text
DC-SG-01,SKU-20871,2026-10-10,1,398,452,521
DC-SG-01,SKU-20871,2026-10-11,2,410,468,540
DC-SG-01,SKU-20871,2026-10-12,3,385,441,509
DC-SG-01,SKU-20871,2026-10-13,4,240,288,345
DC-TH-02,SKU-31045,2026-10-10,1,48,63,81
```

#### 這一步改了什麼

Batch Transform 讀 S3 上的輸入檔、輸出同名加 .out 的檔案。join_source='Input' 把輸入的 ID 欄位接回輸出，否則只拿到一串預測值、對不回是哪個倉庫哪個 SKU。之後再載入 Iceberg 或 Redshift 才變成「表」。

#### Code (Python (SageMaker Python SDK))

```
tf = model.transformer(instance_count=2, instance_type="ml.m5.xlarge",
                       output_path="s3://retail-ml/batch-out/2026-10-09/",
                       accept="text/csv")
tf.transform("s3://retail-ml/batch-in/2026-10-09/", content_type="text/csv",
             split_type="Line", join_source="Input")
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=batch&plat=aws)

<a id="step-19"></a>
## 19. Use: Warehouse inventory forecast

### On Databricks: Use: Warehouse inventory forecast

預測 + 庫存 + 在途 + 安全庫存 → 建議採購量，送回 ERP 等人核准。

#### Before

**Inputs for DC-SG-01 × SKU-20871**

| column | type | tag | note |
|---|---|---|---|
| `item` | STRING |  |  |
| `value` | STRING |  |  |

| item | value |
|---|---|
| on_hand_eod (gold) | 1180 |
| open_po_qty (gold) | 600 |
| p50, next 7 days (forecast) | 452 + 468 + 441 + 288 + 275 + 262 + 270 = 2456 |
| safety_stock (policy) | 420 |
| case_pack (supplier terms) | 24 |

#### After

**Suggested purchase order sent to ERP**

| column | type | tag | note |
|---|---|---|---|
| `dc_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `suggested_qty` | INT |  | 2456 + 420 − 1180 − 600 |
| `order_qty` | INT |  | rounded up to 46 cases × 24 |
| `need_by` | DATE |  |  |
| `status` | STRING |  |  |

| row note | dc_id | sku | suggested_qty | order_qty | need_by | status |
|---|---|---|---|---|---|---|
| hl: a planner approves in the ERP | DC-SG-01 | SKU-20871 | 1096 | 1104 | 2026-10-13 | PENDING_APPROVAL |

#### 這一步改了什麼

預測本身不下單。建議量 = 未來 7 天需求 + 安全庫存 − 現有庫存 − 在途 = 1096，再按箱規 24 往上取整成 1104（46 箱）。ERP 仍是下單系統，平台只送「建議」，由採購核准。

#### Code (SQL)

```
SELECT f.dc_id, f.sku,
       SUM(f.p50) + 420 - s.on_hand_eod - s.open_po_qty            AS suggested_qty,
       CEIL((SUM(f.p50) + 420 - s.on_hand_eod - s.open_po_qty) / 24) * 24 AS order_qty
FROM retail.gold.dc_sku_forecast f
JOIN retail.gold.dc_stock_daily s USING (dc_id, sku)
WHERE f.horizon_day <= 7 AND s.business_date = DATE'2026-10-09'
GROUP BY f.dc_id, f.sku, s.on_hand_eod, s.open_po_qty;
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=inv&plat=dbx)

### On AWS: Use: Warehouse inventory forecast

預測 + 庫存 + 在途 + 安全庫存 → 建議採購量，送回 ERP 等人核准。

#### Before

**Inputs for DC-SG-01 × SKU-20871**

| column | type | tag | note |
|---|---|---|---|
| `item` | STRING |  |  |
| `value` | STRING |  |  |

| item | value |
|---|---|
| on_hand_eod (gold) | 1180 |
| open_po_qty (gold) | 600 |
| p50, next 7 days (forecast) | 452 + 468 + 441 + 288 + 275 + 262 + 270 = 2456 |
| safety_stock (policy) | 420 |
| case_pack (supplier terms) | 24 |

#### After

**Suggested purchase order sent to ERP**

| column | type | tag | note |
|---|---|---|---|
| `dc_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `suggested_qty` | INT |  | 2456 + 420 − 1180 − 600 |
| `order_qty` | INT |  | rounded up to 46 cases × 24 |
| `need_by` | DATE |  |  |
| `status` | STRING |  |  |

| row note | dc_id | sku | suggested_qty | order_qty | need_by | status |
|---|---|---|---|---|---|---|
| hl: a planner approves in the ERP | DC-SG-01 | SKU-20871 | 1096 | 1104 | 2026-10-13 | PENDING_APPROVAL |

#### 這一步改了什麼

計算邏輯完全一樣：未來 7 天 p50 + 安全庫存 − 現有庫存 − 在途 = 1096，按箱規 24 取整成 1104。AWS 上通常在 Athena 對 Iceberg 表跑這段 SQL，結果經 API 或檔案送回 ERP，由採購核准。

#### Code (SQL (Athena, Iceberg tables in S3 Tables))

```
SELECT f.dc_id, f.sku,
       SUM(f.p50) + 420 - s.on_hand_eod - s.open_po_qty            AS suggested_qty,
       CEIL((SUM(f.p50) + 420 - s.on_hand_eod - s.open_po_qty) / 24.0) * 24 AS order_qty
FROM "s3tablescatalog/retail-lake"."gold"."dc_sku_forecast" f
JOIN "s3tablescatalog/retail-lake"."gold"."dc_stock_daily" s
  ON f.dc_id = s.dc_id AND f.sku = s.sku
WHERE f.horizon_day <= 7 AND s.business_date = DATE '2026-10-09'
GROUP BY f.dc_id, f.sku, s.on_hand_eod, s.open_po_qty;
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=inv&plat=aws)

<a id="step-20"></a>
## 20. Serve: Model Serving endpoint

### On Databricks: Serve: Model Serving endpoint

店經理 app 即時問：今天這個 SKU 還會賣多少？

#### Before

**Request: POST /serving-endpoints/demand-forecaster/invocations**

| column | type | tag | note |
|---|---|---|---|
| `dataframe_records` | list<struct<store_id,sku,as_of>> |  |  |

- Only the keys: the endpoint looks up features itself

```json
{
  "dataframe_records": [
    {
      "store_id": "SG-ORCH-001",
      "sku": "SKU-20871",
      "as_of": "2026-10-10T10:15:00+08:00"
    }
  ]
}
```

#### After

**Response**

| column | type | tag | note |
|---|---|---|---|
| `predictions` | list<struct<p10,p50,p90>> |  |  |


```json
{
  "predictions": [
    {
      "p10": 182,
      "p50": 205,
      "p90": 236
    }
  ]
}
```

#### 這一步改了什麼

請求只帶 key（店、SKU）。endpoint 用 automatic feature lookup 自己去 Online Feature Store 查 units_15m、units_lag_7 等特徵，所以 app 很薄、也不會和訓練時的特徵算法不一致。每次請求和回應都會被記到 inference table（見回饋迴圈那一步）。

#### Code (Bash)

```
curl -X POST "$DATABRICKS_HOST/serving-endpoints/demand-forecaster/invocations" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"dataframe_records":[{"store_id":"SG-ORCH-001","sku":"SKU-20871"}]}'
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=rt&plat=dbx)

### On AWS: Serve: SageMaker AI real-time endpoint

店經理 app 即時問：今天這個 SKU 還會賣多少？

#### Before

**InvokeEndpoint body (built by a Lambda)**

| column | type | tag | note |
|---|---|---|---|
| `instances` | list<struct<store_id,sku,units_lag_7,…,units_15m,units_60m,dow>> |  | features included by the caller |

- The caller fetched the features first with GetRecord

```json
{
  "instances": [
    {
      "store_id": "SG-ORCH-001",
      "sku": "SKU-20871",
      "units_lag_7": 97,
      "units_mean_28": 89.6,
      "is_promo": 1,
      "promo_days_left": 2,
      "units_15m": 7,
      "units_60m": 13,
      "dow": 6
    }
  ]
}
```

#### After

**Response (format defined by your inference container)**

| column | type | tag | note |
|---|---|---|---|
| `predictions` | list<struct<p10,p50,p90>> |  |  |


```json
{
  "predictions": [
    {
      "p10": 182,
      "p50": 205,
      "p90": 236
    }
  ]
}
```

#### 這一步改了什麼

SageMaker endpoint 不會幫你查特徵：Lambda 要先呼叫 GetRecord、把字串轉成數字、組進 body，再 InvokeEndpoint。請求與回應的格式由推論容器決定。

#### Code (Python (boto3, inside the Lambda))

```
rt = boto3.client("sagemaker-runtime")
resp = rt.invoke_endpoint(EndpointName="demand-forecaster",
                          ContentType="application/json",
                          Body=json.dumps(payload))
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=rt&plat=aws)

<a id="step-21"></a>
## 21. Use: Store sales forecast

### On Databricks: Use: Store sales forecast

店經理看到的不是圖表，是一個動作。

#### Before

**Inputs at 10:15 Saturday**

| column | type | tag | note |
|---|---|---|---|
| `item` | STRING |  |  |
| `value` | STRING |  |  |

| item | value |
|---|---|
| forecast today, p50 (endpoint) | 205 |
| sold so far today | 31 |
| on the shelf | 40 |
| in the backroom | 96 |

#### After

**Store manager app: SG-ORCH-001**

| column | type | tag | note |
|---|---|---|---|
| `sku` | STRING |  |  |
| `forecast_today` | INT |  |  |
| `sold_so_far` | INT |  |  |
| `on_shelf` | INT |  |  |
| `backroom` | INT |  |  |
| `action` | STRING |  |  |

| row note | sku | forecast_today | sold_so_far | on_shelf | backroom | action |
|---|---|---|---|---|---|---|
| hl: promo weekend | SKU-20871 | 205 | 31 | 40 | 96 | Top up 60 from the backroom before 12:00 |

#### 這一步改了什麼

今天預計賣 205、已賣 31，還要 174；貨架只剩 40，所以中午前從後倉補 60。這才是門店要的輸出。Databricks 上這個 app 可以是 Databricks App，讀 endpoint 和 Lakebase。

[Open this step in the 3D lab](../lab/retail-3d.html#step=sales&plat=dbx)

### On AWS: Use: Store sales forecast

店經理看到的不是圖表，是一個動作。

#### Before

**Inputs at 10:15 Saturday**

| column | type | tag | note |
|---|---|---|---|
| `item` | STRING |  |  |
| `value` | STRING |  |  |

| item | value |
|---|---|
| forecast today, p50 (endpoint) | 205 |
| sold so far today | 31 |
| on the shelf | 40 |
| in the backroom | 96 |

#### After

**Store manager app: SG-ORCH-001**

| column | type | tag | note |
|---|---|---|---|
| `sku` | STRING |  |  |
| `forecast_today` | INT |  |  |
| `sold_so_far` | INT |  |  |
| `on_shelf` | INT |  |  |
| `backroom` | INT |  |  |
| `action` | STRING |  |  |

| row note | sku | forecast_today | sold_so_far | on_shelf | backroom | action |
|---|---|---|---|---|---|---|
| hl: promo weekend | SKU-20871 | 205 | 31 | 40 | 96 | Top up 60 from the backroom before 12:00 |

#### 這一步改了什麼

AWS 上這個 app 常見是 API Gateway + Lambda 呼叫 endpoint，前端另外託管。計算邏輯一樣。

[Open this step in the 3D lab](../lab/retail-3d.html#step=sales&plat=aws)

<a id="step-22"></a>
## 22. Serve: Data quality monitoring

### On Databricks: Serve: Data quality monitoring

記下每次預測，追蹤資料與預測是否漂移；漂移就觸發重訓。

#### Before

**Inference table of the serving endpoint (auto-logged)**

| column | type | tag | note |
|---|---|---|---|
| `request_date` | DATE |  |  |
| `databricks_request_id` | STRING |  |  |
| `request_time` | TIMESTAMP |  |  |
| `status_code` | INT |  |  |
| `execution_duration_ms` | BIGINT |  |  |
| `request` | STRING |  |  |
| `response` | STRING |  |  |
| `served_entity_id` | STRING |  |  |

| request_date | databricks_request_id | request_time | status_code | execution_duration_ms | request | response | served_entity_id |
|---|---|---|---|---|---|---|---|
| 2026-10-10 | 9c2b7e41-… | 2026-10-10 02:15:03.118 | 200 | 38 | {"dataframe_records":[{"store_id":"SG-ORCH-001","sku":"SKU-20871"}]} | {"predictions":[{"p50":205.0}]} | demand-forecaster-7 |
| 2026-10-10 | 1f80aa03-… | 2026-10-10 02:15:07.402 | 200 | 41 | {"dataframe_records":[{"store_id":"MY-KUL-014","sku":"SKU-20871"}]} | {"predictions":[{"p50":248.0}]} | demand-forecaster-7 |
| 2026-10-10 | 77d1c9be-… | 2026-10-10 02:16:55.630 | 200 | 36 | {"dataframe_records":[{"store_id":"TH-BKK-007","sku":"SKU-31045"}]} | {"predictions":[{"p50":3.0}]} | demand-forecaster-7 |

#### After

**Drift metrics table (data profiling, inference profile)**

| column | type | tag | note |
|---|---|---|---|
| `window` | STRUCT<start,end> |  |  |
| `window_cmp` | STRUCT<start,end> |  |  |
| `drift_type` | STRING |  |  |
| `column_name` | STRING |  |  |
| `population_stability_index` | DOUBLE |  |  |
| `js_distance` | DOUBLE |  |  |

| row note | window | window_cmp | drift_type | column_name | population_stability_index | js_distance |
|---|---|---|---|---|---|---|
| chg: expected: the promo started | {"start":"2026-10-10","end":"2026-10-11"} | {"start":"2026-10-09","end":"2026-10-10"} | CONSECUTIVE | discount_pct | 0.41 | 0.29 |
|  | {"start":"2026-10-10","end":"2026-10-11"} | {"start":"2026-10-09","end":"2026-10-10"} | CONSECUTIVE | units_15m | 0.12 | 0.08 |
| drift: predictions moved away from the training baseline → alert → retrain → version 8 | {"start":"2026-10-10","end":"2026-10-11"} | *null* | BASELINE | p50 | 0.27 | 0.19 |

**Anomaly detection on schema retail.bronze (summary)**

| column | type | tag | note |
|---|---|---|---|
| `table` | STRING |  |  |
| `freshness` | STRING |  |  |
| `completeness` | STRING |  |  |

| row note | table | freshness | completeness |
|---|---|---|---|
|  | retail.bronze.pos_events | healthy | healthy |
| drift: ERP feed late: check CDC before blaming the model | retail.bronze.sap_mard | stale: no commit for 6 h | healthy |
|  | retail.bronze.promo_calendar | healthy | healthy |

#### 這一步改了什麼

Model Serving 自動把每次請求和回應寫進 inference table（一張 Delta 表）。Data profiling 對它算分布與漂移，產出 profile metrics 與 drift metrics 兩張表（上面是 drift）。discount_pct 漂移是促銷開始，屬於預期；p50 相對訓練基準漂移才觸發重訓。Anomaly detection 另外看每張表是否準時、筆數是否正常——大多數預測事故其實是資料晚到。

#### Code (Python (data profiling API))

```
import databricks.lakehouse_monitoring as lm
lm.create_monitor(
    table_name="retail.ml.demand_forecaster_payload",
    profile_type=lm.InferenceLog(
        problem_type="regression", prediction_col="p50",
        timestamp_col="request_time", model_id_col="served_entity_id",
        granularities=["1 day"]),
    output_schema_name="retail.monitoring")
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=monitor&plat=dbx)

### On AWS: Serve: Data Capture + MLflow Apps + Evidently

記下每次預測，追蹤資料與預測是否漂移；漂移就觸發重訓。

#### Before

**Data Capture record in S3 (JSON Lines)**

| column | type | tag | note |
|---|---|---|---|
| `captureData.endpointInput` | struct<observedContentType,mode,data,encoding> |  |  |
| `captureData.endpointOutput` | struct<…> |  |  |
| `eventMetadata` | struct<eventId,inferenceTime> |  |  |
| `eventVersion` | string |  |  |


```json
{
  "captureData": {
    "endpointInput": {
      "observedContentType": "application/json",
      "mode": "INPUT",
      "data": "{\"instances\":[{\"store_id\":\"SG-ORCH-001\",\"sku\":\"SKU-20871\",\"units_15m\":7}]}",
      "encoding": "JSON"
    },
    "endpointOutput": {
      "observedContentType": "application/json",
      "mode": "OUTPUT",
      "data": "{\"predictions\":[{\"p50\":205.0}]}",
      "encoding": "JSON"
    }
  },
  "eventMetadata": {
    "eventId": "0b7e3c1a-…",
    "inferenceTime": "2026-10-10T02:15:03Z"
  },
  "eventVersion": "0"
}
```

#### After

**Drift run logged to an MLflow App (Evidently AI)**

| column | type | tag | note |
|---|---|---|---|
| `metrics.drift_share` | double |  | share of drifted features |
| `metrics.psi.*` | double |  |  |
| `alert` | string |  |  |


```json
{
  "experiment": "retail-monitoring",
  "run": "drift-2026-10-10",
  "metrics": {
    "drift_share": 0.33,
    "psi.discount_pct": 0.41,
    "psi.units_15m": 0.12,
    "psi.prediction_p50": 0.27
  },
  "alert": "SNS: drift_share 0.33 > 0.30 threshold"
}
```

#### 這一步改了什麼

重要：SageMaker Model Monitor 已不開放新客戶。AWS 官方替代方案是：endpoint 開 Data Capture 把請求/回應存成 JSON Lines 到 S3，用開源的 SageMaker AI 監控方案（MLflow Apps + Evidently AI）算漂移，CloudWatch / SNS 告警，Quick Sight 做治理儀表板。這些都要自己部署與維運。

#### Code (Python (SageMaker Python SDK))

```
predictor = model.deploy(
    initial_instance_count=1, instance_type="ml.c5.xlarge",
    data_capture_config=DataCaptureConfig(
        enable_capture=True, sampling_percentage=100,
        destination_s3_uri="s3://retail-ml/data-capture/"))
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=monitor&plat=aws)

<a id="step-23"></a>
## 23. Use: Genie + Agent Bricks

### On Databricks: Use: Genie + Agent Bricks

採購問：為什麼這週要訂 1,104 個？agent 查資料、查預測、查合約，再起草訂單。

#### Before

**Planner question**

| column | type | tag | note |
|---|---|---|---|
| `user` | string |  |  |
| `question` | string |  |  |


```json
{
  "user": "planner.sg@retail.example",
  "question": "Why is DC-SG-01 ordering 1,104 units of SKU-20871 this week?"
}
```

#### After

**Agent trace (MLflow Tracing, stored in Unity Catalog)**

| column | type | tag | note |
|---|---|---|---|
| `span` | STRING |  |  |
| `type` | STRING |  |  |
| `input` | STRING |  |  |
| `output` | STRING |  |  |
| `latency_ms` | BIGINT |  |  |

| row note | span | type | input | output | latency_ms |
|---|---|---|---|---|---|
|  | replenishment_agent | AGENT | question | answer + draft PO | 4120 |
|  | genie_ask | TOOL (Genie One MCP) | daily units, SKU-20871, last 14 days | promo lifted SG units from 92 to 186 | 1830 |
|  | get_forecast | TOOL (Model Serving) | DC-SG-01, SKU-20871, 7 days | p50 total 2,456 | 210 |
|  | search_supplier_terms | RETRIEVER (AI Search) | SKU-20871 supplier contract | case pack 24, lead time 3 days (clause 4.2) | 340 |
|  | compose_answer | LLM (via Unity Gateway) | facts above | promo through 12 Oct; 46 cases needed by 13 Oct | 1650 |
| hl: a human approves; the agent never places the order | create_draft_po | TOOL (UC function) | 1104 units | PO-DRAFT-88213, PENDING_APPROVAL | 90 |

_Child spans add up to the parent: 1830 + 210 + 340 + 1650 + 90 = 4120 ms._

#### 這一步改了什麼

Agent 的每一步都被記成一個 span：用了哪個工具、讀了哪份文件、花了多久。答案可以追溯到 gold 表、預測 endpoint 和合約條款，這就是可稽核的 GenAI。最後一步只「起草」訂單，由人核准。

#### Code (Python (MLflow Tracing))

```
import mlflow
mlflow.set_experiment("/Shared/retail/replenishment-agent")

@mlflow.trace(span_type="TOOL")
def get_forecast(dc_id: str, sku: str, days: int = 7) -> float:
    ...
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=genai&plat=dbx)

### On AWS: Use: Amazon Quick + Bedrock AgentCore

採購問：為什麼這週要訂 1,104 個？agent 查資料、查預測、查合約，再起草訂單。

#### Before

**Planner question**

| column | type | tag | note |
|---|---|---|---|
| `user` | string |  |  |
| `question` | string |  |  |


```json
{
  "user": "planner.sg@retail.example",
  "question": "Why is DC-SG-01 ordering 1,104 units of SKU-20871 this week?"
}
```

#### After

**Agent trace (AgentCore Observability, OpenTelemetry spans in CloudWatch)**

| column | type | tag | note |
|---|---|---|---|
| `span` | string |  |  |
| `type` | string |  |  |
| `input` | string |  |  |
| `output` | string |  |  |
| `latency_ms` | long |  |  |

| row note | span | type | input | output | latency_ms |
|---|---|---|---|---|---|
|  | replenishment_agent | AGENT (AgentCore Runtime) | question | answer + draft PO | 4120 |
|  | query_sales | TOOL (AgentCore Gateway → Lambda → Athena) | daily units, SKU-20871, last 14 days | promo lifted SG units from 92 to 186 | 1830 |
|  | get_forecast | TOOL (AgentCore Gateway → endpoint) | DC-SG-01, SKU-20871, 7 days | p50 total 2,456 | 210 |
|  | search_supplier_terms | RETRIEVER (Bedrock Knowledge Bases) | SKU-20871 supplier contract | case pack 24, lead time 3 days (clause 4.2) | 340 |
|  | compose_answer | LLM (Amazon Bedrock) | facts above | promo through 12 Oct; 46 cases needed by 13 Oct | 1650 |
| hl: Policy allows drafting, not ordering | create_draft_po | TOOL (Gateway, checked by AgentCore Policy) | 1104 units | PO-DRAFT-88213, PENDING_APPROVAL | 90 |

#### 這一步改了什麼

AgentCore Observability 用 OpenTelemetry 記同樣結構的 span（在 CloudWatch）；工具透過 AgentCore Gateway 包成 MCP，AgentCore Policy 決定 agent 能呼叫什麼。結構幾乎一樣，差別在資料與工具的權限分屬不同服務。

#### Code (Python (Strands Agents on AgentCore Runtime))

```
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent

app = BedrockAgentCoreApp()
agent = Agent(tools=gateway_tools)      # tools exposed by AgentCore Gateway

@app.entrypoint
def invoke(payload):
    return {"result": str(agent(payload["question"]))}
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=genai&plat=aws)

<a id="step-24"></a>
## 24. Layer: Governance

### On Databricks: Layer: Governance

同一張 gold 表，不同的人看到不同的列與欄。

#### Before

**retail.gold.daily_store_sku (materialized view)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | DATE |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `units` | BIGINT |  |  |
| `net_revenue` | DECIMAL(14,2) |  |  |
| `currency` | STRING |  |  |
| `txn_count` | BIGINT |  |  |
| `is_promo` | BOOLEAN |  | joined from the promo calendar |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: includes E1's 2 units plus the rest of the day | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | 1026.72 | SGD | 141 | true |
|  | 2026-10-09 | SG-ORCH-001 | SKU-10023 | 64 | 252.80 | SGD | 60 | false |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | 1398.00 | MYR | 170 | true |
|  | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | 6069.00 | THB | 21 | false |

_Full-day totals. The four silver rows above are part of these numbers (for example E1 contributes 2 of the 186 units)._

#### After

**What a Thailand planner sees**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | DATE |  |  |
| `store_id` | STRING |  |  |
| `sku` | STRING |  |  |
| `units` | BIGINT |  |  |
| `net_revenue` | DECIMAL(14,2) |  | masked: not in the finance group |
| `currency` | STRING |  |  |
| `txn_count` | BIGINT |  |  |
| `is_promo` | BOOLEAN |  |  |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: row filter: TH stores only; column mask: revenue null | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | *null* | THB | 21 | false |

#### 這一步改了什麼

Unity Catalog 用 row filter（函數決定看哪些列）和 column mask（函數決定欄位值怎麼顯示）。同一張表、同一個 SQL，泰國採購只看到 TH 的列、營收被遮成 null；欄位本身仍在，所以報表結構不變。

#### Code (SQL (Unity Catalog))

```
CREATE FUNCTION retail.gov.store_filter(store_id STRING)
RETURN is_account_group_member('planners_all')
    OR (is_account_group_member('planners_th') AND store_id LIKE 'TH-%');
ALTER TABLE retail.gold.daily_store_sku
  SET ROW FILTER retail.gov.store_filter ON (store_id);

CREATE FUNCTION retail.gov.mask_revenue(v DECIMAL(14,2))
RETURN CASE WHEN is_account_group_member('finance') THEN v ELSE NULL END;
ALTER TABLE retail.gold.daily_store_sku
  ALTER COLUMN net_revenue SET MASK retail.gov.mask_revenue;
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=p:gov&plat=dbx)

### On AWS: Layer: Governance

同一張 gold 表，不同的人看到不同的列與欄。

#### Before

**s3tablescatalog/retail-lake.gold.daily_store_sku (Iceberg, refreshed by a scheduled job)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | date |  |  |
| `store_id` | string |  |  |
| `sku` | string |  |  |
| `units` | long |  |  |
| `net_revenue` | decimal(14,2) |  |  |
| `currency` | string |  |  |
| `txn_count` | long |  |  |
| `is_promo` | boolean |  | joined from the promo calendar |

| row note | business_date | store_id | sku | units | net_revenue | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|---|
| hl: includes E1's 2 units plus the rest of the day | 2026-10-09 | SG-ORCH-001 | SKU-20871 | 186 | 1026.72 | SGD | 141 | true |
|  | 2026-10-09 | SG-ORCH-001 | SKU-10023 | 64 | 252.80 | SGD | 60 | false |
|  | 2026-10-09 | MY-KUL-014 | SKU-20871 | 233 | 1398.00 | MYR | 170 | true |
|  | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | 6069.00 | THB | 21 | false |

_Full-day totals. The four silver rows above are part of these numbers (for example E1 contributes 2 of the 186 units)._

#### After

**What a Thailand planner sees (Lake Formation data cells filter)**

| column | type | tag | note |
|---|---|---|---|
| `business_date` | date |  |  |
| `store_id` | string |  |  |
| `sku` | string |  |  |
| `units` | long |  |  |
| `currency` | string |  |  |
| `txn_count` | long |  |  |
| `is_promo` | boolean |  |  |

| row note | business_date | store_id | sku | units | currency | txn_count | is_promo |
|---|---|---|---|---|---|---|---|
| hl: row filter: TH stores; net_revenue column excluded | 2026-10-09 | TH-BKK-007 | SKU-31045 | 21 | THB | 21 | false |

#### 這一步改了什麼

Lake Formation 的 data cells filter 同時做列過濾與欄過濾：被排除的欄位直接「消失」（不是遮成 null），所以下游報表的結構會因人而異。另外，模型與 endpoint 的權限不在 Lake Formation，而在 IAM。

#### Code (Python (boto3, Lake Formation))

```
lf.create_data_cells_filter(TableData={
    "TableCatalogId": ACCOUNT_ID, "DatabaseName": "gold",
    "TableName": "daily_store_sku", "Name": "th_planners",
    "RowFilter": {"FilterExpression": "store_id LIKE 'TH-%'"},
    "ColumnWildcard": {"ExcludedColumnNames": ["net_revenue"]}})
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=p:gov&plat=aws)

<a id="step-25"></a>
## 25. Layer: Storage

### On Databricks: Layer: Storage

所有表最後都是 S3 上的 Parquet 檔，加上一份交易日誌。

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**retail.gold.daily_store_sku on S3 (Delta Lake)**

```text
s3://retail-uc-managed/…/tables/8d1f…/
  _delta_log/
    00000000000000000040.checkpoint.parquet
    00000000000000000041.json        ← commit: add 2 files, remove 1
  part-00000-3a9c…-c000.snappy.parquet
  part-00001-77e2…-c000.snappy.parquet
```

#### 這一步改了什麼

Delta 的 _delta_log 是一串 JSON commit（每 10 個做一次 checkpoint）；讀表時先讀日誌，就知道哪些 Parquet 屬於哪個版本，所以能 time travel、能 ACID。資料在客戶自己的 S3。

[Open this step in the 3D lab](../lab/retail-3d.html#step=p:store&plat=dbx)

### On AWS: Layer: Storage

所有表最後都是 S3 上的 Parquet 檔，加上一份交易日誌。

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**gold.daily_store_sku in an S3 table bucket (Apache Iceberg)**

```text
table bucket: retail-lake   (object layout managed by S3 Tables)
  metadata/
    00012-5be1….metadata.json        ← current snapshot pointer
    snap-4471…-1-c2f0….avro           ← manifest list
    c2f0…-m0.avro                     ← manifest: which data files
  data/
    00000-0-a91d….parquet
    00001-0-0f3c….parquet
```

#### 這一步改了什麼

Iceberg 用 metadata.json → manifest list → manifest → Parquet 的樹狀結構記錄版本，同樣能 time travel。S3 Tables 自動做 compaction 和快照清理。兩邊底層都是 S3 上的 Parquet，差別在交易日誌的格式。

[Open this step in the 3D lab](../lab/retail-3d.html#step=p:store&plat=aws)

<a id="step-26"></a>
## 26. Layer: Where people work and how jobs run

### On Databricks: Layer: Where people work and how jobs run

每晚的工作流程：一個 DAG 串起管線、訓練、比較、上線、打分。

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**Lakeflow Jobs: retail_nightly**

| column | type | tag | note |
|---|---|---|---|
| `task` | STRING |  |  |
| `type` | STRING |  |  |
| `depends_on` | STRING |  |  |
| `does` | STRING |  |  |

| task | type | depends_on | does |
|---|---|---|---|
| ingest | pipeline task | — | refresh bronze → silver → gold |
| features | pipeline task | ingest | refresh feature tables |
| train | notebook task | features | many-models training, logged to MLflow |
| gate | if/else condition | train | challenger wMAPE < champion? |
| promote | notebook task | gate = true | move alias @champion |
| score | notebook task | promote, or gate = false | write gold.dc_sku_forecast |
| dashboard | dashboard task | score | refresh the AI/BI dashboard |

#### 這一步改了什麼

Lakeflow Jobs 一個 DAG 裡就能放 pipeline、notebook、條件分支與儀表板刷新；資料端與 ML 端是同一個排程器。

[Open this step in the 3D lab](../lab/retail-3d.html#step=p:ws&plat=dbx)

### On AWS: Layer: Where people work and how jobs run

每晚的工作流程：一個 DAG 串起管線、訓練、比較、上線、打分。

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**SageMaker Pipelines: retail-nightly (plus data jobs elsewhere)**

| column | type | tag | note |
|---|---|---|---|
| `step` | string |  |  |
| `type` | string |  |  |
| `depends_on` | string |  |  |
| `does` | string |  |  |

| step | type | depends_on | does |
|---|---|---|---|
| (before) | Glue workflow / Step Functions | — | bronze → silver → gold jobs |
| Features | ProcessingStep | data jobs | build the training set |
| Train | TrainingStep | Features | many-models script |
| Evaluate | ProcessingStep | Train | compute wMAPE |
| Gate | ConditionStep | Evaluate | challenger better? |
| Register | RegisterModel | Gate = true | PendingManualApproval |
| Score | TransformStep | Register | Batch Transform |

#### 這一步改了什麼

ML 這段用 SageMaker Pipelines；資料這段通常是 Glue workflow、Step Functions 或 MWAA。兩個排程器要互相觸發（例如 EventBridge），這是要說清楚的整合成本。

[Open this step in the 3D lab](../lab/retail-3d.html#step=p:ws&plat=aws)

<a id="step-27"></a>
## 27. Layer: Foundation: identity, security, cost, CI/CD

### On Databricks: Layer: Foundation: identity, security, cost, CI/CD

最底層的身分、金鑰、稽核、成本與部署。這裡問：昨天每一條工作負載各花了多少？

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**Cost per workload on 2026-10-09, from system.billing.usage**

| column | type | tag | note |
|---|---|---|---|
| `usage_date` | DATE |  |  |
| `workload` | STRING |  | custom_tags['workload'], set on every job, pipeline, warehouse and endpoint |
| `dbus` | DECIMAL(38,2) |  | SUM(usage_quantity) where usage_unit = 'DBU' |

| row note | usage_date | workload | dbus |
|---|---|---|---|
| hl: many-models training is the biggest line | 2026-10-09 | nightly_training | 118.2 |
|  | 2026-10-09 | store_endpoint | 61.3 |
|  | 2026-10-09 | ingest_pipeline | 42.5 |
|  | 2026-10-09 | bi_warehouse | 37.9 |
|  | 2026-10-09 | batch_scoring | 23.75 |

_Synthetic values. Multiply DBUs by your contract price for cost; the S3, KMS and network charges stay on the AWS bill._

**databricks.yml (Declarative Automation Bundles)**

```text
bundle:
  name: retail-forecast

resources:
  jobs:
    retail_nightly:
      name: retail_nightly
      tags: {workload: nightly_training}
      tasks:
        - task_key: train
          notebook_task: {notebook_path: ./src/train_many_models.py}

targets:
  dev:  {mode: development, default: true}
  prod: {mode: production}
```

#### 這一步改了什麼

在 Databricks 裡，成本和稽核本身就是表：system.billing.usage 用 custom tag 把用量分到每條工作負載，system.access.audit 記錄誰改了權限。部署用 Declarative Automation Bundles（前身是 Databricks Asset Bundles）：job、pipeline、endpoint 都寫成 YAML，放在 Git，由 CI 部署到 dev / prod，tag 也一起寫死。金鑰（KMS）、網路（VPC、私有連線）和 CloudTrail 仍在客戶自己的 AWS 帳號，這部分兩個方案共用。

#### Code (SQL (system tables))

```
SELECT usage_date,
       custom_tags['workload'] AS workload,
       SUM(usage_quantity)     AS dbus
FROM system.billing.usage
WHERE usage_date = DATE'2026-10-09' AND usage_unit = 'DBU'
GROUP BY ALL
ORDER BY dbus DESC;
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=p:found&plat=dbx)

### On AWS: Layer: Foundation: identity, security, cost, CI/CD

最底層的身分、金鑰、稽核、成本與部署。這裡問：昨天每一條工作負載各花了多少？

#### Before

_No input: this is where the data starts (a customer system outside the platform)._

#### After

**Cost per workload on 2026-10-09, from Cost Explorer (grouped by the workload tag)**

| column | type | tag | note |
|---|---|---|---|
| `usage_date` | date |  |  |
| `workload` | string |  | cost allocation tag |
| `unblended_cost_usd` | decimal(12,2) |  | Glue, EMR, SageMaker AI, Athena, Flink… |

| row note | usage_date | workload | unblended_cost_usd |
|---|---|---|---|
| hl: only adds up if every service was tagged | 2026-10-09 | nightly_training | 96.4 |
|  | 2026-10-09 | store_endpoint | 52.8 |
|  | 2026-10-09 | ingest_pipeline | 38.1 |
|  | 2026-10-09 | bi_queries | 21.6 |
|  | 2026-10-09 | batch_scoring | 17.25 |

_Synthetic values. Cost Explorer refreshes at least once every 24 hours._

#### 這一步改了什麼

AWS 的成本看 Cost Explorer：以 cost allocation tag 分攤，資料至少每 24 小時更新一次；稽核看 CloudTrail；部署用 CloudFormation 或 CDK。一條管線的費用分散在 Glue、EMR、SageMaker AI、Athena、Flink 各自的帳上，所以 tag 規範要一開始就定好，否則加不起來。

#### Code (Python (boto3, Cost Explorer))

```
ce.get_cost_and_usage(
    TimePeriod={"Start": "2026-10-09", "End": "2026-10-10"},
    Granularity="DAILY",
    Metrics=["UnblendedCost"],
    GroupBy=[{"Type": "TAG", "Key": "workload"}])
```

[Open this step in the 3D lab](../lab/retail-3d.html#step=p:found&plat=aws)
