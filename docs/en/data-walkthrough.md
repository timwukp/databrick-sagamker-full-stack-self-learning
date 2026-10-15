# Data walkthrough: every step, before and after

Generated from `lab/retail-3d.html` by `scripts/export_from_lab.mjs`. The same content is interactive in the
[3D lab](../../en/lab/retail-3d.html#data). Sample rows are synthetic; columns, types and system fields follow the official docs.

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

Each checkout makes the POS send one JSON event; one event holds several items (nested lines).

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

#### What this step changed

The raw events have six typical problems planted on purpose: time is a local-time “string”, one event holds several items, events get resent (E2), events arrive late (E3), a firmware upgrade adds a new field (E4), and there is occasional bad data (E5). Each later step deals with them one by one.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=pos&plat=dbx)

### On AWS: Sources: Store POS terminals

Each checkout makes the POS send one JSON event; one event holds several items (nested lines).

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

#### What this step changed

The raw events have six typical problems planted on purpose: time is a local-time “string”, one event holds several items, events get resent (E2), events arrive late (E3), a firmware upgrade adds a new field (E4), and there is occasional bad data (E5). Each later step deals with them one by one.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=pos&plat=aws)

<a id="step-2"></a>
## 2. Sources: Promo calendar, weather, holidays

### On Databricks: Sources: Promo calendar, weather, holidays

The promotions team drops a CSV into S3 every week; weather and holidays are similar external files or APIs.

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

#### What this step changed

CSV has no types: dates and numbers are just text; store_scope joins several countries with |. Downstream must split it and cast types before it can be joined with sales data.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=ext&plat=dbx)

### On AWS: Sources: Promo calendar, weather, holidays

The promotions team drops a CSV into S3 every week; weather and holidays are similar external files or APIs.

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

#### What this step changed

CSV has no types: dates and numbers are just text; store_scope joins several countries with |. Downstream must split it and cast types before it can be joined with sales data.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=ext&plat=aws)

<a id="step-3"></a>
## 3. Sources: ERP and WMS (SAP, Oracle)

### On Databricks: Sources: ERP and WMS (SAP, Oracle)

The ERP inventory table (SAP as the example). This is the system of record; the platform only reads it and never changes it.

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

#### What this step changed

SAP column names are German abbreviations: MATNR material number, WERKS plant, LGORT storage location, LABST unrestricted stock; material numbers are zero-padded to 18 digits. In silver these all become business names and the SKU format.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=erp&plat=dbx)

### On AWS: Sources: ERP and WMS (SAP, Oracle)

The ERP inventory table (SAP as the example). This is the system of record; the platform only reads it and never changes it.

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

#### What this step changed

SAP column names are German abbreviations: MATNR material number, WERKS plant, LGORT storage location, LABST unrestricted stock; material numbers are zero-padded to 18 digits. In silver these all become business names and the SKU format.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=erp&plat=aws)

<a id="step-4"></a>
## 4. Ingest: Apache Kafka

### On Databricks: Ingest: Apache Kafka

POS events are written to a Kafka topic. Kafka does not parse the content; it only stores bytes plus position information.

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

#### What this step changed

The Kafka schema is always these seven columns (key, value, topic, partition, offset, timestamp, timestampType); your fields are all wrapped inside value as BINARY. With store_id as the key, the same store always goes to the same partition, so ordering is guaranteed. Kafka does not remove duplicates caused by POS resends; that is silver's job.

#### Code (Bash)

```
kafka-topics.sh --create --topic pos.sales.v1 \
  --partitions 12 --replication-factor 3 \
  --bootstrap-server <brokers>        # Confluent, self-managed, or Amazon MSK
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=stream&plat=dbx)

### On AWS: Ingest: Amazon MSK

POS events are written to a Kafka topic. Kafka does not parse the content; it only stores bytes plus position information.

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

#### What this step changed

MSK is Kafka, so this cell is identical on both sides: same topic, same schema. If you use Kinesis Data Streams instead, the equivalent concepts are partition key, shard and sequence number. Duplicates from POS resends still have to be removed downstream.

#### Code (Bash)

```
kafka-topics.sh --create --topic pos.sales.v1 \
  --partitions 12 --replication-factor 3 \
  --bootstrap-server <brokers>        # Confluent, self-managed, or Amazon MSK
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=stream&plat=aws)

<a id="step-5"></a>
## 5. Ingest: Auto Loader

### On Databricks: Ingest: Auto Loader

Incrementally load new files arriving in S3 into a table; files already processed are not read again.

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

#### What this step changed

Auto Loader (cloudFiles) only processes newly arrived files. Two details to remember: (1) CSV / JSON columns are inferred as STRING by default (unless you enable cloudFiles.inferColumnTypes), leaving type casting to silver; (2) it adds _rescued_data automatically, so values that do not match the schema land in this column instead of disappearing. When a new column appears, the default addNewColumns mode stops, updates the schema and restarts; rescue mode holds the values as in the table above.

#### Code (SQL (Lakeflow pipeline))

```
CREATE OR REFRESH STREAMING TABLE retail.bronze.promo_calendar AS
SELECT *, _metadata.file_path AS _source_file
FROM STREAM read_files(
  's3://retail-landing/promo/',
  format => 'csv', header => true);
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=files&plat=dbx)

### On AWS: Ingest: S3 landing + AWS Glue jobs

Incrementally load new files arriving in S3 into a table; files already processed are not read again.

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

#### What this step changed

A Glue job uses a job bookmark to remember which files it has processed (via transformation_ctx, saved by job.commit() at the end). Types are inferred by Glue, and numbers become bigint. When a new column appears, the DynamicFrame carries it, but the catalog schema has to be updated separately; there is no 'hold it for now' column like _rescued_data.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=files&plat=aws)

<a id="step-6"></a>
## 6. Ingest: Lakeflow Connect

### On Databricks: Ingest: Lakeflow Connect

Continuously capture changes from the ERP database (CDC) instead of a full table dump every night.

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

#### What this step changed

SCD type 2 means every change keeps a version, with __START_AT / __END_AT marking the valid period. Old versions are 'closed' rather than overwritten, so you can answer 'what was the stock at 10 pm last night', which is exactly the history model training needs. Database-based ERPs can use a Lakeflow Connect database connector; for SAP, OpenSharing (formerly Delta Sharing) through SAP Business Data Cloud is also common in practice, depending on the customer's SAP version.

#### Code (SQL (Lakeflow pipeline, from a change feed))

```
CREATE OR REFRESH STREAMING TABLE retail.bronze.sap_mard;

CREATE FLOW sap_mard_cdc AS AUTO CDC INTO retail.bronze.sap_mard
FROM STREAM(retail.raw.sap_mard_changes)
KEYS (MATNR, WERKS, LGORT)
SEQUENCE BY change_ts
STORED AS SCD TYPE 2;
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=cdc&plat=dbx)

### On AWS: Ingest: Zero-ETL or AWS DMS

Continuously capture changes from the ERP database (CDC) instead of a full table dump every night.

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

#### What this step changed

Each row of a DMS CDC file has an extra Op column (I / U / D) and carries only the change itself. To merge it into the latest state or an SCD2 history table, you write MERGE INTO yourself in Glue / EMR. Zero-ETL gives you a replicated table directly (latest state), but does not keep every version.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=cdc&plat=aws)

<a id="step-7"></a>
## 7. Raw (bronze): Bronze streaming tables

### On Databricks: Raw (bronze): Bronze streaming tables

Land it as is: each Kafka message becomes one row in a table, plus “where it came from and when it arrived”.

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

#### What this step changed

Bronze does one thing: keep everything. Duplicates (E2), bad records (E5) and late arrivals (E3) are all kept, and the payload is still the raw JSON string. The benefit: if the cleaning logic changes later, you can recompute silver and gold from bronze without going back to the source systems.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=bronze&plat=dbx)

### On AWS: Raw (bronze): Raw Iceberg tables in S3 Tables

Land it as is: each Kafka message becomes one row in a table, plus “where it came from and when it arrived”.

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

#### What this step changed

On AWS the same table is an Iceberg table in S3 Tables (type names follow Iceberg: string, long, timestamptz…), usually written directly from MSK by Amazon Data Firehose, or by a Glue streaming job. The concept is identical: keep the data as is and add metadata columns.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=bronze&plat=aws)

<a id="step-8"></a>
## 8. Clean (silver): Silver: Lakeflow Spark Declarative Pipelines

### On Databricks: Clean (silver): Silver: Lakeflow Spark Declarative Pipelines

Clean and standardize: dedupe, drop bad data, flatten nesting, convert time zones and types.

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

#### What this step changed

Silver does six things in order: (1) deduplicate on event_id, so E2 disappears; (2) an expectation drops E5 and counts it, with the number written to the event log; (3) flatten the nested lines into one row per item, so E1 becomes two rows; (4) local time strings plus time zone → UTC timestamp, while keeping the store's local business_date, so the late E3 still counts for the 9th; (5) cast amounts to DECIMAL; (6) loyalty_id is added through schema evolution, null for old data. ERP inventory is also renamed to business names in this layer (second table below).

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=silver&plat=dbx)

### On AWS: Clean (silver): AWS Glue ETL or EMR Serverless

Clean and standardize: dedupe, drop bad data, flatten nesting, convert time zones and types.

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

#### What this step changed

You write the same logic yourself in Spark on Glue or EMR Serverless (dropDuplicates, explode, to_timestamp) and write to Iceberg tables in S3 Tables. Data quality uses Glue Data Quality DQDL rules, and the results are stored separately; to “drop bad rows” you filter them yourself, unlike an expectation, where one decorator both blocks and counts them.

#### Code (DQDL + PySpark (AWS Glue))

```
# Glue Data Quality ruleset
Rules = [ IsComplete "sku", ColumnValues "qty" != 0 ]

# Spark job (Glue / EMR Serverless)
df = (bronze.dropDuplicates(["event_id"])
            .withColumn("l", F.explode("lines"))
            .filter("l.sku IS NOT NULL AND l.qty <> 0"))
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=silver&plat=aws)

<a id="step-9"></a>
## 9. Curated (gold): Gold materialized views

### On Databricks: Curated (gold): Gold materialized views

Business grain: one row per day × store × SKU; then join the promotion calendar and warehouse stock.

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

#### What this step changed

Gold is the 'answer table' for people and models: units, revenue and transaction counts are aggregated from silver, and a join with the promotion calendar gives is_promo. The other table holds each warehouse's closing stock and inbound purchase orders per day. BI dashboards and ML read the same gold, so the numbers match. The materialized view refreshes incrementally and automatically.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=gold&plat=dbx)

### On AWS: Curated (gold): Curated Iceberg tables

Business grain: one row per day × store × SKU; then join the promotion calendar and warehouse stock.

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

#### What this step changed

On AWS the same SQL can run in Athena or Glue Spark, with results written to an Iceberg table. The difference is refresh: there is no materialized view with automatic incremental refresh, so you schedule CTAS / MERGE INTO yourself (or use a Redshift materialized view).

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=gold&plat=aws)

<a id="step-10"></a>
## 10. Curated (gold): Databricks SQL warehouse

### On Databricks: Curated (gold): Databricks SQL warehouse

BI and Genie do not read files directly: a SQL engine queries the gold tables, with the governance layer's permissions applied. The question here: how much did the promotion lift sales, and where?

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

#### What this step changed

A SQL warehouse is compute built for SQL and BI: serverless, it stops when idle, and Photon is the default engine. Whether the query comes from an AI/BI dashboard, Genie or Power BI, Unity Catalog row filters and column masks apply (see the governance layer), so a Thailand planner still sees only TH. Every query is recorded in system tables, so you can audit who queried what.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=sqlwh&plat=dbx)

### On AWS: Curated (gold): Amazon Athena or Redshift Serverless

BI and Genie do not read files directly: a SQL engine queries the gold tables, with the governance layer's permissions applied. The question here: how much did the promotion lift sales, and where?

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

#### What this step changed

Athena runs the same SQL directly on the Iceberg tables and charges by data scanned; high-concurrency dashboards usually move to Redshift Serverless. Lake Formation data cells filters still apply. The difference is the other side of the join: on AWS the features sit in the SageMaker Feature Store offline store (a table in Glue), keyed by the combined store_sku, and it keeps every write, so the query has to pick the latest record itself.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=sqlwh&plat=aws)

<a id="step-11"></a>
## 11. Curated (gold): OpenSharing (formerly Delta Sharing)

### On Databricks: Curated (gold): OpenSharing (formerly Delta Sharing)

Share sell-through with a supplier: they query a live table instead of receiving a CSV every day. Supplier A should see only its own SKU, and no revenue.

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

#### What this step changed

OpenSharing shares tables or views from Unity Catalog. Here a view keeps only the supplier's SKU and drops the revenue columns, and the view is added to a share. The supplier always reads the latest version, with no daily exports; to stop, revoke the recipient. If the supplier also uses Databricks, the table appears directly in its own Unity Catalog.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=share&plat=dbx)

### On AWS: Curated (gold): Lake Formation cross-account sharing

Share sell-through with a supplier: they query a live table instead of receiving a CSV every day. Supplier A should see only its own SKU, and no revenue.

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

#### What this step changed

Lake Formation can grant a table or a data cells filter to another AWS account, delivered through AWS RAM; the supplier creates a resource link and queries it with Athena. This assumes the supplier is on AWS, and both sides manage cross-account permissions. Suppliers outside AWS need another route, such as scheduled file exports.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=share&plat=aws)

<a id="step-12"></a>
## 12. Features: Feature Store in Unity Catalog

### On Databricks: Features: Feature Store in Unity Catalog

Turn gold into the features the model needs; every feature value carries its “as of” date.

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

#### What this step changed

A feature table is just a Delta table with a primary key: (store_id, sku) plus business_date as the TIMESERIES key. Training uses a point-in-time join: each sample only gets features known 'before that day', so there is no peeking into the future. The model remembers which features it used, and lineage connects all the way back to gold.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=fs_off&plat=dbx)

### On AWS: Features: SageMaker Feature Store, offline

Turn gold into the features the model needs; every feature value carries its “as of” date.

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

#### What this step changed

SageMaker Feature Store uses a different model: one record identifier (only a single column, so store and SKU are concatenated) plus an event time; there are only three feature types, Integral, Fractional and String, so booleans become 0/1; the offline store automatically adds three columns, write_time, api_invocation_time and is_deleted (written to S3 within about 15 minutes after PutRecord).

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=fs_off&plat=aws)

<a id="step-13"></a>
## 13. Raw (bronze): Structured Streaming, real-time mode

### On Databricks: Raw (bronze): Structured Streaming, real-time mode

Compute “units sold in the last 15 / 60 minutes” from the live event stream and write it straight to the online feature store.

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

#### What this step changed

Individual events → sliding-window aggregates: slide every 5 minutes, computing the last 15 minutes (7 units) and 60 minutes (13 units). Databricks uses Structured Streaming (real-time mode) through Stream Feature Views to write straight to the Online Feature Store; the same Spark engine also does batch.

#### Code (Python (Structured Streaming))

```
from pyspark.sql import functions as F
feats = (spark.readStream.table("retail.silver.sales_lines")
    .withWatermark("event_ts_utc", "10 minutes")
    .groupBy("store_id", "sku",
             F.window("event_ts_utc", "15 minutes", "5 minutes"))
    .agg(F.sum("qty").alias("units_15m")))
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=rtc&plat=dbx)

### On AWS: Raw (bronze): Managed Service for Apache Flink

Compute “units sold in the last 15 / 60 minutes” from the live event stream and write it straight to the online feature store.

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

#### What this step changed

On AWS this is usually Managed Service for Apache Flink computing sliding windows, then calling PutRecord to write into Feature Store. Note two things: Feature Store needs a record identifier (here store and SKU are concatenated into store_sku) and an event time; batch uses Spark and streaming uses Flink, which are two separate engines.

#### Code (SQL (Flink SQL on Managed Flink))

```
SELECT store_id, sku, window_end, SUM(qty) AS units_15m
FROM TABLE(
  HOP(TABLE pos_lines, DESCRIPTOR(event_ts),
      INTERVAL '5' MINUTES, INTERVAL '15' MINUTES))
GROUP BY store_id, sku, window_start, window_end;
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=rtc&plat=aws)

<a id="step-14"></a>
## 14. Features: Online Feature Store

### On Databricks: Features: Online Feature Store

Online feature store: keeps only the latest record per key, for millisecond lookups.

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

#### What this step changed

The offline feature table is for training and the online copy is for serving, both from the same definition, which avoids training/serving skew (feature logic that differs between training and production). The online store is powered by Lakebase, which is Postgres underneath, so you can also query it directly with SQL.

#### Code (SQL (Postgres, Lakebase))

```
SELECT units_lag_7, units_mean_28, is_promo, units_15m, units_60m
FROM store_sku_daily_online
WHERE store_id = 'SG-ORCH-001' AND sku = 'SKU-20871';
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=fs_on&plat=dbx)

### On AWS: Features: SageMaker Feature Store, online

Online feature store: keeps only the latest record per key, for millisecond lookups.

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

#### What this step changed

The online store also keeps only the record with the latest event time. GetRecord returns a list of FeatureName / ValueAsString, so numbers are strings too and the caller has to cast them.

#### Code (Python (boto3))

```
fs = boto3.client("sagemaker-featurestore-runtime")
rec = fs.get_record(
    FeatureGroupName="store-sku-daily",
    RecordIdentifierValueAsString="SG-ORCH-001#SKU-20871")
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=fs_on&plat=aws)

<a id="step-15"></a>
## 15. Train and register: Many-models training

### On Databricks: Train and register: Many-models training

Many-models: train one small model per store × SKU series, in parallel.

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

#### What this step changed

groupBy(store, sku) distributes 48,000 series across Spark workers, and each is trained on its own. applyInPandas requires the output schema to be declared up front (the five columns in the table above); this is where the schema changes from 'training data' to 'training results'. In practice you wrap all the small models into one MLflow pyfunc 'router model' and register that; at serving time it picks the right small model by store and sku.

#### Code (Python (PySpark))

```
schema = "store_id string, sku string, n_train_days int, wmape_holdout double, model_path string"

results = (train_df.groupBy("store_id", "sku")
                   .applyInPandas(fit_one_series, schema=schema))
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=train&plat=dbx)

### On AWS: Train and register: SageMaker AI training jobs

Many-models: train one small model per store × SKU series, in parallel.

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

#### What this step changed

On SageMaker the unit is the training job: your script fans out inside the container itself (or you split into multiple jobs), the artifact is model.tar.gz on S3, and metrics are scraped from logs with a regex. The per-series results table has to be written to S3 by your script. For low code, you can use time series forecasting in SageMaker Canvas (per item ID); Amazon Forecast is no longer open to new customers.

#### Code (Python (SageMaker Python SDK))

```
from sagemaker.sklearn import SKLearn
est = SKLearn(entry_point="train_many.py", framework_version="1.2-1",
              role=ROLE, instance_type="ml.m5.4xlarge", instance_count=4,
              metric_definitions=[{"Name": "validation:wmape",
                                   "Regex": "wmape=([0-9.]+)"}])
est.fit({"train": "s3://retail-ml/train/2026-10-09/"})
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=train&plat=aws)

<a id="step-16"></a>
## 16. Train and register: MLflow 3, managed

### On Databricks: Train and register: MLflow 3, managed

One training run = one MLflow run: it records parameters, metrics and artifacts.

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

#### What this step changed

Both sides use MLflow, so this cell is almost identical: parameters, metrics, artifacts, tags. wMAPE (weighted mean absolute percentage error) is the most common metric in retail forecasting. The only difference is where the tracking server lives: on Databricks it is built in and natively integrated with Unity Catalog.

#### Code (Python (MLflow))

```
mlflow.set_experiment("/Shared/retail/demand")
with mlflow.start_run():
    mlflow.log_params({"num_leaves": 31, "learning_rate": 0.05, "horizon_days": 30})
    mlflow.log_metric("wmape_holdout", 0.187)
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=mlflow&plat=dbx)

### On AWS: Train and register: Managed MLflow on SageMaker AI

One training run = one MLflow run: it records parameters, metrics and artifacts.

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

#### What this step changed

Same MLflow API; the tracking server is the SageMaker AI MLflow App, and its ARN is the tracking URI. Models can be set to register automatically in SageMaker Model Registry.

#### Code (Python (MLflow on SageMaker AI))

```
mlflow.set_tracking_uri(
    "arn:aws:sagemaker:ap-southeast-1:111122223333:mlflow-app/app-EXAMPLE")
mlflow.set_experiment("retail-demand")
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=mlflow&plat=aws)

<a id="step-17"></a>
## 17. Train and register: Models in Unity Catalog

### On Databricks: Train and register: Models in Unity Catalog

Register the trained model as a version, and use an alias to decide which one is live.

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

#### What this step changed

Model names are three-part like tables (catalog.schema.model), and version numbers increment automatically. Going live uses an alias: the endpoint points to @champion; switching versions means pointing the alias to 8, and rolling back means pointing it back to 7. Permissions and lineage are the same system as for tables.

#### Code (Python (MLflow + Unity Catalog))

```
mlflow.set_registry_uri("databricks-uc")
mv = mlflow.register_model(f"runs:/{run_id}/model", "retail.ml.demand_forecaster")
MlflowClient().set_registered_model_alias(
    "retail.ml.demand_forecaster", "challenger", mv.version)
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=registry&plat=dbx)

### On AWS: Train and register: SageMaker Model Registry

Register the trained model as a version, and use an alias to decide which one is live.

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

#### What this step changed

SageMaker Model Registry manages versions with a model package group; the release gate is ModelApprovalStatus (PendingManualApproval → Approved), and deployment is usually triggered by an EventBridge event. Permissions go through IAM, separate from data permissions (Lake Formation).

#### Code (Python (boto3))

```
sm.update_model_package(
    ModelPackageArn=arn_v8,
    ModelApprovalStatus="Approved")   # EventBridge rule triggers the deploy
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=registry&plat=aws)

<a id="step-18"></a>
## 18. Serve: Batch inference job

### On Databricks: Serve: Batch inference job

Every night, score each warehouse × SKU × next 30 days and write the results to a table.

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

#### What this step changed

The output is not a single number but quantiles: p10 / p50 / p90. Replenishment uses p50 plus safety stock; stockout risk is read from p90. Note that the promotion ends on the 13th and the forecast drops from 441 to 288: the model has learned the promotion effect. This gold table is read by ERP, dashboards and the agent alike.

#### Code (Python (batch scoring in a Lakeflow Jobs task))

```
predict = mlflow.pyfunc.spark_udf(
    spark, "models:/retail.ml.demand_forecaster@champion")
scored = score_df.withColumn("pred", predict(*feature_cols))
scored.write.mode("overwrite").saveAsTable("retail.gold.dc_sku_forecast")
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=batch&plat=dbx)

### On AWS: Serve: SageMaker Batch Transform

Every night, score each warehouse × SKU × next 30 days and write the results to a table.

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

#### What this step changed

Batch Transform reads input files on S3 and writes output files with the same name plus .out. join_source='Input' joins the input ID columns back onto the output; otherwise you get only a list of predictions and cannot tell which warehouse and SKU each belongs to. It becomes a 'table' only after you load it into Iceberg or Redshift.

#### Code (Python (SageMaker Python SDK))

```
tf = model.transformer(instance_count=2, instance_type="ml.m5.xlarge",
                       output_path="s3://retail-ml/batch-out/2026-10-09/",
                       accept="text/csv")
tf.transform("s3://retail-ml/batch-in/2026-10-09/", content_type="text/csv",
             split_type="Line", join_source="Input")
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=batch&plat=aws)

<a id="step-19"></a>
## 19. Use: Warehouse inventory forecast

### On Databricks: Use: Warehouse inventory forecast

Forecast + on hand + in transit + safety stock → suggested order quantity, sent back to ERP for approval.

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

#### What this step changed

The forecast itself does not place orders. Suggested quantity = next 7 days demand + safety stock − on hand − in transit = 1096, rounded up to the case size of 24 to get 1104 (46 cases). ERP is still the ordering system; the platform only sends a “suggestion” for the planner to approve.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=inv&plat=dbx)

### On AWS: Use: Warehouse inventory forecast

Forecast + on hand + in transit + safety stock → suggested order quantity, sent back to ERP for approval.

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

#### What this step changed

The calculation is exactly the same: next 7 days p50 + safety stock − on hand − in transit = 1096, rounded up to the case size of 24 to get 1104. On AWS this SQL usually runs in Athena against the Iceberg table, and the result goes back to ERP by API or file for the planner to approve.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=inv&plat=aws)

<a id="step-20"></a>
## 20. Serve: Model Serving endpoint

### On Databricks: Serve: Model Serving endpoint

The store manager app asks in real time: how many more of this SKU will sell today?

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

#### What this step changed

The request carries only the keys (store, SKU). The endpoint uses automatic feature lookup to fetch units_15m, units_lag_7 and other features from the Online Feature Store itself, so the app stays thin and cannot drift from the feature logic used in training. Every request and response is logged to the inference table (see the feedback loop step).

#### Code (Bash)

```
curl -X POST "$DATABRICKS_HOST/serving-endpoints/demand-forecaster/invocations" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"dataframe_records":[{"store_id":"SG-ORCH-001","sku":"SKU-20871"}]}'
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=rt&plat=dbx)

### On AWS: Serve: SageMaker AI real-time endpoint

The store manager app asks in real time: how many more of this SKU will sell today?

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

#### What this step changed

A SageMaker endpoint does not look up features for you: Lambda has to call GetRecord first, convert strings to numbers, build the body, then call InvokeEndpoint. The request and response format is defined by the inference container.

#### Code (Python (boto3, inside the Lambda))

```
rt = boto3.client("sagemaker-runtime")
resp = rt.invoke_endpoint(EndpointName="demand-forecaster",
                          ContentType="application/json",
                          Body=json.dumps(payload))
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=rt&plat=aws)

<a id="step-21"></a>
## 21. Use: Store sales forecast

### On Databricks: Use: Store sales forecast

What the store manager sees is not a chart but an action.

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

#### What this step changed

Today's forecast is 205, 31 already sold, so 174 still to go; the shelf has only 40 left, so restock 60 from the back room before noon. This is the output the store needs. On Databricks this app can be a Databricks App that reads the endpoint and Lakebase.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=sales&plat=dbx)

### On AWS: Use: Store sales forecast

What the store manager sees is not a chart but an action.

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

#### What this step changed

On AWS this app is usually API Gateway + Lambda calling the endpoint, with the front end hosted separately. The calculation logic is the same.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=sales&plat=aws)

<a id="step-22"></a>
## 22. Serve: Data quality monitoring

### On Databricks: Serve: Data quality monitoring

Log every prediction and track whether data and predictions drift; drift triggers retraining.

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

#### What this step changed

Model Serving automatically writes every request and response to an inference table (a Delta table). Data profiling computes distributions and drift on it and produces two tables, profile metrics and drift metrics (drift is shown above). Drift in discount_pct is the promotion starting, which is expected; only drift in p50 relative to the training baseline triggers retraining. Anomaly detection separately checks whether each table arrives on time and has a normal row count. Most forecasting incidents are actually late data.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=monitor&plat=dbx)

### On AWS: Serve: Data Capture + MLflow Apps + Evidently

Log every prediction and track whether data and predictions drift; drift triggers retraining.

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

#### What this step changed

Important: SageMaker Model Monitor is closed to new customers. The official AWS alternative: turn on Data Capture on the endpoint to store requests/responses as JSON Lines in S3, compute drift with the open-source SageMaker AI monitoring solution (MLflow Apps + Evidently AI), alert with CloudWatch / SNS, and build governance dashboards in Quick Sight. You deploy and operate all of this yourself.

#### Code (Python (SageMaker Python SDK))

```
predictor = model.deploy(
    initial_instance_count=1, instance_type="ml.c5.xlarge",
    data_capture_config=DataCaptureConfig(
        enable_capture=True, sampling_percentage=100,
        destination_s3_uri="s3://retail-ml/data-capture/"))
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=monitor&plat=aws)

<a id="step-23"></a>
## 23. Use: Genie + Agent Bricks

### On Databricks: Use: Genie + Agent Bricks

The planner asks: why order 1,104 this week? The agent checks the data, the forecast and the contracts, then drafts the order.

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

#### What this step changed

Every step of the agent is recorded as a span: which tool it used, which document it read, how long it took. Answers can be traced back to gold tables, the forecast endpoint and contract terms. That is auditable GenAI. The final step only 'drafts' the order; a person approves it.

#### Code (Python (MLflow Tracing))

```
import mlflow
mlflow.set_experiment("/Shared/retail/replenishment-agent")

@mlflow.trace(span_type="TOOL")
def get_forecast(dc_id: str, sku: str, days: int = 7) -> float:
    ...
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=genai&plat=dbx)

### On AWS: Use: Amazon Quick + Bedrock AgentCore

The planner asks: why order 1,104 this week? The agent checks the data, the forecast and the contracts, then drafts the order.

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

#### What this step changed

AgentCore Observability records spans with the same structure using OpenTelemetry (in CloudWatch); tools are wrapped as MCP through AgentCore Gateway, and AgentCore Policy decides what the agent can call. The structure is almost the same; the difference is that data and tool permissions belong to different services.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=genai&plat=aws)

<a id="step-24"></a>
## 24. Layer: Governance

### On Databricks: Layer: Governance

Same gold table, different people see different rows and columns.

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

#### What this step changed

Unity Catalog uses row filters (a function decides which rows you see) and column masks (a function decides how a column value is shown). Same table, same SQL: the Thailand buyer sees only TH rows and revenue masked to null. The column itself is still there, so the report structure does not change.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=p:gov&plat=dbx)

### On AWS: Layer: Governance

Same gold table, different people see different rows and columns.

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

#### What this step changed

A Lake Formation data cells filter does both row and column filtering: excluded columns simply 'disappear' (they are not masked to null), so the structure of downstream reports differs by user. Also, model and endpoint permissions are not in Lake Formation but in IAM.

#### Code (Python (boto3, Lake Formation))

```
lf.create_data_cells_filter(TableData={
    "TableCatalogId": ACCOUNT_ID, "DatabaseName": "gold",
    "TableName": "daily_store_sku", "Name": "th_planners",
    "RowFilter": {"FilterExpression": "store_id LIKE 'TH-%'"},
    "ColumnWildcard": {"ExcludedColumnNames": ["net_revenue"]}})
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=p:gov&plat=aws)

<a id="step-25"></a>
## 25. Layer: Storage

### On Databricks: Layer: Storage

In the end every table is Parquet files on S3 plus a transaction log.

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

#### What this step changed

Delta's _delta_log is a sequence of JSON commits (with a checkpoint every 10). Reading a table starts with the log, which tells you which Parquet files belong to which version, so you get time travel and ACID. The data stays in the customer's own S3.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=p:store&plat=dbx)

### On AWS: Layer: Storage

In the end every table is Parquet files on S3 plus a transaction log.

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

#### What this step changed

Iceberg records versions in a tree: metadata.json → manifest list → manifest → Parquet, which also supports time travel. S3 Tables handles compaction and snapshot cleanup automatically. Both sides store Parquet on S3 underneath; the difference is the transaction log format.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=p:store&plat=aws)

<a id="step-26"></a>
## 26. Layer: Where people work and how jobs run

### On Databricks: Layer: Where people work and how jobs run

The nightly workflow: one DAG chains the pipeline, training, comparison, promotion and scoring.

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

#### What this step changed

A single Lakeflow Jobs DAG can hold pipelines, notebooks, conditional branches and dashboard refreshes; data and ML share one scheduler.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=p:ws&plat=dbx)

### On AWS: Layer: Where people work and how jobs run

The nightly workflow: one DAG chains the pipeline, training, comparison, promotion and scoring.

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

#### What this step changed

The ML part uses SageMaker Pipelines; the data part is usually Glue workflows, Step Functions or MWAA. The two schedulers have to trigger each other (for example through EventBridge). This is an integration cost to state clearly.

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=p:ws&plat=aws)

<a id="step-27"></a>
## 27. Layer: Foundation: identity, security, cost, CI/CD

### On Databricks: Layer: Foundation: identity, security, cost, CI/CD

The bottom layer: identity, keys, audit, cost and deployment. The question here: what did each workload cost yesterday?

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

#### What this step changed

On Databricks, cost and audit are tables: system.billing.usage splits usage by custom tag into workloads, and system.access.audit records who changed permissions. Deployment uses Declarative Automation Bundles (formerly Databricks Asset Bundles): jobs, pipelines and endpoints are written as YAML, kept in Git and deployed to dev and prod by CI, with the tags fixed in the same files. Keys (KMS), networking (VPC, private connectivity) and CloudTrail stay in the customer's own AWS account, and this part is shared by both designs.

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

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=p:found&plat=dbx)

### On AWS: Layer: Foundation: identity, security, cost, CI/CD

The bottom layer: identity, keys, audit, cost and deployment. The question here: what did each workload cost yesterday?

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

#### What this step changed

On AWS, cost is in Cost Explorer, split by cost allocation tags and refreshed at least once every 24 hours; audit is in CloudTrail; deployment uses CloudFormation or CDK. One pipeline's cost is spread across the Glue, EMR, SageMaker AI, Athena and Flink line items, so agree on a tagging scheme from the start or the numbers will not add up.

#### Code (Python (boto3, Cost Explorer))

```
ce.get_cost_and_usage(
    TimePeriod={"Start": "2026-10-09", "End": "2026-10-10"},
    Granularity="DAILY",
    Metrics=["UnblendedCost"],
    GroupBy=[{"Type": "TAG", "Key": "workload"}])
```

[Open this step in the 3D lab](../../en/lab/retail-3d.html#step=p:found&plat=aws)
