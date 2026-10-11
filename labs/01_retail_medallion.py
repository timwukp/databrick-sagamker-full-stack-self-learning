# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1: POS events through bronze, silver and gold
# MAGIC
# MAGIC Run the first half of the retail use case yourself, with the same five POS events the 3D lab shows
# MAGIC (`data/raw/pos_events.jsonl` in the repository). You will see, in your own tables:
# MAGIC
# MAGIC - **Bronze** keeps everything: the duplicate (E2), the bad scan (E5), the late Bangkok sale (E3).
# MAGIC - **Auto Loader** infers JSON columns as `STRING` by default and adds `_rescued_data`.
# MAGIC - **Silver** removes the duplicate, drops the bad line, flattens the basket, converts local time to UTC
# MAGIC   and keeps the store's local `business_date`.
# MAGIC - **Gold** aggregates one row per day, store and SKU, and joins the promo calendar.
# MAGIC
# MAGIC **Where to run it:** Databricks Free Edition (serverless). Import this file with *Workspace → Import*.
# MAGIC About 15 minutes. Everything is created in one schema, `workspace.retail_lab`, which the last cell can drop.
# MAGIC
# MAGIC This notebook was written against the October 2026 documentation and syntax-checked, but not executed
# MAGIC by its author. If a cell fails, the error message plus the docs link in that cell is the fastest fix.

# COMMAND ----------

CATALOG = "workspace"     # the default catalog in Databricks Free Edition
SCHEMA = "retail_lab"
spark.conf.set("spark.sql.session.timeZone", "UTC")   # show timestamps in UTC, like the lab

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
print("Landing volume:", BASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Land the raw files in a Unity Catalog volume
# MAGIC
# MAGIC In production these files arrive in S3 (or Kafka feeds bronze directly). Here we write them into a volume.
# MAGIC The content is identical to `data/raw/` in the repository.

# COMMAND ----------

POS_JSONL = """{"event_id":"pos-SG001-T03-000184","store_id":"SG-ORCH-001","terminal_id":"T03","event_ts_local":"2026-10-09 18:42:07","tz_offset":"+08:00","currency":"SGD","lines":[{"sku":"SKU-20871","qty":2,"unit_price":6.9,"discount":2.76},{"sku":"SKU-10023","qty":1,"unit_price":3.95,"discount":0}],"payment":{"method":"card","amount":14.99},"send_attempt":1}
{"event_id":"pos-SG001-T03-000184","store_id":"SG-ORCH-001","terminal_id":"T03","event_ts_local":"2026-10-09 18:42:07","tz_offset":"+08:00","currency":"SGD","lines":[{"sku":"SKU-20871","qty":2,"unit_price":6.9,"discount":2.76},{"sku":"SKU-10023","qty":1,"unit_price":3.95,"discount":0}],"payment":{"method":"card","amount":14.99},"send_attempt":2}
{"event_id":"pos-TH007-T01-002291","store_id":"TH-BKK-007","terminal_id":"T01","event_ts_local":"2026-10-09 23:58:40","tz_offset":"+07:00","currency":"THB","lines":[{"sku":"SKU-31045","qty":1,"unit_price":289,"discount":0}],"payment":{"method":"qr","amount":289},"send_attempt":1}
{"event_id":"pos-MY014-T02-000977","store_id":"MY-KUL-014","terminal_id":"T02","event_ts_local":"2026-10-09 12:15:33","tz_offset":"+08:00","currency":"MYR","lines":[{"sku":"SKU-20871","qty":3,"unit_price":7.5,"discount":4.5}],"payment":{"method":"ewallet","amount":18},"send_attempt":1,"loyalty_id":"L-55821"}
{"event_id":"pos-SG001-T05-000201","store_id":"SG-ORCH-001","terminal_id":"T05","event_ts_local":"2026-10-09 19:03:12","tz_offset":"+08:00","currency":"SGD","lines":[{"sku":null,"qty":0,"unit_price":3.95,"discount":0}],"payment":{"method":"cash","amount":0},"send_attempt":1}
"""

PROMO_CSV = """sku,store_scope,start_date,end_date,promo_type,discount_pct
SKU-20871,SG|MY,2026-10-09,2026-10-12,PCT_OFF,20
SKU-31045,TH,2026-10-20,2026-10-26,PCT_OFF,15
SKU-10023,SG,2026-10-16,2026-10-18,BUY2_SAVE,10
"""

dbutils.fs.mkdirs(f"{BASE}/pos")
dbutils.fs.mkdirs(f"{BASE}/promo")
dbutils.fs.put(f"{BASE}/pos/pos_events_2026-10-09.jsonl", POS_JSONL, overwrite=True)
dbutils.fs.put(f"{BASE}/promo/promo_SEA.csv", PROMO_CSV, overwrite=True)
display(dbutils.fs.ls(f"{BASE}/pos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze with Auto Loader
# MAGIC
# MAGIC `cloudFiles` picks up only new files and remembers what it has read in the checkpoint.
# MAGIC `availableNow` processes what is there and stops, which is what serverless supports for this kind of run.
# MAGIC
# MAGIC We give one **schema hint** for the nested `lines` array so the types inside it are fixed. Every other
# MAGIC column is left to inference, so you can see the default behaviour: strings, plus `_rescued_data`.
# MAGIC Docs: [Auto Loader schema inference](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)

# COMMAND ----------

from pyspark.sql import functions as F

bronze_query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{BASE}/_schemas/pos_events")
    .option("cloudFiles.schemaHints",
            "lines ARRAY<STRUCT<sku: STRING, qty: INT, unit_price: DECIMAL(10,2), discount: DECIMAL(10,2)>>")
    .load(f"{BASE}/pos/")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", f"{BASE}/_checkpoints/bronze_pos_events")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.bronze_pos_events")
)
bronze_query.awaitTermination()

bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_pos_events")
bronze.printSchema()     # look for: event_ts_local string, send_attempt string, _rescued_data string
display(bronze)          # 5 rows: the duplicate and the bad scan are still here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver: dedupe, validate, flatten, fix time
# MAGIC
# MAGIC Same six moves as the lab's silver step. In a Lakeflow pipeline you would write this with
# MAGIC `@dp.table` and `@dp.expect_or_drop`; here it is plain PySpark so every intermediate count is visible.

# COMMAND ----------

from pyspark.sql import Window

events = (
    bronze
    .withColumn("event_ts_utc", F.to_timestamp(F.concat_ws(" ", "event_ts_local", "tz_offset"),
                                               "yyyy-MM-dd HH:mm:ss XXX"))
    .withColumn("business_date", F.to_date(F.substring("event_ts_local", 1, 10)))   # the store's local date
)

# (1) dedupe on event_id, keeping the first send attempt
first_send = Window.partitionBy("event_id").orderBy(F.col("send_attempt").cast("int"))
deduped = events.withColumn("_rn", F.row_number().over(first_send)).filter("_rn = 1").drop("_rn")

# (2) flatten the basket: one row per product line
lines = (
    deduped
    .select("*", F.posexplode("lines").alias("pos", "l"))
    .select(
        "event_id",
        (F.col("pos") + 1).alias("line_no"),
        "store_id",
        F.col("l.sku").alias("sku"),
        F.col("l.qty").alias("qty"),
        F.col("l.unit_price").alias("unit_price"),
        F.col("l.discount").alias("discount"),
        (F.col("l.qty") * F.col("l.unit_price") - F.col("l.discount")).cast("decimal(12,2)").alias("net_amount"),
        "currency", "event_ts_utc", "business_date", "loyalty_id",
    )
)

# (3) the expectation: drop bad lines and count them
valid = lines.filter("sku IS NOT NULL AND qty <> 0")

print("bronze events      :", bronze.count())     # 5
print("after dedupe       :", deduped.count())    # 4  (E2 removed)
print("product lines      :", lines.count())      # 5
print("valid lines        :", valid.count())      # 4  (E5 dropped)

(valid.write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_sales_lines"))
display(spark.table(f"{CATALOG}.{SCHEMA}.silver_sales_lines").orderBy("event_ts_utc"))

# COMMAND ----------

# MAGIC %md
# MAGIC Check against the lab's silver step:
# MAGIC
# MAGIC - E1 became **two** rows (`line_no` 1 and 2), `net_amount` 11.04 and 3.95.
# MAGIC - The Bangkok sale (E3) has `event_ts_utc` 16:58:40 but `business_date` **2026-10-09**: event time, not arrival time.
# MAGIC - `loyalty_id` is filled only for E4 and null for the others.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold: one row per day, store and SKU, with the promo flag

# COMMAND ----------

promo = (
    spark.read.option("header", True).csv(f"{BASE}/promo/promo_SEA.csv")
    .withColumn("country", F.explode(F.split("store_scope", "\\|")))   # 'SG|MY' becomes two rows
    .withColumn("start_date", F.to_date("start_date"))
    .withColumn("end_date", F.to_date("end_date"))
    .withColumn("discount_pct", F.col("discount_pct").cast("int"))
)

s = spark.table(f"{CATALOG}.{SCHEMA}.silver_sales_lines").alias("s")
p = promo.alias("p")
gold = (
    s.join(
        p,
        (F.col("p.sku") == F.col("s.sku"))
        & (F.col("p.country") == F.substring(F.col("s.store_id"), 1, 2))
        & F.col("s.business_date").between(F.col("p.start_date"), F.col("p.end_date")),
        "left",
    )
    .groupBy(F.col("s.business_date").alias("business_date"),
             F.col("s.store_id").alias("store_id"),
             F.col("s.sku").alias("sku"))
    .agg(
        F.sum("s.qty").alias("units"),
        F.sum("s.net_amount").alias("net_revenue"),
        F.first("s.currency").alias("currency"),
        F.countDistinct("s.event_id").alias("txn_count"),
        (F.max(F.col("p.sku").isNotNull().cast("int")) == 1).alias("is_promo"),
    )
)
(gold.write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_daily_store_sku"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your gold table holds only the five sample events, so the numbers are small.
# MAGIC -- The lab's gold step shows full-day totals (for example 186 units, of which E1 contributes 2).
# MAGIC SELECT * FROM workspace.retail_lab.gold_daily_store_sku ORDER BY store_id, sku

# COMMAND ----------

rows = {(r.store_id, r.sku): r for r in spark.table(f"{CATALOG}.{SCHEMA}.gold_daily_store_sku").collect()}
assert len(rows) == 4, rows
sg_noodles = rows[("SG-ORCH-001", "SKU-20871")]
assert sg_noodles.units == 2 and str(sg_noodles.net_revenue) == "11.04" and sg_noodles.is_promo is True
assert rows[("MY-KUL-014", "SKU-20871")].is_promo is True
assert rows[("TH-BKK-007", "SKU-31045")].is_promo is False
print("All checks passed: your tables match the lab.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC - Rewrite bronze and silver as a **Lakeflow pipeline** (`from pyspark import pipelines as dp`), using the
# MAGIC   code in the lab's bronze and silver steps, and watch the expectation counts in the pipeline event log.
# MAGIC - Turn gold into a **feature table** with a `TIMESERIES` key (lab step 10) and look at the lineage graph in
# MAGIC   Catalog Explorer.
# MAGIC - Compare each table's schema with the AWS version in the 3D lab (`#plat=aws`).

# COMMAND ----------

# Clean up when you are done (uncomment to run):
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
