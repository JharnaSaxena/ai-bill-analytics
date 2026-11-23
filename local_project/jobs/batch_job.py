import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import transformations as T

DATA_DIR  = "/opt/data"
PRICE_CSV = f"{DATA_DIR}/price/price_budget.csv"

PG_URL = f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}"
PG_PROPS = {
    "user":     os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver":   "org.postgresql.Driver",
}


def main():
    spark = SparkSession.builder.appName("llm-batch").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    prices = T.load_prices(spark, PRICE_CSV)

    raw = spark.read.schema(T.USAGE_EVENT_SCHEMA).json(f"{DATA_DIR}/raw")
    raw_count = raw.count()
    print(f"[batch][STAGE 1/RAW]   {raw_count} events read from disk", flush=True)
    if raw_count == 0:
        print("[batch] no data â€” exiting", flush=True)
        spark.stop(); return

    deduped = T.dedup(raw)
    deduped_count = deduped.count()
    dropped = raw_count - deduped_count
    print(f"[batch][STAGE 2/DEDUP] {dropped} duplicates removed, {deduped_count} remain", flush=True)

    valid, invalid = T.split_valid_invalid(deduped, prices)
    n_invalid = invalid.count()
    print(f"[batch][STAGE 3/VALID] {n_invalid} rejected, {deduped_count - n_invalid} valid", flush=True)

    if n_invalid > 0:
        dl = (invalid
              .withColumn("layer", F.lit("batch"))
              .withColumn("event", F.to_json(F.struct("*")))
              .select("layer", F.col("reject_reason").alias("reason"), "event"))
        (dl.write.mode("overwrite").option("truncate", "true")
           .jdbc(PG_URL, "serving.dead_letter", properties=PG_PROPS))
        print(f"[batch][STAGE 4/DEAD ] {n_invalid} rows â†’ serving.dead_letter", flush=True)

    priced = T.with_cost(valid, prices)
    priced = T.add_slot_start(priced).filter(F.col("slot_start").isNotNull())
    print(f"[batch][STAGE 5/PRICE] cost computed, slot_start assigned", flush=True)

    agg = (priced
           .groupBy("team_id", "model", "slot_start")
           .agg(F.sum("tokens_in").alias("tokens_in"),
                F.sum("tokens_out").alias("tokens_out"),
                F.sum("cost").alias("cost"))
           .select(F.col("team_id"), F.col("model"), F.col("slot_start"),
                   F.col("tokens_in").cast("long"),
                   F.col("tokens_out").cast("long"),
                   F.col("cost").cast("decimal(14,6)")))
    n_agg = agg.count()
    print(f"[batch][STAGE 6/AGG  ] {n_agg} slot aggregates", flush=True)

    # cutoff = start of the LATEST slot with data (not current time).
    # Batch shows earlier slots; speed fills the latest partial slot.
    max_slot = priced.agg(F.max("slot_start")).collect()[0][0]
    cutoff = str(max_slot)
    print(f"[batch][STAGE 7/CUTOF] cutoff_ts = {cutoff} (start of latest slot with data)", flush=True)

    watermark = (priced
                 .groupBy("team_id", "model")
                 .agg(F.max("event_ts").alias("max_event_ts"))
                 .withColumn("cutoff_ts", F.lit(cutoff).cast("timestamp"))
                 .withColumn("status", F.lit("complete"))
                 .select("team_id", "model", "max_event_ts", "cutoff_ts", "status"))

    (agg.write.mode("overwrite").option("truncate", "true")
        .jdbc(PG_URL, "serving.batch_agg", properties=PG_PROPS))
    (watermark.write.mode("overwrite").option("truncate", "true")
        .jdbc(PG_URL, "serving.batch_meta", properties=PG_PROPS))
    print(f"[batch][STAGE 8/WRITE] batch_agg={n_agg}, batch_meta={watermark.count()}", flush=True)

    stats = spark.createDataFrame(
        [("batch", int(raw_count), int(deduped_count), int(dropped))],
        ["layer", "raw_count", "after_dedup_count", "dropped_count"])
    stats.write.mode("append").option("truncate", "false") \
        .jdbc(PG_URL, "serving.dedup_stats", properties=PG_PROPS)
    print(f"[batch][STAGE 9/AUDIT] dedup_stats row written", flush=True)

    merged = spark.read.jdbc(PG_URL, "serving.merged_view", properties=PG_PROPS)
    for r in merged.groupBy("layer").count().collect():
        print(f"[batch] merged_view: layer={r['layer']} rows={r['count']}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()