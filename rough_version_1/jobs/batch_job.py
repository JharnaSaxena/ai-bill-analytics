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
    spark = SparkSession.builder.appName("llm-batch-layer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    prices = T.load_prices(spark, PRICE_CSV)

    raw = spark.read.schema(T.USAGE_EVENT_SCHEMA).json(f"{DATA_DIR}/raw")
    raw_count = raw.count()
    print(f"[batch] read {raw_count} raw events from {DATA_DIR}/raw", flush=True)
    if raw_count == 0:
        print("[batch] no data, exiting", flush=True)
        spark.stop()
        return

    deduped = T.dedup(raw)
    valid, invalid = T.split_valid_invalid(deduped, prices)

    n_invalid = invalid.count()
    print(f"[batch] {n_invalid} invalid events (dead-lettered)", flush=True)
    if n_invalid > 0:
        dl = (invalid
              .withColumn("layer", F.lit("batch"))
              .withColumn("event", F.to_json(F.struct("*")))
              .select("layer", F.col("reject_reason").alias("reason"), "event"))
        (dl.write.mode("overwrite").option("truncate", "true")
           .jdbc(PG_URL, "serving.dead_letter", properties=PG_PROPS))
        print(f"[batch] wrote {n_invalid} rows to serving.dead_letter", flush=True)

    priced = T.with_cost(valid, prices)

    try:
        reports = (spark.read
                   .schema("request_id STRING, billed_tokens LONG, status STRING, reported_at TIMESTAMP")
                   .json(f"{DATA_DIR}/raw_provider"))
        n_reports = reports.count()
        print(f"[batch] {n_reports} provider reports", flush=True)
        if n_reports > 0:
            priced = (priced.alias("u")
                .join(reports.alias("p"), on="request_id", how="left")
                .withColumn("ratio",
                    F.when(F.col("p.billed_tokens").isNotNull() &
                           ((F.col("tokens_in") + F.col("tokens_out")) > 0),
                           F.col("p.billed_tokens").cast("double") /
                           (F.col("tokens_in") + F.col("tokens_out")))
                     .otherwise(F.lit(1.0)))
                .withColumn("tokens_in",  (F.col("tokens_in")  * F.col("ratio")).cast("long"))
                .withColumn("tokens_out", (F.col("tokens_out") * F.col("ratio")).cast("long"))
                .withColumn("cost",
                    (F.col("tokens_in")  / 1000.0) * F.col("input_price_per_1k") +
                    (F.col("tokens_out") / 1000.0) * F.col("output_price_per_1k"))
                .drop("ratio", "p.billed_tokens", "p.status", "p.reported_at"))
            print(f"[batch] applied provider reconciliation", flush=True)
    except Exception as e:
        print(f"[batch] provider join skipped: {e}", flush=True)

    hourly = (priced
              .withColumn("hour_start", F.date_trunc("hour", "event_ts"))
              .groupBy("team_id", "model", "hour_start")
              .agg(F.sum("tokens_in").alias("tokens_in"),
                   F.sum("tokens_out").alias("tokens_out"),
                   F.sum("cost").alias("cost"))
              .select(F.col("team_id"), F.col("model"), F.col("hour_start"),
                      F.col("tokens_in").cast("long").alias("tokens_in"),
                      F.col("tokens_out").cast("long").alias("tokens_out"),
                      F.col("cost").cast("decimal(14,6)").alias("cost")))

    watermark = (priced
                 .groupBy("team_id", "model")
                 .agg(F.max("event_ts").alias("max_event_ts"))
                 .withColumn("cutoff_ts", F.date_trunc("hour", F.col("max_event_ts")))
                 .withColumn("status", F.lit("complete"))
                 .select("team_id", "model", "max_event_ts", "cutoff_ts", "status"))

    n_h = hourly.count()
    n_w = watermark.count()
    print(f"[batch] {n_h} hourly aggregates, {n_w} watermark rows", flush=True)

    (hourly.write.mode("overwrite").option("truncate", "true")
        .jdbc(PG_URL, "serving.batch_agg", properties=PG_PROPS))
    print(f"[batch] wrote {n_h} rows to serving.batch_agg", flush=True)

    (watermark.write.mode("overwrite").option("truncate", "true")
        .jdbc(PG_URL, "serving.batch_meta", properties=PG_PROPS))
    print(f"[batch] wrote {n_w} rows to serving.batch_meta", flush=True)

    merged = spark.read.jdbc(PG_URL, "serving.merged_view", properties=PG_PROPS)
    for row in merged.groupBy("layer").count().collect():
        print(f"[batch] merged_view: layer={row['layer']} rows={row['count']}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()