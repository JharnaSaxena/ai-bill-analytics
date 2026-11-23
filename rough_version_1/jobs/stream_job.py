"""
Speed layer: Spark Structured Streaming.

Kafka -> parse JSON -> dedup -> validate -> price -> 1-min windows
      -> JDBC into serving.speed_agg

Boundary rule:
  This job writes EVERY window it sees. The merged_view in Postgres
  uses batch_meta.cutoff_ts to hide speed rows for hours already covered
  by batch, so no double counting ever appears on the dashboard.
  Keeping all speed rows is simpler than trying to filter them here
  and makes merged_view correct even if batch advances mid-hour.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import transformations as T

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC           = "llm-usage-events"
PRICE_CSV       = "/opt/data/price/price_budget.csv"
CHECKPOINT_DIR  = "/opt/checkpoints/stream"
TRIGGER_SECONDS = "10 seconds"
WATERMARK       = "30 seconds"
WINDOW          = "1 minute"

PG_URL = f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}"
PG_PROPS = {
    "user":     os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver":   "org.postgresql.Driver",
}


def upsert_speed_agg(batch_df, batch_id):
    cnt = batch_df.count()
    print(f"[stream] batch {batch_id}: {cnt} rows in batch_df", flush=True)
    if cnt == 0:
        return

    out = batch_df.select(
        F.col("team_id").alias("team_id"),
        F.col("model").alias("model"),
        F.col("window.start").alias("minute_start"),
        F.col("tokens_in").alias("tokens_in"),
        F.col("tokens_out").alias("tokens_out"),
        F.col("cost").cast("double").alias("cost"),
    )

    try:
        existing = spark.read.jdbc(PG_URL, "serving.speed_agg", properties=PG_PROPS)
        kept = existing.join(
            out.select("team_id", "model", "minute_start"),
            on=["team_id", "model", "minute_start"],
            how="left_anti",
        )
        final = kept.unionByName(out, allowMissingColumns=True)
    except Exception:
        final = out

    (final.write
        .mode("overwrite")
        .option("truncate", "true")
        .jdbc(PG_URL, "serving.speed_agg", properties=PG_PROPS))

    print(f"[stream] batch {batch_id}: wrote {out.count()} window rows "
          f"(speed_agg now holds {final.count()} rows)", flush=True)


def main():
    global spark
    spark = (SparkSession.builder
             .appName("llm-speed-layer")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    prices = T.load_prices(spark, PRICE_CSV)

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw
              .select(F.from_json(F.col("value").cast("string"),
                                  T.USAGE_EVENT_SCHEMA).alias("e"))
              .select("e.*"))

    deduped = (parsed
               .withWatermark("event_ts", WATERMARK)
               .dropDuplicates(["event_id"]))
    valid  = T.filter_valid(deduped, prices)
    priced = T.with_cost(valid, prices)

    windowed = (priced
                .groupBy(F.window(F.col("event_ts"), WINDOW), "team_id", "model")
                .agg(F.sum("tokens_in").alias("tokens_in"),
                     F.sum("tokens_out").alias("tokens_out"),
                     F.sum("cost").alias("cost")))

    query = (windowed.writeStream
             .outputMode("update")
             .option("checkpointLocation", CHECKPOINT_DIR)
             .foreachBatch(upsert_speed_agg)
             .trigger(processingTime=TRIGGER_SECONDS)
             .start())

    print(f"[stream] started. Kafka={KAFKA_BOOTSTRAP} topic={TOPIC} "
          f"checkpoint={CHECKPOINT_DIR}", flush=True)

    query.awaitTermination()


if __name__ == "__main__":
    main()