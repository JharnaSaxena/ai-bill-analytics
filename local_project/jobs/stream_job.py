import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import transformations as T

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC           = "llm-usage-events"
PRICE_CSV       = "/opt/data/price/price_budget.csv"
CKPT_WINDOWED   = "/opt/checkpoints/stream_windowed"
CKPT_CUMULATIVE = "/opt/checkpoints/stream_cumulative"
TRIGGER_SECONDS = "10 seconds"
WATERMARK       = "30 seconds"
WINDOW          = "1 minute"

PG_URL = f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}"
PG_PROPS = {
    "user":     os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver":   "org.postgresql.Driver",
}


def write_windowed(batch_df, batch_id):
    n = batch_df.count()
    if n == 0: return
    out = batch_df.select(
        F.col("team_id"), F.col("model"),
        F.col("window.start").alias("minute_start"),
        F.col("tokens_in").cast("long"),
        F.col("tokens_out").cast("long"),
        F.col("cost").cast("double"))
    staging = f"speed_agg_stg_{batch_id}"
    out.write.mode("overwrite").jdbc(PG_URL, staging, properties=PG_PROPS)
    jvm = spark._jvm
    conn = jvm.java.sql.DriverManager.getConnection(PG_URL, PG_PROPS["user"], PG_PROPS["password"])
    try:
        stmt = conn.createStatement()
        stmt.execute(
            f"INSERT INTO serving.speed_agg "
            f"(team_id, model, minute_start, tokens_in, tokens_out, cost) "
            f"SELECT team_id, model, minute_start, tokens_in, tokens_out, cost FROM {staging} "
            f"ON CONFLICT (team_id, model, minute_start) DO UPDATE "
            f"SET tokens_in = EXCLUDED.tokens_in, tokens_out = EXCLUDED.tokens_out, cost = EXCLUDED.cost")
        stmt.execute(
            "DELETE FROM serving.speed_agg s "
            "WHERE EXISTS (SELECT 1 FROM serving.batch_meta m "
            "WHERE m.team_id = s.team_id AND m.model = s.model AND s.minute_start < m.cutoff_ts)")
        stmt.execute(f"DROP TABLE IF EXISTS {staging}")
        stmt.close()
    finally:
        conn.close()
    print(f"[stream][windowed {batch_id}] upserted {n} rows", flush=True)


def write_cumulative(batch_df, batch_id):
    n = batch_df.count()
    if n == 0: return
    budget = spark.read.jdbc(PG_URL, "serving.team_budget", properties=PG_PROPS)
    enriched = (batch_df.join(budget, on="team_id", how="left")
                .withColumn("pct_used",
                    F.round((F.col("total_cost") / F.col("budget_limit") * 100).cast("double"), 2))
                .withColumn("alert_status",
                    F.when(F.col("pct_used") >= 100, "critical")
                     .when(F.col("pct_used") >= 80,  "warning")
                     .otherwise("ok")))
    try:
        prev = spark.read.jdbc(PG_URL, "serving.speed_cumulative", properties=PG_PROPS)
        prev_small = prev.select(F.col("team_id"), F.col("alert_status").alias("prev_status"))
    except Exception:
        prev_small = None
    if prev_small is not None:
        joined = enriched.join(prev_small, on="team_id", how="left")
        transitions = joined.filter(
            ((F.col("prev_status").isNull() | (F.col("prev_status") == "ok")) &
             (F.col("alert_status") == "warning")) |
            ((F.col("prev_status").isNull() | (F.col("prev_status") == "ok") |
              (F.col("prev_status") == "warning")) &
             (F.col("alert_status") == "critical")))
        n_tr = transitions.count()
        if n_tr > 0:
            alerts = transitions.select(
                F.col("team_id"),
                F.when(F.col("alert_status") == "critical", "critical_100")
                 .otherwise("warning_80").alias("threshold"),
                F.col("pct_used"), F.col("total_cost"))
            alerts.write.mode("append").jdbc(PG_URL, "serving.speed_alerts", properties=PG_PROPS)
            print(f"[stream][ALERT {batch_id}] {n_tr} threshold crossings emitted", flush=True)
    staging = f"speed_cum_stg_{batch_id}"
    final = enriched.select(
        "team_id", "total_tokens_in", "total_tokens_out", "total_cost",
        "budget_limit", "pct_used", "alert_status", "last_event_ts",
        F.current_timestamp().alias("last_updated"))
    final.write.mode("overwrite").jdbc(PG_URL, staging, properties=PG_PROPS)
    jvm = spark._jvm
    conn = jvm.java.sql.DriverManager.getConnection(PG_URL, PG_PROPS["user"], PG_PROPS["password"])
    try:
        stmt = conn.createStatement()
        stmt.execute(
            f"INSERT INTO serving.speed_cumulative "
            f"(team_id, total_tokens_in, total_tokens_out, total_cost, "
            f"budget_limit, pct_used, alert_status, last_event_ts, last_updated) "
            f"SELECT team_id, total_tokens_in, total_tokens_out, total_cost, "
            f"budget_limit, pct_used, alert_status, last_event_ts, last_updated FROM {staging} "
            f"ON CONFLICT (team_id) DO UPDATE "
            f"SET total_tokens_in = EXCLUDED.total_tokens_in, "
            f"total_tokens_out = EXCLUDED.total_tokens_out, "
            f"total_cost = EXCLUDED.total_cost, pct_used = EXCLUDED.pct_used, "
            f"alert_status = EXCLUDED.alert_status, last_event_ts = EXCLUDED.last_event_ts, "
            f"last_updated = EXCLUDED.last_updated")
        stmt.execute(f"DROP TABLE IF EXISTS {staging}")
        stmt.close()
    finally:
        conn.close()
    print(f"[stream][cumulative {batch_id}] {n} teams state updated", flush=True)


def main():
    global spark
    spark = SparkSession.builder.appName("llm-stream-stateful").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    prices = T.load_prices(spark, PRICE_CSV)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw
              .select(F.from_json(F.col("value").cast("string"), T.USAGE_EVENT_SCHEMA).alias("e"))
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

    q_windowed = (windowed.writeStream
                  .outputMode("update")
                  .option("checkpointLocation", CKPT_WINDOWED)
                  .foreachBatch(write_windowed)
                  .trigger(processingTime=TRIGGER_SECONDS)
                  .start())

    cumulative = (priced
                  .groupBy("team_id")
                  .agg(F.sum("tokens_in").alias("total_tokens_in"),
                       F.sum("tokens_out").alias("total_tokens_out"),
                       F.sum("cost").alias("total_cost"),
                       F.max("event_ts").alias("last_event_ts")))

    q_cumulative = (cumulative.writeStream
                    .outputMode("complete")
                    .option("checkpointLocation", CKPT_CUMULATIVE)
                    .foreachBatch(write_cumulative)
                    .trigger(processingTime=TRIGGER_SECONDS)
                    .start())

    print(f"[stream] BOTH queries started", flush=True)
    print(f"[stream]   windowed   -> {CKPT_WINDOWED}", flush=True)
    print(f"[stream]   cumulative -> {CKPT_CUMULATIVE}", flush=True)
    print(f"[stream]   watermark  = {WATERMARK}", flush=True)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()