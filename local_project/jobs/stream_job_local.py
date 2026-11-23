import os
from pathlib import Path

if os.name == "nt":
    os.environ["HADOOP_HOME"] = r"C:\Hadoop"
    os.environ["PATH"] += r";C:\Hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)

HERE      = Path(__file__).resolve().parent
PROJECT   = HERE.parent
INPUT_DIR = PROJECT / "local_test" / "input"
CKPT_DIR  = PROJECT / "local_test" / "ckpt"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
CKPT_DIR.mkdir(parents=True, exist_ok=True)

EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),    True),
    StructField("request_id", StringType(),    True),
    StructField("event_ts",   TimestampType(), True),
    StructField("team_id",    StringType(),    True),
    StructField("app_id",     StringType(),    True),
    StructField("model",      StringType(),    True),
    StructField("tokens_in",  LongType(),      True),
    StructField("tokens_out", LongType(),      True),
    StructField("status",     StringType(),    True),
])

spark = (SparkSession.builder
         .appName("StreamLocalDemo")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

stream_df = (spark.readStream
             .schema(EVENT_SCHEMA)
             .option("maxFilesPerTrigger", 1)
             .json(str(INPUT_DIR).replace("\\", "/")))

deduped = (stream_df
           .withWatermark("event_ts", "30 seconds")
           .dropDuplicates(["event_id"]))

priced = deduped.withColumn("cost",
    (F.col("tokens_in")  / 1000.0) * F.lit(0.005) +
    (F.col("tokens_out") / 1000.0) * F.lit(0.015))

windowed = (priced
            .groupBy(F.window(F.col("event_ts"), "1 minute"), "team_id", "model")
            .agg(F.sum("tokens_in").alias("tokens_in"),
                 F.sum("tokens_out").alias("tokens_out"),
                 F.sum("cost").alias("cost")))

def show_batch(batch_df, batch_id):
    n = batch_df.count()
    print("=" * 70, flush=True)
    print(f"[microbatch {batch_id}] received {n} windowed rows", flush=True)
    print("=" * 70, flush=True)
    if n > 0:
        (batch_df
         .select(F.col("window.start").alias("window_start"),
                 F.col("team_id"), F.col("model"),
                 F.col("tokens_in"), F.col("tokens_out"),
                 F.round(F.col("cost"), 4).alias("cost"))
         .orderBy("window_start", "team_id", "model")
         .show(n=50, truncate=False))

query = (windowed.writeStream
         .outputMode("update")
         .option("checkpointLocation", str(CKPT_DIR).replace("\\", "/"))
         .foreachBatch(show_batch)
         .trigger(processingTime="5 seconds")
         .start())

print(f"[local-stream] watching {INPUT_DIR}", flush=True)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n[local-stream] stopping...", flush=True)
finally:
    try: query.stop()
    except Exception: pass
    try: spark.stop()
    except Exception: pass