import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH", "DLQ_PATH", "PRICE_PATH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

raw_df = spark.read.json(args["RAW_PATH"])
prices_df = spark.read.option("header", True).option("inferSchema", True).csv(args["PRICE_PATH"])

is_valid = (
    F.col("event_id").isNotNull()
    & F.col("team_id").isNotNull()
    & F.col("model").isNotNull()
    & F.col("tokens_in").isNotNull()
    & F.col("tokens_out").isNotNull()
    & (F.col("tokens_in") >= 0)
    & (F.col("tokens_out") >= 0)
)

invalid_df = raw_df.filter(~is_valid)
if invalid_df.limit(1).count() > 0:
    invalid_df.write.mode("append").parquet(args["DLQ_PATH"])

valid_df = (
    raw_df.filter(is_valid)
    .dropDuplicates(["event_id"])
    .withColumn("event_ts_parsed", F.to_timestamp("event_ts"))
    .withColumn("dt", F.date_format("event_ts_parsed", "yyyy-MM-dd"))
)

joined_df = valid_df.join(prices_df, on=["team_id", "model"], how="inner").withColumn(
    "cost",
    (F.col("tokens_in") / 1000.0) * F.col("input_price_per_1k")
    + (F.col("tokens_out") / 1000.0) * F.col("output_price_per_1k"),
)

joined_df.write.mode("overwrite").partitionBy("dt").parquet(f"{args['CURATED_PATH']}/events_parquet/")

agg_df = (
    joined_df.withColumn("slot_start", F.date_trunc("hour", "event_ts_parsed"))
    .groupBy("team_id", "model", "slot_start", "dt")
    .agg(
        F.sum("tokens_in").cast("bigint").alias("tokens_in"),
        F.sum("tokens_out").cast("bigint").alias("tokens_out"),
        F.sum("cost").cast("decimal(14,6)").alias("cost"),
    )
)

agg_df.write.mode("overwrite").partitionBy("dt").parquet(f"{args['CURATED_PATH']}/batch_agg_parquet/")